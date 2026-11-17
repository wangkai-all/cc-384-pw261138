"""A single independently-deployed business data service.

The same implementation backs the orders domain and the settlement domain.  A
service owns one SQLite database, publishes immutable batch snapshots, exposes
them over HTTP and supports leases (pins) that protect snapshots from garbage
collection while a report is using them.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from .common import (JsonRequestHandler, ServiceHTTPServer, append_query,
                     batch_sort_key, http_json, utc_now_iso)
from .seed import (BATCH_DATES, canonical_json, checksum_for, load_batches,
                   snapshot_version_for)

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id        TEXT PRIMARY KEY,
    sequence        INTEGER NOT NULL,
    business_date   TEXT NOT NULL,
    published_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id          TEXT PRIMARY KEY,
    batch_id             TEXT NOT NULL REFERENCES batches(batch_id),
    version              INTEGER NOT NULL,
    checksum             TEXT NOT NULL,
    record_count         INTEGER NOT NULL,
    total_amount_cents   INTEGER NOT NULL,
    confirmed            INTEGER NOT NULL DEFAULT 1,
    note                 TEXT NOT NULL DEFAULT '',
    created_at           TEXT NOT NULL,
    UNIQUE (batch_id, version)
);

CREATE TABLE IF NOT EXISTS records (
    snapshot_id  TEXT NOT NULL REFERENCES snapshots(snapshot_id),
    record_id    TEXT NOT NULL,
    payload      TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, record_id)
);

CREATE TABLE IF NOT EXISTS pins (
    pin_id       TEXT PRIMARY KEY,
    snapshot_id  TEXT NOT NULL,
    owner        TEXT NOT NULL,
    lease_token  TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    expires_at   REAL NOT NULL,
    released_at  TEXT
);

CREATE INDEX IF NOT EXISTS idx_pins_snapshot ON pins(snapshot_id);
"""


class DomainStore:
    """Persistent state for one domain."""

    def __init__(self, domain: str, db_path: Path, seed_path: Path,
                 initial_publish_count: int = 4) -> None:
        self.domain = domain
        self.db_path = Path(db_path)
        self.seed_path = Path(seed_path)
        self.initial_publish_count = max(1, initial_publish_count)
        self._write_lock = threading.RLock()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._seed = load_batches(self.seed_path)
        with self._connect() as conn:
            conn.executescript(SCHEMA)
        self._bootstrap()

    # -- database ----------------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _bootstrap(self) -> None:
        with self._write_lock, self._connect() as conn:
            published = {row["batch_id"] for row in
                         conn.execute("SELECT batch_id FROM batches")}
            if not published:
                for batch in self._seed["batches"][:self.initial_publish_count]:
                    self._insert_batch(conn, batch, version=1)
                conn.commit()

    def _insert_batch(self, conn: sqlite3.Connection, batch: dict[str, Any],
                      version: int, note: str = "",
                      confirmed: bool = True) -> dict[str, Any]:
        batch_id = batch["batch_id"]
        created_at = utc_now_iso()
        snapshot_id = self._snapshot_id(batch_id, version)
        records = batch["records"]
        checksum = checksum_for(self.domain, batch_id, records)
        total = sum(int(rec.get("amount_cents", 0)) for rec in records)
        conn.execute(
            "INSERT OR IGNORE INTO batches(batch_id, sequence, business_date, published_at)"
            " VALUES (?, ?, ?, ?)",
            (batch_id, batch.get("sequence", 0), batch.get("business_date", ""),
             created_at),
        )
        conn.execute(
            "INSERT INTO snapshots(snapshot_id, batch_id, version, checksum,"
            " record_count, total_amount_cents, confirmed, note, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (snapshot_id, batch_id, version, checksum, len(records), total,
             1 if confirmed else 0, note, created_at),
        )
        record_id_field = "order_id" if self.domain == "orders" else "settlement_id"
        for record in records:
            conn.execute(
                "INSERT INTO records(snapshot_id, record_id, payload) VALUES (?, ?, ?)",
                (snapshot_id, record[record_id_field], canonical_json(record)),
            )
        return self._snapshot_meta(conn, snapshot_id)

    @staticmethod
    def _snapshot_id(batch_id: str, version: int) -> str:
        return f"{batch_id}/v{version}"

    def _snapshot_meta(self, conn: sqlite3.Connection,
                       snapshot_id: str) -> dict[str, Any]:
        row = conn.execute(
            "SELECT s.*, b.business_date, b.sequence FROM snapshots s"
            " JOIN batches b ON b.batch_id = s.batch_id WHERE s.snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
        if row is None:
            raise KeyError(snapshot_id)
        return {
            "domain": self.domain,
            "snapshot_id": row["snapshot_id"],
            "batch_id": row["batch_id"],
            "version": row["version"],
            "business_date": row["business_date"],
            "sequence": row["sequence"],
            "checksum": row["checksum"],
            "record_count": row["record_count"],
            "total_amount_cents": row["total_amount_cents"],
            "confirmed": bool(row["confirmed"]),
            "note": row["note"],
            "created_at": row["created_at"],
        }

    # -- reads -------------------------------------------------------------
    def health(self) -> dict[str, Any]:
        with self._connect() as conn:
            published = conn.execute("SELECT COUNT(*) AS n FROM batches").fetchone()["n"]
            pending = len(self.pending_batches())
            pins = self._active_pins(conn)
            latest = conn.execute(
                "SELECT MAX(published_at) AS ts FROM batches").fetchone()["ts"]
        return {
            "status": "ok",
            "domain": self.domain,
            "published_batches": published,
            "pending_batches": pending,
            "active_pins": len(pins),
            "latest_publish_at": latest,
            "database": str(self.db_path),
        }

    def pending_batches(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            published = {row["batch_id"] for row in
                         conn.execute("SELECT batch_id FROM batches")}
        pending = [b for b in self._seed["batches"] if b["batch_id"] not in published]
        return [{
            "batch_id": b["batch_id"],
            "sequence": b.get("sequence", 0),
            "business_date": b.get("business_date", ""),
            "record_count": len(b["records"]),
        } for b in sorted(pending, key=lambda b: batch_sort_key(b["batch_id"]))]

    def list_batches(self, include_pending: bool = False) -> dict[str, Any]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT b.batch_id, b.sequence, b.business_date, b.published_at,"
                " (SELECT MAX(version) FROM snapshots s WHERE s.batch_id = b.batch_id)"
                "   AS current_version"
                " FROM batches b ORDER BY b.sequence, b.batch_id"
            ).fetchall()
            batches = []
            for row in rows:
                snapshot_id = self._snapshot_id(row["batch_id"], row["current_version"])
                meta = self._snapshot_meta(conn, snapshot_id)
                batches.append({
                    "batch_id": row["batch_id"],
                    "sequence": row["sequence"],
                    "business_date": row["business_date"],
                    "published_at": row["published_at"],
                    "current_snapshot_id": snapshot_id,
                    "current_version": row["current_version"],
                    "checksum": meta["checksum"],
                    "record_count": meta["record_count"],
                    "total_amount_cents": meta["total_amount_cents"],
                    "confirmed": meta["confirmed"],
                })
        result: dict[str, Any] = {"domain": self.domain, "batches": batches}
        if include_pending:
            result["pending"] = self.pending_batches()
        return result

    def snapshot_versions(self, batch_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT snapshot_id FROM snapshots WHERE batch_id = ? ORDER BY version",
                (batch_id,)).fetchall()
            if not rows:
                raise KeyError(batch_id)
            versions = [self._snapshot_meta(conn, row["snapshot_id"]) for row in rows]
        return {"domain": self.domain, "batch_id": batch_id, "versions": versions}

    def get_snapshot(self, batch_id: str, version: int | None = None) -> dict[str, Any]:
        with self._connect() as conn:
            if version is None:
                row = conn.execute(
                    "SELECT MAX(version) AS v FROM snapshots WHERE batch_id = ?",
                    (batch_id,)).fetchone()
                if row["v"] is None:
                    raise KeyError(batch_id)
                version = row["v"]
            snapshot_id = self._snapshot_id(batch_id, version)
            meta = self._snapshot_meta(conn, snapshot_id)
            records = [json.loads(row["payload"]) for row in conn.execute(
                "SELECT payload FROM records WHERE snapshot_id = ?"
                " ORDER BY record_id", (snapshot_id,))]
        meta["records"] = records
        return meta

    # -- writes ------------------------------------------------------------
    def publish_next_batch(self, batch_id: str | None = None,
                           confirmed: bool = True) -> dict[str, Any]:
        with self._write_lock, self._connect() as conn:
            published = {row["batch_id"] for row in
                         conn.execute("SELECT batch_id FROM batches")}
            candidates = [b for b in self._seed["batches"]
                          if b["batch_id"] not in published]
            if batch_id is not None:
                candidates = [b for b in candidates if b["batch_id"] == batch_id]
            if not candidates:
                raise LookupError("no pending batch available")
            batch = sorted(candidates, key=lambda b: batch_sort_key(b["batch_id"]))[0]
            meta = self._insert_batch(conn, batch, version=1, confirmed=confirmed)
            conn.commit()
        return {"published": meta}

    def confirm_batch(self, batch_id: str) -> dict[str, Any]:
        """Mark the newest version of a batch as confirmed.

        A batch can be published before the upstream domain has finished
        confirming it.  The coordinator ignores unconfirmed snapshots, so this
        is the operation that makes a batch joinable.
        """
        with self._write_lock, self._connect() as conn:
            row = conn.execute(
                "SELECT MAX(version) AS v FROM snapshots WHERE batch_id = ?",
                (batch_id,)).fetchone()
            if row["v"] is None:
                raise KeyError(batch_id)
            snapshot_id = self._snapshot_id(batch_id, row["v"])
            already = conn.execute(
                "SELECT confirmed FROM snapshots WHERE snapshot_id = ?",
                (snapshot_id,)).fetchone()["confirmed"]
            conn.execute("UPDATE snapshots SET confirmed = 1 WHERE snapshot_id = ?",
                         (snapshot_id,))
            conn.commit()
            meta = self._snapshot_meta(conn, snapshot_id)
        return {"confirmed": meta, "already_confirmed": bool(already)}

    def revise_batch(self, batch_id: str, note: str = "") -> dict[str, Any]:
        with self._write_lock, self._connect() as conn:
            row = conn.execute(
                "SELECT MAX(version) AS v FROM snapshots WHERE batch_id = ?",
                (batch_id,)).fetchone()
            if row["v"] is None:
                raise KeyError(batch_id)
            version = int(row["v"]) + 1
            base_snapshot = self._snapshot_id(batch_id, row["v"])
            base_confirmed = bool(conn.execute(
                "SELECT confirmed FROM snapshots WHERE snapshot_id = ?",
                (base_snapshot,)).fetchone()["confirmed"])
            records = [json.loads(r["payload"]) for r in conn.execute(
                "SELECT payload FROM records WHERE snapshot_id = ? ORDER BY record_id",
                (base_snapshot,))]
            self._mutate_for_revision(records, version)
            meta = self._insert_batch(conn, {
                "batch_id": batch_id,
                "sequence": 0,
                "business_date": BATCH_DATES.get(batch_id, ""),
                "records": records,
            }, version=version, note=note or f"revision v{version}",
                confirmed=base_confirmed)
            conn.commit()
        return {"published": meta}

    @staticmethod
    def _mutate_for_revision(records: list[dict[str, Any]], version: int) -> None:
        """Deterministic correction so a revision is visibly different."""
        for index, record in enumerate(records):
            if index % 5 == 1:
                record["amount_cents"] = int(record.get("amount_cents", 0)) + 100 * version
                if "fee_cents" in record:
                    record["net_cents"] = record["amount_cents"] - int(record["fee_cents"])
                record["revision"] = version

    # -- leases / pins -----------------------------------------------------
    def _sweep_pins(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            "UPDATE pins SET released_at = ? WHERE released_at IS NULL AND expires_at <= ?",
            (utc_now_iso(), time.time()),
        )

    def _active_pins(self, conn: sqlite3.Connection,
                     snapshot_id: str | None = None) -> list[dict[str, Any]]:
        query = ("SELECT * FROM pins WHERE released_at IS NULL AND expires_at > ?")
        params: list[Any] = [time.time()]
        if snapshot_id is not None:
            query += " AND snapshot_id = ?"
            params.append(snapshot_id)
        query += " ORDER BY created_at"
        return [dict(row) for row in conn.execute(query, params)]

    def acquire_pin(self, snapshot_id: str, owner: str, lease_token: str,
                    ttl_seconds: float) -> dict[str, Any]:
        with self._write_lock, self._connect() as conn:
            self._sweep_pins(conn)
            self._snapshot_meta(conn, snapshot_id)  # raises KeyError if unknown
            pin_id = "pin-" + uuid.uuid4().hex[:16]
            now = time.time()
            conn.execute(
                "INSERT INTO pins(pin_id, snapshot_id, owner, lease_token, created_at,"
                " expires_at, released_at) VALUES (?, ?, ?, ?, ?, ?, NULL)",
                (pin_id, snapshot_id, owner, lease_token, utc_now_iso(),
                 now + ttl_seconds),
            )
            conn.commit()
            pin = dict(conn.execute("SELECT * FROM pins WHERE pin_id = ?",
                                    (pin_id,)).fetchone())
            pin["active_pins_on_snapshot"] = len(self._active_pins(conn, snapshot_id))
        return pin

    def renew_pin(self, pin_id: str, lease_token: str,
                  ttl_seconds: float) -> dict[str, Any]:
        with self._write_lock, self._connect() as conn:
            self._sweep_pins(conn)
            row = conn.execute("SELECT * FROM pins WHERE pin_id = ?",
                               (pin_id,)).fetchone()
            if row is None:
                raise KeyError(pin_id)
            pin = dict(row)
            if pin["released_at"] is not None:
                raise PermissionError("pin already released")
            if pin["lease_token"] != lease_token:
                raise PermissionError("stale lease token: pin belongs to another task")
            expires_at = time.time() + ttl_seconds
            conn.execute("UPDATE pins SET expires_at = ? WHERE pin_id = ?",
                         (expires_at, pin_id))
            conn.commit()
            pin["expires_at"] = expires_at
        return pin

    def release_pin(self, pin_id: str, lease_token: str) -> dict[str, Any]:
        with self._write_lock, self._connect() as conn:
            self._sweep_pins(conn)
            row = conn.execute("SELECT * FROM pins WHERE pin_id = ?",
                               (pin_id,)).fetchone()
            if row is None:
                raise KeyError(pin_id)
            pin = dict(row)
            if pin["lease_token"] != lease_token:
                # A stale coordination task must not delete a snapshot lease
                # that is still owned and possibly in use.
                raise PermissionError("stale lease token: pin was not released")
            already = pin["released_at"] is not None
            if not already:
                conn.execute("UPDATE pins SET released_at = ? WHERE pin_id = ?",
                             (utc_now_iso(), pin_id))
                conn.commit()
            pin["released_at"] = pin["released_at"] or utc_now_iso()
        return {"pin_id": pin_id, "already_released": already,
                "released_at": pin["released_at"]}

    def list_pins(self, snapshot_id: str | None = None) -> dict[str, Any]:
        with self._connect() as conn:
            self._sweep_pins(conn)
            conn.commit()
            pins = self._active_pins(conn, snapshot_id)
        return {"domain": self.domain, "active_pins": pins}

    def garbage_collect(self) -> dict[str, Any]:
        """Drop superseded snapshots unless pinned. Never touches pinned data."""
        with self._write_lock, self._connect() as conn:
            self._sweep_pins(conn)
            conn.commit()
            pinned = {p["snapshot_id"] for p in self._active_pins(conn)}
            candidates = conn.execute(
                "SELECT s.snapshot_id, s.batch_id, s.version FROM snapshots s"
                " WHERE s.version < (SELECT MAX(version) FROM snapshots s2"
                "                    WHERE s2.batch_id = s.batch_id)"
            ).fetchall()
            deleted: list[str] = []
            retained: list[str] = []
            for row in candidates:
                snapshot_id = row["snapshot_id"]
                if snapshot_id in pinned:
                    retained.append(snapshot_id)
                    continue
                conn.execute("DELETE FROM records WHERE snapshot_id = ?", (snapshot_id,))
                conn.execute("DELETE FROM snapshots WHERE snapshot_id = ?", (snapshot_id,))
                deleted.append(snapshot_id)
            conn.commit()
        return {
            "domain": self.domain,
            "deleted_snapshots": deleted,
            "retained_pinned_snapshots": retained,
            "active_pins": len(pinned),
        }


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------

class DomainHandler(JsonRequestHandler):
    store: DomainStore  # set on the server class

    def do_GET(self) -> None:  # noqa: N802
        try:
            self._route_get()
        except KeyError as exc:
            self.send_error_json(404, "not_found", f"unknown resource: {exc.args[0]}")
        except Exception as exc:  # defensive: a bug must not kill the thread
            self.send_error_json(500, "internal_error", f"{type(exc).__name__}: {exc}")

    def do_POST(self) -> None:  # noqa: N802
        try:
            self._route_post()
        except KeyError as exc:
            self.send_error_json(404, "not_found", f"unknown resource: {exc.args[0]}")
        except PermissionError as exc:
            self.send_error_json(409, "stale_lease_token", str(exc),
                                 hint="旧协调任务不能释放属于其他任务的快照租约")
        except LookupError as exc:
            self.send_error_json(409, "no_pending_batch", str(exc))
        except ValueError as exc:
            self.send_error_json(400, "bad_request", str(exc))
        except Exception as exc:
            self.send_error_json(500, "internal_error", f"{type(exc).__name__}: {exc}")
    # -- routing -----------------------------------------------------------
    def _route_get(self) -> None:
        parts = self.path_parts()
        store = self.server.store  # type: ignore[attr-defined]
        if parts == ["health"]:
            self.send_json(200, store.health())
            return
        if parts == ["batches"]:
            include_pending = self.query_one("include_pending") in ("1", "true", "yes")
            self.send_json(200, store.list_batches(include_pending=include_pending))
            return
        if len(parts) == 2 and parts[0] == "batches":
            self.send_json(200, store.snapshot_versions(parts[1]))
            return
        if len(parts) >= 3 and parts[0] == "snapshots":
            batch_id = parts[1]
            version = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else None
            if len(parts) >= 3 and parts[2] == "latest":
                version = None
            self.send_json(200, store.get_snapshot(batch_id, version))
            return
        if parts == ["pins"]:
            self.send_json(200, store.list_pins(self.query_one("snapshot_id")))
            return
        self.send_error_json(404, "not_found", f"no route for GET /{'/'.join(parts)}")

    def _route_post(self) -> None:
        parts = self.path_parts()
        store = self.server.store  # type: ignore[attr-defined]
        body = self.read_json()

        if parts == ["admin", "publish-next"]:
            batch_id = body.get("batch_id")
            confirmed = body.get("confirmed", True)
            self.send_json(201, store.publish_next_batch(
                batch_id, confirmed=bool(confirmed)))
            return
        if len(parts) == 3 and parts[:2] == ["admin", "confirm"]:
            self.send_json(200, store.confirm_batch(parts[2]))
            return
        if len(parts) == 3 and parts[:2] == ["admin", "revise"]:
            note = str(body.get("note", ""))
            self.send_json(201, store.revise_batch(parts[2], note))
            return
        if parts == ["admin", "gc"]:
            self.send_json(200, store.garbage_collect())
            return
        if parts == ["pins"]:
            snapshot_id = body.get("snapshot_id")
            owner = str(body.get("owner", "")).strip()
            token = str(body.get("lease_token", "")).strip()
            if not snapshot_id or not owner or not token:
                raise ValueError("snapshot_id, owner and lease_token are required")
            ttl = float(body.get("ttl_seconds", 60))
            ttl = min(max(ttl, 1.0), 86400.0)
            self.send_json(201, store.acquire_pin(snapshot_id, owner, token, ttl))
            return
        if len(parts) == 3 and parts[0] == "pins" and parts[2] == "renew":
            token = str(body.get("lease_token", "")).strip()
            if not token:
                raise ValueError("lease_token is required")
            ttl = min(max(float(body.get("ttl_seconds", 60)), 1.0), 86400.0)
            self.send_json(200, store.renew_pin(parts[1], token, ttl))
            return
        if len(parts) == 2 and parts[0] == "pins":
            token = str(body.get("lease_token") or
                        self.header_or_query("X-Lease-Token", "lease_token") or "")
            if not token:
                raise ValueError("lease_token is required")
            self.send_json(200, store.release_pin(parts[1], token))
            return
        if len(parts) == 3 and parts[0] == "pins" and parts[2] == "release":
            token = str(body.get("lease_token") or
                        self.header_or_query("X-Lease-Token", "lease_token") or "")
            if not token:
                raise ValueError("lease_token is required")
            self.send_json(200, store.release_pin(parts[1], token))
            return
        self.send_error_json(404, "not_found", f"no route for POST /{'/'.join(parts)}")

    # DELETE for pins, with token supplied via header or query string.
    def do_DELETE(self) -> None:  # noqa: N802
        try:
            parts = self.path_parts()
            store = self.server.store  # type: ignore[attr-defined]
            if len(parts) == 2 and parts[0] == "pins":
                token = (self.headers.get("X-Lease-Token") or
                         self.query_one("lease_token") or "")
                if not token:
                    raise ValueError("lease_token is required")
                self.send_json(200, store.release_pin(parts[1], token))
                return
            self.send_error_json(404, "not_found",
                                 f"no route for DELETE /{'/'.join(parts)}")
        except KeyError as exc:
            self.send_error_json(404, "not_found", f"unknown resource: {exc.args[0]}")
        except PermissionError as exc:
            self.send_error_json(409, "stale_lease_token", str(exc))
        except ValueError as exc:
            self.send_error_json(400, "bad_request", str(exc))
        except Exception as exc:
            self.send_error_json(500, "internal_error", f"{type(exc).__name__}: {exc}")


class DomainServer(ServiceHTTPServer):
    def __init__(self, address: tuple[str, int], store: DomainStore) -> None:
        super().__init__(address, DomainHandler)
        self.store = store


def create_domain_server(domain: str, db_path: str | Path, seed_path: str | Path,
                         host: str = "127.0.0.1", port: int = 0,
                         initial_publish_count: int = 4) -> DomainServer:
    store = DomainStore(domain, Path(db_path), Path(seed_path),
                        initial_publish_count=initial_publish_count)
    return DomainServer((host, port), store)


def run_domain(domain: str, data_dir: str | Path, port: int,
               host: str = "127.0.0.1") -> None:
    from .seed import ensure_seed
    data_dir = Path(data_dir)
    seeds = ensure_seed(data_dir)
    server = create_domain_server(
        domain, data_dir / f"{domain}.sqlite3", seeds[domain],
        host=host, port=port)
    print(f"[{domain}] listening on http://{host}:{server.server_address[1]}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


# Helpers used by the supervisor / checks
def domain_url(base: str, path: str, **params: Any) -> str:
    return append_query(base.rstrip("/") + path, **params)


def fetch_json(base: str, path: str, timeout: float = 1.5,
               **params: Any) -> Any:
    _, body, _ = http_json("GET", domain_url(base, path, **params), timeout=timeout)
    return body


if __name__ == "__main__":  # pragma: no cover - process entry point
    import argparse
    parser = argparse.ArgumentParser(description="Run one business data domain")
    parser.add_argument("--domain", required=True, choices=["orders", "settlement"])
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--publish-count", type=int, default=4,
                        help="batches published on first boot of an empty database")
    options = parser.parse_args()
    from .seed import ensure_seed
    seeds = ensure_seed(Path(options.data_dir))
    server = create_domain_server(
        options.domain, Path(options.data_dir) / f"{options.domain}.sqlite3",
        seeds[options.domain], host=options.host, port=options.port,
        initial_publish_count=options.publish_count)
    print(f"[{options.domain}] listening on "
          f"http://{options.host}:{server.server_address[1]}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
