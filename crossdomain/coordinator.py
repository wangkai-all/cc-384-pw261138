"""Snapshot coordinator.

Selects one mutually-confirmed business batch from two independently updated
domains, pins the exact snapshots involved, verifies their checksums and only
then joins the two record sets.  It never fabricates a pair out of each side's
latest version: the batch id is the join key.

Availability policy (also exposed via GET /api/policy):

aligned_with_explicit_partial
  * Every query returns within ``max_wait_ms`` (default 1500, hard cap 5000).
  * Cross-domain data is joined only when both domains have a confirmed
    snapshot for the same batch id.
  * If the newest common batch is older than either domain's newest batch, the
    coordinator falls back to that common older batch and reports the lag
    (mode=degraded_stale).  It never mixes batches.
  * If a domain is unreachable or there is no common confirmed batch, the
    coordinator returns HTTP 200 with mode=partial and an itemised ``uncovered``
    block instead of pretending the report is complete.
  * policy=strict disables every fallback: after the same wait window it
    returns HTTP 503 strict_failed.
  * Snapshots are leased on both domains while data is read.  Leases are
    released by token; stale tokens are rejected and GC never removes a
    snapshot with a live lease.
"""

from __future__ import annotations

import hashlib
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .common import (JsonRequestHandler, ServiceHTTPServer, UpstreamError,
                     UpstreamUnavailable, append_query, batch_sort_key,
                     http_json, utc_now_iso)
from .seed import canonical_json

POLICY_NAME = "aligned_with_explicit_partial"
DEFAULT_MAX_WAIT_MS = 1500
HARD_MAX_WAIT_MS = 5000
POLL_INTERVAL_SECONDS = 0.15
DOMAIN_TIMEOUT_SECONDS = 1.5
DEFAULT_LEASE_TTL_SECONDS = 60.0

AVAILABILITY_COMMITMENT: dict[str, Any] = {
    "policy": POLICY_NAME,
    "latency": "每次查询的轮询等待不超过 max_wait_ms（默认 1500ms，服务端硬上限 "
               "5000ms），另加本地读取/校验开销（通常 <10ms），不会无限期等待两域对齐。",
    "consistency": "跨域关联只使用两域都已确认且 batch_id 相同的快照；"
                   "绝不把两域各自最新的不同批次拼在一起。",
    "fallback": "最新批次无法对齐时，退回两域共同已确认的最大批次，"
                "并在 freshness.lag_batches 中公开滞后批次数（mode=degraded_stale）。",
    "degraded": "某一域不可达或不存在共同已确认批次时，HTTP 200 返回 mode=partial："
                "公开各域可达性、最新批次清单与 uncovered 明细，但 records 为空，"
                "不会把单域数据伪装成跨域合计；协调器恢复后重新查询即可。",
    "strict": "policy=strict 时在同一个 max_wait_ms 等待窗口内仍无法对齐，"
              "则返回 HTTP 503 strict_failed，不做降级。",
    "leases": "跨域读取前在两域对选定快照加租约，读取完成后按 lease_token 释放；"
              "旧任务/过期 token 的释放请求返回 409，GC 永不删除仍有活跃租约的快照。",
}


class CoordinatorError(Exception):
    def __init__(self, code: str, message: str, status: int = 503,
                 details: dict[str, Any] | None = None) -> None:
        self.code = code
        self.message = message
        self.status = status
        self.details = details or {}
        super().__init__(message)


def _clamp_wait(value: Any) -> int:
    try:
        wait = int(value)
    except (TypeError, ValueError):
        wait = DEFAULT_MAX_WAIT_MS
    return max(0, min(wait, HARD_MAX_WAIT_MS))


class DomainClient:
    def __init__(self, name: str, base_url: str, timeout: float = DOMAIN_TIMEOUT_SECONDS) -> None:
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str, **params: Any) -> Any:
        url = append_query(self.base_url + path, **params)
        _, payload, _ = http_json("GET", url, timeout=self.timeout)
        return payload

    def _post(self, path: str, payload: dict[str, Any],
              timeout: float | None = None) -> Any:
        _, body, _ = http_json("POST", self.base_url + path, payload,
                               timeout=timeout or self.timeout)
        return body

    def batches(self, include_pending: bool = False) -> dict[str, Any]:
        return self._get("/batches", include_pending="1" if include_pending else None)

    def snapshot(self, batch_id: str, version: int) -> dict[str, Any]:
        return self._get(f"/snapshots/{batch_id}/{version}")

    def pin(self, snapshot_id: str, owner: str, lease_token: str,
            ttl: float) -> dict[str, Any]:
        return self._post("/pins", {"snapshot_id": snapshot_id, "owner": owner,
                                    "lease_token": lease_token, "ttl_seconds": ttl})

    def release(self, pin_id: str, lease_token: str) -> dict[str, Any]:
        return self._post(f"/pins/{pin_id}/release",
                          {"lease_token": lease_token}, timeout=2.0)

    def health(self) -> dict[str, Any]:
        return self._get("/health")


class SnapshotCoordinator:
    def __init__(self, domains: dict[str, str]) -> None:
        self.clients = {name: DomainClient(name, url)
                        for name, url in domains.items()}
        self._tasks: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    # -- fleet reads -------------------------------------------------------
    def _read_batches(self) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        ok: dict[str, dict[str, Any]] = {}
        errors: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=len(self.clients)) as pool:
            futures = {name: pool.submit(client.batches, True)
                       for name, client in self.clients.items()}
            for name, future in futures.items():
                try:
                    ok[name] = future.result()
                except UpstreamError as exc:
                    errors[name] = {
                        "domain": name,
                        "error": "unreachable" if isinstance(exc, UpstreamUnavailable)
                                 else "upstream_error",
                        "status": exc.status,
                        "message": exc.message,
                    }
        return ok, errors

    @staticmethod
    def _confirmed_map(domain_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
        confirmed: dict[str, dict[str, Any]] = {}
        for batch in domain_payload.get("batches", []):
            if batch.get("confirmed"):
                confirmed[batch["batch_id"]] = batch
        return confirmed

    def _plan(self, domain_payloads: dict[str, dict[str, Any]],
              errors: dict[str, dict[str, Any]],
              policy: str) -> dict[str, Any]:
        """Choose a common confirmed batch. Pure function of fleet state."""
        names = sorted(self.clients)
        reachable = [name for name in names if name in domain_payloads]
        confirmed = {name: self._confirmed_map(domain_payloads[name])
                     for name in reachable}
        common: set[str] = set(confirmed[reachable[0]]) if reachable else set()
        for name in reachable[1:]:
            common &= set(confirmed[name])

        latest_by_domain = {
            name: max(domain_payloads[name]["batches"],
                      key=lambda b: (b["sequence"], b["batch_id"]),
                      default=None)
            for name in reachable
        }

        unconfirmed_latest: list[dict[str, Any]] = []
        for name in reachable:
            latest = latest_by_domain[name]
            if latest is not None and not latest.get("confirmed"):
                unconfirmed_latest.append({
                    "domain": name, "batch_id": latest["batch_id"],
                    "version": latest.get("current_version"),
                })

        if len(reachable) != len(names):
            return {
                "joint": False,
                "mode": "partial",
                "reason": "domain_unreachable",
                "reachable": reachable,
                "selected": {},
                "unconfirmed_latest": unconfirmed_latest,
            }
        if not common:
            return {
                "joint": False,
                "mode": "partial",
                "reason": "no_common_confirmed_batch",
                "reachable": reachable,
                "selected": {},
                "unconfirmed_latest": unconfirmed_latest,
            }

        chosen_batch = max(common, key=batch_sort_key)
        selected = {name: confirmed[name][chosen_batch] for name in names}
        is_latest = all(
            latest_by_domain[name] is not None
            and latest_by_domain[name]["batch_id"] == chosen_batch
            for name in names)
        lag = {name: (max(0, latest_by_domain[name]["sequence"]
                           - selected[name]["sequence"])
                      if latest_by_domain[name] else 0)
               for name in names}
        mode = "aligned" if is_latest else "degraded_stale"
        # Policy enforcement happens in resolve() *after* the wait window, so
        # strict mode still gets a chance to observe the domains aligning.
        return {
            "joint": True,
            "mode": mode,
            "reason": ("两域最新已确认批次一致" if is_latest
                       else "两域最新批次不一致，退回共同已确认的最大批次"),
            "reachable": reachable,
            "selected": selected,
            "chosen_batch": chosen_batch,
            "lag_batches": lag,
            "is_latest": is_latest,
            "unconfirmed_latest": unconfirmed_latest,
            "latest_by_domain": latest_by_domain,
        }

    # -- leases ------------------------------------------------------------
    def _acquire_selected(self, selected: dict[str, dict[str, Any]],
                          lease_token: str, owner: str,
                          ttl: float = DEFAULT_LEASE_TTL_SECONDS,
                          acquired: dict[str, dict[str, Any]] | None = None
                          ) -> dict[str, dict[str, Any]]:
        """Pin every selected snapshot.

        Pins acquired before a failure are returned in ``acquired`` so the
        caller can release them instead of leaking a lease until its TTL.
        """
        held: dict[str, dict[str, Any]] = acquired if acquired is not None else {}
        for name, batch in sorted(selected.items()):
            client = self.clients[name]
            pin = client.pin(batch["current_snapshot_id"], owner, lease_token, ttl)
            held[name] = {
                "domain": name,
                "snapshot_id": batch["current_snapshot_id"],
                "pin_id": pin["pin_id"],
                "expires_at": pin["expires_at"],
            }
        return held

    def _release_all(self, leases: dict[str, dict[str, Any]],
                     lease_token: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for name, lease in leases.items():
            try:
                self.clients[name].release(lease["pin_id"], lease_token)
                results.append({"domain": name, "pin_id": lease["pin_id"],
                                "released": True})
            except UpstreamError as exc:
                results.append({"domain": name, "pin_id": lease["pin_id"],
                                "released": False, "message": exc.message})
        return results

    # -- core --------------------------------------------------------------
    def resolve(self, policy: str = POLICY_NAME,
                max_wait_ms: Any = DEFAULT_MAX_WAIT_MS) -> dict[str, Any]:
        if policy not in (POLICY_NAME, "strict"):
            raise CoordinatorError("bad_policy", f"未知策略: {policy}", status=400)
        wait_ms = _clamp_wait(max_wait_ms)
        started = time.monotonic()
        deadline = started + wait_ms / 1000.0
        attempts = 0
        domain_payloads, errors = self._read_batches()
        attempts += 1

        # Wait only while both domains are reachable: retrying a stopped
        # service inside the same request would just burn the deadline.  Polls
        # are scheduled so the request never runs past its stated deadline.
        while len(errors) == 0:
            probe = self._plan(domain_payloads, errors, policy)
            if probe["mode"] in ("aligned", "partial"):
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(POLL_INTERVAL_SECONDS, remaining))
            if time.monotonic() > deadline:
                break
            domain_payloads, errors = self._read_batches()
            attempts += 1

        plan = self._plan(domain_payloads, errors, policy)
        waited_ms = round((time.monotonic() - started) * 1000.0, 1)

        run_id = "run-" + uuid.uuid4().hex[:12]
        lease_token = "lease-" + uuid.uuid4().hex
        owner = f"coordinator/{run_id}"
        result: dict[str, Any] = {
            "run_id": run_id,
            "policy": policy,
            "availability_commitment": AVAILABILITY_COMMITMENT,
            "requested_at": utc_now_iso(),
            "waited_ms": waited_ms,
            "max_wait_ms": wait_ms,
            "attempts": attempts,
            "consistency": {
                "mode": plan["mode"],
                "reason": plan["reason"],
                "joined": plan["joint"],
                "joined_batch_ids": ([plan["chosen_batch"]]
                                     if plan["joint"] else []),
                "selection_rule": "两域同 batch_id 且 confirmed=true 的快照；"
                                  "取共同集合中序号最大的批次",
                "waited_ms": waited_ms,
            },
            "domains": {},
            "leases": {},
            "records": {"orders": [], "settlement": []},
            "uncovered": {},
        }

        if policy == "strict" and plan["mode"] != "aligned":
            raise CoordinatorError(
                "strict_failed",
                "strict 策略要求两域最新已确认批次一致",
                status=503,
                details={"consistency": result["consistency"],
                         "chosen_batch": plan.get("chosen_batch"),
                         "lag_batches": plan.get("lag_batches"),
                         "unreachable_domains": sorted(errors)},
            )

        # Domain availability and selection rationale are always reported.
        for name in sorted(self.clients):
            if name in errors:
                result["domains"][name] = {
                    "domain": name, "reachable": False,
                    "error": errors[name]["message"],
                }
                continue
            latest = plan.get("latest_by_domain", {}).get(name)
            selected = plan["selected"].get(name)
            result["domains"][name] = {
                "domain": name,
                "reachable": True,
                "latest_batch_id": latest["batch_id"] if latest else None,
                "latest_confirmed": bool(latest and latest.get("confirmed")),
                "selected": bool(selected),
                "role": ("跨域关联选定域" if selected
                         else "不可用/无共同批次，未参与跨域关联"),
            }

        selected = plan["selected"]
        if selected:
            result["selection"] = {
                name: {
                    "domain": name,
                    "batch_id": batch["batch_id"],
                    "version": batch["current_version"],
                    "snapshot_id": batch["current_snapshot_id"],
                    "checksum": batch["checksum"],
                    "record_count": batch["record_count"],
                    "total_amount_cents": batch["total_amount_cents"],
                    "business_date": batch["business_date"],
                    "reason": plan["reason"],
                    "is_domain_latest": batch["batch_id"] == (
                        plan["latest_by_domain"][name]["batch_id"]
                        if plan["latest_by_domain"].get(name) else None),
                    "lag_batches": plan["lag_batches"].get(name, 0),
                }
                for name, batch in sorted(selected.items())
            }
            snapshots: dict[str, dict[str, Any]] = {}
            leases: dict[str, dict[str, Any]] = {}
            try:
                # Publish pins as they are acquired: if the second domain fails,
                # the finally block still releases the first domain's lease.
                self._acquire_selected(selected, lease_token, owner,
                                       acquired=leases)
                result["leases"] = leases
                for name, batch in selected.items():
                    snapshots[name] = self.clients[name].snapshot(
                        batch["batch_id"], batch["current_version"])
                # Verify both checksums before joining; fail closed on mismatch.
                bad = [name for name, snapshot in snapshots.items()
                       if not self._verify(snapshot)]
                for name, snapshot in snapshots.items():
                    result["domains"][name]["checksum_verified"] = name not in bad
                    result["domains"][name]["selected_snapshot_id"] = \
                        snapshot["snapshot_id"]
                if bad:
                    result["consistency"]["mode"] = "partial"
                    result["consistency"]["reason"] = "checksum_mismatch"
                    result["consistency"]["joined"] = False
                    result["consistency"]["joined_batch_ids"] = []
                    result["records"]["orders"] = []
                    result["records"]["settlement"] = []
                    result["errors"] = [{"domain": name,
                                         "message": "checksum mismatch, join refused"}
                                        for name in bad]
                    snapshots = {}
                for name, snapshot in snapshots.items():
                    result["records"][name] = snapshot.get("records", [])
            except UpstreamError as exc:
                result["consistency"]["mode"] = "partial"
                result["consistency"]["reason"] = "snapshot_read_failed"
                result["consistency"]["joined"] = False
                result["consistency"]["joined_batch_ids"] = []
                result["records"]["orders"] = []
                result["records"]["settlement"] = []
                result["errors"] = [{"domain": exc.url, "message": exc.message}]
            finally:
                result["leases"] = leases
                result["lease_release"] = self._release_all(leases, lease_token)

        result["uncovered"] = self._uncovered(domain_payloads, errors, plan,
                                              result, selected)
        result["freshness"] = {
            name: {
                "selected_batch_id": (result.get("selection", {}).get(name, {})
                                      .get("batch_id")),
                "latest_batch_id": (plan.get("latest_by_domain", {}).get(name) or {})
                                    .get("batch_id"),
                "lag_batches": plan.get("lag_batches", {}).get(name, 0),
                "is_latest": (result.get("selection", {}).get(name, {})
                              .get("is_domain_latest")),
            }
            for name in sorted(self.clients)
        }
        result["finished_at"] = utc_now_iso()
        return result

    @staticmethod
    def _verify(snapshot: dict[str, Any]) -> bool:
        material = canonical_json({
            "domain": snapshot["domain"],
            "batch_id": snapshot["batch_id"],
            "records": snapshot.get("records", []),
        })
        digest = "sha256:" + hashlib.sha256(material.encode("utf-8")).hexdigest()
        return digest == snapshot["checksum"]

    def _uncovered(self, domain_payloads: dict[str, dict[str, Any]],
                   errors: dict[str, dict[str, Any]], plan: dict[str, Any],
                   result: dict[str, Any], selected: dict[str, Any]) -> dict[str, Any]:
        uncovered: dict[str, Any] = {
            "unreachable_domains": [name for name in sorted(self.clients)
                                    if name in errors],
            "unpaired_batches": {},
            "pending_batches": {},
            "unconfirmed_latest": plan.get("unconfirmed_latest", []),
            "excluded_versions": {},
            "orders_without_settlement": {"count": 0, "order_ids": []},
            "batch_gap": None,
        }
        for name, payload in domain_payloads.items():
            published = {b["batch_id"] for b in payload.get("batches", [])}
            other_names = [n for n in domain_payloads if n != name]
            for other in other_names:
                other_published = {b["batch_id"] for b in
                                   domain_payloads[other].get("batches", [])}
                only_here = sorted(published - other_published)
                if only_here:
                    uncovered["unpaired_batches"].setdefault(name, [])
                    uncovered["unpaired_batches"][name] = only_here
            uncovered["pending_batches"][name] = [
                b["batch_id"] for b in payload.get("pending", [])
            ]

        peer_batches = {
            name: {b["batch_id"]: b for b in payload.get("batches", [])}
            for name, payload in domain_payloads.items()
        }
        for name, batch in selected.items():
            excluded = []
            peers = [other for other in peer_batches if other != name]
            for candidate in domain_payloads[name].get("batches", []):
                candidate_id = candidate["batch_id"]
                if candidate_id == batch["batch_id"]:
                    continue
                missing_peers = [other for other in peers
                                 if candidate_id not in peer_batches[other]]
                if not candidate.get("confirmed"):
                    code = "unconfirmed_here"
                    reason = "本域该批次尚未确认（confirmed=false），不参与关联"
                elif missing_peers:
                    code = "peer_missing"
                    reason = (f"对端域（{', '.join(sorted(missing_peers))}）"
                              "未发布该批次")
                elif any(not peer_batches[other][candidate_id].get("confirmed")
                         for other in peers):
                    code = "unconfirmed_peer"
                    reason = "对端域该批次尚未确认（confirmed=false），不能与单域版本拼接"
                else:
                    code = "superseded"
                    reason = (f"更早的共同已确认批次，已被选定批次 "
                              f"{batch['batch_id']} 取代")
                excluded.append({"batch_id": candidate_id,
                                 "reason_code": code, "reason": reason})
            if excluded:
                uncovered["excluded_versions"][name] = excluded

        orders = result["records"].get("orders", [])
        settlements = result["records"].get("settlement", [])
        if orders or settlements:
            settled_ids = {s.get("order_id") for s in settlements}
            missing = sorted(o["order_id"] for o in orders
                             if o.get("status") == "completed"
                             and o["order_id"] not in settled_ids)
            uncovered["orders_without_settlement"] = {
                "count": len(missing),
                "order_ids": missing[:25],
                "truncated": len(missing) > 25,
                "scope": "选定共同批次内已完成但无结算记录的订单",
            }
            orphan = sorted(s.get("order_id") or "" for s in settlements
                            if s.get("order_id") not in
                            {o["order_id"] for o in orders})
            uncovered["settlements_without_order"] = {
                "count": len(orphan), "order_ids": [o for o in orphan if o][:25],
            }

        if selected:
            business_dates = sorted(v["business_date"] for v in selected.values()
                                    if v.get("business_date"))
            uncovered["covered_business_dates"] = {
                "from": business_dates[0] if business_dates else None,
                "to": business_dates[-1] if business_dates else None,
                "note": "仅覆盖选定共同批次；之后已发布或尚未发布的批次见上方列表",
            }
            all_published = sorted({b["batch_id"] for payload in
                                    domain_payloads.values()
                                    for b in payload.get("batches", [])},
                                   key=batch_sort_key)
            if all_published:
                covered = selected[next(iter(selected))]["batch_id"]
                later = [b for b in all_published if batch_sort_key(b) > batch_sort_key(covered)]
                uncovered["batch_gap"] = {
                    "covered_batch": covered,
                    "later_published_batches": later,
                    "count": len(later),
                }
        return uncovered

    # -- long-lived task leases (demo of stale coordinators) ---------------
    def hold_task(self, hold_seconds: float = 60.0,
                  policy: str = POLICY_NAME) -> dict[str, Any]:
        hold_seconds = max(1.0, min(float(hold_seconds), 600.0))
        resolved = self.resolve(policy=policy, max_wait_ms=0)
        if not resolved.get("selection"):
            raise CoordinatorError(
                "no_selection",
                "当前没有可加租约的跨域共同批次",
                status=409,
                details={"consistency": resolved.get("consistency")},
            )
        # resolve() released its short-lived pins; take a dedicated long lease.
        task_id = "task-" + uuid.uuid4().hex[:10]
        lease_token = "lease-" + uuid.uuid4().hex
        owner = f"held/{task_id}"
        selected = {name: {
            "batch_id": sel["batch_id"],
            "current_version": sel["version"],
            "current_snapshot_id": sel["snapshot_id"],
        } for name, sel in resolved["selection"].items()}
        leases: dict[str, dict[str, Any]] = {}
        try:
            # Fill ``leases`` as pins are acquired so a failure on the second
            # domain does not leave the first pin alive for its whole TTL
            # (which for a held task can be minutes).
            self._acquire_selected(selected, lease_token, owner,
                                   ttl=hold_seconds + 30.0, acquired=leases)
        except UpstreamError as exc:
            released = self._release_all(leases, lease_token)
            raise CoordinatorError(
                "lease_failed",
                f"长任务租约获取失败，已回滚已获取的 {len(leases)} 个租约",
                status=503,
                details={"failed_domain_url": exc.url,
                         "upstream_message": exc.message,
                         "lease_release": released},
            ) from exc
        task = {
            "task_id": task_id,
            "owner": owner,
            "lease_token": lease_token,
            "pins": leases,
            "snapshot_ids": {name: lease["snapshot_id"]
                             for name, lease in leases.items()},
            "started_at": utc_now_iso(),
            "holds_until": time.time() + hold_seconds,
            "hold_seconds": hold_seconds,
            "released": False,
            "stale_release_attempt": None,
            "batch_id": next(iter(resolved["selection"].values()))["batch_id"],
        }
        with self._lock:
            self._tasks[task_id] = task
        return self._public_task(task)

    @staticmethod
    def _public_task(task: dict[str, Any]) -> dict[str, Any]:
        return {
            "task_id": task["task_id"],
            "owner": task["owner"],
            "batch_id": task["batch_id"],
            "pins": task["pins"],
            "started_at": task["started_at"],
            "hold_seconds": task["hold_seconds"],
            "released": task["released"],
            "stale_release_attempt": task["stale_release_attempt"],
        }

    def list_tasks(self) -> dict[str, Any]:
        with self._lock:
            return {"tasks": [self._public_task(t) for t in self._tasks.values()]}

    def release_task(self, task_id: str, stale: bool = False) -> dict[str, Any]:
        with self._lock:
            task = self._tasks.get(task_id)
        if task is None:
            raise KeyError(task_id)
        token = ("stale-" + task["lease_token"]) if stale else task["lease_token"]
        results = []
        for name, lease in task["pins"].items():
            try:
                self.clients[name].release(lease["pin_id"], token)
                results.append({"domain": name, "pin_id": lease["pin_id"],
                                "released": True, "token_used": "stale" if stale else "current"})
            except UpstreamError as exc:
                results.append({"domain": name, "pin_id": lease["pin_id"],
                                "released": False,
                                "token_used": "stale" if stale else "current",
                                "status": exc.status, "message": exc.message})
        if not stale:
            with self._lock:
                task["released"] = True
        else:
            task["stale_release_attempt"] = {
                "at": utc_now_iso(),
                "results": results,
                "explanation": "旧任务的 token 已无效，域服务拒绝释放；快照租约仍在保护数据",
            }
        return {"task_id": task_id, "stale": stale, "results": results}


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------

class CoordinatorHandler(JsonRequestHandler):
    coordinator: SnapshotCoordinator  # on server

    def do_GET(self) -> None:  # noqa: N802
        try:
            parts = self.path_parts()
            coord = self.server.coordinator  # type: ignore[attr-defined]
            if parts == ["health"]:
                status = {}
                for name, client in coord.clients.items():
                    try:
                        status[name] = client.health()
                    except UpstreamError as exc:
                        status[name] = {"status": "unreachable", "domain": name,
                                        "message": exc.message}
                self.send_json(200, {"status": "ok", "domains": status})
                return
            if parts == ["api", "policy"]:
                self.send_json(200, AVAILABILITY_COMMITMENT)
                return
            if parts == ["api", "tasks"]:
                self.send_json(200, coord.list_tasks())
                return
            if parts == ["api", "domains"]:
                self.send_json(200, {
                    "domains": [{"domain": name, "url": client.base_url}
                                for name, client in coord.clients.items()],
                })
                return
            self.send_error_json(404, "not_found", "未知路径")
        except Exception as exc:
            self.send_error_json(500, "internal_error", f"{type(exc).__name__}: {exc}")

    def do_POST(self) -> None:  # noqa: N802
        try:
            parts = self.path_parts()
            coord = self.server.coordinator  # type: ignore[attr-defined]
            body = self.read_json()
            if parts == ["api", "report"]:
                policy = str(body.get("policy", POLICY_NAME))
                resolved = coord.resolve(policy=policy,
                                         max_wait_ms=body.get("max_wait_ms",
                                                              DEFAULT_MAX_WAIT_MS))
                self.send_json(200, resolved)
                return
            if parts == ["api", "tasks", "hold"]:
                task = coord.hold_task(body.get("hold_seconds", 60),
                                       body.get("policy", POLICY_NAME))
                self.send_json(201, task)
                return
            if len(parts) == 4 and parts[:2] == ["api", "tasks"] and parts[3] == "release":
                stale = bool(body.get("stale", False))
                self.send_json(200, coord.release_task(parts[2], stale=stale))
                return
            self.send_error_json(404, "not_found", "未知路径")
        except CoordinatorError as exc:
            details = {key: value for key, value in exc.details.items()
                       if key not in ("code", "message", "status")}
            self.send_error_json(exc.status, exc.code, exc.message, **details)
        except KeyError as exc:
            self.send_error_json(404, "not_found", f"未知任务: {exc.args[0]}")
        except ValueError as exc:
            self.send_error_json(400, "bad_request", str(exc))
        except Exception as exc:
            self.send_error_json(500, "internal_error", f"{type(exc).__name__}: {exc}")


class CoordinatorServer(ServiceHTTPServer):
    def __init__(self, address: tuple[str, int],
                 coordinator: SnapshotCoordinator) -> None:
        super().__init__(address, CoordinatorHandler)
        self.coordinator = coordinator


def create_coordinator_server(orders_url: str, settlement_url: str,
                              host: str = "127.0.0.1", port: int = 0) -> CoordinatorServer:
    coord = SnapshotCoordinator({"orders": orders_url,
                                "settlement": settlement_url})
    return CoordinatorServer((host, port), coord)


def run_coordinator(orders_url: str, settlement_url: str, port: int,
                    host: str = "127.0.0.1") -> None:
    server = create_coordinator_server(orders_url, settlement_url, host, port)
    print(f"[coordinator] listening on http://{host}:{server.server_address[1]}",
          flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":  # pragma: no cover - process entry point
    import argparse
    parser = argparse.ArgumentParser(description="Run the cross-domain snapshot coordinator")
    parser.add_argument("--orders-url", required=True)
    parser.add_argument("--settlement-url", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    options = parser.parse_args()
    run_coordinator(options.orders_url, options.settlement_url,
                    options.port, options.host)
