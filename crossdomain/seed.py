"""Deterministic fictional data for the two business domains.

Both domains are generated from the same set of business batches so that batch
ids are the only join key between them.  The settlement domain intentionally
settles only a subset of completed orders; that gap is reported as uncovered
scope rather than silently smoothed over.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import tempfile
from pathlib import Path
from typing import Any

BATCH_IDS = [f"BIZ-202611{d:02d}" for d in range(1, 7)]
BATCH_DATES = {batch: f"2026-11-{int(batch[-2:]):02d}" for batch in BATCH_IDS}

REGIONS = ["华东", "华北", "华南", "西南", "东北"]
CHANNELS = ["APP", "WEB", "门店"]
PRODUCTS = ["智能音箱", "无线耳机", "便携电源", "机械键盘", "显示器支架", "智能手表"]
PAY_CHANNELS = ["微信支付", "支付宝", "银行卡"]


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def checksum_for(domain: str, batch_id: str, records: list[dict[str, Any]]) -> str:
    material = canonical_json({"domain": domain, "batch_id": batch_id,
                                "records": records})
    return "sha256:" + hashlib.sha256(material.encode("utf-8")).hexdigest()


def snapshot_version_for(domain: str, batch_id: str) -> str:
    return f"{domain}-{batch_id}-v1"


def _iso(day: str, hour: int, minute: int, second: int = 0) -> str:
    return f"{day}T{hour:02d}:{minute:02d}:{second:02d}+08:00"


def generate(seed: int = 20261117) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return (order_seed, settlement_seed) documents."""
    rng = random.Random(seed)
    order_batches: list[dict[str, Any]] = []
    settlement_batches: list[dict[str, Any]] = []

    for batch_index, batch_id in enumerate(BATCH_IDS, start=1):
        day = BATCH_DATES[batch_id]
        order_records: list[dict[str, Any]] = []
        settlement_records: list[dict[str, Any]] = []
        order_count = rng.randint(8, 13)

        for n in range(1, order_count + 1):
            order_id = f"ORD-{day.replace('-', '')}-{n:03d}"
            quantity = rng.randint(1, 3)
            unit_cents = rng.randrange(3900, 129900, 100)
            amount_cents = unit_cents * quantity
            status = "completed" if rng.random() < 0.88 else "cancelled"
            created = _iso(day, rng.randint(8, 21), rng.randint(0, 59),
                           rng.randint(0, 59))
            order_records.append({
                "order_id": order_id,
                "batch_id": batch_id,
                "customer_id": f"C-{rng.randint(1000, 9999)}",
                "region": rng.choice(REGIONS),
                "channel": rng.choice(CHANNELS),
                "product": rng.choice(PRODUCTS),
                "quantity": quantity,
                "amount_cents": amount_cents,
                "currency": "CNY",
                "status": status,
                "created_at": created,
            })

            # A completed order is usually, but not always, settled in the
            # same business batch.  This is the in-batch coverage gap.
            if status == "completed" and rng.random() < 0.86:
                fee_cents = max(1, round(amount_cents * 0.006))
                settlement_records.append({
                    "settlement_id": f"STL-{day.replace('-', '')}-{n:03d}",
                    "batch_id": batch_id,
                    "order_id": order_id,
                    "amount_cents": amount_cents,
                    "fee_cents": fee_cents,
                    "net_cents": amount_cents - fee_cents,
                    "currency": "CNY",
                    "pay_channel": rng.choice(PAY_CHANNELS),
                    "status": "settled",
                    "settled_at": _iso(day, rng.randint(22, 23), rng.randint(0, 59)),
                })

        # Guarantee at least one completed-but-unsettled order per batch so the
        # coverage gap is reproducible rather than a lucky roll of the RNG.
        order_records[0]["status"] = "completed"
        settled_order_ids = {r["order_id"] for r in settlement_records}
        completed_ids = [r["order_id"] for r in order_records
                         if r["status"] == "completed"]
        if all(order_id in settled_order_ids for order_id in completed_ids):
            drop_id = completed_ids[-1]
            settlement_records = [r for r in settlement_records
                                  if r["order_id"] != drop_id]

        order_total = sum(r["amount_cents"] for r in order_records)
        settled_total = sum(r["amount_cents"] for r in settlement_records)
        order_batches.append({
            "batch_id": batch_id,
            "business_date": day,
            "sequence": int(batch_id[-8:]),
            "generated_at": _iso(day, 23, 55),
            "record_count": len(order_records),
            "total_amount_cents": order_total,
            "records": order_records,
        })
        settlement_batches.append({
            "batch_id": batch_id,
            "business_date": day,
            "sequence": int(batch_id[-8:]),
            "generated_at": _iso(day, 23, 59),
            "record_count": len(settlement_records),
            "total_amount_cents": settled_total,
            "records": settlement_records,
        })

    meta = {
        "seed": seed,
        "generated_for": "fictional demonstration data",
        "batch_ids": BATCH_IDS,
    }
    return ({"meta": meta, "domain": "orders", "batches": order_batches},
            {"meta": meta, "domain": "settlement", "batches": settlement_batches})


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name, suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.replace(tmp, path)
    finally:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass


def ensure_seed(data_dir: Path, force: bool = False) -> dict[str, Path]:
    """Generate seed files on first use; never overwrite silently."""
    seed_dir = data_dir / "seed"
    orders_path = seed_dir / "orders_batches.json"
    settlement_path = seed_dir / "settlement_batches.json"
    if force or not orders_path.exists() or not settlement_path.exists():
        orders, settlements = generate()
        write_json_atomic(orders_path, orders)
        write_json_atomic(settlement_path, settlements)
    return {"orders": orders_path, "settlement": settlement_path}


def load_batches(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


if __name__ == "__main__":  # pragma: no cover - convenience CLI
    import argparse
    parser = argparse.ArgumentParser(description="Generate fictional cross-domain data")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    print(ensure_seed(Path(args.data_dir), force=args.force))
