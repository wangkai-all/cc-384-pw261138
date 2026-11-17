"""Domain-level invariants: immutable snapshots, revisions, leases, GC."""

from __future__ import annotations

import time

import pytest


def test_seed_is_deterministic_and_batches_are_shared():
    from crossdomain.seed import generate
    first_orders, first_settlements = generate()
    second_orders, second_settlements = generate()
    assert first_orders == second_orders
    assert first_settlements == second_settlements
    order_batches = {b["batch_id"] for b in first_orders["batches"]}
    settlement_batches = {b["batch_id"] for b in first_settlements["batches"]}
    assert order_batches == settlement_batches
    # Settlement intentionally settles only a subset of completed orders.
    completed = sum(1 for b in first_orders["batches"] for r in b["records"]
                    if r["status"] == "completed")
    settled = sum(len(b["records"]) for b in first_settlements["batches"])
    assert 0 < settled < completed


def test_bootstrap_publishes_four_batches_and_keeps_two_pending(two_stores):
    orders, settlements = two_stores
    listing = orders.list_batches(include_pending=True)
    assert len(listing["batches"]) == 4
    assert [b["batch_id"] for b in listing["pending"]] == \
        ["BIZ-20261105", "BIZ-20261106"]
    # Both domains publish the same batch ids.
    assert [b["batch_id"] for b in listing["batches"]] == \
        [b["batch_id"] for b in settlements.list_batches()["batches"]]


def test_publish_next_is_monotonic(two_stores):
    orders, _ = two_stores
    published = orders.publish_next_batch()
    assert published["published"]["batch_id"] == "BIZ-20261105"
    with pytest.raises(LookupError):
        orders.publish_next_batch("BIZ-20261101")  # already published


def test_revision_creates_new_version_and_keeps_old(two_stores):
    orders, _ = two_stores
    before = orders.get_snapshot("BIZ-20261101", 1)
    revision = orders.revise_batch("BIZ-20261101", note="修正金额")
    assert revision["published"]["version"] == 2
    assert revision["published"]["note"] == "修正金额"
    versions = orders.snapshot_versions("BIZ-20261101")["versions"]
    assert [v["version"] for v in versions] == [1, 2]
    old = orders.get_snapshot("BIZ-20261101", 1)
    new = orders.get_snapshot("BIZ-20261101", 2)
    assert old["checksum"] == before["checksum"]
    assert new["checksum"] != old["checksum"]
    assert new["record_count"] == old["record_count"]


def test_lease_release_requires_matching_token(two_stores):
    orders, _ = two_stores
    snapshot_id = orders.list_batches()["batches"][0]["current_snapshot_id"]
    pin = orders.acquire_pin(snapshot_id, "coordinator/t1", "token-a", 60)
    assert pin["snapshot_id"] == snapshot_id

    with pytest.raises(PermissionError):
        orders.release_pin(pin["pin_id"], "token-b")

    # The stale-token rejection did not release anything.
    assert len(orders.list_pins(snapshot_id)["active_pins"]) == 1

    released = orders.release_pin(pin["pin_id"], "token-a")
    assert released["already_released"] is False
    # Second release with the same token is idempotent, not destructive.
    again = orders.release_pin(pin["pin_id"], "token-a")
    assert again["already_released"] is True
    assert orders.list_pins(snapshot_id)["active_pins"] == []


def test_expired_lease_is_swept(two_stores):
    orders, _ = two_stores
    snapshot_id = orders.list_batches()["batches"][0]["current_snapshot_id"]
    orders.acquire_pin(snapshot_id, "coordinator/t1", "token-a", 1)
    time.sleep(1.2)
    assert orders.list_pins(snapshot_id)["active_pins"] == []


def test_gc_keeps_pinned_snapshot_and_removes_unpinned_old_version(two_stores):
    orders, _ = two_stores
    orders.revise_batch("BIZ-20261101", note="v2")
    orders.revise_batch("BIZ-20261101", note="v3")
    snapshot_id = "BIZ-20261101/v1"
    pin = orders.acquire_pin(snapshot_id, "coordinator/t1", "token-a", 60)

    result = orders.garbage_collect()
    assert snapshot_id in result["retained_pinned_snapshots"]
    assert "BIZ-20261101/v2" in result["deleted_snapshots"]
    assert snapshot_id not in result["deleted_snapshots"]
    # The pinned snapshot is still fully readable.
    snapshot = orders.get_snapshot("BIZ-20261101", 1)
    assert snapshot["records"]

    orders.release_pin(pin["pin_id"], "token-a")
    second = orders.garbage_collect()
    assert snapshot_id in second["deleted_snapshots"]


def test_snapshot_checksum_matches_records(two_stores):
    orders, _ = two_stores
    snapshot = orders.get_snapshot("BIZ-20261102", 1)
    from crossdomain.seed import canonical_json
    import hashlib
    material = canonical_json({"domain": "orders",
                               "batch_id": snapshot["batch_id"],
                               "records": snapshot["records"]})
    assert snapshot["checksum"] == "sha256:" + hashlib.sha256(
        material.encode()).hexdigest()
