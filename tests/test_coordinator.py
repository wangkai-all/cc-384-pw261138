"""Coordinator behaviour: selection, degradation, strict mode, availability."""

from __future__ import annotations

import time

import pytest

from crossdomain.common import http_json


def post(url: str, payload: dict) -> tuple[int, dict]:
    try:
        status, body, _ = http_json("POST", url, payload, timeout=10)
        return status, body
    except Exception as exc:  # UpstreamError carries a status
        return getattr(exc, "status", 0), getattr(exc, "payload", {}) or {}


def get(url: str) -> tuple[int, dict]:
    try:
        status, body, _ = http_json("GET", url, timeout=10)
        return status, body
    except Exception as exc:
        return getattr(exc, "status", 0), getattr(exc, "payload", {}) or {}


def test_aligned_report_joins_same_batch(fleet):
    status, report = post(fleet["coordinator"].url + "/api/report",
                          {"max_wait_ms": 300})
    assert status == 200
    consistency = report["consistency"]
    assert consistency["mode"] == "aligned"
    assert consistency["joined"] is True
    assert consistency["joined_batch_ids"] == ["BIZ-20261104"]
    assert {s["batch_id"] for s in report["selection"].values()} == {"BIZ-20261104"}
    assert all(s["checksum"].startswith("sha256:") for s in report["selection"].values())
    assert all(d["checksum_verified"] for d in report["domains"].values())
    assert report["records"]["orders"]
    assert report["records"]["settlement"]
    # Leases are acquired and released by the coordinator.
    assert len(report["leases"]) == 2
    assert all(item["released"] for item in report["lease_release"])


def test_never_mixes_latest_versions(fleet):
    # Advance only orders: settlement still sits on BIZ-20261104.
    status, _ = post(fleet["orders"].url + "/admin/publish-next", {})
    assert status == 201
    _, report = post(fleet["coordinator"].url + "/api/report", {"max_wait_ms": 0})
    consistency = report["consistency"]
    assert consistency["mode"] == "degraded_stale"
    assert consistency["joined_batch_ids"] == ["BIZ-20261104"]
    selection = report["selection"]
    assert selection["orders"]["batch_id"] == selection["settlement"]["batch_id"]
    assert selection["orders"]["is_domain_latest"] is False
    assert selection["orders"]["lag_batches"] == 1
    assert report["domains"]["orders"]["latest_batch_id"] == "BIZ-20261105"
    assert report["uncovered"]["unpaired_batches"] == {"orders": ["BIZ-20261105"]}
    assert report["freshness"]["orders"]["lag_batches"] == 1


def test_strict_policy_refuses_degraded_selection(fleet):
    post(fleet["orders"].url + "/admin/publish-next", {})
    status, body = post(fleet["coordinator"].url + "/api/report",
                        {"max_wait_ms": 0, "policy": "strict"})
    assert status == 503
    assert body["error"] == "strict_failed"


def test_strict_policy_accepts_aligned_fleet(fleet):
    status, report = post(fleet["coordinator"].url + "/api/report",
                          {"max_wait_ms": 0, "policy": "strict"})
    assert status == 200
    assert report["consistency"]["mode"] == "aligned"


def test_unreachable_domain_returns_partial_within_deadline(fleet):
    fleet["settlement"].stop()
    started = time.monotonic()
    status, report = post(fleet["coordinator"].url + "/api/report",
                          {"max_wait_ms": 1200})
    elapsed = time.monotonic() - started
    assert status == 200
    assert elapsed < 3.0  # bounded wait, no infinite alignment polling
    assert report["consistency"]["mode"] == "partial"
    assert report["consistency"]["reason"] == "domain_unreachable"
    assert report["consistency"]["joined"] is False
    assert report["uncovered"]["unreachable_domains"] == ["settlement"]
    assert report["records"]["orders"] == []
    assert report["records"]["settlement"] == []
    # The available domain is still described, not silently dropped.
    assert report["domains"]["orders"]["reachable"] is True
    assert report["domains"]["orders"]["selected"] is False
    assert report["domains"]["settlement"]["reachable"] is False


def test_wait_is_capped_by_server(fleet):
    post(fleet["orders"].url + "/admin/publish-next", {})
    status, report = post(fleet["coordinator"].url + "/api/report",
                          {"max_wait_ms": 999999})
    assert status == 200
    assert report["max_wait_ms"] == 5000
    # No common fresh batch ever appears; the request still returns promptly
    # because waiting stops once one side is known to have no matching batch.
    assert report["consistency"]["joined_batch_ids"] == ["BIZ-20261104"]


def test_wait_returns_as_soon_as_both_domains_align(fleet):
    # Orders is one batch ahead; as soon as settlement catches up the report
    # joins the new batch without burning the whole deadline.
    post(fleet["orders"].url + "/admin/publish-next", {})
    started = time.monotonic()
    # Publish settlement's next batch from another thread while the report waits.
    import threading

    def catch_up():
        time.sleep(0.3)
        post(fleet["settlement"].url + "/admin/publish-next", {})

    thread = threading.Thread(target=catch_up)
    thread.start()
    status, report = post(fleet["coordinator"].url + "/api/report",
                          {"max_wait_ms": 3000})
    thread.join()
    elapsed = time.monotonic() - started
    assert status == 200
    assert report["consistency"]["mode"] == "aligned"
    assert report["consistency"]["joined_batch_ids"] == ["BIZ-20261105"]
    assert elapsed < 2.5


def test_report_survives_snapshot_revision_during_read(fleet):
    # Revise a batch and verify the report uses the pinned version's checksum,
    # not whichever version happens to be newest later.
    post(fleet["orders"].url + "/admin/revise/BIZ-20261104", {"note": "late fix"})
    _, report = post(fleet["coordinator"].url + "/api/report", {"max_wait_ms": 0})
    selected = report["selection"]["orders"]
    assert selected["batch_id"] == "BIZ-20261104"
    assert selected["version"] == 2


def test_domain_endpoints_expose_pins_and_gc(fleet):
    _, report = post(fleet["coordinator"].url + "/api/report", {"max_wait_ms": 0})
    pin_id = report["leases"]["orders"]["pin_id"]
    status, pins = get(fleet["orders"].url + "/pins")
    assert status == 200
    assert isinstance(pins["active_pins"], list)


def test_held_task_lease_blocks_gc_and_stale_token_is_rejected(fleet):
    _, task = post(fleet["coordinator"].url + "/api/tasks/hold", {"hold_seconds": 60})
    assert task["task_id"].startswith("task-")
    orders_snapshot = task["pins"]["orders"]["snapshot_id"]
    assert orders_snapshot

    # Create an older version to make the pinned snapshot a GC candidate.
    post(fleet["orders"].url + "/admin/revise/BIZ-20261104", {"note": "newer"})
    status, gc = post(fleet["orders"].url + "/admin/gc", {})
    assert status == 200
    assert orders_snapshot in gc["retained_pinned_snapshots"]

    # A stale coordination task must not be able to release the live lease.
    status, released = post(
        fleet["coordinator"].url + f"/api/tasks/{task['task_id']}/release",
        {"stale": True})
    assert status == 200
    assert all(not r["released"] for r in released["results"])
    assert all(r.get("status") == 409 for r in released["results"])
    _, pins = get(fleet["orders"].url + f"/pins?snapshot_id={orders_snapshot}")
    assert len(pins["active_pins"]) == 1

    # The owning task can still release it.
    status, released = post(
        fleet["coordinator"].url + f"/api/tasks/{task['task_id']}/release", {})
    assert status == 200
    assert all(r["released"] for r in released["results"])


def test_failed_hold_task_releases_the_pin_it_already_took(fleet):
    # Make the first pin succeed and the second one fail: the coordinator must
    # not leave the first pin alive for the task's multi-minute TTL.
    coordinator = fleet["coord"]
    original = coordinator.clients["settlement"].pin

    def failing_pin(*args, **kwargs):
        from crossdomain.common import UpstreamUnavailable
        raise UpstreamUnavailable(coordinator.clients["settlement"].base_url,
                                  "simulated failure")

    coordinator.clients["settlement"].pin = failing_pin
    try:
        status, body = post(fleet["coordinator"].url + "/api/tasks/hold",
                            {"hold_seconds": 120})
    finally:
        coordinator.clients["settlement"].pin = original
    assert status == 503
    assert body["error"] == "lease_failed"
    assert body["lease_release"][0]["released"] is True
    _, pins = get(fleet["orders"].url + "/pins")
    assert pins["active_pins"] == []
    assert coordinator.list_tasks()["tasks"] == []


def test_uncovered_lists_orders_missing_settlement(fleet):
    _, report = post(fleet["coordinator"].url + "/api/report", {"max_wait_ms": 0})
    missing = report["uncovered"]["orders_without_settlement"]
    settled_ids = {r["order_id"] for r in report["records"]["settlement"]}
    expected = [o["order_id"] for o in report["records"]["orders"]
                if o["status"] == "completed" and o["order_id"] not in settled_ids]
    assert missing["count"] == len(expected)
    # The seed is designed to have some coverage gap.
    assert missing["count"] > 0


def test_excluded_versions_explain_why_each_batch_was_not_joined(fleet):
    # Both sides publish BIZ-20261105; settlement has not confirmed it yet.
    post(fleet["orders"].url + "/admin/publish-next", {})
    post(fleet["settlement"].url + "/admin/publish-next", {"confirmed": False})
    _, report = post(fleet["coordinator"].url + "/api/report",
                     {"max_wait_ms": 0})
    assert report["consistency"]["joined_batch_ids"] == ["BIZ-20261104"]

    def reasons(report: dict) -> dict[str, dict[str, str]]:
        return {
            name: {item["batch_id"]: item["reason_code"] for item in items}
            for name, items in report["uncovered"]["excluded_versions"].items()
        }

    codes = reasons(report)
    # BIZ-20261101..03 are simply older common batches.
    assert codes["orders"]["BIZ-20261101"] == "superseded"
    # Orders has BIZ-20261105 confirmed, but its peer has not confirmed it.
    assert codes["orders"]["BIZ-20261105"] == "unconfirmed_peer"
    # Settlement's own BIZ-20261105 is unconfirmed there.
    assert codes["settlement"]["BIZ-20261105"] == "unconfirmed_here"

    # Once settlement confirms, the batch joins and only older batches remain.
    post(fleet["settlement"].url + "/admin/confirm/BIZ-20261105", {})
    _, report = post(fleet["coordinator"].url + "/api/report",
                     {"max_wait_ms": 0})
    assert report["consistency"]["joined_batch_ids"] == ["BIZ-20261105"]
    assert "BIZ-20261105" not in reasons(report).get("orders", {})
    assert reasons(report)["orders"]["BIZ-20261104"] == "superseded"

    # Orders moves on to BIZ-20261106, which settlement never published.
    post(fleet["orders"].url + "/admin/publish-next", {})
    _, report = post(fleet["coordinator"].url + "/api/report",
                     {"max_wait_ms": 0})
    assert report["consistency"]["joined_batch_ids"] == ["BIZ-20261105"]
    assert reasons(report)["orders"]["BIZ-20261106"] == "peer_missing"


def test_unconfirmed_batch_is_not_joined_until_confirmed(fleet):
    # Both domains publish BIZ-20261105 but do not confirm it yet.
    for name in ("orders", "settlement"):
        status, body = post(fleet[name].url + "/admin/publish-next",
                            {"confirmed": False})
        assert status == 201
        assert body["published"]["confirmed"] is False
        assert body["published"]["version"] == 1

    status, report = post(fleet["coordinator"].url + "/api/report",
                          {"max_wait_ms": 0})
    assert status == 200
    # Nothing merges the unconfirmed batch; the previous confirmed batch is used.
    assert report["consistency"]["joined_batch_ids"] == ["BIZ-20261104"]
    assert report["consistency"]["mode"] == "degraded_stale"
    assert [item["batch_id"] for item in report["uncovered"]["unconfirmed_latest"]] \
        == ["BIZ-20261105", "BIZ-20261105"]

    # Confirming both sides makes the batch joinable without any new publish.
    for name in ("orders", "settlement"):
        status, body = post(fleet[name].url + "/admin/confirm/BIZ-20261105", {})
        assert status == 200
        assert body["confirmed"]["confirmed"] is True
        assert body["already_confirmed"] is False

    status, report = post(fleet["coordinator"].url + "/api/report",
                          {"max_wait_ms": 0})
    assert report["consistency"]["mode"] == "aligned"
    assert report["consistency"]["joined_batch_ids"] == ["BIZ-20261105"]


def test_report_waits_for_confirmation_within_deadline(fleet):
    import threading

    for name in ("orders", "settlement"):
        post(fleet[name].url + "/admin/publish-next", {"confirmed": False})

    def confirm_later():
        time.sleep(0.35)
        post(fleet["orders"].url + "/admin/confirm/BIZ-20261105", {})
        time.sleep(0.25)
        post(fleet["settlement"].url + "/admin/confirm/BIZ-20261105", {})

    thread = threading.Thread(target=confirm_later)
    thread.start()
    started = time.monotonic()
    status, report = post(fleet["coordinator"].url + "/api/report",
                          {"max_wait_ms": 3000})
    thread.join()
    elapsed = time.monotonic() - started
    assert status == 200
    assert report["consistency"]["mode"] == "aligned"
    assert report["consistency"]["joined_batch_ids"] == ["BIZ-20261105"]
    assert report["attempts"] >= 2
    assert elapsed < 2.5


def test_strict_mode_waits_for_alignment_before_failing(fleet):
    import threading

    for name in ("orders", "settlement"):
        post(fleet[name].url + "/admin/publish-next", {"confirmed": False})

    def confirm_later():
        time.sleep(0.3)
        for name in ("orders", "settlement"):
            post(fleet[name].url + "/admin/confirm/BIZ-20261105", {})

    thread = threading.Thread(target=confirm_later)
    thread.start()
    status, report = post(fleet["coordinator"].url + "/api/report",
                          {"max_wait_ms": 2500, "policy": "strict"})
    thread.join()
    assert status == 200
    assert report["consistency"]["mode"] == "aligned"


def test_revision_inherits_confirmation_state(fleet):
    post(fleet["orders"].url + "/admin/publish-next", {"confirmed": False})
    status, revised = post(fleet["orders"].url + "/admin/revise/BIZ-20261105",
                           {"note": "unconfirmed revision"})
    assert status == 201
    assert revised["published"]["version"] == 2
    assert revised["published"]["confirmed"] is False
    status, confirmed = post(fleet["orders"].url + "/admin/confirm/BIZ-20261105", {})
    assert status == 200
    assert confirmed["confirmed"]["version"] == 2
