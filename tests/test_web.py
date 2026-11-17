"""Web-level tests: gateway API, report rendering, and the UI asset contract."""

from __future__ import annotations

import json
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import urllib.error  # noqa: E402
import urllib.request  # noqa: E402

from crossdomain.gateway import GatewayServer, Supervisor  # noqa: E402
from crossdomain.report import summarize, to_html_fragment, to_markdown  # noqa: E402


def call(base: str, method: str, path: str,
         payload: dict | None = None) -> tuple[int, dict, str]:
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Accept": "application/json"}
    if data:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(base + path, data=data, method=method,
                                     headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read()
            ctype = response.headers.get("Content-Type", "")
            if ctype.startswith("text/markdown"):
                return response.status, {}, raw.decode()
            return response.status, json.loads(raw or b"{}"), raw.decode()
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}"), ""


@pytest.fixture()
def gateway(tmp_path):
    ports = {"orders": 0, "settlement": 0, "coordinator": 0, "gateway": 0}
    supervisor = Supervisor(tmp_path, ports,
                            env={"CROSSDOMAIN_DATA_DIR": str(tmp_path / "data")})
    # Allocate free ports up front so children and the gateway can bind.
    import socket

    def free_port() -> int:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    for name in ports:
        ports[name] = free_port()
    supervisor.ports = ports
    supervisor.start_domain("orders")
    supervisor.start_domain("settlement")
    supervisor.start_coordinator()
    server = GatewayServer(("127.0.0.1", ports["gateway"]), supervisor)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{ports['gateway']}", supervisor
    finally:
        server.shutdown()
        server.server_close()
        supervisor.shutdown()


def test_gateway_serves_ui_and_policy(gateway):
    base, _ = gateway
    request = urllib.request.Request(base + "/")
    with urllib.request.urlopen(request, timeout=5) as response:
        html = response.read().decode()
    assert "跨域对账控制台" in html
    assert 'id="run-report"' in html
    assert 'data-start="orders"' in html
    assert 'data-start="settlement"' in html

    status, policy, _ = call(base, "GET", "/api/policy")
    assert status == 200
    assert policy["policy"] == "aligned_with_explicit_partial"
    assert "batch_id" in policy["consistency"]


def test_gateway_report_and_compare(gateway):
    base, _ = gateway
    status, first, _ = call(base, "POST", "/api/report", {"max_wait_ms": 300})
    assert status == 200
    assert first["consistency"]["mode"] == "aligned"
    assert first["selection"]

    # Advance orders only, then take a second report.
    status, advanced, _ = call(base, "POST", "/api/control/advance",
                               {"domain": "orders"})
    assert status == 200
    status, second, _ = call(base, "POST", "/api/report", {"max_wait_ms": 300})
    assert second["consistency"]["mode"] == "degraded_stale"

    status, comparison, _ = call(
        base, "GET", f"/api/compare?left={first['run_id']}&right={second['run_id']}")
    assert status == 200
    fields = {d["field"] for d in comparison["differences"]}
    assert "mode" in fields
    assert comparison["same_batch"] is True
    assert comparison["left"]["selection"]["orders"]["batch_id"] == \
        comparison["right"]["selection"]["orders"]["batch_id"]


def test_stop_one_domain_query_restart_and_compare(gateway):
    base, supervisor = gateway
    _, baseline, _ = call(base, "POST", "/api/report", {"max_wait_ms": 300})
    status, stopped, _ = call(base, "POST", "/api/control/stop",
                              {"domain": "settlement"})
    assert status == 200
    assert stopped["state"] == "stopped"

    # The other domain must still accept work while its peer is down.
    status, advanced, _ = call(base, "POST", "/api/control/advance",
                               {"domain": "orders"})
    assert status == 200

    status, partial, _ = call(base, "POST", "/api/report", {"max_wait_ms": 800})
    assert status == 200
    assert partial["consistency"]["mode"] == "partial"
    assert partial["records"]["orders"] == []
    assert partial["uncovered"]["unreachable_domains"] == ["settlement"]
    assert "settlement" in partial["domains"]

    status, started, _ = call(base, "POST", "/api/control/start",
                              {"domain": "settlement"})
    assert status == 200
    assert started["state"] == "running"

    status, recovered, _ = call(base, "POST", "/api/report", {"max_wait_ms": 500})
    assert status == 200
    assert recovered["consistency"]["mode"] == "degraded_stale"

    status, comparison, _ = call(
        base, "GET", f"/api/compare?left={baseline['run_id']}"
                     f"&right={recovered['run_id']}")
    assert status == 200
    assert comparison["same_batch"] is True


def test_strict_failure_passes_through_gateway(gateway):
    base, _ = gateway
    call(base, "POST", "/api/control/advance", {"domain": "orders"})
    status, body, _ = call(base, "POST", "/api/report",
                           {"policy": "strict", "max_wait_ms": 0})
    assert status == 503
    assert body["error"] == "strict_failed"


def test_gateway_reports_are_listed_and_markdown_renders(gateway):
    base, _ = gateway
    call(base, "POST", "/api/report", {"max_wait_ms": 300})
    status, listing, _ = call(base, "GET", "/api/reports")
    assert status == 200
    assert listing["reports"]
    assert listing["reports"][0]["summary"]["mode"] == "aligned"

    status, markdown, raw = call(base, "POST", "/api/report",
                                 {"max_wait_ms": 300, "format": "markdown"})
    assert status == 200
    assert "跨域对账报告" in raw
    assert "选版依据" in raw
    assert "未覆盖范围" in raw


def test_gateway_rejects_unknown_actions(gateway):
    base, _ = gateway
    status, body, _ = call(base, "POST", "/api/control/nope", {})
    assert status == 400
    status, body, _ = call(base, "GET", "/api/nope")
    assert status == 404


# ---------------------------------------------------------------------------
# Pure rendering helpers
# ---------------------------------------------------------------------------

def sample_report(mode="aligned", joined=True):
    return {
        "run_id": "run-test",
        "policy": "aligned_with_explicit_partial",
        "max_wait_ms": 1500,
        "waited_ms": 12.0,
        "consistency": {"mode": mode, "joined": joined, "reason": "测试",
                        "joined_batch_ids": ["BIZ-1"] if joined else []},
        "selection": {"orders": {"batch_id": "BIZ-1", "version": 1,
                                 "record_count": 2, "total_amount_cents": 30000,
                                 "is_domain_latest": True, "lag_batches": 0,
                                 "checksum": "sha256:abc", "business_date": "2026-11-01"}},
        "domains": {"orders": {"reachable": True, "selected": joined,
                               "latest_batch_id": "BIZ-1"}},
        "records": {"orders": [{"order_id": "O1", "status": "completed",
                                "amount_cents": 10000},
                               {"order_id": "O2", "status": "completed",
                                "amount_cents": 20000}],
                    "settlement": [{"order_id": "O1", "amount_cents": 9900}]},
        "uncovered": {"orders_without_settlement": {"count": 1,
                                                    "order_ids": ["O2"]},
                      "pending_batches": {}, "unpaired_batches": {},
                      "unreachable_domains": []},
        "lease_release": [],
    }


def test_summarize_counts_and_missing_settlements():
    summary = summarize(sample_report())
    assert summary["order_count"] == 2
    assert summary["order_total_cents"] == 30000
    assert summary["completed_orders_settled"] == 1
    assert summary["batch_id"] == "BIZ-1"


def test_html_fragment_escapes_domain_values():
    report = sample_report()
    report["selection"]["orders"]["batch_id"] = "<script>alert(1)</script>"
    fragment = to_html_fragment(report)
    assert "<script>alert(1)</script>" not in fragment
    assert "&lt;script&gt;" in fragment


def test_markdown_contains_selection_and_uncovered():
    text = to_markdown(sample_report("degraded_stale"))
    assert "degraded_stale" in text
    assert "BIZ-1" in text
    assert "已发货" not in text
    assert "无结算" in text or "无结算" in text.replace("完成", "")


def test_unknown_domain_is_rejected_not_silently_routed(gateway):
    base, _ = gateway
    for action in ("start", "stop", "advance", "revise", "gc"):
        payload = {"domain": "warehouse"}
        if action == "revise":
            payload["batch_id"] = "BIZ-20261101"
        status, body, _ = call(base, "POST", f"/api/control/{action}", payload)
        assert status == 400, (action, status, body)
        assert body["error"] == "bad_request"


def test_unknown_log_name_is_rejected(gateway):
    base, _ = gateway
    status, body, _ = call(base, "GET", "/api/logs?name=../../etc/passwd")
    assert status == 400
    assert body["error"] == "bad_request"


def test_non_object_json_body_is_rejected(gateway):
    base, _ = gateway
    for payload in ([], "text", 42):
        status, body, _ = call(base, "POST", "/api/report", payload)
        assert status == 400, (payload, status, body)
        assert body["error"] == "bad_request"
