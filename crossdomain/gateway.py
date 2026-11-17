"""Report query gateway + demo control plane + static web UI.

The gateway is the only component the browser talks to.  Report requests are
proxied to the coordinator over real HTTP; the control plane starts and stops
the two domain services (separate OS processes, persistent SQLite state) and can
advance one domain while the other is down.
"""

from __future__ import annotations

import atexit
import os
import subprocess
import sys
import threading
import time
import urllib.parse
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .common import (JsonRequestHandler, UpstreamError, UpstreamUnavailable,
                     append_query, http_json, utc_now_iso)
from .report import summarize, to_markdown

ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = ROOT / "web"
MAX_STORED_REPORTS = 30


class Supervisor:
    """Owns the domain/coordinator child processes and demo state."""

    def __init__(self, base_dir: Path, ports: dict[str, int],
                 env: dict[str, str] | None = None) -> None:
        self.base_dir = Path(base_dir)
        # The domain databases live in a caller-provided data directory (the
        # demo passes a fresh one); logs stay next to them.
        self.data_dir = Path(env["CROSSDOMAIN_DATA_DIR"]) if env and \
            env.get("CROSSDOMAIN_DATA_DIR") else self.base_dir / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / "runtime").mkdir(parents=True, exist_ok=True)
        self.log_dir = self.data_dir / "runtime"
        self.ports = ports
        self.env = dict(os.environ)
        if env:
            self.env.update(env)
        self.processes: dict[str, subprocess.Popen[Any]] = {}
        self.logs: dict[str, list[str]] = {"orders": [], "settlement": [],
                                           "coordinator": []}
        self.lock = threading.RLock()
        self.reports: list[dict[str, Any]] = []
        self.events: list[dict[str, Any]] = []
        # Every Popen is registered here and cleaned up on normal exit so a
        # stopped/reloaded demo never leaves orphaned services behind.
        atexit.register(self.shutdown)

    # -- urls --------------------------------------------------------------
    def domain_url(self, name: str) -> str:
        return f"http://127.0.0.1:{self.ports[name]}"

    @property
    def coordinator_url(self) -> str:
        return f"http://127.0.0.1:{self.ports['coordinator']}"

    @property
    def gateway_url(self) -> str:
        return f"http://127.0.0.1:{self.ports['gateway']}"

    # -- lifecycle ---------------------------------------------------------
    def _spawn(self, name: str, module: str, args: list[str]) -> None:
        log_path = self.log_dir / f"{name}.log"
        log_file = log_path.open("a", encoding="utf-8")
        process = subprocess.Popen(
            [sys.executable, "-u", "-m", module, *args],
            cwd=str(ROOT), env=self.env, stdout=log_file,
            stderr=subprocess.STDOUT, start_new_session=True)
        log_file.close()  # child keeps its own descriptor
        self.processes[name] = process
        self.logs.setdefault(name, [])
        self.logs[name].append(f"{utc_now_iso()} started pid={process.pid}")

    def _stop_process(self, name: str, process: subprocess.Popen[Any],
                      graceful: bool) -> None:
        if graceful:
            process.terminate()
        else:
            process.kill()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    def _await_listen(self, name: str, url: str, timeout: float = 2.0) -> bool:
        """Wait briefly for a freshly spawned service to answer /health."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            process = self.processes.get(name)
            if process is not None and process.poll() is not None:
                return False
            try:
                _, body, _ = http_json("GET", url, timeout=0.4)
                if body and body.get("status") == "ok":
                    return True
            except UpstreamError:
                pass
            time.sleep(0.08)
        return False

    def start_domain(self, name: str) -> dict[str, Any]:
        if name not in ("orders", "settlement"):
            raise ValueError(f"未知域: {name}")
        with self.lock:
            existing = self.processes.get(name)
            if existing is not None and existing.poll() is None:
                return {"domain": name, "state": "running", "pid": existing.pid}
            self._spawn(name, "crossdomain.domain_service",
                        ["--domain", name, "--data-dir", str(self.data_dir),
                         "--port", str(self.ports[name])])
            ready = self._await_listen(name, self.domain_url(name) + "/health")
            self.logs[name].append(f"{utc_now_iso()} start requested "
                                   f"ready={ready}")
        self._record_event("start", name, {"ready": ready})
        result = {"domain": name, "state": "running" if ready else "starting",
                  "pid": self.processes[name].pid}
        if not ready:
            result["warning"] = "服务未能及时响应 /health，请查看日志"
        return result

    def stop_domain(self, name: str, graceful: bool = True) -> dict[str, Any]:
        with self.lock:
            process = self.processes.get(name)
            if process is None or process.poll() is not None:
                self.processes.pop(name, None)
                return {"domain": name, "state": "stopped", "was_running": False}
            self._stop_process(name, process, graceful)
            self.processes.pop(name, None)
            self.logs[name].append(f"{utc_now_iso()} stopped graceful={graceful}")
        self._record_event("stop", name, {"graceful": graceful})
        return {"domain": name, "state": "stopped", "was_running": True}

    def start_coordinator(self) -> dict[str, Any]:
        with self.lock:
            existing = self.processes.get("coordinator")
            if existing is not None and existing.poll() is None:
                return {"state": "running", "pid": existing.pid}
            self._spawn("coordinator", "crossdomain.coordinator",
                        ["--orders-url", self.domain_url("orders"),
                         "--settlement-url", self.domain_url("settlement"),
                         "--port", str(self.ports["coordinator"])])
            ready = self._await_listen("coordinator",
                                       self.coordinator_url + "/health")
        return {"state": "running" if ready else "starting",
                "pid": self.processes["coordinator"].pid}

    def stop_coordinator(self) -> dict[str, Any]:
        process = self.processes.pop("coordinator", None)
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        return {"state": "stopped"}

    def shutdown(self) -> None:
        if getattr(self, "_shutting_down", False):
            return
        self._shutting_down = True
        with self.lock:
            names = list(self.processes)
        for name in names:
            try:
                self.stop_domain(name)
            except Exception:
                pass
        try:
            self.stop_coordinator()
        except Exception:
            pass

    def wait_ready(self, timeout: float = 10.0) -> dict[str, Any]:
        """Wait for every service to answer /health."""
        deadline = time.monotonic() + timeout
        status: dict[str, Any] = {}
        targets = {
            "orders": self.domain_url("orders") + "/health",
            "settlement": self.domain_url("settlement") + "/health",
            "coordinator": self.coordinator_url + "/health",
        }
        for name, url in targets.items():
            status[name] = {"ready": False, "url": url}
        while time.monotonic() < deadline:
            for name, url in targets.items():
                if status[name]["ready"]:
                    continue
                try:
                    _, body, _ = http_json("GET", url, timeout=0.5)
                    if body and body.get("status") in ("ok",):
                        status[name] = {"ready": True, "url": url,
                                        "body": body}
                except UpstreamError:
                    pass
            if all(item["ready"] for item in status.values()):
                break
            time.sleep(0.1)
        return status

    # -- bookkeeping -------------------------------------------------------
    def _record_event(self, kind: str, scope: str, detail: dict[str, Any]) -> None:
        self.events.insert(0, {"at": utc_now_iso(), "kind": kind, "scope": scope,
                               "detail": detail})
        del self.events[100:]

    def store_report(self, report: dict[str, Any]) -> None:
        self.reports.append(report)
        del self.reports[:-MAX_STORED_REPORTS]

    def status(self) -> dict[str, Any]:
        with self.lock:
            domains: dict[str, Any] = {}
            for name in ("orders", "settlement"):
                process = self.processes.get(name)
                running = process is not None and process.poll() is None
                info: dict[str, Any] = {
                    "domain": name,
                    "process": "running" if running else "stopped",
                    "pid": process.pid if running else None,
                    "url": self.domain_url(name),
                }
                if running:
                    try:
                        _, health, _ = http_json("GET",
                                                 self.domain_url(name) + "/health",
                                                 timeout=0.5)
                        info["health"] = health
                    except UpstreamError as exc:
                        info["health"] = {"status": "unreachable",
                                          "message": exc.message}
                domains[name] = info
            coordinator = self.processes.get("coordinator")
            coord_running = coordinator is not None and coordinator.poll() is None
            task_state: dict[str, Any] = {"tasks": []}
            if coord_running:
                try:
                    _, task_state, _ = http_json(
                        "GET", self.coordinator_url + "/api/tasks", timeout=0.5)
                except UpstreamError:
                    pass
            return {
                "at": utc_now_iso(),
                "domains": domains,
                "coordinator": "running" if coord_running else "stopped",
                "tasks": task_state.get("tasks", []),
                "reports": [{"run_id": r.get("run_id"),
                             "mode": r.get("consistency", {}).get("mode"),
                             "batch_id": (r.get("consistency", {})
                                          .get("joined_batch_ids") or [None])[0],
                             "at": r.get("finished_at")}
                            for r in self.reports],
                "events": self.events[:20],
                "ports": self.ports,
            }


class GatewayHandler(JsonRequestHandler):
    """Serves the web UI, report API and demo controls."""

    # -- static ------------------------------------------------------------
    def _serve_static(self, path: str) -> bool:
        if path in ("/", "/index.html"):
            file_path = WEB_DIR / "index.html"
        else:
            relative = path.lstrip("/")
            file_path = (WEB_DIR / relative).resolve()
            if WEB_DIR not in file_path.parents and file_path != WEB_DIR:
                return False
        if not file_path.is_file():
            return False
        content_type = {
            ".html": "text/html; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".svg": "image/svg+xml",
        }.get(file_path.suffix, "application/octet-stream")
        raw = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            pass
        return True

    def do_GET(self) -> None:  # noqa: N802
        path = urllib.parse.urlparse(self.path).path
        try:
            sup: Supervisor = self.server.supervisor  # type: ignore[attr-defined]
            if path == "/api/policy":
                _, body, _ = http_json("GET", sup.coordinator_url + "/api/policy",
                                       timeout=1.0)
                self.send_json(200, body)
                return
            if path == "/api/status":
                self.send_json(200, sup.status())
                return
            if path == "/api/reports":
                self.send_json(200, {"reports": [
                    {"run_id": r.get("run_id"), "mode": r.get("consistency", {}).get("mode"),
                     "batch_id": (r.get("consistency", {}).get("joined_batch_ids")
                                  or [None])[0], "at": r.get("finished_at"),
                     "summary": summarize(r)} for r in sup.reports]})
                return
            if path == "/api/compare":
                left = self.query_one("left")
                right = self.query_one("right")
                self._compare(sup, left, right)
                return
            if path == "/api/logs":
                name = self.query_one("name", "orders") or "orders"
                if name not in ("orders", "settlement", "coordinator"):
                    raise ValueError(f"未知日志名: {name}")
                lines = (sup.logs.get(name) or [])[-40:]
                file_lines: list[str] = []
                log_path = sup.log_dir / f"{name}.log"
                if log_path.is_file():
                    file_lines = log_path.read_text(encoding="utf-8",
                                                    errors="replace").splitlines()[-60:]
                self.send_json(200, {"name": name, "supervisor": lines,
                                     "service_log_tail": file_lines})
                return
            if path.startswith("/api/"):
                self.send_error_json(404, "not_found", "未知 API 路径")
                return
            if not self._serve_static(path):
                self.send_error_json(404, "not_found", f"未找到文件 {path}")
        except ValueError as exc:
            self.send_error_json(400, "bad_request", str(exc))
        except UpstreamError as exc:
            self.send_error_json(503, "upstream_unavailable",
                                 f"协调器不可用: {exc.message}",
                                 hint="可调用 POST /api/control/start-coordinator 恢复")
        except Exception as exc:
            self.send_error_json(500, "internal_error", f"{type(exc).__name__}: {exc}")

    def do_POST(self) -> None:  # noqa: N802
        path = urllib.parse.urlparse(self.path).path
        try:
            sup: Supervisor = self.server.supervisor  # type: ignore[attr-defined]
            body = self.read_json()
            if path == "/api/report":
                self._report(sup, body)
                return
            if path.startswith("/api/control/"):
                self._control(sup, path.rsplit("/", 1)[-1], body)
                return
            self.send_error_json(404, "not_found", "未知 API 路径")
        except UpstreamError as exc:
            self.send_error_json(503, "upstream_unavailable", exc.message)
        except ValueError as exc:
            self.send_error_json(400, "bad_request", str(exc))
        except LookupError as exc:
            self.send_error_json(409, "conflict", str(exc))
        except Exception as exc:
            self.send_error_json(500, "internal_error", f"{type(exc).__name__}: {exc}")

    # -- report ------------------------------------------------------------
    def _report(self, sup: Supervisor, body: dict[str, Any]) -> None:
        payload = {
            "policy": body.get("policy", "aligned_with_explicit_partial"),
            "max_wait_ms": body.get("max_wait_ms", 1500),
        }
        try:
            _, report, _ = http_json("POST", sup.coordinator_url + "/api/report",
                                     payload, timeout=8.0)
        except UpstreamUnavailable as exc:
            # Transport failure: the coordinator process is not answering.
            self.send_json(503, {
                "error": "coordinator_unavailable",
                "message": exc.message,
                "availability_commitment": "协调器不可用时报告端不会读取任一域的最新数据拼接",
                "hint": "启动协调器后重试",
            })
            return
        except UpstreamError as exc:
            # The coordinator answered with a deliberate refusal (e.g. strict
            # policy).  Preserve its status and body instead of masking it.
            self.send_json(exc.status, exc.payload or {
                "error": "coordinator_error", "message": exc.message})
            return
        report.setdefault("run_id", "run-unknown")
        sup.store_report(report)
        sup._record_event("report", report["run_id"],
                          {"mode": report["consistency"]["mode"]})
        fmt = body.get("format", "json")
        if fmt == "markdown":
            raw = to_markdown(report).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/markdown; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return
        self.send_json(200, report)

    # -- control -----------------------------------------------------------
    @staticmethod
    def _domain_name(value: Any) -> str:
        if value not in ("orders", "settlement"):
            raise ValueError(f"未知域: {value!r}（只支持 orders / settlement）")
        return str(value)

    def _control(self, sup: Supervisor, action: str, body: dict[str, Any]) -> None:
        domain = body.get("domain")
        if action == "start":
            self.send_json(200, sup.start_domain(self._domain_name(domain)))
            return
        if action == "stop":
            self.send_json(200, sup.stop_domain(
                self._domain_name(domain),
                graceful=bool(body.get("graceful", True))))
            return
        if action == "restart":
            name = self._domain_name(domain)
            sup.stop_domain(name)
            self.send_json(200, sup.start_domain(name))
            return
        if action == "start-coordinator":
            self.send_json(200, sup.start_coordinator())
            return
        if action == "stop-coordinator":
            self.send_json(200, sup.stop_coordinator())
            return
        if action == "advance":
            target = self._domain_name(domain)
            url = sup.domain_url(target) + "/admin/publish-next"
            confirmed = bool(body.get("confirmed", True))
            _, result, _ = http_json(
                "POST", url, {"batch_id": body.get("batch_id"),
                              "confirmed": confirmed}, timeout=3.0)
            sup._record_event("advance", target,
                              {**result.get("published", {}),
                               "confirmed": confirmed})
            self.send_json(200, result)
            return
        if action == "confirm":
            target = self._domain_name(domain)
            batch_id = body.get("batch_id")
            if not batch_id:
                raise ValueError("batch_id is required")
            url = sup.domain_url(target) + f"/admin/confirm/{batch_id}"
            _, result, _ = http_json("POST", url, {}, timeout=3.0)
            sup._record_event("confirm", target, result.get("confirmed", {}))
            self.send_json(200, result)
            return
        if action == "revise":
            target = self._domain_name(domain)
            batch_id = body.get("batch_id")
            note = body.get("note", "演示修订")
            if not batch_id:
                raise ValueError("batch_id is required")
            url = sup.domain_url(target) + f"/admin/revise/{batch_id}"
            _, result, _ = http_json("POST", url, {"note": note}, timeout=3.0)
            sup._record_event("revise", target, result.get("published", {}))
            self.send_json(200, result)
            return
        if action == "gc":
            target = self._domain_name(domain)
            _, result, _ = http_json("POST", sup.domain_url(target) + "/admin/gc",
                                     {}, timeout=3.0)
            sup._record_event("gc", target, result)
            self.send_json(200, result)
            return
        if action == "hold-task":
            _, result, _ = http_json(
                "POST", sup.coordinator_url + "/api/tasks/hold",
                {"hold_seconds": body.get("hold_seconds", 60),
                 "policy": body.get("policy", "aligned_with_explicit_partial")},
                timeout=8.0)
            sup._record_event("hold-task", result.get("task_id"), result)
            self.send_json(200, result)
            return
        if action == "release-task":
            task_id = body.get("task_id")
            if not task_id:
                raise ValueError("task_id is required")
            _, result, _ = http_json(
                "POST", sup.coordinator_url + f"/api/tasks/{task_id}/release",
                {"stale": bool(body.get("stale", False))}, timeout=5.0)
            sup._record_event("release-task", task_id, result)
            self.send_json(200, result)
            return
        if action == "pins":
            target = self._domain_name(domain)
            url = append_query(sup.domain_url(target) + "/pins",
                               snapshot_id=body.get("snapshot_id"))
            _, result, _ = http_json("GET", url, timeout=1.5)
            self.send_json(200, result)
            return
        raise ValueError(f"未知控制动作: {action}")

    # -- compare -----------------------------------------------------------
    def _compare(self, sup: Supervisor, left_id: str | None,
                 right_id: str | None) -> None:
        by_id = {r["run_id"]: r for r in sup.reports}
        if left_id not in by_id or right_id not in by_id:
            raise ValueError("需要两次已保存的报告 run_id")
        left, right = by_id[left_id], by_id[right_id]
        keys = ["mode", "batch_id", "order_count", "settlement_count",
                "order_total_cents", "settlement_total_cents",
                "completed_orders_settled", "waited_ms"]
        ls, rs = summarize(left), summarize(right)
        diff = []
        for key in keys:
            if ls.get(key) != rs.get(key):
                diff.append({"field": key, "left": ls.get(key), "right": rs.get(key)})
        self.send_json(200, {
            "left": {"run_id": left_id, "summary": ls,
                     "at": left.get("finished_at"),
                     "selection": left.get("selection", {}),
                     "uncovered": left.get("uncovered", {})},
            "right": {"run_id": right_id, "summary": rs,
                      "at": right.get("finished_at"),
                      "selection": right.get("selection", {}),
                      "uncovered": right.get("uncovered", {})},
            "differences": diff,
            "same_batch": ls.get("batch_id") == rs.get("batch_id"),
        })


class GatewayServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], supervisor: Supervisor) -> None:
        super().__init__(address, GatewayHandler)
        self.supervisor = supervisor


def run_gateway(supervisor: Supervisor, host: str = "127.0.0.1",
                port: int = 18080) -> None:
    import signal

    server = GatewayServer((host, port), supervisor)

    def _handle_signal(signum: int, _frame: Any) -> None:
        # A plain SIGTERM must run the cleanup path too; otherwise the demo
        # leaves its child services running.
        raise KeyboardInterrupt

    previous = signal.signal(signal.SIGTERM, _handle_signal)
    print(f"[gateway] listening on http://{host}:{server.server_address[1]}",
          flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        signal.signal(signal.SIGTERM, previous)
        server.server_close()
        supervisor.shutdown()


if __name__ == "__main__":  # pragma: no cover - process entry point
    import argparse
    parser = argparse.ArgumentParser(description="Run the report gateway and web UI")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--orders-port", type=int, default=18081)
    parser.add_argument("--settlement-port", type=int, default=18082)
    parser.add_argument("--coordinator-port", type=int, default=18083)
    parser.add_argument("--supervise", action="store_true",
                        help="start the two domains and coordinator automatically")
    parser.add_argument("--data-dir", default=None,
                        help="domain database directory (default: ./data)")
    options = parser.parse_args()
    ports = {"orders": options.orders_port,
             "settlement": options.settlement_port,
             "coordinator": options.coordinator_port,
             "gateway": options.port}
    env = {"CROSSDOMAIN_DATA_DIR": options.data_dir} if options.data_dir else None
    supervisor = Supervisor(ROOT, ports, env=env)
    if options.supervise:
        supervisor.start_domain("orders")
        supervisor.start_domain("settlement")
        supervisor.start_coordinator()
        print(supervisor.wait_ready(), flush=True)
    run_gateway(supervisor, options.host, options.port)
