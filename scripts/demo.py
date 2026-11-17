#!/usr/bin/env python3
"""Full demo from an empty directory with real HTTP requests.

Starts both domain services, the coordinator and the gateway as separate OS
processes, then drives the scenario through the gateway's public HTTP API:

  1. aligned query
  2. stop the settlement domain (orders keeps accepting work)
  3. advance orders while settlement is down
  4. query -> partial + uncovered report
  5. restart settlement, query -> degraded_stale with batch lag
  6. hold a coordinator lease, try an old-token release, run GC
  7. print the selection rationale and uncovered scope for every run

Usage:
    .venv/bin/python scripts/demo.py --data-dir /tmp/crossdomain-demo --port 18090
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def request(method: str, url: str, payload: dict | None = None,
            timeout: float = 10.0) -> tuple[int, dict]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


def wait_for(url: str, label: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status, body = request("GET", url, timeout=0.5)
            if status == 200:
                print(f"  ✓ {label} 就绪")
                return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(0.15)
    raise SystemExit(f"{label} 未在 {timeout}s 内就绪: {url}")


def show_report(tag: str, report: dict) -> None:
    consistency = report.get("consistency", {})
    print(f"\n[{tag}] run={report.get('run_id')} "
          f"mode={consistency.get('mode')} "
          f"joined={consistency.get('joined')} "
          f"waited={report.get('waited_ms')}ms")
    print(f"  选版依据: {consistency.get('reason')}")
    for name, sel in sorted((report.get("selection") or {}).items()):
        print(f"    {name:11s} batch={sel['batch_id']} v{sel['version']} "
              f"records={sel['record_count']} 最新={sel['is_domain_latest']} "
              f"滞后={sel['lag_batches']} checksum={sel['checksum'][:21]}…")
    if not report.get("selection"):
        print("    （未形成跨域共同批次；没有读取任何单域最新版拼接）")
    domains = report.get("domains") or {}
    for name, domain in sorted(domains.items()):
        print(f"    域 {name:11s} 可达={domain.get('reachable')} "
              f"选中={domain.get('selected')} "
              f"最新批次={domain.get('latest_batch_id')}")
    records = report.get("records") or {}
    print(f"  记录: orders={len(records.get('orders', []))} "
          f"settlement={len(records.get('settlement', []))}")
    uncovered = report.get("uncovered") or {}
    print(f"  未覆盖: 不可达域={uncovered.get('unreachable_domains')} "
          f"待发布={uncovered.get('pending_batches')}")
    if uncovered.get("unpaired_batches"):
        print(f"          仅单域存在的批次={uncovered['unpaired_batches']}")
    missing = uncovered.get("orders_without_settlement") or {}
    print(f"          已完成未结算订单={missing.get('count')} "
          f"{missing.get('order_ids', [])}")
    if uncovered.get("batch_gap"):
        print(f"          选定批次之后未覆盖的已发布批次="
              f"{uncovered['batch_gap']['later_published_batches']}")
    release = report.get("lease_release") or []
    if release:
        print(f"  租约释放: " + ", ".join(
            f"{item['domain']}={'ok' if item['released'] else 'FAIL'}"
            for item in release))


def main() -> int:
    parser = argparse.ArgumentParser(description="跨域对账完整演示")
    parser.add_argument("--data-dir", default="/tmp/crossdomain-demo")
    parser.add_argument("--port", type=int, default=18090)
    parser.add_argument("--fresh", action="store_true",
                        help="删除数据目录后从空目录开始")
    parser.add_argument("--keep-running", action="store_true",
                        help="演示结束后保持网页运行，直到 Ctrl-C")
    options = parser.parse_args()

    data_dir = Path(options.data_dir)
    if options.fresh and data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    ports = {"orders": options.port + 1, "settlement": options.port + 2,
             "coordinator": options.port + 3, "gateway": options.port}
    env = dict(os.environ, PYTHONPATH=str(ROOT),
               CROSSDOMAIN_DATA_DIR=str(data_dir))
    log = (data_dir / "demo-gateway.log").open("w", encoding="utf-8")
    print(f"从空目录启动四个独立进程: {data_dir}")
    gateway = subprocess.Popen(
        [sys.executable, "-u", "-m", "crossdomain.gateway",
         "--supervise",
         "--data-dir", str(data_dir),
         "--port", str(ports["gateway"]),
         "--orders-port", str(ports["orders"]),
         "--settlement-port", str(ports["settlement"]),
         "--coordinator-port", str(ports["coordinator"])],
        cwd=str(ROOT), env=env, stdout=log, stderr=subprocess.STDOUT,
        start_new_session=True)

    base = f"http://127.0.0.1:{ports['gateway']}"
    try:
        wait_for(f"{base}/api/status", "报告网关")
        wait_for(f"{base}/api/policy", "协调器")
        _, status = request("GET", f"{base}/api/status")
        for name, domain in status["domains"].items():
            assert domain["process"] == "running", domain
            print(f"  ✓ {name} 域进程运行中，已发布 "
                  f"{domain['health']['published_batches']} 批")

        print("\n=== 1. 两域同步时的查询 ===")
        _, aligned = request("POST", f"{base}/api/report",
                             {"max_wait_ms": 800})
        show_report("aligned", aligned)

        print("\n=== 2. 让结算域停机，订单域继续推进 ===")
        request("POST", f"{base}/api/control/stop", {"domain": "settlement"})
        print("  结算域已停机")
        status_code, advanced = request(
            "POST", f"{base}/api/control/advance", {"domain": "orders"})
        print(f"  订单域推进批次 -> HTTP {status_code} "
              f"{advanced.get('published', {}).get('batch_id')}")
        time.sleep(0.3)
        _, partial = request("POST", f"{base}/api/report",
                             {"max_wait_ms": 1000})
        show_report("partial", partial)
        assert partial["consistency"]["mode"] == "partial"
        assert partial["consistency"]["joined"] is False

        print("\n=== 3. 恢复结算域后查询（退回共同旧版，不拼接最新版） ===")
        request("POST", f"{base}/api/control/start", {"domain": "settlement"})
        wait_for(f"{base}/api/status", "网关状态")
        for _ in range(50):
            _, status = request("GET", f"{base}/api/status")
            if status["domains"]["settlement"]["process"] == "running":
                break
            time.sleep(0.1)
        _, degraded = request("POST", f"{base}/api/report", {"max_wait_ms": 800})
        show_report("degraded_stale", degraded)
        assert degraded["consistency"]["mode"] == "degraded_stale"
        sel = degraded["selection"]
        assert sel["orders"]["batch_id"] == sel["settlement"]["batch_id"]

        print("\n=== 3b. strict 策略：同一不对齐状态下拒绝降级 ===")
        code, strict = request("POST", f"{base}/api/report",
                               {"max_wait_ms": 0, "policy": "strict"})
        print(f"  HTTP {code} -> {strict.get('error')}: {strict.get('message')}")
        print(f"  chosen_batch={strict.get('chosen_batch')} "
              f"lag={strict.get('lag_batches')}")
        assert code == 503

        print("\n=== 3c. 等待两域确认同一批次（待确认 -> 确认后对齐） ===")
        target_batch = "BIZ-20261106"
        # Settlement still owes BIZ-20261105; catch it up first, then both
        # domains publish the target batch *unconfirmed*.
        status_code, caught_up = request(
            "POST", f"{base}/api/control/advance", {"domain": "settlement"})
        print(f"  settlement 补齐 -> {caught_up.get('published', {}).get('batch_id')}")
        for name in ("orders", "settlement"):
            status_code, published = request(
                "POST", f"{base}/api/control/advance",
                {"domain": name, "batch_id": target_batch, "confirmed": False})
            print(f"  {name} 发布 {target_batch} confirmed=False -> HTTP {status_code}")
            assert status_code == 200
        _, waiting = request("POST", f"{base}/api/report", {"max_wait_ms": 0})
        print(f"  未确认时 -> mode={waiting['consistency']['mode']} "
              f"batch={waiting['consistency']['joined_batch_ids']} "
              f"unconfirmed={[x['batch_id'] for x in waiting['uncovered']['unconfirmed_latest']]}")
        assert target_batch not in waiting["consistency"]["joined_batch_ids"]
        import threading

        def confirm_both() -> None:
            time.sleep(0.4)
            for name in ("orders", "settlement"):
                request("POST", f"{base}/api/control/confirm",
                        {"domain": name, "batch_id": target_batch})

        thread = threading.Thread(target=confirm_both)
        thread.start()
        _, aligned_new = request("POST", f"{base}/api/report",
                                 {"max_wait_ms": 2500})
        thread.join()
        print(f"  确认后 -> mode={aligned_new['consistency']['mode']} "
              f"batch={aligned_new['consistency']['joined_batch_ids']} "
              f"attempts={aligned_new['attempts']} "
              f"waited={aligned_new['waited_ms']}ms")
        assert aligned_new["consistency"]["mode"] == "aligned"
        assert aligned_new["consistency"]["joined_batch_ids"] == [target_batch]

        print("\n=== 5. 长任务租约 + 旧协调任务释放 + GC ===")
        _, task = request("POST", f"{base}/api/control/hold-task",
                          {"hold_seconds": 120})
        print(f"  持有任务 {task['task_id']}，批次 {task['batch_id']}，"
              f"快照 {[p['snapshot_id'] for p in task['pins'].values()]}")
        # Revise the held batch so the pinned version becomes a GC candidate.
        for name in ("orders", "settlement"):
            request("POST", f"{base}/api/control/revise",
                    {"domain": name, "batch_id": task["batch_id"],
                     "note": "演示修订"})
        print(f"  已在两域修订 {task['batch_id']}，产生 v2")
        _, stale = request("POST", f"{base}/api/control/release-task",
                           {"task_id": task["task_id"], "stale": True})
        refused = all(not item["released"] for item in stale["results"])
        print(f"  旧 token 释放: {'全部被拒绝（符合预期）' if refused else '意外成功'}")
        assert refused
        for name in ("orders", "settlement"):
            _, gc = request("POST", f"{base}/api/control/gc", {"domain": name})
            print(f"  GC {name}: 删除 {gc['deleted_snapshots']} "
                  f"保留受租约保护 {gc['retained_pinned_snapshots']}")
            assert gc["retained_pinned_snapshots"], "pinned snapshot was collected"
        _, task_release = request("POST", f"{base}/api/control/release-task",
                                  {"task_id": task["task_id"]})
        print(f"  正常释放: "
              f"{[item['released'] for item in task_release['results']]}")
        for name in ("orders", "settlement"):
            _, gc = request("POST", f"{base}/api/control/gc", {"domain": name})
            print(f"  释放后 GC {name}: 删除 {gc['deleted_snapshots']} "
                  f"保留 {gc['retained_pinned_snapshots']}")

        print("\n=== 6. 比较两次查询输出 ===")
        _, reports = request("GET", f"{base}/api/reports")
        ids = [r["run_id"] for r in reports["reports"]]
        left, right = ids[0], ids[-1]
        _, comparison = request(
            "GET", f"{base}/api/compare?left={left}&right={right}")
        print(f"  同批次={comparison['same_batch']}")
        for diff in comparison["differences"]:
            print(f"    {diff['field']}: {diff['left']!r} -> {diff['right']!r}")

        print("\n=== 7. Markdown 报告（节选） ===")
        req = urllib.request.Request(
            f"{base}/api/report",
            data=json.dumps({"max_wait_ms": 500,
                             "format": "markdown"}).encode(),
            method="POST",
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as response:
            markdown = response.read().decode()
        print("\n".join(markdown.splitlines()[:22]))

        print("\n演示完成。")
        if options.keep_running:
            print("网页仍运行在 " + base + "（Ctrl-C 结束）")
            gateway.wait()
        else:
            print("加 --keep-running 可保持网页运行；当前直接关闭演示服务。")
    except KeyboardInterrupt:
        print("\n结束演示")
    finally:
        gateway.send_signal(signal.SIGTERM)
        try:
            gateway.wait(timeout=10)
        except subprocess.TimeoutExpired:
            gateway.kill()
        log.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
