"""Pure formatting helpers for the report gateway and CLI.

Keeping rendering separate makes the same numbers testable without HTTP.
"""

from __future__ import annotations

import html
from typing import Any


def _money(cents: Any) -> str:
    try:
        return f"¥{int(cents) / 100:,.2f}"
    except (TypeError, ValueError):
        return "—"


def summarize(report: dict[str, Any]) -> dict[str, Any]:
    """Derive the headline numbers shown at the top of the report."""
    orders = report.get("records", {}).get("orders", [])
    settlements = report.get("records", {}).get("settlement", [])
    consistency = report.get("consistency", {})
    joined = bool(consistency.get("joined"))
    settled_ids = {s.get("order_id") for s in settlements}
    completed = [o for o in orders if o.get("status") == "completed"]
    return {
        "mode": consistency.get("mode", "unknown"),
        "joined": joined,
        "batch_id": (consistency.get("joined_batch_ids") or [None])[0],
        "order_count": len(orders),
        "order_total_cents": sum(int(o.get("amount_cents", 0)) for o in orders),
        "settlement_count": len(settlements),
        "settlement_total_cents": sum(int(s.get("amount_cents", 0))
                                      for s in settlements),
        "completed_order_count": len(completed),
        "completed_orders_settled": sum(1 for o in completed
                                        if o["order_id"] in settled_ids),
        "waited_ms": report.get("waited_ms"),
    }


def to_markdown(report: dict[str, Any]) -> str:
    s = summarize(report)
    consistency = report.get("consistency", {})
    lines: list[str] = []
    lines.append("# 跨域对账报告")
    lines.append("")
    lines.append(f"- 运行编号: `{report.get('run_id')}`")
    lines.append(f"- 策略: `{report.get('policy')}`")
    lines.append(f"- 一致性模式: **{s['mode']}** — {consistency.get('reason', '')}")
    lines.append(f"- 选定共同批次: `{s['batch_id'] or '无'}`")
    lines.append(f"- 等待耗时: {s['waited_ms']} ms (上限 {report.get('max_wait_ms')} ms)")
    lines.append("")
    lines.append("## 选版依据")
    lines.append("")
    lines.append("| 域 | 选定批次 | 版本 | 是否该域最新 | 滞后批次 | 校验和 | 记录数 | 金额 |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for name, sel in sorted((report.get("selection") or {}).items()):
        lines.append(
            f"| {name} | {sel['batch_id']} | v{sel['version']} | "
            f"{'是' if sel['is_domain_latest'] else '否'} | {sel['lag_batches']} | "
            f"`{sel['checksum'][:19]}…` | {sel['record_count']} | "
            f"{_money(sel['total_amount_cents'])} |")
    if not report.get("selection"):
        lines.append("| — | 未形成跨域共同批次 | — | — | — | — | — | — |")
    lines.append("")
    lines.append("## 各域可用性")
    lines.append("")
    for name, domain in sorted((report.get("domains") or {}).items()):
        if domain.get("reachable"):
            lines.append(
                f"- **{name}**: 可达；最新批次 `{domain.get('latest_batch_id')}`；"
                f"{'参与关联' if domain.get('selected') else '未参与关联'}；"
                f"校验和{'通过' if domain.get('checksum_verified') else '未验证/失败'}")
        else:
            lines.append(f"- **{name}**: 不可达 — {domain.get('error')}")
    lines.append("")
    uncovered = report.get("uncovered", {})
    lines.append("## 未覆盖范围")
    lines.append("")
    lines.append(f"- 不可达域: {', '.join(uncovered.get('unreachable_domains') or []) or '无'}")
    missing = uncovered.get("orders_without_settlement") or {}
    lines.append(f"- 已拣货/完成但无结算的订单: {missing.get('count', 0)}"
                 + (f"（示例: {', '.join(missing.get('order_ids', [])[:5])}）"
                    if missing.get("order_ids") else ""))
    gap = uncovered.get("batch_gap") or {}
    if gap:
        lines.append(f"- 选定批次之后已发布但未覆盖的批次: "
                     f"{gap.get('count', 0)} 个 {gap.get('later_published_batches', [])}")
    for name, batches in sorted((uncovered.get("pending_batches") or {}).items()):
        lines.append(f"- {name} 尚未发布批次: {', '.join(batches) or '无'}")
    for name, batches in sorted((uncovered.get("unpaired_batches") or {}).items()):
        lines.append(f"- {name} 独有、对端未发布的批次: {', '.join(batches) or '无'}")
    for name, items in sorted((uncovered.get("excluded_versions") or {}).items()):
        detail = "；".join(f"{item['batch_id']}（{item['reason']}）"
                          for item in items)
        lines.append(f"- {name} 被排除的版本: {detail}")
    unconfirmed = uncovered.get("unconfirmed_latest") or []
    if unconfirmed:
        lines.append("- 最新未确认快照: " + ", ".join(
            f"{item['domain']}/{item['batch_id']}" for item in unconfirmed))
    lines.append("")
    lines.append("## 记录汇总")
    lines.append("")
    lines.append(f"- 订单: {s['order_count']} 条，合计 {_money(s['order_total_cents'])}")
    lines.append(f"- 结算: {s['settlement_count']} 条，合计 "
                 f"{_money(s['settlement_total_cents'])}")
    lines.append(f"- 已完成订单中已结算: {s['completed_orders_settled']}"
                 f"/{s['completed_order_count']}")
    lines.append("")
    return "\n".join(lines)


def to_html_fragment(report: dict[str, Any]) -> str:
    """Used by tests to assert escaping of values coming from the domains."""
    s = summarize(report)
    rows = "".join(
        f"<tr><td>{html.escape(name)}</td><td>{html.escape(str(sel['batch_id']))}</td>"
        f"<td>{sel['record_count']}</td><td>{_money(sel['total_amount_cents'])}</td></tr>"
        for name, sel in sorted((report.get("selection") or {}).items()))
    return (f"<section data-mode=\"{html.escape(s['mode'])}\">"
            f"<h1>跨域对账报告</h1><table>{rows}</table></section>")
