# 跨域对账：快照协调与可用性承诺

订单域与结算域各自独立更新、没有共享事务。本仓库实现两个独立数据服务、一个快照协调器和
一个报告查询端（含网页控制台），演示**如何在不拆分事务的前提下，按已确认的业务批次对齐两域**，
并把「等待一致」「退回共同旧版」「返回不完整结果」三种选择的代价显式公开。

## 快速开始

```bash
# 依赖：Python 3.11+（仅标准库；测试用 pytest）
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt

# 一键从空目录跑完整场景（真实 HTTP 请求，四个独立进程）
.venv/bin/python scripts/demo.py --data-dir /tmp/crossdomain-demo --fresh

# 只启动网页控制台（默认 http://127.0.0.1:18080）
.venv/bin/python -m crossdomain.gateway --supervise \
  --data-dir /tmp/crossdomain-demo

# 测试
.venv/bin/python -m pytest tests -q
```

网页控制台可以分别启停订单域/结算域、单边推进批次（含待确认批次）、确认批次、修订快照、
发起查询、恢复服务、比较两次输出，并对协调器长任务租约做旧 token 释放与 GC 演示。

## 架构

```
浏览器 ──> 报告网关 gateway (:18080)         ← 静态页面 + /api/*
              │  控制面：启停子进程（真进程、真端口）
              ├──> 快照协调器 coordinator (:18083)
              │       ├─ 读取两域已发布批次清单
              │       ├─ 选共同已确认批次 → 在两域加租约
              │       ├─ 校验快照 checksum → 按 order_id 关联
              │       └─ 按 lease_token 释放租约
              ├──> 订单域 orders (:18081)      SQLite + 不可变快照
              └──> 结算域 settlement (:18082)  SQLite + 不可变快照
```

四个组件都是可独立停机的 HTTP 服务。域服务把每个批次发布为**不可变快照**
（`batch_id/version`，带 SHA-256 checksum），修订产生新版本而不是覆盖旧版本。
批次可以先以 `confirmed=false` 发布，等上游真正确认后再 `POST /admin/confirm/{batch}`
转为可关联；协调器只把 `confirmed=true` 的快照纳入选版。它只通过 batch_id 关联两域，
绝不把各自最新的不同批次拼在一起。

## 关键设计

### 批次是唯一关联键

两域数据由同一组虚构业务批次（`BIZ-20261101` … `BIZ-20261106`）生成，`order_id`
在结算记录中回指订单。协调器在共同集合 `orders.confirmed ∩ settlement.confirmed`
中取序号最大的批次，两侧必须同 `batch_id`；不满足就整体降级，而不是局部拼接。

### 三种不一致后果，显式选择其一

| 模式 | 触发条件 | 返回 | 影响 |
|---|---|---|---|
| `aligned` | 两域最新已确认批次相同 | 完整关联结果 | 无 |
| `degraded_stale` | 最新批次不同，但存在更早的共同已确认批次 | 退回共同旧版，`freshness.lag_batches` 公开滞后批次数 | 数据较旧，但不撕裂 |
| `partial` | 任一域不可达 / 无共同已确认批次 | HTTP 200，`records` 为空，`uncovered` 逐项列出原因 | 明确“不完整”，不伪造合计 |
| `strict`（opt-in） | 等待窗口结束后仍非 aligned | HTTP 503 `strict_failed` | 拒绝降级，由调用方重试 |

默认策略 `aligned_with_explicit_partial` 的完整承诺由 `GET /api/policy` 公开：

- **延迟**：每次查询轮询等待不超过 `max_wait_ms`（默认 1500ms，硬上限 5000ms），
  另加本地读取/校验开销（通常 <10ms）；不会无限期等两域对齐。
- **一致性**：只关联两域都已确认且 `batch_id` 相同的快照。
- **降级**：最新批次无法对齐时退回共同旧版并公开滞后；绝不混批。
- **不完整**：某域不可达时返回 `partial`，`records` 为空，未覆盖范围逐项列出。
  协调器恢复后重新查询即可。
- **strict**：任何非对齐直接 503。
- **租约**：读取前在两域对选定快照加租约，读取后按 `lease_token` 释放；
  旧任务/过期 token 的释放返回 409，GC 永不删除仍有活跃租约的快照。

### 租约与垃圾回收

- 快照不可变，修订产生 v2/v3…；GC 只删除被更高版本取代的旧快照。
- 协调器读取前调用 `POST /pins` 获取租约（`pin_id` + `lease_token` + TTL），
  读取完成在 `finally` 中释放，报告里公开 `leases` 与 `lease_release` 结果。
- 释放必须携带匹配的 `lease_token`；旧协调任务用过期 token 释放会得到
  `409 stale_lease_token`，不会误删仍在使用的快照租约。
- `POST /admin/gc` 返回 `deleted_snapshots` 与 `retained_pinned_snapshots`。

### 未覆盖范围

每次报告都包含 `uncovered`：

- `unreachable_domains`：本次不可达的域；
- `unpaired_batches`：仅单域已发布、对端没有的批次；
- `pending_batches`：各域尚未发布的批次；
- `excluded_versions`：被排除的版本及原因码（`superseded` 更早的共同批次、
  `peer_missing` 对端未发布、`unconfirmed_here` 本域未确认、`unconfirmed_peer` 对端未确认）；
- `orders_without_settlement`：已完成但无结算记录的订单（含示例 ID 与截断标记）；
- `batch_gap`：选定批次之后、已发布但本次未覆盖的批次。

## HTTP API 摘要

| 组件 | 方法/路径 | 说明 |
|---|---|---|
| 域 | `GET /health` | 已发布/待发布批次数、活跃租约 |
| 域 | `GET /batches?include_pending=1` | 批次清单与当前快照 |
| 域 | `GET /snapshots/{batch}/{version}` | 不可变快照 + 记录 |
| 域 | `POST /admin/publish-next` | 发布下一批（可指定 batch_id、`confirmed=false`） |
| 域 | `POST /admin/confirm/{batch}` | 将批次最新版本标记为已确认，使其可参与跨域关联 |
| 域 | `POST /admin/revise/{batch}` | 产生新版本（继承确认状态） |
| 域 | `POST /pins`、`POST /pins/{id}/release`、`POST /admin/gc` | 租约与 GC |
| 协调器 | `POST /api/report` | 选版、加租约、校验、关联、释放 |
| 协调器 | `GET /api/policy` | 公开可用性承诺 |
| 协调器 | `POST /api/tasks/hold`、`POST /api/tasks/{id}/release` | 长任务租约演示 |
| 网关 | `/`、`/api/status`、`/api/report`、`/api/reports`、`/api/compare` | 网页与报告端 |

## 虚构数据

`crossdomain/seed.py` 用固定种子生成 6 个业务批次，覆盖订单、结算、地区、渠道、支付方式等字段。
结算域按 86% 的概率结算已完成订单，并保证每批至少有一笔“已完成但未结算”，
因此未覆盖范围在每次运行中都可见、可断言。数据只写入 `data/seed/*.json`（首次）与各域 SQLite。

## 已知边界

- 协调器对两域批次清单的读取不是原子快照；它用「共同已确认 batch_id」来消除这种竞态，
  极端情况下只会多等一轮或退回更旧批次，不会混批。
- 租约 TTL 到期会自动失效（域服务侧惰性清扫）。长任务需要在 TTL 内续租
  （域服务提供 `POST /pins/{id}/renew`），演示中的持有任务 TTL 为 `hold_seconds + 30`。
- 网页控制面通过 gateway 启动子进程，仅适合单机演示；生产部署应把启停交给编排系统。
- 数据为虚构演示数据，不代表任何真实业务口径。
