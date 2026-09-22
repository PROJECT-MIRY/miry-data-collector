# Liveness 取消传播与未关闭 gap（2026-09-22）

## 状态

代码修复，尚未发布或部署。本次没有访问 107，没有删除、关闭或回写任何历史 gap。

## 生产证据

2026-09-22 04:59 UTC 只读核查时，Vultr 运行 v0.5.11，存在 28 条未关闭的
`CONNECTION_LOST_GAP`。它们首次登记于 09-15 18:55:53--54 UTC，来自 public-1 上
14 个 symbol 的 depth/bookTicker liveness 超时。同期 journal 记录 route 重连、transport
恢复与 snapshot bridge 成功，但这些币级 gap 此后持续跨日延续。

最近 raw 抽样中，仍在 active universe 的 12 个 symbol 两种 stream 都有新事件；HOME、SEI
已经退出名单，对应 gap 仍存在。因此不能把 28 条 OPEN 简单解释为六天连续没有 raw。
这些观测证明账本与采集状态不一致；历史每段实际完整性仍需单独重建验证。

证据入口：Vultr `/srv/miry-data-rsync/control/open-gaps/`、各日 `SEALED.json`/day-index，
以及 `journalctl -u miry-data-collector.service` 的 09-15 18:55--18:57 UTC 窗口。
历史进程任务栈未保留，因此下面的可重复代码故障是与事故吻合的机制，不是对现场任务栈的读回。

## 已复现的故障链

1. `RouteRunner.liveness_loop` 打开 scoped gap，并经 `_submit_update` 等待
   started、acknowledged、completion 三个共享 future。
2. WebSocket 断开时，连接清理会取消 controller 的 `run_updates`。
3. `_fail_subscription_update`，以及 snapshot recovery 的 completion 路径，会将
   `CancelledError` 放进共享 future，或取消对应 future。
4. 等待方收到 `CancelledError`，即使 liveness task 本身从未被请求取消，也会退出。
5. `asyncio.TaskGroup` 不把已取消的子任务视为普通故障。其他 source 继续采集，
   liveness 的局部 `active_gaps` 再无人协调，持久 OPEN 在午夜继续续记。

源码：[routes.py](../src/miry/collector/routes.py)、
[ws_control.py](../src/miry/collector/ws_control.py)、
[websocket.py](../src/miry/collector/websocket.py)、
[sources.py](../src/miry/collector/sources.py)。

## 修复

在 `_submit_update` 的等待边界检查当前 task 的取消请求：

- task 本身正在取消：原样传播 `CancelledError`，保证服务关闭正常。
- 只有共享 future 被连接侧取消：抛出可恢复的 `ConnectionError`，交给现有
  liveness retry/reconciliation 路径。连接重建不再取消 route 级监控。
- queue admission 与等待 started 共用已有 180 秒 deadline，避免队列满且没有
  consumer 时无限等待。ACK 和 completion 保持各自原有 deadline。

不增加恢复线程、不扩大缓冲、不改变超时策略数值，不修改行情热路径。
恢复或移除 stream 后，由原有协调逻辑关闭 scoped gap；没有新事件时保持 OPEN。
独立的 `L2_REANCHOR_GAP`/`L2_SEQUENCE_GAP` 仍受 snapshot bridge 证明约束。

## 回归验证

[test_liveness_recovery.py](../tests/test_liveness_recovery.py) 使用实际
`liveness_loop -> _submit_update -> SubscriptionController.run_updates`，以及实际
WebSocket snapshot completion 代码；网络请求用本地可控对象代替。

- 在锁、ACK、snapshot 三阶段取消连接，监控仍必须存活。
- 恢复新事件、移除成员、继续静默三种结果分别验证 gap 关闭或保留。
- 直接取消任一共享 future 必须成为可恢复失败。
- 真正取消 liveness task 仍应及时退出，不能关闭未证明恢复的 gap。
- 队列满且无 consumer 时必须超时，不能永久阻塞。

原始代码在九种连接取消组合上均失败；修复后十四项回归均通过。
完整回归为 `236 passed`；Ruff 通过，mypy 对 60 个源码文件检查通过。

## 历史记录与部署边界

这项修改阻止新运行中的取消泄漏，不凭空恢复线上已经退出的任务，也不回写旧 gap。
既有 28 条记录的真实恢复时间必须结合当时 raw、connection、snapshot bridge、sequence
与 membership 证据确定。不能删除 OPEN 文件制造健康状态，也不能把当前时间当历史恢复点。
未来升级前需设计并验证历史质量账本的修正方式，保留原始事实及修正依据。
