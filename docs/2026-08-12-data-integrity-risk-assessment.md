# 数据完整性风险与 v0.3.1 修复评估（2026-08-12）

## 结论摘要

本报告检查 `v0.3.0` 与本地待发布的 `v0.3.1` 代码，范围是单 stream 静默、日覆盖率、
writer durability/事件循环阻塞和事件去重。跨日 L2 checkpoint 另见
[跨日 L2 重建评估](2026-08-12-cross-day-l2-reconstruction-assessment.md)。

| 风险 | v0.3.0 结论 | v0.3.1 处理 |
| --- | --- | --- |
| 单个 stream 静默停止未被识别 | **真实存在，P0** | `(stream, symbol)` 独立 liveness；public 30 秒、`markPrice@1s` 5 秒；每 60 秒审计服务端订阅集合，审计 ACK 自身也有 deadline。刷新后必须看到对应 stream 新事件才关闭 gap。 |
| `_PROCESSED` 缺少覆盖率门槛 | **真实存在，P1** | raw 中的 universe decision 绑定权威 60 币；每币要求 `valid_ratio >= 99.9%`、`accounted_ratio == 100%`、`unclassified_ns == 0`、`conflicting_ns == 0`，否则只写 `_QUALITY_REJECTED.json`。 |
| “writer durability 测试阻塞” | 测试没有挂死，但同步 `fsync` 阻塞 event loop 和异常退出无 gap 均为**真实 P1** | chunk/day-index/gap/formal-start 阻塞 I/O 移入 worker thread；gap artifact 与索引在同一锁内登记；持久化 collector lease 让 SIGKILL/OOM/重启在下次启动生成 recovered `COLLECTOR_STOPPED_GAP`。 |
| 部分事件未去重 | **真实存在，P1/P2** | 补齐 market overlap 精确 replay identity，depth/book ID 冲突 fail closed；去重状态限定 10 分钟并跨 UTC 日 checkpoint；D0 排除 `is_duplicate=true`。 |

这些结论均已用最小失败用例复现，再由 v0.3.1 回归测试锁定。跨日 L2 checkpoint 修复另见
[跨日 L2 重建评估](2026-08-12-cross-day-l2-reconstruction-assessment.md)。`forceOrder` 每秒最多提供
最新一笔的源端语义无法由本项目修复，因此数据集不得把它解释为完整逐笔强平日志。

## v0.3.1 最终参数和验证

- public `depth`/`bookTicker` liveness：30 秒；`markPrice@1s`：5 秒；订阅集合审计：60 秒，
  审计响应 deadline：10 秒；
- 日切 seal grace：90 秒，使前一日 affected gap 有时间进入 sealed inventory；
- collector lease heartbeat：30 秒；recovered gap 从 depth 与 trades/market 两组共同 durable watermark 起算；
- finalize：正式首日使用 formal-start partial window，其余日要求前一日 L2 checkpoint；操作员传入的
  symbol 文件必须恰好 60 个且与 raw universe 完全一致；
- normalize 去重窗口：10 分钟，checkpoint 跨午夜继承；raw 和 typed 仍保留重复证据，仅汇总时排除；
- 本地最终验证：`90 passed`，`ruff`、`mypy`、部署 shell 语法检查全部通过。

## 资料与方法

协议语义以 Binance USDⓈ-M Futures 官方文档和 Binance 官方 Python connector 为准；异步和
持久化语义以 Python 与 Linux 官方文档为准。Binance 文档在命令行请求时可能返回 WAF challenge，
因此同时引用了官方 connector 的固定 commit，避免把第三方解释当成协议事实：

- Binance 官方 connector 固定版本：
  [`a6bfbbf`](https://github.com/binance/binance-futures-connector-python/blob/a6bfbbf10fe2c1b4eb76fc24ffb82eb94bf9df89/binance/websocket/um_futures/websocket_client.py#L34-L270)
- Binance 官方 WebSocket 连接说明：
  [Connect](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Connect)
- Python 官方文档：
  [`asyncio.to_thread`](https://docs.python.org/3/library/asyncio-task.html#asyncio.to_thread)、
  [运行阻塞代码](https://docs.python.org/3/library/asyncio-dev.html#running-blocking-code)、
  [`os.fsync`](https://docs.python.org/3/library/os.html#os.fsync)
- Linux `fsync(2)`：
  [Linux man-pages](https://man7.org/linux/man-pages/man2/fsync.2.html)

代码判断基于：

- [edge/sources.py](../src/miry/collector/routes.py)
- [edge/writer.py](../src/miry/collector/writer.py)
- [edge/day_index.py](../src/miry/collector/day_index.py)
- [edge/gaps.py](../src/miry/collector/gaps.py)
- [edge/spool.py](../src/miry/collector/spool.py)
- [central/normalize.py](../src/miry/pipeline/normalize.py)
- [central/binance.py](../src/miry/pipeline/parsing.py)
- [central/process_cli.py](../src/miry/cli/process.py)

## 1. 单 stream 静默停止

### 1.1 官方推送语义

不能把所有 stream 都按固定周期检测。正式配置中的流可分为三类：

| stream | Binance 官方语义 | 能否以“长时间无事件”判故障 |
| --- | --- | --- |
| `markPrice@1s` | 每个 symbol 每秒推送 mark price 与 funding rate；官方 connector 明确写为 every second，update speed 1000ms。[文档](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream) / [固定源码](https://github.com/binance/binance-futures-connector-python/blob/a6bfbbf10fe2c1b4eb76fc24ffb82eb94bf9df89/binance/websocket/um_futures/websocket_client.py#L50-L63) | **可以**，它是最强的逐 symbol 周期性 liveness 信号。 |
| `depth@100ms` | 订单簿价格/数量更新，100ms 是 update speed。[文档](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams) / [固定源码](https://github.com/binance/binance-futures-connector-python/blob/a6bfbbf10fe2c1b4eb76fc24ffb82eb94bf9df89/binance/websocket/um_futures/websocket_client.py#L221-L234) | **协议上不能当作每 100ms 心跳**；没有订单簿变化时可以安静。但正式 60 币是流动性筛选后的合约，可使用较长的保守超时并把误报记为显式无效，而不能声称 Binance 保证了周期事件。 |
| `bookTicker` | 仅在最优 bid/ask 的价格或数量更新时实时推送。[文档](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams) / [固定源码](https://github.com/binance/binance-futures-connector-python/blob/a6bfbbf10fe2c1b4eb76fc24ffb82eb94bf9df89/binance/websocket/um_futures/websocket_client.py#L202-L219) | **不能使用很短的硬超时**；无 BBO 变化是合法情况。 |
| `aggTrade` | 仅有市场成交才推送，并按单个 taker order 在 100ms 内聚合。[文档](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams) / [固定源码](https://github.com/binance/binance-futures-connector-python/blob/a6bfbbf10fe2c1b4eb76fc24ffb82eb94bf9df89/binance/websocket/um_futures/websocket_client.py#L34-L48) | **不能**；无成交可以合法安静。 |
| `forceOrder` | 每个 symbol 每 1000ms 最多推送这一窗口内最新的一笔强平；若窗口内没有强平则完全不推送。[文档](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams) / [固定源码](https://github.com/binance/binance-futures-connector-python/blob/a6bfbbf10fe2c1b4eb76fc24ffb82eb94bf9df89/binance/websocket/um_futures/websocket_client.py#L253-L270) | **绝对不能**。而且该 feed 本身是 snapshot/sampling 语义，不是完整逐笔强平日志。 |
| `!contractInfo` | 只在 contract info 更新时推送。[文档](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Contract-Info-Stream) | **不能**；正常情况下可能长时间无变化。 |
| `openInterest`、clock、universe REST | 由本项目定时发起 REST 请求，不是事件驱动 stream。 | 可按本地 poll deadline 判断；当前代码已经在请求失败时打开 scoped gap。 |

WebSocket ping/pong 和 receive timeout 只能说明**连接**仍可通信，不能证明组合连接中的每个订阅仍在
交付数据。Binance 的 live subscription API 支持查询当前订阅列表，但这也只证明控制面仍认为订阅存在，
不证明数据面没有丢事件：
[Live Subscribing/Unsubscribing](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams)。

### 1.2 v0.3.0 为什么会漏报

`RouteRunner._last_event` 只以 `symbol` 为 key，`_mark_event(stream_type, symbol)` 忽略
`stream_type`。因此同一 public connection 上：

```text
depth 静默 + bookTicker 继续到达 -> symbol 时间戳持续更新 -> 不刷新、不写 gap
bookTicker 静默 + depth 继续到达 -> 同样漏报
```

market route 同时承载 `aggTrade`、`markPrice@1s`、`forceOrder` 和全局 `contractInfo`，但没有启动
`liveness_loop`。只要其他 symbol 的 mark price 仍在到达，连接级 30 秒 receive timeout 也不会发现某个
symbol 的 `markPrice` 静默。以上均指 v0.3.0 基线。

L2 的 `pu` 连续性只能在 depth **恢复后**证明中间丢过更新，不能及时打开 gap；若 stream 一直不恢复，
缺口会一直保持未登记。这确认了 P0。

### 1.3 v0.3.1 liveness 契约

1. 活性状态必须至少以 `(route, symbol, stream_type)` 为 key，不能只以 symbol 为 key。
2. `markPrice@1s` 采用 5 秒硬 deadline（5 个周期），超时立即打开只覆盖该
   symbol/stream 的 gap，并刷新订阅；阈值应扣除或同时记录 event-loop lag，避免主机卡顿被误判为远端故障。
3. public route 的 `depth` 与 `bookTicker` 分别计时。对正式流动性 cohort 可采用 30 秒保守阈值；
   一旦超时，宁可打开 scoped gap、重订阅并重新抓 depth snapshot，也不能让潜在丢失保持隐式。
   这是一项本实验的保守质量策略，不是 Binance 的 30 秒交付保证。
4. `aggTrade`、`forceOrder`、`contractInfo` 不得因“无事件”单独报警。它们依赖连接健康、
   `markPrice@1s` 周期信号、订阅列表核验和 23 小时连接重建。若强平事件必须达到逐笔完整性，当前
   Binance `forceOrder` feed 从源头就不满足要求，不能靠 liveness 或去重补足。
5. liveness gap 需要同时保存 `detected_at` 和保守的 `affected_from`。后者应是该 stream 最后一次
   成功事件之后的边界；当前 `GapEvent` 只有 `observed_at_realtime_ns`，不足以表达“在超时后才发现，
   但可能从此前已经缺失”。central coverage 必须使用 affected interval，而不是只从报警时刻扣减。
6. refresh 后，`depth` 只有 REST snapshot 与 diff 成功 bridge 后才能关闭无效区间；订阅 ACK 本身
   不能恢复 L2 validity。`bookTicker`/`markPrice` 至少等到该 stream 的第一条新事件再关闭 gap。

## 2. `_PROCESSED` 的覆盖率和成员契约

### 2.1 v0.3.0 的两个独立漏洞

本地跨日修复已使 finalize 拒绝：缺文件、空 validity、区间越出 UTC 日、区间重叠、checkpoint
损坏或 identity 不一致。但 [central/process_cli.py](../src/miry/cli/process.py)
在 v0.3.0 中只要求每个 symbol 有**至少一个非空区间**，没有累加有效时长。一天只有 1 秒 VALID
也能通过。

另一个更基础的问题在当时的 `campus-107/submit-day.sh`（现已删除）：预期 symbol
完全来自命令行传入的文本文件。脚本只验证“非空、格式正确、无重复”，不要求 60 个，不核对 generation、
`universe_hash` 或 sealed day。传入只含 1 个 symbol 的文件时，finalize 可以合法写出只声明该币的
`_PROCESSED.json`。

因此完整性不能只新增一个百分比计算；必须先把**应处理谁**绑定到 raw 权威证据。

### 2.2 v0.3.1 expected window

每个 symbol、每个 UTC 日先确定一个权威 expected interval：

```text
expected_start = max(UTC day start, FORMAL_COLLECTION_STARTED, membership effective_at)
expected_end   = min(UTC day end, membership removal effective_at)
expected_ns    = expected_end - expected_start
```

- 正式首日只计算 formal start 之后的部分，不把实验开始前算成缺失。
- 午夜成员替换时，旧成员在 effective boundary 结束，新成员从 boundary 开始；没有成员变化时应是完整
  24 小时窗口。
- symbol 当天没有 membership 时不能靠一个空 validity 进入或退出处理列表。
- expected membership 必须来自随 sealed day 一起冻结且可校验的 universe registry/manifest，不能来自
  操作者临时提供的 `symbols.txt`。sealed artifact 至少应包含 generation、universe hash、成员表和有效区间；
  central 需核对每个 chunk 的 `universe_hash`。

当前 `DayManifest` 只有 chunk refs，没有成员表；raw `universe_decision` 可能在更早一天发出，也不能仅靠
读取目标日文件恢复 membership。因此应发布一个随 raw 拉取的、不可变且按 hash 寻址的 universe registry，
或升级 day manifest 明确携带当天 membership windows。

### 2.3 两种覆盖率必须同时报告

对每个 expected interval 求不重叠区间并输出：

```text
valid_ns              = union(L2 VALID intervals) 与 expected interval 的交集
explicit_invalid_ns   = union(有 affected_from/to 的 transport、sequence、liveness gaps)
unclassified_ns       = expected_ns - union(valid, explicit_invalid)
valid_ratio           = valid_ns / expected_ns
accounted_ratio       = (valid_ns + explicit_invalid_ns) / expected_ns
```

关键规则：

- 显式 gap **不能从 valid_ratio 的分母删除**，否则“明确停机 23 小时”仍会显示 100% quality。
- `accounted_ratio` 必须严格为 100%，即 `unclassified_ns == 0`；这才实现 loss-explicit。
- 正式默认要求每个 60 币 `valid_ratio >= 99.9%`，即完整 24 小时最多约 86.4 秒显式无效。
  这是实验质量政策而非 Binance 标准；v0.3.1 以 `l2-coverage-v1` 写入 marker。
- 若业务决定接受更长的已知 gap，可以调整版本化门槛，但绝不能放宽 `unclassified_ns == 0`。
- `_PROCESSED.json` 必须记录 membership hash、expected/valid/invalid/unclassified 时长、ratio、策略版本
  和输入 sealed manifest hash。任何 symbol 不达标则不得写成功 marker；可另写 `_QUALITY_REJECTED.json`
  保存失败证据，避免“作业成功”和“数据质量通过”混为一谈。

## 3. Writer durability 与事件循环阻塞

### 3.1 已经正确的持久化顺序

当前 raw chunk 的核心顺序是：关闭 Parquet -> `fsync(file)` -> rename 到 `ready` ->
`fsync(ready directory)` -> 计算 SHA-256 -> atomic 写 manifest（临时文件 fsync、rename、目录 fsync）。
107 pull 也先把数据写入 partial、`fsync`、校验 size/hash、rename、目录 fsync、atomic 写本地 manifest，
之后才发 SHA-256 精确匹配的 ACK。Vultr 收 ACK 后，未 seal 的 manifest 会移入
`control/acked-manifests`，保证日 seal 仍能恢复 chunk inventory。

这个顺序与 OS durability 语义一致：Python `os.fsync()` 请求把文件写入持久存储；Linux 明确说明
仅 fsync 文件并不保证包含它的目录项已经落盘，因此 rename/unlink 后还要 fsync 目录：
[Python `os.fsync`](https://docs.python.org/3/library/os.html#os.fsync)、
[Linux `fsync(2)`](https://man7.org/linux/man-pages/man2/fsync.2.html)。

核心 Parquet `flush()`/`finish()` 已通过 `asyncio.to_thread()` 执行；service 的 spool status 与 ACK
扫描也通过 worker thread 执行。Python 官方说明 `to_thread` 的用途正是避免 I/O-bound 函数阻塞 event
loop：[Python 文档](https://docs.python.org/3/library/asyncio-task.html#asyncio.to_thread)。

### 3.2 v0.3.0 问题：部分 durability I/O 阻塞 event loop

- `DayIndex.record()` 是 async 函数，但在 event loop 内直接执行 mkdir、append、flush 和 `os.fsync()`。
- `DayIndex.seal()` 在 event loop 内读全部 refs、校验 manifests、atomic write、清理目录并 fsync。
- `GapJournal.open/close/rollover()` 的 `_write()`、gap state atomic write、hash 和目录 fsync 也在 event
  loop 内直接执行。恰好在连接或 sequence 异常时，写 gap 的慢 I/O 反而会暂停其他 WebSocket receive。
- formal-start 的最终 atomic write 等少量低频路径也直接在 event loop 执行。

Python 官方明确要求不要直接在 event loop 运行阻塞的文件 I/O：
[Running Blocking Code](https://docs.python.org/3/library/asyncio-dev.html#running-blocking-code)。
在正常 NVMe 上这些调用很快，但 1c1g Vultr 的共享磁盘延迟尖峰、磁盘回写压力或目录变大时，不能把
“通常很快”当成正确性保证。

修复应把一次 durability transaction 整体封装为同步函数，再以 `asyncio.to_thread()` 调用；不要把
write、fsync、rename 拆成多个可交错的 thread call。`DayIndex` 仍需串行锁，gap state 也要保证同一个
gap 的 open/close 不会乱序。性能验收应观测 event-loop lag，而不仅是 finalize 用时。

### 3.3 v0.3.0 问题：非正常退出形成隐式 gap

v0.3.0 的 SIGINT/SIGTERM 优雅停止会先写 `COLLECTOR_STOPPED_GAP`，但 SIGKILL、OOM、宿主机重启
或断电无法执行该路径。下次启动只会关闭**此前已经存在**的 open gap；该版本没有 durable
clean-shutdown marker 或跨 boot heartbeat 来推断上一次存活终点。因此 OOM 后到新 boot 全源 ready
之间可能是未登记 raw gap。

v0.3.1 已持久保存低频 collector lease：boot ID、最后 durable chunk watermark 和 clean-shutdown
状态。启动发现上一 boot 未 clean close 时，先发布 recovered collector gap，affected start 使用
最后可证明的共同 durable watermark，结束于新 boot 的全部 source readiness；不做逐事件 fsync。

### 3.4 测试结论与应补矩阵

本机命令：

```text
uv run pytest -q
90 passed
```

所以“测试挂死”**不能复现**；真实问题是生产事件循环上的同步 I/O。v0.3.1 回归覆盖 happy path、
精确 ACK、queue hard limit、slow-fsync event-loop heartbeat、gap reserve/index 重试、lease recovery、
open gap 跨进程状态和多日 rollover。后续故障注入仍可继续扩展：

1. 在 file fsync、data rename、directory fsync、manifest publish、ACK data unlink、manifest move、day
   seal publish 的每个边界注入 crash，再启动恢复并证明 chunk 不会从 inventory 消失。
2. 将 fsync/atomic write 注入可控延迟，同时运行 1 秒 ticker，证明 event-loop liveness 不被拖住。
3. SIGKILL/OOM 风格的跨 boot 测试，验证 recovered gap 的 affected interval。
4. ACK 与 seal 的并发/串行化测试，以及 seal 后重复 ACK 的幂等测试。

## 4. 事件去重

### 4.1 Binance 可用的 identity

官方事件 payload 说明见：

- [Aggregate Trade Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams)
- [Trade Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Trade-Streams)
- [Diff Book Depth Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams)
- [Individual Symbol Book Ticker](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams)
- [Mark Price Stream](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream)
- [Liquidation Order Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams)
- [Contract Info Stream](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Contract-Info-Stream)

| stream | 官方字段与安全策略 | 当前状态 |
| --- | --- | --- |
| `aggTrade` | `(symbol, a)`，`a` 是 aggregate trade ID。 | 已实现。 |
| 可选 `trade` | `(symbol, t)`，`t` 是 trade ID。 | 已实现，正式 D0 关闭。 |
| `depth`/可选 `rpiDepth` | `(symbol, U, u, pu)` 是 update ID 区间和前序 ID。官方 ID 相同但 payload 不同时应报告 conflict，而不是当成两个正常事件。 | v0.3.1 已移除 identity 中的 payload hash，冲突会 fail closed。 |
| `bookTicker` | `(symbol, u)`，`u` 是 order book update ID；payload hash 应用于 conflict 校验。 | v0.3.1 已按 ID 去重并检测冲突。 |
| `markPrice` | 没有官方唯一 ID。只能做**精确 replay 去重**：`(symbol, E, canonical payload hash)`；不能仅按价格值去重，因为每秒相同价格仍是合法观测。 | v0.3.1 已实现精确 replay 去重。 |
| `forceOrder` | 没有 order ID；可用 `(symbol, E, order.T, canonical order payload hash)` 标记两个连接收到的精确同一 snapshot。 | v0.3.1 已实现；不得把去重后数量解释为逐笔强平数。 |
| `contractInfo` | 没有唯一 ID；可用 `(symbol, E, canonical payload hash)` 做精确 replay 去重。 | v0.3.1 已实现精确 replay 去重。 |
| REST snapshot/OI/clock/universe | 每个 request 是独立观测或 L2 anchor，即使值相同也有研究意义。 | 不应做通用 payload 去重。 |

### 4.2 v0.3.0 派生链的其他去重缺口

`DayNormalizer.seen` 每个 UTC 日从空 set 开始。若旧、新 WebSocket 连接的 overlap 恰好跨午夜，
同一 exchange event 可以按本地 receive time 分到两个 raw 日，第二天不会知道前一天已经见过该 identity。
应从前一天 checkpoint 继承至少覆盖 `connection_overlap_seconds + 最大乱序余量` 的 bounded identity 状态。

当前 `seen` 会保存一整天所有 depth identity。`depth@100ms` 的理论量级很大，使用无界 Python tuple set
不是必要条件。去重只需覆盖连接 overlap 和有限乱序窗口，建议按 `(stream, symbol)` 使用有界 LRU/ID
watermark，并把窗口大小写入 marker；这既支持跨日继承，也避免 107 normalize 内存随全天事件数线性增长。

最后，typed rows 虽然有 `is_duplicate`，但 [central/d0.py](../src/miry/pipeline/d0.py)
汇总 `aggTrade`、trade 和 depth 时没有读取或过滤它。因此已经正确标出的 overlap duplicate 仍会增加
事件数、成交量和 notional。任何事件计数、强平统计和成交汇总都必须显式采用
`is_duplicate == false`；raw 与 typed 应保留重复记录以便审计，不能物理删除证据。

## 5. v0.3.1 验收状态

代码级 P0/P1 修复和 `90 passed` 全套回归已经完成。发布前还要构建并校验容器产物、执行双轴 diff
review；部署后用真实 Vultr 完成启动、订阅、gap、RSS/CPU 和 ACK 检查。24 小时资源指标与真实 UTC
日切属于持续生产验收，不通过时保留 explicit gap 并修复或扩容，不能通过删除质量门槛掩盖。
