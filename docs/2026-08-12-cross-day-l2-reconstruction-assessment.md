# 跨 UTC 日 L2 重建评估（2026-08-12）

## 结论

**问题确认存在。** 在成员不变且 public WebSocket 连接跨过 `00:00 UTC` 的正常情况下，
edge 会继续完整采集 `depth` diff，但当前 `L2DayReconstructor` 只读取目标日文件，并为每个
`connection_id` 从 `UNANCHORED` 空状态开始。目标日没有新的 `depth_snapshot` 时，diff 只会
进入 pending，不能产生 `VALID` 状态或有效区间。因此：

- 这是 **derived L2 的跨日状态继承缺失**，不是已经证明的 raw 数据丢失；
- 影响所有跨日未重锚的成员，**包括 50 个 core**，不只 boundary/probe；
- 从 `00:00 UTC` 到该 symbol 下一次连接轮换、连接恢复、sequence 重锚或定向刷新所产生的
  snapshot 之前，当前日处理不能声明 L2 有效；
- 默认连接轮换周期为 82,800 秒（23 小时），所以某个 UTC 日的无有效 L2 区间在不利相位下
  可接近 23 小时，而不是一个短暂的日切窗口；
- 当前 finalize 只检查 `l2-validity.jsonl` 是否存在，空文件也能通过，所以该问题可能静默进入
  `_PROCESSED.json`。

核查基线是仓库提交 `393b090`（tag `v0.3.0`）。

## v0.3.1 修复状态

v0.3.1 已实现日末 authoritative checkpoint、跨日未完成 snapshot bridge 保存、gap OPEN/CLOSED
时序阻断、`pu` 连续性验证、finalize fail-closed 和 107 乱序提交预检。修复不改变 raw 合同，已有
v0.3.0 raw 可从首个 formal partial UTC day 开始重新生成 derived，无需重新采集。

## 证据链

### 1. 正常日切只切 writer，不重建连接或全量 snapshot

日切流程先 rollover gap journal，再应用到期的 universe decision，随后调用 writer barrier；
只有存在 decision 时才调用 `update_instruments()`。成员不变时不会触碰 source：
[edge/service.py L207-L238](../src/miry/collector/service.py)。对应测试明确断言
成员不变的日切没有 source update、没有 planned gap，只有 writer rotation 和前一日 seal：
[test_collector_service.py L192-L202](../tests/test_collector_service.py#L192-L202)。项目计划也把这项行为写成正式
合同：[collection-contract.md L59-L68](collection-contract.md#L59-L68)。

即使当天 universe 发生变化，public route 也只为 `added` symbol 生成 snapshot request：
[edge/sources.py L108-L126](../src/miry/collector/routes.py)、
[edge/sources.py L345-L358](../src/miry/collector/routes.py)。因此新增币可在当天获得
snapshot，但未变化的 core/boundary/probe 不会因为 universe decision 自动重锚。

### 2. 全量 snapshot 与 23 小时连接轮换绑定，而不是与 UTC 日绑定

每次新 WebSocket connection 启动时，route 把当前 shard 的全部 snapshot requests 交给
`BinanceWebSocketConnection`：[edge/sources.py L291-L319](../src/miry/collector/routes.py)。
收到首次订阅 ACK 后才逐个抓取 snapshot，并在全部完成后把新连接标记 ready：
[edge/binance.py L288-L320](../src/miry/collector/websocket.py)、
[edge/binance.py L365-L389](../src/miry/collector/websocket.py)。

route 的下一次 replacement 由相对时长 timer 驱动，不对齐 UTC 午夜：
[edge/sources.py L387-L401](../src/miry/collector/routes.py)。默认时长是 82,800 秒，
即 23 小时：[edge/config.py L136-L147](../src/miry/collector/config.py)。两个 public
shard 还按 snapshot 数量、请求间隔和 overlap 设置启动 offset：
[edge/sources.py L1009-L1034](../src/miry/collector/routes.py)。所以“下一次 snapshot
何时出现”取决于连接启动相位和该 symbol 在 shard 内的抓取顺序，不能当作午夜附近的固定锚点。

其他能产生 snapshot 的路径是新增 symbol、public stream 30 秒 liveness refresh，以及检测到 `pu/u`
不连续后的 reanchor：[edge/sources.py L134-L189](../src/miry/collector/routes.py)、
[edge/binance.py L396-L425](../src/miry/collector/websocket.py)。这些是变更或故障恢复路径，
不能作为每日日切的常规前提。

### 3. raw diff 跨日仍按接收日持久化

ingest 的 boundary lock 把事件入队和 writer rotation 串行化，保证 barrier 不会与新事件 admission
交错：[edge/ingest.py L10-L24](../src/miry/collector/ingest.py)。writer 根据每个 raw
event 的 `app_receive_realtime_ns` 计算 UTC 日期；日期改变时先 finalize 旧 chunk，再为新日期建立
chunk：[edge/writer.py L290-L321](../src/miry/collector/writer.py)。chunk manifest 同时
记录 UTC date 与接收时间范围：[edge/writer.py L150-L175](../src/miry/collector/writer.py)。

因此，在没有另一个已记录 transport/sequence gap 的前提下，午夜后的 diff 仍进入新日 raw；当前
问题本身不会制造 raw gap。writer barrier 和 day seal 的作用是划清、完整列出日文件，不是保存
L2 内存状态。

### 4. normalizer 和 L2 task 都严格限于一个 UTC 日

`DayNormalizer` 只加载目标日 `SEALED.json`，并拒绝 manifest 日期与目标日不一致的 chunk：
[central/normalize.py L109-L143](../src/miry/pipeline/normalize.py)。它还验证每个 raw
chunk 的最小/最大接收时间均落在目标日范围内：
[central/normalize.py L175-L192](../src/miry/pipeline/normalize.py)。输出也只写到
`typed/.../date=<utc_date>`：[central/normalize.py L32-L53](../src/miry/pipeline/normalize.py)。

L2 CLI 只接受一个 `--date`，并仅把该日期传给 reconstructor：
[central/process_cli.py L16-L28](../src/miry/cli/process.py)、
[central/process_cli.py L51-L58](../src/miry/cli/process.py)。Slurm array 同样只传
`MIRY_UTC_DATE`，没有前一日 checkpoint 或依赖参数：
[l2-array.sbatch L8-L26](../deploy/campus-107/slurm/l2-array.sbatch#L8-L26)。

### 5. 新日 reconstructor 必须看到当日 snapshot 才能进入 VALID

`L2DayReconstructor.run()` 每次创建全新的 `books = {}`；每个新 `connection_id` 又创建默认
`UNANCHORED` 的 `ConnectionBook`：[central/l2.py L54-L64](../src/miry/pipeline/l2.py)、
[central/l2.py L196-L210](../src/miry/pipeline/l2.py)。它只遍历目标日 typed 目录：
[central/l2.py L164-L194](../src/miry/pipeline/l2.py)、
[central/l2.py L238-L265](../src/miry/pipeline/l2.py)。

处于非 `VALID` 状态时，diff 只加入 pending；`_try_bridge()` 在没有 snapshot 的
`anchor_last_update_id` 和 `anchor_received_ns` 时立即返回：
[central/l2.py L66-L78](../src/miry/pipeline/l2.py)、
[central/l2.py L97-L123](../src/miry/pipeline/l2.py)。只有 snapshot 与某个 buffered
diff 建立 update-ID bridge 后才产生 `VALID` state change：
[central/l2.py L126-L156](../src/miry/pipeline/l2.py)。

所以，如果前一天已有有效 snapshot 和连续 diff，而新一天只有同一连接的连续 diff，当前日会输出
零个 `VALID` change 和零个 validity interval。最小黑盒复现验证了这一点：期望继承产生 1 个有效
区间，实际返回 `intervals == 0`。

### 6. 当前 finalize 不会拦截空有效区间

reconstructor 无论 interval 是否为空都会写 `l2-validity.jsonl`：
[central/l2.py L298-L318](../src/miry/pipeline/l2.py)。finalize 只检查 60 个 symbol
对应文件存在，不检查文件非空、日覆盖率或首个有效时间：
[central/process_cli.py L76-L108](../src/miry/cli/process.py)。因此一次 Slurm
流水线可以全部成功并写 `_PROCESSED.json`，但某些或全部 symbol 在该日早段没有可声明的 L2 有效区间。

## 影响边界

1. **影响 50 个 core。** role 不参与 reconstructor 的状态机；只要 symbol 跨日沿用连接且当日尚无
   snapshot，就会处于 UNANCHORED。daily candidate replacement 只让新增成员获得 snapshot，不会给
   未变化的 50 个 core 补 snapshot。
2. **不应登记为 transport gap。** 这段时间 raw diff 可以完整存在，gap journal 也不会因为正常日切
   写 `PLANNED_BOUNDARY_GAP`。正确术语是“派生 L2 validity 缺失”或“跨日未锚定区间”。若同一时段另有
   `CONNECTION_LOST_GAP`/`L2_SEQUENCE_GAP`，则需叠加处理真实缺失。
3. **前一日的 validity 不能直接当状态继承。** 当前代码把有效区间延伸到 `utc_day_end`，但输出只含
   时间和 authority，不含 bids、asks、`previous_update_id` 等下一日继续应用 diff 所需状态：
   [central/l2.py L214-L235](../src/miry/pipeline/l2.py)、
   [central/l2.py L298-L318](../src/miry/pipeline/l2.py)。
4. **raw 保留使离线修复可行。** 只要真实 gap ledger 未标记缺失，就可以从此前最近的 snapshot 开始
   连续重放跨日 diff，重新生成正确的派生结果；不需要丢弃或重新采集现有 raw。
5. **还存在处理性能风险。** `ConnectionBook.on_diff()` 会把未锚定 diff 连同盘口变更内容持续追加到
   `pending`；在当天 snapshot 很晚才出现时，单个 symbol 可能积累数小时的对象，然后在 bridge 时统一
   排序和过滤。这并不证明现有 8 GiB Slurm job 必然 OOM，但当前实现没有跨日 checkpoint，也没有为
   `pending` 设置独立上限，因此不能把影响只理解为 validity 文件少一段时间。

Binance 官方的本地订单簿流程也把 snapshot 的 `lastUpdateId` 与连续 diff 的 `U/u/pu` bridge 作为
有效重建基础；参见 [How to manage a local order book correctly](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)
和 [Diff Book Depth Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams)。

## 修复约束与建议

首选修复应在 central 派生层实现 **跨日 checkpoint**，而不是把正常午夜伪装成 gap：

1. 每个 symbol 的日处理持久化日末 authority checkpoint，至少包含完整 bids/asks、
   `connection_id`、`previous_update_id`、最后事件时间/sequence、data-contract/universe 身份和内容 hash。
2. 次日任务必须依赖前一日 checkpoint。仅在 identity 合法、前一日状态在午夜仍有效、且次日第一条
   同连接 diff 的 `pu` 与 checkpoint update ID 连续时继承；否则保持 UNANCHORED，等待当日 snapshot。
3. formal start 所在的首个 partial day 仍从实际 snapshot bootstrap；新增 symbol 同理从它加入后的
   snapshot bootstrap。连接在午夜附近切换时，按实际 authority/connection checkpoint 继承，不能跨
   connection 猜测。
4. transport gap 或 sequence gap 穿过午夜时，不得把 VALID 状态跨过去；只有后续 snapshot bridge
   成功后才能重新开放有效区间。
5. finalize 必须验证预期 symbol 的 validity coverage，而不只是文件存在；持续成员如果没有真实 gap，
   其有效区间应能从日初连续开始。空文件必须使处理失败。
6. 加入跨日回归用例：同连接连续 diff、snapshot 在前一日；午夜 sequence 不连续；snapshot/连接轮换
   恰好跨午夜；candidate 新增/移除；transport gap 跨午夜；缺失或损坏 checkpoint。
7. 增加跨日高事件率样本的内存与耗时验收，确保 L2 array 不再通过长时间累积 `pending` 才等待当天
   snapshot。

备选方案是每次处理新日时从前一日（或更早）最近 snapshot 重新回放到午夜，但会重复读取和计算大量
depth 数据，随着日期增长不够高效。edge 在每天午夜强制为 60 币抓 snapshot 也能缩短未锚定区间，
但按默认每次 snapshot 至少间隔 2 秒的全局串行限制，60 币完整抓取本身约需 120 秒以上：
[edge/binance.py L117-L155](../src/miry/collector/websocket.py)。它还把本可无损继承的
派生状态问题转成新的 REST 依赖，因此只能作为额外恢复锚点，不应代替 checkpoint。
