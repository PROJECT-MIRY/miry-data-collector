# v0.3.8 正式采集实施合同

## 本阶段目标

本版本就是正式实验采集版本。Vultr 从空的 active 路径启动，不读取、转换或迁移旧 generation
状态。107 保留旧 raw 的原始字节，新 runtime 只按新 formal-start 处理后续日期。`7.0` 一次启动
全部 60 个合约，不存在分级扩容配置。

Vultr 是 universe 决策者和执行者。107 仅拉取 immutable raw chunk、完成哈希校验、回传
ACK，并把重计算提交给 Slurm。正式采集过程中不依赖 GitHub，也不依赖 107 回传选币决策。

## Universe 角色

- `core` 固定 50 个槽位，代表长期稳定样本；
- `boundary` 固定 5 个槽位，代表流动性排名边界；
- `probe` 固定 5 个槽位，代表最新上市的合格永续合约；
- 三个角色始终互斥，总数始终等于 60。

新 `7.0` 由 [冻结证据](formal-universe-7.0-evidence.json) 记录，并在首次
启动必须用新的双重状态、14 日 Kline 和盘口证据验证已冻结的 50/5/5 全部仍通过角色硬门槛，
并同时绑定离线 evidence hash 与实时 source hashes。任何成员失效或不再合格就拒绝写正式
起点；瞬时盘口导致的合格成员内部排名变化不会擅自改写冻结名单。

Vultr 每天 `23:50 UTC` 用两次 `exchangeInfo` 包围完整证据抓取。只有两次响应都为
`TRADING` 的 USDT 保证金、USDT 报价永续合约才合格。历史流动性来自完整 UTC 日 Kline，
当天未结束的 Kline 永不进入决策。首次抓 14 日，随后从已落盘证据增量追加刚结束的一日。
当前可采集性由 5 次全市场 bookTicker 和稳健 Top200 与最新 100 并集的 3 次 depth 验证。
原始内容、时间和 SHA-256 都写入 decision evidence 和 raw metadata。

## 自动轮换

候选角色每天 `00:00 UTC` 生效：

- core/boundary 使用最近 14 个完整 UTC 日；probe 上市后至少有 7/7 个完整 UTC 日；
- 每日成交额要求中位数 `>=10M`、P25 `>=5M`、最小值 `>=3M USDT`、CV `<=1.2`；
- 每日交易数要求中位数 `>=100K`、P25 `>=50K`、最小值 `>=25K`；
- 5 次 bookTicker 与 3 次 depth 最大点差 `<=10 bps`，depth 较薄侧
  `+/-10 bps >=800`、`+/-50 bps >=10K USDT`；
- boundary 目标为非 core、非 probe 的 Top5，现有成员在 Top10 内可保留；
- probe 在通过全部门槛的合约中优先最新上市者；
- 正常情况下每天最多替换 2 个币，boundary 和 probe 各最多 1 个；
- candidate 成员至少停留 48 小时；
- 两次状态请求确认停止交易后，允许为恢复可采集性进行强制替换。

core 只在周一 `00:00 UTC` 评估：

- 14/14 个完整日，合约年龄至少 30 天；
- 新成员必须进入 Top45；现有成员跌出 Top55 后才具备退出资格；
- core 成员至少停留 14 天；
- 每周最多替换 5 个 core；
- 已被两次状态请求确认停止交易的 core 可优先替换。

合格 stable 池少于 65 时报警。任何角色候选不足都 fail closed：保留当前采集、记录评估并
报警，不自动放宽门槛、不产出少于 60 个成员的 decision。

每次评估都写 evaluation。成员变化时写带结构化 universe 版本、角色、证据 hash、原因、
`effective_at` 和 `universe_hash` 的 decision。版本不是浮点数：50 个 core 变化时
`core_generation += 1` 且 `candidate_revision = 0`；仅 boundary/probe 变化时只执行
`candidate_revision += 1`；成员完全不变不写 decision。`decision_sequence` 对所有实际 decision
单调加一并用于排序，`universe_version` 只是 `<core_generation>.<candidate_revision>` 展示字符串，
`universe_hash` 仍绑定精确 50/5/5 身份。`automation_enabled: false` 可暂停自动决策；
手工 override 只能修改 boundary/probe，不能直接修改 core。

## 日切和 gap

无成员变化的 UTC 日切 rollover gap journal，并通过 writer barrier finalize 前一天所有 chunk 后
seal；它不停止或重建任何 Binance 连接，也不产生 `PLANNED_BOUNDARY_GAP`。writer barrier
由 ingest lock 串行化，因此不会丢弃边界上的事件。

有成员变化时先切换 writer 的 `universe_hash`，再通过现有连接发送
`UNSUBSCRIBE/SUBSCRIBE`。新增币完成订阅 ACK、L2 snapshot 和第一次 OI 后关闭 gap。gap 的
`exchange_symbols` 只包含集合差集，不包含未变化的币。因此 candidate 轮换不会让 50 个
core 出现计划中断。

WebSocket 30 秒无任何消息会重连整个异常连接。每个币的 `depth` 与 `bookTicker` 分别以 30 秒
保守阈值监控，`markPrice@1s` 以 15 秒监控；超时只重订阅准确的 `(stream, symbol)`，并从最后已
证明事件时刻打开 symbol/stream-scoped `CONNECTION_LOST_GAP`。控制 ACK 使用独立 20 秒 deadline，
snapshot completion 最长等待 180 秒；ACK 不代表恢复，必须看到对应 stream 的第一条新事件才关闭
scoped gap，L2 validity 还必须等待 snapshot bridge。同一活跃连接连续两次局部恢复失败时才重建所属 route，并为该 route
被主动中断的全部 symbol/stream 打开 transport gap；其他 route、REST poller 和 writer 继续工作。
每条连接每 60 秒执行一次 `LIST_SUBSCRIPTIONS`，单次响应 deadline 为 20 秒；集合不一致立即失败，
但无响应必须连续发生 3 次才使当前 route 连接失败，gap 从上一次成功审计的 proof timestamp 起算。`aggTrade`、`forceOrder` 和
`contractInfo` 因天然稀疏不使用事件 deadline。L2 `pu/u` 不连续时单独记录
`L2_SEQUENCE_GAP` 并重新取 snapshot。

前一日 seal 延迟 150 秒，确保 30/60/120 秒监控发现的 affected interval 能先进入 day inventory。
collector 每 30 秒写 lease；若上次启动没有 clean shutdown，下次启动会从 depth 与 market/trades
共同 durable watermark 打开 recovered `COLLECTOR_STOPPED_GAP`，直到全部 source ready 才关闭。

## 正式起点

空数据根启动后，collector 必须完成：

1. 全部 60 个币的 public 和 market WebSocket 订阅；
2. 全部 L2 初始 snapshot；
3. 全部 60 个币的第一次 open-interest 请求；
4. discovery 和 clock 首次请求。

随后写入 raw `universe_decision` 和 `FORMAL_COLLECTION_STARTED` 事件，强制 finalize writer，
再持久化 `control/formal-start.json`。`7.0` 决策在写入前必须绑定上述双重状态响应和
ticker 响应的 SHA-256。该事件时间之后的数据属于正式实验。24 小时资源观察是生产监控，
不会清空或重启已经采集的数据。

## rsync 可靠性

107 每分钟执行一个短生命周期任务：

1. 用固定私钥和 known_hosts 将 Vultr `ready/` rsync 到 `runtime/rsync/ready`；
2. 读取 manifest，将数据写入 `.partial`，fsync，校验 size 与 SHA-256；
3. 原子 rename 到 `data/raw/collector=<id>/...`，再持久化本地 manifest；
4. 生成 ACK 并 rsync 到 Vultr `control/acks/`；
5. Vultr 只有在 ACK 的 chunk ID 和 SHA-256 都匹配后才删除 ready 数据。

两端必须持久记录 `LOCAL_DURABLE`、`ACK_PUSHED`、`ACK_VALIDATED` 和 `REMOTE_GC`；Vultr 删除
ready 前必须先写可恢复 transaction。损坏、未知或 hash 冲突 ACK 只能隔离和报警，不能删除 ready
或终止全部采集。详细合同见 [ACK 传输审计](transfer-ack-observability.md)。

禁止使用 `--remove-source-files`。暂存镜像不是永久数据，下一次同步可删除已从 Vultr GC 的
镜像文件；`data/raw` 才是 107 上的永久原始数据。

## 1C1G 性能合同

目标机器为 1 vCPU、1GiB RAM、25GB 磁盘，不允许通过减少币数或降低采集频率达标。

- Docker：`1.00 CPU`、`768MiB`、`256 PIDs`；
- 4 个稳定加权 public shards，初始按生产消息率最小负载分配，成员未变化时不跨 route 搬迁；
- WebSocket queue 为 16，单消息上限 2MiB；
- 1,000 档 snapshot 起点全局最小间隔 0.75 秒、最多 4 个 HTTP 在途，持续上限约 1,600
  request-weight/min；恢复 snapshot 排队/在途时暂停 discovery REST；
- raw queue 总字节上限 64MiB，70% 告警，50% 恢复；
- writer batch 上限 2000 events 或 2MiB；
- RSS p95 不超过 600MiB，峰值不超过 700MiB，无 OOM；
- CPU 平均不超过 65%，p95 不超过 80%；
- event-loop lag p99 小于 100ms；
- queue p99 小于 50%，不得连续 10 秒超过 70%；
- Parquet finalize p99 小于 5 秒；
- 24 小时内无性能原因 gap，ACK 通常小于 3 分钟；
- 磁盘可用空间至少 2GiB。

首次 24 小时只做观察和判定。任何硬指标失败都应扩容或优化实现，不得改变 60 币正式合同。

## 数据层级边界

v0.3 采集 `depth@100ms` 与 1,000 档 snapshot，用于可验证地重建 market-by-price L2；
它不采集、也不声称能重建带全市场 resting order ID 和同价排队关系的 true L3。
Binance 公开 USD-M 行情接口没有提供这种 market-by-order feed。可选 D0 的 individual trade
和 RPI depth 也不构成 L3，正式配置保持 `d0_enabled: false`。

当前实验研究价差、价位深度、冲击成本、成交与 L2 order-flow imbalance，不需要 L3。
只有研究 queue position、逐订单寿命、撤单行为或订单级成交概率时，才另立第三方数据源与
基础设施项目；容量和来源评估见 [L3 数据评估](l3-data-assessment-2026-08-12.md)。

## 跨日 L2 派生合同

每个 symbol 的日处理必须原子写出 `l2-checkpoint.json`。checkpoint 保存 UTC 日末 authoritative
盘口和 update ID，并保留恰好跨日的未完成 snapshot bridge。下一日只能从前一日、同 collector、
同 symbol 且恰好有效到 UTC 边界的 checkpoint 继承；第一条 diff 的 `pu` 不连续时立即结束继承。

transport gap OPEN 会使相关盘口失效。只有 snapshot bridge 已成功且同一 gap CLOSED 后才能重新
产生 validity。finalize 必须拒绝空、重叠、越出目标 UTC 日或缺少 checkpoint 的结果。107 必须从
formal start 所在 partial day 开始按 UTC 日期顺序处理，不能跳日提交。

## 日质量与去重合同

normalizer 从 sealed raw 的 `UNIVERSE_DECISION` 提取权威结构化版本、universe hash 和恰好 60 个
成员。107 的 symbol 文件只是提交参数，必须恰好 60 个唯一大写 symbol，且 finalize 会再次要求它与
raw 权威集合完全一致，不能靠少传 symbol 缩小验收范围。

每币 expected window 在首日从 `FORMAL_COLLECTION_STARTED` 开始，其余日期覆盖完整 UTC 日。
`_PROCESSED.json` 仅在以下条件全部满足时生成：

- `valid_ratio >= 99.9%`；显式 gap 仍保留在分母中；
- `accounted_ratio == 100%` 且 `unclassified_ns == 0`；
- VALID 与 explicit invalid 没有重叠，即 `conflicting_ns == 0`；
- checkpoint、sealed manifest hash、结构化 universe 版本、universe hash 和 60 币 identity 全部一致。

不满足时写 `_QUALITY_REJECTED.json`，不得写成功 marker。normalizer 用 10 分钟有界 identity 状态标记
连接 overlap replay，并通过 `_DEDUP_CHECKPOINT.json` 跨午夜继承；raw 与 typed 保留重复行供审计，
D0 汇总排除 `is_duplicate=true`。`forceOrder` 本身每秒最多提供最新一笔，去重不会把它变成完整逐笔
强平数据。
