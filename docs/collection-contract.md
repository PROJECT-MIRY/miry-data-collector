# v0.5.10 正式采集与处理合同

## 本阶段目标

本版本继续现有正式实验，不重置 `formal-start`、raw、ready、ACK、gap 或 active universe。
当前 `7.0 / sequence 8` 的 50/5/5 身份保持不变；v0.5.10 上线本身不触发重选，只有新的每日
完整证据按本合同形成有效 decision 后才发生增量轮换。

Vultr 是 universe 决策者和执行者。107 仅拉取 immutable raw chunk、完成哈希校验、回传
ACK，并把重计算提交给 Slurm。正式采集过程中不依赖 GitHub，也不依赖 107 回传选币决策。

## Universe 角色

- `core` 固定 50 个槽位，代表长期稳定样本；
- `boundary` 固定 5 个槽位，代表流动性排名边界；
- `probe` 固定 5 个槽位，代表最新上市的合格永续合约；
- 三个角色始终互斥，总数始终等于 60。

`7.0` 的历史身份由 [冻结证据](formal-universe-7.0-evidence.json) 记录。该文件只绑定名单、版本、
source hashes 和 universe hash，不再携带已经删除的绝对流动性门槛。正式运行以持久化的
`active.json` 为权威，升级不得改写该 decision。

Vultr 每天 `23:50 UTC` 用两次 `exchangeInfo` 包围完整证据抓取。只有两次响应都为
`TRADING` 的 USDT 保证金、USDT 报价永续合约才合格。历史活跃度来自完整 UTC 日 Kline，
当天未结束的 Kline 永不进入决策。首次抓 35 日，随后从已落盘证据增量追加刚结束的一日；
最近 14 日用于单币排名，前 28 日与最近 1/3/7 日用于市场状态。盘口证据覆盖活跃度预排的
mature Top200、recent Top100 和当前 active 60 的并集，在约 20 秒内采集 21 次全市场 bookTicker，
另采集 3 次 `limit=100` depth。原始内容、时间和 SHA-256 都写入 decision evidence 和 raw
metadata。

硬拒绝只用于技术资格和证据有效性：角色要求的完整 UTC 日齐备；bookTicker/depth 样本数量、
数值和盘口结构可解析。成交额、交易数、点差和两档 depth 不设绝对流动性门槛，也不存在 CV
门槛。

## 自动轮换

候选角色每天 `00:00 UTC` 生效：

- core/boundary 使用最近 14 个完整 UTC 日且上市至少 30 日；probe 优先在上市不足 30 日、至少
  有 7 个完整日的 recent cohort 内排名；人数不足时才从已排除 stable Top55 的 mature 排名中补足；
- 每个池分别对 P25 quote volume、P25 trades、10 bps 较薄侧 depth、50 bps 较薄侧 depth
  降序排名，对 21 次 bookTicker 点差的 q95 升序排名；单个极端点不进入 q95，depth snapshot
  只计算深度，不再把 3 个样本的最大点差混入排名；
- 聚合顺序为“最差单项名次、名次总和、五项名次元组、symbol”，防止一个极强指标掩盖另一项
  极弱指标，同时保持结果确定；
- 首次分配严格按角色优先级执行：mature Top50 为 core，mature 第 51--55 名为 boundary，然后从
  recent 排名取 probe，不足部分只能由 mature 第 56 名以后补足；probe 不得先占用 stable Top55；
- boundary 目标为非 core、非 probe 的 Top5，现有成员在候选相对 Top10 内可保留；
- 正常情况下每天最多替换 2 个币，boundary 和 probe 各最多 1 个；
- candidate 成员至少停留 48 小时；
- 两次状态请求确认停止交易后，允许为恢复可采集性进行强制替换。

滚动阶段不会把 active core/boundary 直接降级成 probe。probe fallback 排除更新后的 core 以及当前
core/boundary，再从剩余 mature 排名补足；已有 probe 随时间进入 mature cohort 时仍按 dwell 和每日
最多 1 个 probe 替换的既有规则平滑退出，不做全量角色洗牌。

core 只在周一 `00:00 UTC` 评估：

- 14/14 个完整日，合约年龄至少 30 天；
- 新成员必须进入 Top45；现有成员跌出 Top55 后才具备退出资格；
- core 成员至少停留 14 天；
- 每周最多替换 5 个 core；
- 已被两次状态请求确认停止交易的 core 可优先替换。

mature 证据池少于 65 时报警。任何角色证据或候选不足都 fail closed：保留当前 60 币并记录
评估，不产出残缺 decision。当前 active 成员缺少角色所需证据时同样冻结，不把抓取失败解释成
流动性恶化；两次状态确认的停止交易成员走强制替换。

市场状态由固定完整面板独立计算。以前 28 日为基线，quote volume 与 trade count 必须同方向，
横截面中位变化达到 `1.25x` 或 `0.8x` 且各自同方向 breadth 至少 70% 才算广泛变化。1 日或
持续 3 日输出 `ACTIVITY_SHOCK_PENDING`，冻结非必要轮换；持续 7 日输出
`ACTIVITY_SHIFT_CONFIRMED`，恢复正常评估。停牌替换不受 pending 冻结影响。市场状态只节流
轮换，不直接指定任何单币进出。

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
由 admission barrier 串行化，因此不会丢弃边界上的事件。

有成员变化时先切换 writer 的 `universe_hash`，再通过现有连接发送
在线两阶段订阅更新：所有目标 public/market route 先增加新币，完成订阅 ACK、L2 snapshot、关键流
首事件和第一次 OI 后，才从旧 route/poller 移除退出成员。gap 的
`exchange_symbols` 只包含集合差集，不包含未变化的币。因此 candidate 轮换不会让 50 个
core 出现计划中断。

public route 每小时使用最近 24 个完整小时的实际消息峰值评估一次负载；日封存完成后也触发一次
评估。它继承当前 assignment，只有最大 route 预计负载超过平均值 `1.25x` 且一次 pair swap 能改善
最大最小差时才搬迁。单批最多交换一对 symbol，成功后冷却 5 分钟并继续评估直到收敛；raw queue
达到 50% 或任一 public route 正在恢复时暂停。迁移复用同一两阶段交接，不重启 collector，也不
产生 universe generation 或 `PLANNED_BOUNDARY_GAP`。交接期可能有可去重的重复 raw，但不会先退订
形成未登记空窗。裁剪失败时保留扩展覆盖，route 恢复后先收敛到最后提交的 assignment，再继续限幅
均衡。成员实际变化同样继承现有 route，只为退出/进入成员改变订阅；失败时 changed-symbol planned
gap 保持 OPEN，并按 30/60/120/300 秒退避后台重试，不阻断 UTC 日封存。

WebSocket 30 秒无任何消息会重连整个异常连接。每个币的 `depth` 与 `bookTicker` 分别以 30 秒
保守阈值监控，`markPrice@1s` 以 15 秒监控；超时只重订阅准确的 `(stream, symbol)`，并从最后已
证明事件时刻打开 symbol/stream-scoped `CONNECTION_LOST_GAP`。控制 ACK 使用独立 20 秒 deadline；
请求在本机等待 audit/control 锁时不消耗该预算。ACK 不代表恢复，必须看到对应 stream 的第一条新
事件才关闭 transport gap。L2 独立使用官方 local-order-book 协议：先缓存 diff，snapshot 返回后
丢弃 `u < lastUpdateId` 的事件，第一条保留事件必须满足 `U <= lastUpdateId <= u`，以后每条必须满足
`pu == previous.u`。HTTP 成功只记为 `snapshot fetched`；只有 overlap 与后续连续性得到证明才记为
`snapshot bridged`。snapshot 太旧或落在事件之间时只重抓准确的 symbol，并由共享 REST scheduler
限速；连续 5 次不能 bridge 时重建所属 route。transport 恢复后到全部 bridge 前保持独立的
`L2_REANCHOR_GAP` OPEN。同一活跃连接连续两次局部恢复失败时才重建所属 route，并为该 route
被主动中断的全部 symbol/stream 打开 transport gap；其他 route、REST poller 和 writer 继续工作。
每条连接每 60 秒执行一次 `LIST_SUBSCRIPTIONS`，单次响应 deadline 为 20 秒；集合不一致立即失败，
但无响应必须连续发生 3 次才使当前 route 连接失败，gap 从上一次成功审计的 proof timestamp 起算。`aggTrade`、`forceOrder` 和
`contractInfo` 因天然稀疏不使用事件 deadline。L2 `pu/u` 不连续时单独记录
`L2_SEQUENCE_GAP` 并重新取 snapshot；该 gap 也只能在实际 bridge 后关闭。

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

1. 用固定私钥和 known_hosts 单独同步 Vultr `ready/` 的 manifest/SEALED inventory；
2. 按 manifest 的 `size_bytes` 将互斥 `data_path` 均衡分配给最多 4 条并行 rsync lane；
3. 所有 lane 成功后对 staging 数据 fsync，并校验 size 与 SHA-256；
4. staging/raw 同设备时原子 rename 到 `data/raw/collector=<id>/...`；跨设备时才复制到 `.partial`
   并再次校验，然后持久化本地 manifest；
5. 生成 ACK 并 rsync 到 Vultr `control/acks/`；
6. Vultr 只有在 ACK 的 chunk ID 和 SHA-256 都匹配后才删除 ready 数据。

两端必须持久记录 `LOCAL_DURABLE`、`ACK_PUSHED`、`ACK_VALIDATED` 和 `REMOTE_GC`；Vultr 删除
ready 前必须先写可恢复 transaction。损坏、未知或 hash 冲突 ACK 只能隔离和报警，不能删除 ready
或终止全部采集。详细合同见 [ACK 传输审计](transfer-ack-contract.md)。

lane 只允许读取互斥 files-from 清单，不能并发执行多个完整 `ready/` 镜像或共享删除阶段。任一 lane
失败时本轮不进入 ingest/ACK，已下载 partial 和旧 staging 保留到下一次重试。禁止使用
`--remove-source-files`。暂存镜像不是永久数据，下一次成功同步可删除已从 Vultr GC 的镜像文件；
`data/raw` 才是 107 上的永久原始数据。

## 1C1G 性能合同

目标机器为 1 vCPU、1GiB RAM、25GB 磁盘，不允许通过减少币数或降低采集频率达标。

- Docker：`1.00 CPU`、`768MiB`、`256 PIDs`；
- 正式基线使用 4 个 public shards；配置支持 8 条受控 A/B，满 24 个完整小时证据后才切换；
  运行期按上述每小时评估、5 分钟限幅批次无重启再均衡；
- WebSocket queue 为 16，单消息上限 2MiB；
- 1,000 档 snapshot 起点全局最小间隔 0.75 秒、最多 4 个 HTTP 在途，持续上限约 1,600
  request-weight/min；恢复 snapshot 排队/在途时暂停 discovery REST；
- OI 稳态仍为每币 30 秒，启动首轮只在 5 秒窗口内确定性错峰；realtime readiness 不等待完整
  universe discovery；
- raw queue 总字节上限 192MiB；70%/50% 是只观测、不阻塞接收的告警滞回水位；
- writer batch 上限 8,000 events 或 8MiB；
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
基础设施项目；容量和来源评估见 [L3 数据评估](2026-08-12-l3-data-assessment.md)。

## 跨日 L2 派生合同

每个 symbol 的日处理必须原子写出 `l2-checkpoint.json`。checkpoint 保存 UTC 日末 authoritative
盘口和 update ID，并保留恰好跨日的未完成 snapshot bridge。下一日只能从前一日、同 collector、
同 symbol 且恰好有效到 UTC 边界的 checkpoint 继承；第一条 diff 的 `pu` 不连续时立即结束继承。

transport gap OPEN 会使相关盘口失效。只有 snapshot bridge 已成功且同一 gap CLOSED 后才能重新
产生 validity。finalize 必须拒绝空、重叠、越出目标 UTC 日或缺少 checkpoint 的结果。107 必须从
formal start 所在 partial day 开始按 UTC 日期顺序处理，不能跳日提交。

## 日质量与去重合同

normalizer 从 sealed raw 的 `UNIVERSE_DECISION` 提取权威结构化版本、universe hash 和恰好 60 个
成员。调度器不接受人工 symbol 文件；L2 array 和 finalize 都从 `_NORMALIZED.json` 读取同一权威
集合，不能靠少传 symbol 缩小验收范围。

normalize marker 持久化最终 typed file identity；L2 projection 阶段直接继承该 identity，且只允许
一次 iter_batches 扫描并按 symbol 分区。每个 L2 task 只能打开自己的
单一 partition。array 按 partition 行数从大到小调度并最多并发 32 个单核 task，避免重币延迟到第二批
形成长尾。projection 遵守共享 `miry.market-data/l2-symbol-projection/v1` schema，是包含 duplicate、
未 bridge diff 与 gap 内行的性能投影，不是 canonical replay。它至少保留 7 日，之后只可在无活跃
pipeline job、normalized source 仍可重建且 marker/shard 校验通过时由显式 retention 删除。
站点必须配置硬字节预算；缺少预算不运行删除。retention 从最老日期开始，仅在预算超限时删除，
若最近 7 日本身超过预算则报警并 fail closed，不缩短最小窗口。

normalize 可并行执行每个独立 raw chunk 的 SHA、Parquet 解码、payload parse 与 typed 写入，但
dedup、formal-start 和 universe reducer 必须按 sealed manifest 的原始顺序串行提交结果。
`max_workers` 必须来自实测吞吐。v0.5.6 在同一 `32 CPU / 128GiB` allocation、同一 974 万事件
混合样本上的 4/8/16/31 workers 均值为 `105.30s`、`104.11s`、`103.34s`、`107.81s`。16 workers
相对 4 的改善不足 2%，31 workers 已因 ordered reducer 和共享存储竞争回退；当前 107 保持 4，
32 核配额用于可按 symbol 线性并行的 L2 array。
reducer 按列读取 identity 字段，并以 1 秒 bucket 维护精确 10 分钟去重窗口；同一秒内最多执行一次
expiry prune。normalizer 只解码解析所需 raw 列，typed Parquet 使用 zstd level 1，所有 typed 文件
原子 rename 完成后执行一次目录 durability barrier。这些优化不能省略 chunk SHA、Parquet schema、
Decimal、payload conflict、跨日 checkpoint 或 unknown stream 校验。

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
