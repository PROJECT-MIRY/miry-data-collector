# miry-data-collector

Binance USD-M 正式数据采集与重建流水线。v0.3.10 持续采集 60 个合约：
50 core、5 boundary、5 probe。

仓库、Python distribution、OCI image 和后续 release artifact 统一使用
`miry-data-collector`。为保持现有生产部署与历史数据合同稳定，Python import
`ft_shadow_data_plane`、`ft-data-*` CLI、systemd unit 和运行目录名称不变。

```text
Binance -> Vultr collector -> Parquet/Zstd ready/
        -> restricted rsync over SSH -> 107 data/raw
        -> Slurm -> 107 data/derived
        -> ACK -> Vultr spool GC
```

Vultr 负责采集、完整 UTC 日流动性证据、排名和增量换币。107 只负责每分钟短时拉取、持久化校验、
ACK 和 Slurm 处理，不参与选币。成员不变的 UTC 日切不会停止数据源；替换一个币只在线更新
这个币涉及的订阅和 OI 任务，其余 59 个币保持在线。

当前 `7.0` 正式名单证据见
[结构化 universe clean start](docs/v0.3.5-structured-universe-clean-start.md)，规则、
边界语义和性能标准见 [实施合同](docs/implementation-plan.md)。部署入口：

- [Vultr 正式采集部署](deploy/vultr/README.md)
- [校园 107 拉取与处理部署](deploy/campus-107/README.md)
- [端到端部署顺序](docs/deployment.md)

本地验证：

```bash
uv sync --dev
uv run ruff check src tests
uv run mypy src
uv run pytest -q
```

`ready/` 中的文件只有在 107 校验 SHA-256、原子写入 `data/raw` 并回传 ACK 后才会由
Vultr 删除。任何无法证明完整性的时间段都必须用显式 gap 事件记录。

v0.3.1 在 107 派生处理中持久化每个 symbol 的日末 L2 checkpoint，次日先验证
`connection_id` 与 `pu` 连续性再继承盘口；transport/sequence gap 会阻断继承，恢复 snapshot
bridge 与 gap close 后才重新声明有效。正式 raw 合同和 generation 1 名单没有变化。

v0.3.2 把异常重连的 transport recovery 与 L2 snapshot readiness 分开：订阅 ACK 和每个受监控
stream 的首事件证明 raw 恢复后即关闭 transport gap，但每个币仍须独立完成 snapshot bridge 才能
重新进入 L2 `VALID`。正式 public 路由使用 4 个分片，降低单连接故障的币种范围和最慢重锚时间。
官方约束和定量依据见 [重连恢复调研](docs/binance-reconnect-recovery-research-2026-08-12.md)。

v0.3.3 将静默 stream 的恢复精确到 `(stream, symbol)`，控制 ACK 采用独立 10 秒 deadline；局部刷新
失败时只重建所属 route，并为主动中断的 route 完整登记 gap，不再让 180 秒 refresh timeout 终止
全部 60 币。历史 gap 内未收到的事件不能补回，边界与生产清点见
[v0.3.3 完整性调研](docs/v0.3.3-gap-integrity-recovery-research-2026-08-17.md)。

v0.3.4 修复持久化 `STORAGE_EXHAUSTED_GAP` 跨进程恢复时重复启动 Binance sources 的 crash loop；
存储硬限制仍会先登记 gap 再暂停采集，空间恢复后只启动一次 sources，并在完整 readiness 后关闭 gap。
107 协议和数据合同没有变化。事故边界和升级要求见
[v0.3.4 存储恢复事故记录](docs/v0.3.4-storage-recovery-incident-2026-08-20.md)。

v0.3.5 将 universe 身份拆成 `core_generation.candidate_revision`：50 个 core 变化才增加
`core_generation` 并把 revision 归零，仅 boundary/probe 变化只增加 revision，成员完全不变不产生
新版本。两个分量均为整数，`decision_sequence` 提供全序，`universe_hash` 继续绑定精确 50/5/5。
本版本还修复 storage recovery 等待 source readiness 超时会终止 collector 的问题；超时后保持
storage gap OPEN、清理半启动 sources，并在下一轮重试。旧 generation raw 保持原始字节，
运行时代码不含兼容层；新旧实验由 formal-start 时间边界区分。部署边界见
[v0.3.5 结构化 universe clean start](docs/v0.3.5-structured-universe-clean-start.md)。

v0.3.6 避免 107 每分钟对已经发布且 manifest 完全一致的历史 sealed day 重复扫描全部 raw
SHA-256。某日首次发布时仍逐 chunk 校验，远端 sealed manifest 冲突仍 fail closed；该补丁不改变
edge、raw、universe 或正式 60 币身份。

v0.3.7 为 107 与 Vultr 增加持久 ACK transfer ledger 和原子状态快照；Vultr 使用可恢复 transaction
保护 ready GC，损坏、未知或 hash 冲突 ACK 被隔离，不再终止 collector 或触发重复全量扫描。
central 同时拒绝不安全 `collector_id`，磁盘最小可用空间保护线调整为 2 GiB。详见
[ACK 传输审计合同](docs/transfer-ack-observability.md)。

v0.3.8 针对 1C1G 正式采集器的重连风暴做生产优化：public route 改为按实测消息速率稳定加权
分片，WebSocket queue 增至 16；审计连续 3 次、定向刷新连续 2 次失败才重连，旧连接遗留任务
不能中断新连接；异常重试使用 30 秒封顶的指数退避。L2 snapshot 安全间隔由 2 秒降为 1 秒，
在当前 2,400 weight/min 观测限额下保留约一半预算。正式 `7.0` 名单、raw schema、rsync 和 107
处理合同不变。诊断、容量依据与验收见
[v0.3.8 collector 可靠性记录](docs/v0.3.8-collector-reliability-2026-08-23.md)。

v0.3.9 删除成交额、交易数、CV、点差和 depth 的绝对选币门槛，改为五指标横截面最弱项优先
排名。独立 market context 使用 28 日基线与最近 1/3/7 日识别广泛活动冲击：pending 时冻结
普通轮换，7 日确认后恢复评估，停牌替换仍立即执行。升级保留正式 `7.0`、raw、ACK、gap 与
formal start，首次 35 日证据完整前不会改变名单。

v0.3.10 保留 v0.3.8 的路由均衡和恢复门禁以及 v0.3.9 的选币规则，把 L2 snapshot 调度从
“限速锁覆盖整个 HTTP 请求”改成“只预约请求起点”：全局每 0.75 秒启动一个 1,000 档
snapshot，同时最多允许 4 个慢 HTTP 在途，恢复队列存在时暂停低优先级 discovery REST。这样
仍把 snapshot 控制在约 1,600 weight/min，且 4 个 route 同时恢复时不再因单个慢请求把所有
symbol 串行阻塞。实时 source readiness 与约 6 分钟的 universe discovery readiness 分离，OI
首轮在 5 秒内错峰完成，正式重启不再等待整轮选币证据。正式名单、raw、rsync/ACK 和 107 合同
不变。设计与验收见
[v0.3.10 snapshot 调度记录](docs/v0.3.10-snapshot-scheduler-2026-08-23.md)。

当前代码把静态路由基准升级为简单的长期流量观测：`message_rates` 表示每币每分钟 public
WebSocket 消息数，采集器按完整分钟计数、每 60 分钟保存一个峰值块，并只保留最近 24 块。状态少于
6 个完整块时继续使用配置基准；证据充足后仅在下一次进程启动时采用观测峰值，运行中不迁移已有
symbol。该状态不改变 50/5/5 身份、raw 或 107 合同，详见
[public 流量均衡](docs/traffic-balancing.md)。

当前选币的点差指标使用 21 次、1 秒间隔全市场 bookTicker 的 q95，替代少量 depth/bookTicker
样本的最大值；3 次 depth snapshot 仅用于 10/50 bps 深度。该变化降低单个异常报价对横截面排名
的影响，不改变点差仅参与排名、不作为绝对拒绝门槛的规则。
