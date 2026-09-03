# miry-data-collector

Binance USD-M 正式数据采集与重建流水线。v0.5.11 持续采集 60 个合约：
50 core、5 boundary、5 probe。

仓库、Python distribution、OCI image 和后续 release artifact 统一使用
`miry-data-collector`，Python import 根为 `miry`。CLI、systemd unit、环境变量和运行目录统一使用
`miry` 前缀；源码不包含旧 Python 包、命令别名或运行时兼容层。

```text
Binance -> Vultr collector -> Parquet/Zstd ready/
        -> restricted rsync over SSH -> 107 data/raw
        -> Slurm -> 107 data/derived
        -> ACK -> Vultr spool GC
```

Vultr 负责采集、完整 UTC 日流动性证据、排名和增量换币。107 只负责每分钟短时拉取、持久化校验、
ACK 和 Slurm 处理，不参与选币。成员不变的 UTC 日切不会停止数据源；替换一个币只在线更新
这个币涉及的订阅和 OI 任务，其余 59 个币保持在线。

源码按职责组织：

```text
src/miry/
  collector/  Vultr 实时连接、采集、gap、writer 和 spool
  orderbook/  collector 与 pipeline 共享的 canonical L2 bridge
  universe/   与部署位置无关的证据解析、排名和选币规则
  pipeline/   107 pull、normalize、L2 重建和 retention
  contracts/  两端共享的不可变数据合同
  cli/        命令行入口，只做参数解析和依赖装配
```

依赖只允许从运行层指向领域/合同层：collector 与 pipeline 共用纯 `orderbook` bridge，
`collector -> universe -> contracts`，`pipeline -> contracts`。领域层不反向导入运行层。完整权衡见
[架构与选币职责](docs/architecture.md)。

完整文档分类和适用性见 [文档索引](docs/README.md)。

当前 `7.0` 正式名单证据见
[结构化 Universe 正式起点](docs/v0.3.5-structured-universe.md)，规则、
边界语义和性能标准见 [采集与处理合同](docs/collection-contract.md)。部署入口：

- [Vultr 正式采集部署](deploy/vultr/README.md)
- [校园 107 拉取与处理部署](deploy/campus-107/README.md)
- [端到端部署指南](docs/deployment.md)

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
官方约束和定量依据见 [重连恢复调研](docs/2026-08-12-binance-reconnect-l2-recovery-research.md)。

v0.3.3 将静默 stream 的恢复精确到 `(stream, symbol)`，控制 ACK 采用独立 10 秒 deadline；局部刷新
失败时只重建所属 route，并为主动中断的 route 完整登记 gap，不再让 180 秒 refresh timeout 终止
全部 60 币。历史 gap 内未收到的事件不能补回，边界与生产清点见
[v0.3.3 数据缺口与订阅恢复说明](docs/v0.3.3-gap-recovery.md)。

v0.3.4 修复持久化 `STORAGE_EXHAUSTED_GAP` 跨进程恢复时重复启动 Binance sources 的 crash loop；
存储硬限制仍会先登记 gap 再暂停采集，空间恢复后只启动一次 sources，并在完整 readiness 后关闭 gap。
107 协议和数据合同没有变化。事故边界和升级要求见
[v0.3.4 存储恢复事故复盘](docs/v0.3.4-storage-recovery-incident.md)。

v0.3.5 将 universe 身份拆成 `core_generation.candidate_revision`：50 个 core 变化才增加
`core_generation` 并把 revision 归零，仅 boundary/probe 变化只增加 revision，成员完全不变不产生
新版本。两个分量均为整数，`decision_sequence` 提供全序，`universe_hash` 继续绑定精确 50/5/5。
本版本还修复 storage recovery 等待 source readiness 超时会终止 collector 的问题；超时后保持
storage gap OPEN、清理半启动 sources，并在下一轮重试。旧 generation raw 保持原始字节，
运行时代码不含兼容层；新旧实验由 formal-start 时间边界区分。部署边界见
[v0.3.5 结构化 Universe 正式起点说明](docs/v0.3.5-structured-universe.md)。

v0.3.6 避免 107 每分钟对已经发布且 manifest 完全一致的历史 sealed day 重复扫描全部 raw
SHA-256。某日首次发布时仍逐 chunk 校验，远端 sealed manifest 冲突仍 fail closed；该补丁不改变
edge、raw、universe 或正式 60 币身份。

v0.3.7 为 107 与 Vultr 增加持久 ACK transfer ledger 和原子状态快照；Vultr 使用可恢复 transaction
保护 ready GC，损坏、未知或 hash 冲突 ACK 被隔离，不再终止 collector 或触发重复全量扫描。
central 同时拒绝不安全 `collector_id`，磁盘最小可用空间保护线调整为 2 GiB。详见
[ACK 传输审计合同](docs/transfer-ack-contract.md)。

v0.3.8 针对 1C1G 正式采集器的重连风暴做生产优化：public route 改为按实测消息速率稳定加权
分片，WebSocket queue 增至 16；审计连续 3 次、定向刷新连续 2 次失败才重连，旧连接遗留任务
不能中断新连接；异常重试使用 30 秒封顶的指数退避。L2 snapshot 安全间隔由 2 秒降为 1 秒，
在当前 2,400 weight/min 观测限额下保留约一半预算。正式 `7.0` 名单、raw schema、rsync 和 107
处理合同不变。诊断、容量依据与验收见
[v0.3.8 采集器可靠性优化说明](docs/v0.3.8-collector-reliability.md)。

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
[v0.3.10 L2 快照调度优化说明](docs/v0.3.10-snapshot-scheduling.md)。

当前代码把静态路由基准升级为简单的长期流量观测：`message_rates` 表示每币每分钟 public
WebSocket 消息数，采集器按完整分钟计数、每 60 分钟保存一个峰值块，并只保留最近 24 块。状态少于
24 个完整块时继续使用配置基准；证据充足后使用最近 24 小时的观测峰值。动态路由调整必须采用
add-ready-remove 两阶段交接，不能为了均衡重启采集器或先删除旧订阅。该状态不改变 50/5/5
身份、raw 或 107 合同，详见
[Public WebSocket 流量均衡](docs/traffic-balancing.md)。

当前选币的点差指标使用 21 次、1 秒间隔全市场 bookTicker 的 q95，替代少量 depth/bookTicker
样本的最大值；3 次 depth snapshot 仅用于 10/50 bps 深度。该变化降低单个异常报价对横截面排名
的影响，不改变点差仅参与排名、不作为绝对拒绝门槛的规则。

v0.4.0 删除旧 Python 包和 `central`/`edge` 源码拓扑，改用职责明确的
`miry.collector`、`miry.pipeline`、`miry.universe`、`miry.contracts` 和 `miry.cli`。这是 Python
import/API 的破坏性变更，但不改变 raw schema、rsync/ACK、gap、universe identity 或磁盘数据布局；
两端必须升级 runtime，但禁止 clean start 或删除历史数据。详见
[v0.4.0 架构重构发布说明](docs/v0.4.0-architecture-refactor.md)。

v0.4.0 完成运行接口改名：命令统一为 `miry-data-*`，环境变量统一为 `MIRY_*`，Vultr 使用
`miry-data-collector.service`、`/opt/miry-data-collector`、`/etc/miry-data-collector` 和
`/srv/miry-data-rsync`。107 的永久 `data/raw`、`data/derived`、transfer ledger 以及 Vultr
现有 spool 均原地保留；部署只重命名目录和配置引用，不重写正式数据。

v0.4.1 为 4/8 public shard 生产 A/B 增加有界分片、宿主机 TCP/cgroup/PSI/HTTPS 诊断采样和
peer/订阅/snapshot 延迟日志。正式配置仍以 4 shards 开始，满 24 小时证据后才允许受控切到 8；
8-shard 最大 route 限制为 9 币。107 的质量拒绝日现在是可继续 checkpoint 链的终态，不再永久
阻断后续日期；质量成功门槛和 raw 合同不变。实验门禁见
[Public WebSocket 4/8 分片 A/B 评估](docs/2026-08-23-public-shard-ab-assessment.md)。
v0.4.1 支持 Binance 真实的 Unicode canonical symbol，例如 `币安人生USDT`；安全验证仍拒绝空白、
路径分隔符、控制字符和非 canonical 大小写。

v0.4.2 删除 WebSocket 热路径中每条消息一次的 receive task、timeout 和 wait-set 分配，改为每条
连接固定的 receiver、watchdog、subscription update 和 audit task。100k 真实消息单核回放由
`1.959s` 降至 `0.449s`；raw、gap、snapshot、universe、rsync/ACK 和 107 合同均不变。诊断与
验收记录见 [Public WebSocket 4/8 分片 A/B 评估](docs/2026-08-23-public-shard-ab-assessment.md)。

v0.5.0 删除旧部署位置式 CLI 名称：`miry-data-edge`、`miry-data-control` 和
`miry-data-release` 分别改为 `miry-data-collect`、`miry-data-override` 和 `miry-data-pin`，不保留
兼容别名。WebSocket 数据面与订阅控制面拆分；CLI 中的日质量判定和 pull 事务编排下沉到 pipeline。
raw、gap、universe、OCI/SIF 名称及 107 的 pull/process/symbols 命令不变。

v0.5.1 针对实测秒级成交洪峰，将 raw queue 从 64MiB 扩到 192MiB，writer batch 从
2,000 events / 2MiB 调整为 8,000 events / 8MiB，并在队列积压时使用无 timeout 分配的直接读取
fast path。70%/50% 水位只做观测，不阻塞接收；只有 192MiB 最终边界耗尽才产生
`ingest_overload`。部署新增目标镜像配置 preflight，必须在停止旧 collector 前通过，避免配置
schema 不匹配造成重启循环。raw schema、gap 语义、universe、ACK 和 107 处理合同不变。

v0.5.2 当时将 107 的三套历史处理入口收敛为一个幂等日流水线：seal ready 后自动提交 normalize、
一次 L2 partition、重币优先的 32 路单核 L2 array 和 finalize。每个 L2 task 只打开自己的 symbol
partition；当时的临时 partition 在 terminal finalize 后校验删除，此生命周期已由 v0.5.10 的
bounded projection 合同取代。partial
submission 会停止而不是自动重投。raw、质量门槛、checkpoint 与跨日依赖语义不变。

v0.5.3 将 normalize 的 chunk SHA、Parquet 解码、JSON parse 和 typed 写入改为 4 个进程并行，
主进程仍按 manifest 原顺序执行 dedup/checkpoint reducer。运行期 identity 使用 tuple，仅在跨日
checkpoint 边界计算稳定 SHA。08-23 真实大 chunk 的完整写入/dedup 基准由 `132.3s` 降到 `62.5s`
（2.12x），串并行 Arrow 表逐文件一致；8 workers 反而受共享存储限制慢于 4 workers。

v0.5.4 将 public route 流量重平衡移出 UTC 日切关键路径，继承当前 assignment 并以每 5 分钟最多
一对 symbol 的批次持续收敛；每小时重评最近 24 个完整块，高水位或连接恢复期间暂停。控制 ACK
deadline 从请求实际开始执行后计时，receiver 定期让出事件循环，raw admission 只在 writer
rotation 时关闭。网络或 Binance 仍可能断开单条连接，但可恢复迁移失败不再终止整个 collector。

v0.5.5 补齐迁移失败后的收敛路径：裁剪失败时先恢复最后提交的 route assignment，再继续限幅均衡；
正式成员更新失败时只保持变更 symbol 的 planned gap OPEN，并在 30/60/120/300 秒退避下后台重试。
UTC 日封存、其余稳定成员和 collector 进程不再受该可恢复错误影响。

v0.5.6 优化 107 normalize 的串行 reducer 和逐行解析：raw Parquet 只解码实际使用列，stream 使用
预校验字符串分派，去重改为列式扫描和 1 秒 expiry bucket，盘口价位不再创建临时校验字典；typed
Parquet 使用 zstd level 1，并把目录 fsync 合并为完成后的单一 barrier。4 parse workers 和全部
SHA、schema、Decimal、去重、checkpoint 校验保持不变。08-24 真实交易样本由 `113.26s` 降到
`86.15s`（快 23.9%），depth/trades/metadata 混合样本由 `70.23s` 降到 `57.03s`（快 18.8%）；
两组 Arrow 表逐文件一致。进一步在同一 `32 CPU / 128GiB` allocation 上用 974 万事件比较
4/8/16/31 workers，均值分别为 `105.30s`、`104.11s`、`103.34s`、`107.81s`；16 workers 的
不足 2% 改善不值得占用 17 CPU，31 workers 已回退，因此生产 normalize 保持 4 workers。

v0.5.7 将 107 的单连接 ready 镜像改为 4 条互斥 rsync lane：先同步 manifest/SEALED inventory，
再按 chunk 字节数做确定性 LPT 分配，所有 lane 成功后才进入原有 SHA、fsync、原子发布和 ACK。
任一 lane 失败不会清理已有 staging 或授权 Vultr GC。相同 60 秒公网 A/B 中，1/2/4 连接总吞吐为
`0.694/1.131/1.712 MB/s`，4 连接相对单连接提高约 147%。

v0.5.8 优化 107 durable 阶段：staging 和 raw 同属 `/home` 共享文件系统时，先对 staging 文件
fsync 并完成一次 SHA-256，再原子 rename 为永久 raw，删除 staging→raw 全量复制和复制后的第二次
读取。raw 已存在的 crash-recovery 路径不再下载数据，但仍重新校验 SHA 后补 ACK；跨设备部署自动
回退到复制路径。

v0.5.9 修复 snapshot HTTP 成功被误当作 L2 ready 的问题。collector 与 pipeline 共用 Binance 官方
`U <= lastUpdateId <= u`、后续 `pu == previous.u` 状态机；collector 先缓存 diff，stale/non-overlap
snapshot 按币最多重抓 5 次，仍失败才升级为 route reconnect。transport 恢复到全部 symbol bridge
之间使用独立 `L2_REANCHOR_GAP`，不会再把小时级 unanchored 窗口隐藏在已关闭的 transport gap 后。
事故证据和历史不可恢复边界见
[2026-08-25 L2 snapshot bridge 卡死诊断与修复合同](docs/2026-08-26-l2-snapshot-bridge-recovery.md)。

v0.5.10 将 normalize 后一次扫描生成的 per-symbol L2 Parquet 从临时 array 输入升级为有界可再生数据
projection：生产 `miry.market-data/l2-symbol-projection/v1` schema，marker v3 绑定完整因果 envelope、
typed source file set、逐文件 size/SHA-256、normalized marker hash 与精确 universe，finalize 不再自动
删除。projection 不是 canonical replay，至少保留 7 日；下游消费者只依赖该 schema，不依赖本仓库
代码或错误类型。旧 sealed/typed 数据保持不可变，历史 projection 通过独立 backfill 生成。

v0.5.11 保持 raw/gap/ACK wire contract 不变，修复 bootstrap role allocation，并降低 WebSocket
typed decode、traffic accounting、queue admission 和 L2 tracker 的逐事件 CPU/分配开销。该版本可以只
升级 Vultr collector；107 v0.5.9 puller 继续接收并 ACK，structured incident artifact 延后到两端同步
升级时发布。
