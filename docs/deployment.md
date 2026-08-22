# v0.3.8 端到端部署顺序

正式链路必须按以下顺序上线，避免 collector 产生数据后没有可用的异地持久化端。

1. 在 107 准备 `~/.ssh/ft-data-puller`，将公钥安全传给 Vultr 管理员；
2. 在 Vultr 安装 `rsync`、`rrsync`、Docker 和 OpenSSH，运行 `deploy/vultr/install.sh`；
3. Vultr 运行 `configure-rsync.sh` 安装 107 公钥，并独立核对 host-key 指纹；
4. 107 保持旧 raw 原始字节不变，把旧 runtime 与控制证据移入只读 archive，再安装 v0.3.7 SIF；
5. 107 配置 `central.yaml`，先用 `rsync --list-only` 和一次前台 pull 验证；
6. 107 安装每分钟 cron，确认至少两个周期均成功；
7. Vultr 写入 immutable image digest 和正式 60 币配置；
8. 启动 collector，运行 `verify.sh`，等待日志中的 `FORMAL_COLLECTION_STARTED`；
9. 确认 Vultr `ready/` 出现 chunk、107 `data/raw` 出现同一 chunk、Vultr 收到 ACK；
10. 连续观察 24 小时资源指标、gap、ACK 延迟和剩余磁盘。

v0.3.8 启动后必须确认日志没有持续的 `subscription audit`、`targeted subscription recovery incomplete`
或 writer failure，
`control/collector-lease.json` 为当前 boot 的 `RUNNING`，且每分钟 ACK/ready 数量有进展。一次受控重启
应产生并在全源 ready 后关闭 `COLLECTOR_STOPPED_GAP`；不能通过删 gap 文件来获得质量通过。

详细命令分别见 [Vultr 手册](../deploy/vultr/README.md) 和
[107 手册](../deploy/campus-107/README.md)。

从 v0.3.0 升级 v0.3.1 不删除 raw，也不重置 Vultr generation/spool。尚未生成派生数据时，从
formal start 所在的首个 partial UTC day 开始按日期顺序提交；已有不可信的 v0.3.0 derived 可单独
删除后重算，不能删除对应 raw。

从 v0.3.1 升级 v0.3.2 只更新 Vultr edge image 和 `public_connection_shards: 4`。不得 clean
start，也不得重置 generation、formal-start、raw、ready、ACK、universe 或 collector lease。
受控重启产生的 stop gap 必须在所有 source 恢复后正常关闭。107 的 v0.3.1 pull 与 central
处理代码可继续使用；升级 107 仅用于统一版本标识，不是接收新 raw 的前置条件。

从 v0.3.2 升级 v0.3.3 只更新 Vultr edge image 和部署脚本。必须先记录当前 generation、
`universe_hash`、formal start 和 open gap，保留全部 raw、ready、writing、ACK、day-index、lease 与
universe 状态。受控重启会产生真实且不可删除的 stop gap；重启后要求 generation 和 60 币集合完全
不变、所有 route ready、open gap 回到 0、107 ACK 继续推进。v0.3.3 没有 central 合同变化，107
不需要升级。

从 v0.3.3 升级 v0.3.4 只更新 Vultr edge image 和部署文档。不得 clean start，不得删除仍为 OPEN 的
`STORAGE_EXHAUSTED_GAP` 或 `COLLECTOR_STOPPED_GAP`；它们必须由新进程达到完整 source readiness 后
正常关闭。升级前后核对 generation、60 币集合、`universe_hash` 和 formal-start 哈希完全不变。
v0.3.4 没有 central、rsync 或 raw schema 变化，107 不需要升级。

从 v0.3.4 升级 v0.3.5 使用 clean active paths，不做运行时兼容或状态迁移。先让 107 拉完 Vultr
ready，再停 cron；107 的旧 raw 保持原始字节和日期分区不变，旧 runtime 与 Vultr 的旧
control/evidence/gap 保存到只读 archive。确认 archive 完整后才重置 Vultr spool/control，并以冻结的
`7.0 / sequence 8` 60 币写入新 formal start。详细步骤见
[v0.3.5 结构化 universe clean start](v0.3.5-structured-universe-clean-start.md)。

只有执行正式 clean start 时，才必须先停 collector 和 107 cron，解析并人工核对每个绝对路径。
107 的旧 raw 不得删除或改写，旧 `runtime/derived` 只能原地移动到只读 archive；Vultr 的旧
`ready/writing/control` 只有在 107 已拉空 ready、接收 control tar 且通过 SHA-256 校验后才能删除。
仓库 checkout 和 SSH 私钥不在归档或删除范围内。

v0.3.6 在 v0.3.5 clean-start 合同上增加 107 sealed-day 幂等快速路径。首次发布 sealed day 仍验证
全部 chunk 大小与 SHA-256；之后只有本地 `SEALED.json` 与远端逐字节一致才跳过历史 raw 重哈希。
ACK 协议和 107 central 仍使用 v0.3.7 合同；v0.3.8 只替换 Vultr edge 与 universe 配置，不要求
107 升级。保留 active `7.0 / sequence 8`、formal start 和全部数据状态。两端 ACK 状态与故障恢复验收见
[ACK 传输审计合同](transfer-ack-observability.md)。
