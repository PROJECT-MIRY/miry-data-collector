# 数据采集器开发周期复盘（2026-09-06）

## 结论

`miry-data-collector` 在 2026-08-10 以 `ft-shadow-data-plane` 启动，2026-09-04 发布
`v0.5.11`。25 天内共有 28 个正式 GitHub Release；主线从 loss-explicit 原型依次走过正式 60 币、
完整性与持久合同、1C1G 稳定性、模块化、107 性能、canonical L2 bridge 和 WebSocket 热路径。

最重要的不变量从首个提交延续至今：Vultr 保存原始事实，107 校验、重建和判定质量；无法证明
连续的区间必须写成显式 gap，不能用 REST 回填或降低质量门槛伪装完整。初始设计见
`2da4f31` 的 `docs/implementation-plan.md`，当前合同见
[collection-contract.md](collection-contract.md) 和 [README.md](../README.md)。

## 状态口径

| 状态 | 定义 | 边界 |
| --- | --- | --- |
| 正式发布 | 有不可变 tag，GitHub Release 非 draft/prerelease | 证明代码和产物已版本化，不证明已部署 |
| 候选提交 | 只在开发分支，未进入正式 tag | 可证明已实现/测试，不算生产能力 |
| 实际部署 | 服务器只读检查确认运行某个不可变 digest | 证明当时线上状态，仍需持续观察 |

当前三者并不一致：`origin/main` 停在 `v0.5.10/e39e2ae`，正式 `v0.5.11/a0f802a`
位于 `origin/codex/v0.5.11-vultr-compatible`；生产 Vultr 已运行 `v0.5.11`。证据来自
`git log --graph --all --simplify-by-decoration`、tag `v0.5.11` 和
[release.yml](../.github/workflows/release.yml)。

## 阶段总览

| 阶段 | 日期 | 版本 | 核心结果 |
| --- | --- | --- | --- |
| 原型与 canary | 08-10 | `v0.1.0` | 建立 loss-explicit 数据面和 20/40/50/60 canary |
| 正式采集起点 | 08-11--12 | `v0.2.0--v0.3.0` | 一次启动 50 core + 5 boundary + 5 probe |
| 完整性硬化 | 08-12--17 | `v0.3.1--v0.3.3` | 跨日 L2、stream liveness、coverage、局部恢复 |
| 持久状态合同 | 08-20--22 | `v0.3.4--v0.3.7` | storage、universe identity、ACK/GC 可恢复 |
| 1C1G 稳定化 | 08-23 | `v0.3.8--v0.3.10` | 路由、选币、snapshot 调度 |
| 架构重构 | 08-23--24 | `v0.4.0--v0.5.0` | 改名、模块拆分、data/control plane 分离 |
| 吞吐优化 | 08-24--26 | `v0.5.1--v0.5.8` | queue、normalize、Slurm、rsync、原子提升 |
| L2 正确性 | 08-26 | `v0.5.9` | HTTP 200 后仍须验证 snapshot bridge |
| 可复用处理视图 | 08-29 | `v0.5.10` | 持久化 per-symbol L2 projection |
| 采集热路径 | 08-28--09-04 | `v0.5.11` | role allocation、typed decode、冻结构建 |

## 重大时间线

### 2026-08-10：从数据合同开始（v0.1.0）

`2da4f31` 一次建立 raw envelope、Parquet/Zstd chunk、不可变 manifest、gap journal、Vultr
spool、中心 pull、normalize、L2 和 Slurm 骨架。chunk 生命周期为
`WRITING -> READY -> ACKED`；107 完成 SHA-256、fsync 和原子发布后才回 ACK；raw 不去重，
逻辑去重留给派生层。

首轮落地也暴露了环境问题：107 不能直接访问 GitHub，Apptainer 需要 module 加载，缺少
SquashFUSE 时必须先把 SIF 展开为 sandbox 并用 writable 模式执行；SSH key 名称和 cron 脚本执行位
也曾造成 pull 失败。这些不是行情协议 bug，却推动了 release 同时提供 SIF/SHA256、hash-named
安装目录、绝对路径 wrapper、`flock` cron 和部署前 `verify.sh`。当前操作合同见
[campus deployment](../deploy/campus-107/README.md)。

### 2026-08-11 至 08-12：跳过 canary，直接正式 60 币（v0.2.0、v0.3.0）

为尽快开始实验，`4a2214b` 删除 20/40/50/60 分级配置，从空状态一次启动 50/5/5。Vultr
改为自行排名和换币，107 只做 rsync、校验、ACK、Slurm；成员不变的日切不重启 source。
`424e95b` 同时补上单 stream 静默恢复，`4850b8e` 收窄 rsync SSH 权限。

`v0.3.0/393b090` 用 14 个完整 UTC 日和流动性/盘口证据重做 generation 1，并明确只能重建
market-by-price L2、不能声称 true L3；证据见该提交的 bootstrap 与 L3 assessment。

### 2026-08-12：四类完整性问题（v0.3.1、v0.3.2）

对 `v0.3.0` 的复查确认：跨日 L2 从空盘口开始；单 stream 静默和异常退出可能不显式；空
validity 文件也可能被标成 processed；同步 fsync、去重和冲突检测不够严格。

`7a6da84` 增加日末 L2 checkpoint 与跨日 `pu` 连续验证；`e1f7761` 增加
`(symbol, stream)` liveness、durable lease 和 recovered stop gap；`1f158aa` 要求每币
`valid_ratio >= 99.9%`、accounted 100%、零冲突，并补 stream-specific dedup。复现与验证见
[cross-day assessment](2026-08-12-cross-day-l2-reconstruction-assessment.md) 和
[integrity assessment](2026-08-12-data-integrity-risk-assessment.md)。

约 63 秒“连接恢复”又暴露指标混淆：订阅已恢复，只是同一 route 的 REST snapshot 仍在串行完成。
`fe248a2` 将 transport ready 与逐币 L2 ready 分开，并把 public route 从 2 条改为 4 条。
协议与验证见 [reconnect research](2026-08-12-binance-reconnect-l2-recovery-research.md)。

### 2026-08-17：局部异常不得击穿全局（v0.3.3）

`v0.3.2` 的 refresh 会动一个 symbol 的全部 streams，ACK 和 snapshot 共用 180 秒 deadline；
异常还能从 `asyncio.gather` 逃逸并终止全部 sources。根因是恢复范围和 supervisor 边界错误。

`e4aae20` 将恢复精确到 `(stream_type, symbol)`，ACK 独立 10 秒；连续失败才重建所属
route，其他 route、poller、writer 和 ACK/GC 不停。完整状态机见
[v0.3.3-gap-recovery.md](v0.3.3-gap-recovery.md)。

### 2026-08-19 至 08-20：磁盘保护后的 crash loop（v0.3.4）

空间低于 5GiB 时系统正确打开 `STORAGE_EXHAUSTED_GAP` 并停源，但重启流程和 storage monitor
先后调用 `SourceManager.start()`，触发 `sources already running`，restart counter 超过 400。
问题是恢复不幂等，不是磁盘保护本身。

`7fd1a52` 把 monitor 改成持久状态协调：仍不足则保持 gap OPEN 和 source 停止；空间恢复时
只启动一次，全部 ready 后关闭原 gap。生产数值和回归见
[v0.3.4-storage-recovery-incident.md](v0.3.4-storage-recovery-incident.md)。

### 2026-08-22：版本身份和 ACK 闭环（v0.3.5--v0.3.7）

`7f09e64` 用 `core_generation.candidate_revision + decision_sequence` 替代粗粒度单整数
generation：core 变动才增加整数部分，只有 boundary/probe 变化只加 revision，不变则不产生
decision。正式起点冻结为 7.0，旧 raw 不改写。见
[v0.3.5-structured-universe.md](v0.3.5-structured-universe.md)。

`2a66f1d` 避免 107 每分钟重 hash 不变的 sealed day。`6a415d1` 将传输变为
`LOCAL_DURABLE -> ACK_PUSHED -> ACK_VALIDATED -> REMOTE_GC`，加入两端 append-only ledger、
restart-safe GC transaction 和坏 ACK 隔离；磁盘保护线降为 2GiB。见
[transfer-ack-contract.md](transfer-ack-contract.md)。

### 2026-08-23：1C1G、选币和 snapshot 三条线同时收敛（v0.3.8--v0.3.10）

生产 16 小时出现 392 次连接失败，热点 route 占 294 次；queue 仅 4.5%、无网卡 drop，但 CPU
P95 接近 0.9 core、lag P95 约 0.83 秒。外部抖动被热点 route、cgroup throttle、激进 deadline
和串行 snapshot 放大。`7c90b53` 改为实测消息率分片、queue 16、3 次 audit/2 次 refresh
门禁、connection generation 隔离和指数退避。见
[v0.3.8-collector-reliability.md](v0.3.8-collector-reliability.md)。

六周市场研究没有确认持续的全市场 regime shift，却证明 CV 会惩罚广泛向上的活动冲击，绝对
成交额/交易数/spread/depth 门槛也会随市场水平失效。`7fb3be4` 删除绝对流动性硬门禁，改为
五指标横截面最弱项排名；28 日基线和 1/3/7 日 breadth 只节流轮换，不直接选币。见
[market-regime research](2026-08-23-market-regime-research.md)。

当时 837 次 transport recovery 平均 5.562 秒，557 次 L2 reanchor 平均 32.837 秒、最长
115.571 秒。全局 snapshot lock 包住完整 HTTP I/O，慢请求串行阻塞所有币。`c17e41f`/
`6c03ba8` 让锁只预约每 0.75 秒的请求起点，最多 4 个 HTTP 在途，并分离 realtime/discovery
readiness。见 [snapshot scheduling](v0.3.10-snapshot-scheduling.md)。

### 2026-08-23 至 08-24：改名和模块化（v0.4.0--v0.5.0）

`efa788a` 将项目改名为 `miry-data-collector`；`ebbdd61`/`7c7d3c2` 删除旧
`ft_shadow_data_plane` 包、CLI 和兼容层，拆为 `collector/pipeline/universe/contracts/orderbook/cli`。
这是 Python API 的破坏性变更，但 raw、gap、ACK、universe 和磁盘数据不变。跨 route 换币也统一
为 add-ready-remove。见 [v0.4.0 architecture](v0.4.0-architecture-refactor.md)。

`v0.4.1` 增加 4/8 shard 所需的 TCP/cgroup/PSI 诊断和 Unicode canonical symbol。`776c93e`
发现每消息创建 receive task/timeout/wait-set，改为每连接固定 receiver/watchdog/control task；
100k 真实 payload 本地回放由 1.959 秒降至 0.449 秒。见
[public shard assessment](2026-08-23-public-shard-ab-assessment.md)。

`32c3dea`/`de703a9` 再分离 WebSocket data/control plane，让 CLI 只装配依赖；当前依赖方向由
[test_architecture.py](../tests/test_architecture.py) 约束。

### 2026-08-24 至 08-26：Vultr 与 107 分别优化（v0.5.1--v0.5.8）

| 版本 | 问题 | 措施 |
| --- | --- | --- |
| `v0.5.1` | 约 57 万 msg/min 撞穿 64MiB queue，15 个 overload gap | 192MiB queue、8k/8MiB batch、无等待 fast path、升级 preflight；见 [gap assessment](2026-08-25-gap-root-cause-and-v0.5.4-assessment.md) |
| `v0.5.2` | 107 三套入口、L2 重复扫描 | 删除旧入口，统一幂等 daily Slurm pipeline；见 [submit-ready-day.sh](../deploy/campus-107/submit-ready-day.sh) |
| `v0.5.3` | normalize 串行 parse | 4 进程并行，主进程保持 manifest 顺序 reducer；见 `70637d0` |
| `v0.5.4` | 空 assignment 使无变化日切搬迁约 41 币 | 继承 assignment，rebalance 移出日切，每批一对；见 [gap assessment](2026-08-25-gap-root-cause-and-v0.5.4-assessment.md) |
| `v0.5.5` | partial route update 无法收敛 | 恢复 committed assignment，changed-symbol gap 保持 OPEN 并后台重试；见 `206cd41` |
| `v0.5.6` | normalize reducer/逐行字典成本高 | 列式 identity、expiry bucket、合并 fsync；真实样本快 18.8%--23.9%，见 `16062f3` |
| `v0.5.7` | 单 rsync 连接吞吐低 | 4 条互斥 lane，60 秒 A/B 从 0.694 到 1.712 MB/s；见 [ACK contract](transfer-ack-contract.md) |
| `v0.5.8` | staging 到 raw 复制并二次 SHA | 同文件系统 fsync/SHA 后原子 rename；跨设备才复制；同上 |

### 2026-08-25 至 08-26：HTTP 200 不等于 L2 ready（v0.5.9）

APTUSDT 出现约 2 小时 18 分、CRVUSDT/BSBUSDT 各约 5 小时 31 分的 L2 无效区间。旧代码在
snapshot HTTP 200 后就宣布 ready，没有验证 snapshot 与缓存 diff overlap；stale snapshot 也不
重抓，只能等下一次 route 重建碰巧恢复。

`1ca90c3` 增加在线/离线共用的 canonical
[bridge.py](../src/miry/orderbook/bridge.py)：首条 diff 必须满足
`U <= lastUpdateId <= u`，随后 `pu == previous.u`；stale snapshot 按币重抓最多 5 次，
失败才 route reconnect，`L2_REANCHOR_GAP` 全程 OPEN。历史区间无法追认。见
[L2 bridge incident](2026-08-26-l2-snapshot-bridge-recovery.md)。

### 2026-08-28 至 08-29：107 projection（v0.5.10）

`70e560d` 将单币 L2 输入从临时 partition 变成持久、可再生 projection；`50f183f` 绑定共享
schema；`e39e2ae` 加入 marker v3、source envelope、文件 size/SHA、升级和保留规则。projection
不是 canonical raw。见 [projection schema](../schemas/l2-symbol-projection-v1.schema.json) 和
[l2_projection.py](../src/miry/contracts/l2_projection.py)。

`v0.5.10` 是正式 Release，也是当前 `origin/main`，但没有部署到 Vultr 或 107；因此 projection
是已发布能力，不是当前 107 生产能力。

### 2026-08-28 至 09-06：bootstrap 与接收热路径（v0.5.11）

旧 bootstrap 先从 `recent + mature fallback` 选 probe，再选 core；recent 不足时 BTC/ETH/XRP
等成熟高排名币会被 probe 抢走。`2184dc5` 改为 mature Top50 core、51--55 boundary，再取
recent probe，不足只从第 56 名以后补。见 [test_selection.py](../tests/test_selection.py)。

2026-08-28 的 26 次 public route failure 发生在最高 66.4 万 msg/min、CPU 90%--96%、lag
281ms、Recv-Q 34.6MiB 的窗口；raw queue 仅 6.1%，无 hard reject、OOM 或 TCP timeout 增长。
根因更符合“上游突发触发、本机单核处理饥饿放大”，不是 writer、磁盘或单个 Binance backend。

`d96f065` typed decode 只物化 stream/symbol/event/`U/u/pu`，raw bytes 原样落盘；
`b74ac42` 复用 timestamp、queue classification、traffic bucket 和 L2 tracker。`1a219a1`
明确 50/60/70 万每分钟只是开发机 synthetic load，不能证明 Vultr EPYC 容量。见
[public receive overload](2026-08-30-public-receive-overload.md)。

### 2026-09-01 至 09-06：生产 gap 推动 v0.5.11 上线

Vultr 的 sealed manifest 在 09-01 至 09-06 09:01 UTC 共记录 210 个唯一 gap ID，全部闭合；
这个数字不是 210 次独立事故，因为一次 public route 断线通常分别产生 transport 与 L2 reanchor
gap，一次全市场异常也可能按 60 个 symbol 分别登记。

- 09-01 market route 静默，60 个 `markPrice` stream 同时超过 liveness deadline；
- 09-03 08:38 UTC 四条 public connection 仍在线，但 60 个币几乎同时出现 `pu/u` 不连续，逐币
  打开 `L2_SEQUENCE_GAP`，最长约 44.93 秒；
- 09-04 12:30--12:49 UTC 出现连接恢复风暴：public 消息率约 48 万--67 万/min，单核 CPU 接近
  饱和，event-loop lag 为 70--207ms，最长 L2 无效约 149.93 秒；queue 仍低且无 hard rejection，
  继续支持“本机接收饥饿放大外部抖动”的判断；
- 09-06 上线前还有两次 public-2 短断线；v0.5.11 readiness 完成后，截至本复盘核查没有新增运行
  故障 gap。

因此 v0.5.11 的发布动机不是 synthetic load 自身，而是 v0.5.9 的生产 CPU/lag/Recv-Q 与 gap
证据；synthetic load 只验证测试工具和热路径不会立即失效，不能替代同规格真实流量验收。

## 发布、候选与实际部署

`v0.5.11` 于 **2026-09-06 07:19 UTC 仅部署 Vultr**，107 未升级。09:13 UTC 只读核验为：

- systemd active，容器 `restart_count=0`、`OOM=false`；
- OCI digest 为 Release 对应的
  `sha256:e8b8b91964bd30afec230b6fae79d794cb8dd4106bfbdd765686c75bb75cd5e7`；
- universe `7.11 / sequence 19`，角色 50/5/5，open gaps 为 0；
- Vultr transfer status 为 `state=ok`，无 hash mismatch、坏/未知 ACK 或 pending transaction，
  说明旧 107 puller 仍能按兼容 wire contract 回 ACK。

部署 gap `gap-5fe2af390da04a4fafdb7e06de2de195` 的 OPEN/CLOSED 时间差为
**58.572380226 秒**。这是保留的 `COLLECTOR_STOPPED_GAP`，不是被隐藏的数据连续性。只读证据
位于 Vultr `control/acked-manifests/date=2026-09-06/`；Vultr-only 兼容边界见 `733b157`。

`codex/structured-incidents-v0.5.10` 的 `c5d8521`/`ea8a6c1` 实现四层
`FailureObservation -> RecoveryAction -> GapEvent -> IncidentEvent`，并新增
`application/vnd.miry.incident+json`。107 v0.5.9 的 `ContentType` 不接受该类型，因此
`733b157` 明确延后到两端同步升级；它不属于 `v0.5.11`。role/typed decode/hot-path 补丁以
`2184dc5`、`d96f065`、`b74ac42` 等价移植到兼容分支。

## 流程反思

做对的部分：

1. 从第一天保留 raw bytes、hash、manifest 和 gap，使派生错误可重算，而不必重采。
2. `v0.4.0` 破坏性清理源码时没有改写 raw/gap/ACK；代码整洁与历史数据保留被分开处理。
3. 事故保留 CPU、lag、Recv-Q、queue、snapshot 和 ACK 证据；测试从 11 个文件增至 23 个，
   当前为 222 passed，Ruff/mypy 通过。

不足与改进：

1. 16 个版本挤在四天内，`v0.5.4` 后约 31 分钟即有 `v0.5.5`；应让非 P0 修复批量发布，
   并至少观察一个峰值窗口和完整 UTC 日。
2. storage 重启、ACK GC crash、partial route update、stale snapshot 等跨阶段状态最初多在生产
   暴露；今后恢复逻辑先做 crash/cancel/replay 故障注入。
3. synthetic load 曾被说成“回放”；`1a219a1` 已纠正。以后热路径发布需隔离同规格 1C1G 的
   captured-payload 测试，开发机合成数据只证明门禁实现。
4. tag、default branch、production 三套“最新”已经分叉。发布后应让 `origin/main`、tag、
   GitHub Release 与生产 digest 收敛，并在 release note 写明 Vultr/107 部署矩阵。
5. 新 wire content type 必须两端同步支持；structured incident 在此之前不能单边上线。

## 当前遗留

- [src/miry/__init__.py](../src/miry/__init__.py) 仍硬编码 `__version__ = "0.5.8"`，而
  `pyproject.toml` 和线上 distribution metadata 是 `0.5.11`。这是版本可观测性 bug；下一版应
  删除双版本源并加镜像内一致性测试。
- `origin/main` 尚未包含生产 `v0.5.11`，后续从 main 开发可能遗漏已部署修复。
- structured incident 延后，当前 gap 能证明“何时无效”，但 `no close frame` 等故障仍不能仅凭
  现有证据归因到 Binance、公网、Vultr 内核或 Python 调度。
- `v0.5.10` projection 未部署到 107；升级 107 时需要与 incident content type 一起明确兼容顺序。
- `v0.5.11` 只能减少本机热路径成本，不能消除真实公网/Binance 断连；单 Vultr 仍是故障域，
  每次升级也会产生显式 stop/reanchor gap。[deployment.md](deployment.md)
- 部署初验和 222 个测试不能替代完整 UTC 日及高流量生产观察。

## 核查方法

本复盘只使用 `git log/tag/show/diff`、GitHub Release、仓库 README/docs/deploy/tests，以及
2026-09-06 的 Vultr 只读 systemd、Docker inspect、gap manifest 和 transfer status。没有连接 107，
没有部署、重启或修改远端。当前 checkout 验证为 222 passed，Ruff 通过，mypy 对 60 个源码文件
无报错。除明确列出的 Vultr 读回外，本文不从 tag 反推某历史版本一定曾在线运行。
