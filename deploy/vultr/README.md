# Vultr 正式采集部署

本手册适用于 `167.179.115.243` 上的 v0.3.9 collector。数据根为
`/srv/ft-data-rsync`，collector 和受限传输账户都使用 UID/GID 10001。

## 1. 前置条件

以 root 安装 Docker Engine、Compose plugin、OpenSSH、rsync 和 rrsync，并确认系统时钟同步：

```bash
docker version
docker compose version
rsync --version
rrsync -h
timedatectl status
```

防火墙只需允许 SSH 管理来源和 107 的出口地址。collector 只向 Binance 发起出站 HTTPS/WSS。

## 2. 安装目录和服务

在 v0.3.9 仓库根目录执行：

```bash
sudo ./deploy/vultr/install.sh
```

安装器创建：

```text
/srv/ft-data-rsync/ready
/srv/ft-data-rsync/writing
/srv/ft-data-rsync/control/acks
/srv/ft-data-rsync/control/applying-acks
/srv/ft-data-rsync/control/rejected-acks
/srv/ft-data-rsync/control/transfer-ledger
/srv/ft-data-rsync/control/universe
/etc/ft-shadow-data-plane/edge.yaml
/etc/ft-shadow-data-plane/edge.env
/opt/ft-shadow-data-plane/deploy/vultr
```

它不会启动 collector，也不会改写已经存在的配置。

## 3. 配置受限 rsync

把 107 的 `~/.ssh/ft-data-puller.pub` 放到 Vultr 的临时管理路径，然后执行：

```bash
sudo /opt/ft-shadow-data-plane/deploy/vultr/configure-rsync.sh \
  /root/ft-data-puller.pub
```

脚本把 key 安装到 root 管理、`data-puller` 只读的 `AuthorizedKeysFile`，强制执行受限网关。
网关将两种操作分别限制为：

```text
读取 ready/         -> /usr/bin/rrsync -ro /srv/ft-data-rsync/ready
写入 control/acks/ -> /usr/bin/rrsync -wo -no-del /srv/ft-data-rsync/control/acks
```

该 key 没有交互 shell、TTY、端口转发或 X11 权限，不能写采集数据、读取其他目录或删除
ACK。不要为
该账户叠加其他 `ForceCommand` 或 chroot，它们会阻止 rsync 的远端进程。升级时，脚本会
识别并禁用旧版仓库生成的 `ft-data-puller.conf`；若发现其他冲突配置，它会在 reload 前失败。

显示并通过独立渠道发给 107 操作者核对 host key：

```bash
ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

## 4. 配置正式 60 币和镜像

`/etc/ft-shadow-data-plane/edge.yaml` 必须使用仓库 v0.3.9 schema。核对三个角色为 50/5/5、
`bootstrap_evidence_sha256` 与正式报告一致、`automation_enabled: true`、public shards 为 4，
queue 为 64MiB，并且 `public_symbol_load_weights` 恰好覆盖当前 60 币。不要加入旧字段。

在 `/etc/ft-shadow-data-plane/edge.env` 中写 immutable digest：

```text
EDGE_IMAGE=ghcr.io/50829/ft-shadow-data-plane@sha256:<release-digest>
EDGE_DATA_ROOT=/srv/ft-data-rsync
EDGE_CONFIG=/etc/ft-shadow-data-plane/edge.yaml
```

拉取并检查架构：

```bash
set -a
. /etc/ft-shadow-data-plane/edge.env
set +a
docker pull "$EDGE_IMAGE"
docker image inspect "$EDGE_IMAGE" --format '{{json .RepoDigests}}'
```

Compose 已固定 0.90 CPU、768MiB RAM、256 PIDs、只读 rootfs 和日志轮换。

## 5. v0.3.9 原地升级

本版本禁止 clean start。必须保留 raw、ready、writing、ACK、transfer ledger、gap、
`formal-start.json` 和整个 `control/universe`。停止服务前记录权威状态：

```bash
sudo systemctl stop ft-shadow-data-plane.service || true
sudo cp -a /etc/ft-shadow-data-plane/edge.yaml \
  /etc/ft-shadow-data-plane/edge.yaml.before-v0.3.9
sudo sha256sum \
  /srv/ft-data-rsync/control/formal-start.json \
  /srv/ft-data-rsync/control/universe/active.json
sudo jq '{core_generation,candidate_revision,decision_sequence,
          universe_version,universe_hash}' \
  /srv/ft-data-rsync/control/universe/active.json
```

安装新 deploy 文件后，只清理旧选择器配置字段并加入新字段；不要覆盖 50/5/5 名单、版本号和
bootstrap hash：

```bash
sudo ./deploy/vultr/install.sh
sudoedit /etc/ft-shadow-data-plane/edge.yaml
```

`universe` 配置应包含 `market_context_baseline_days: 28`、
`market_context_change_ratio: 1.25`、`market_context_breadth_ratio: 0.70`、
`market_context_minimum_instruments: 60`、`depth_mature_candidate_count: 200` 和
`mature_pool_warning_size: 65`。Pydantic 拒绝未知字段，因此旧选择器字段必须删除干净。

## 6. 验证和启动

首次启动前，107 必须已能执行 `rsync --list-only`。然后：

```bash
sudo systemctl enable ft-shadow-data-plane.service
sudo systemctl start ft-shadow-data-plane.service
sudo /opt/ft-shadow-data-plane/deploy/vultr/verify.sh
```

观察启动：

```bash
journalctl -u ft-shadow-data-plane.service -f
```

只有出现以下日志后才进入正式时间范围：

```text
FORMAL_COLLECTION_STARTED ... universe_version=<major.revision> decision_sequence=<n> symbols=60
```

原地升级读取原有 `7.0 / sequence 8`，不会重新写 formal start。首次 discovery 补齐 35 个完整
UTC 日后才评估；缺证据或 market context pending 时保持当前名单。成交额、交易数、点差和 depth
仅参与横截面排名，不再触发绝对门槛拒绝。

同时确认：

```bash
sudo test -s /srv/ft-data-rsync/control/formal-start.json
sudo jq '{core_generation,candidate_revision,decision_sequence,universe_version,
          core,boundary,probe,universe_hash}' \
  /srv/ft-data-rsync/control/universe/active.json
sudo find /srv/ft-data-rsync/ready -type f | head
```

## 7. 日常检查

```bash
systemctl status ft-shadow-data-plane.service
docker stats --no-stream
docker inspect ft-shadow-data-plane-collector-1 \
  --format 'oom={{.State.OOMKilled}} restarts={{.RestartCount}}'
df -h /srv/ft-data-rsync
find /srv/ft-data-rsync/ready -type f | wc -l
find /srv/ft-data-rsync/control/acks -type f | wc -l
sudo jq . /srv/ft-data-rsync/control/transfer-status.json
sudo find /srv/ft-data-rsync/control/rejected-acks -maxdepth 1 -type f -print
journalctl -u ft-shadow-data-plane.service --since '24 hours ago' \
  | grep -E 'GAP|collector status|FORMAL_COLLECTION_STARTED|planned universe'
```

`control/universe/observations` 保存每日增量 Kline 和盘口证据，`evaluations` 保存 mature/recent
池数量、market context 与冻结原因，`decisions` 保存实际 decision。mature 池小于 65 会报警。正常日切没有
成员变化时不会出现计划 gap；若发生替换，gap 只应列出移除和新增币。

正式完整性参数为：public stream 30 秒、`markPrice@1s` 15 秒、订阅集合审计 60 秒、单次响应 deadline
20 秒且连续 3 次无响应才重连、定向刷新连续 2 次失败才重连、前一日 seal grace 150 秒、collector
lease heartbeat 30 秒。订阅集合不一致、连续审计无响应、刷新后没有对应
stream 新事件、`pu/u` 不连续或异常重启都会留下 scoped gap。检查 lease 与 open gap：

```bash
sudo jq . /srv/ft-data-rsync/control/collector-lease.json
sudo find /srv/ft-data-rsync/control/open-gaps -type f -maxdepth 1 -print
```

异常重连时，`connection transport recovered ... recovery_s=` 表示订阅 ACK 与受监控 stream
首事件已经证明 raw transport 恢复；`connection snapshot ready ... reanchor_s=` 表示该路由的
所有快照已经捕获。两者之间 L2 仍由 central 保持无效，直到每个币自己的 snapshot bridge
通过，不能把 transport 日志解释为盘口已经有效。

暂停自动选币时，把 `automation_enabled` 改为 `false` 并重启 collector；采集仍继续，core
不能通过手工 override 直接修改。

## 8. 24 小时性能验收

每分钟 collector status 日志包含 RSS、Arrow bytes、CPU time、steal、event-loop lag、queue
ratio、writer idle 和 finalize 时间。按照 [实施合同](../../docs/implementation-plan.md) 计算
p95/p99。若 OOM、RSS 峰值超过 700MiB、CPU p95 超过 80%、queue 连续过高、磁盘低于 2GiB
或出现性能 gap，不得通过减少 60 币或降低频率规避；应先停止并扩容或优化。

## 9. 升级

停止服务、备份配置文件、重新运行 installer 和 verify，再启动。v0.3.x 内升级保留
`control/universe` 与未 ACK spool；只有明确执行 clean start 才删除它们。

从 v0.3.0 升级 v0.3.1 时不要执行第 5 节 clean start；保留 generation、formal-start、ready、ACK
和 universe evidence。服务重启会产生显式停机 gap，107 继续按 hash 幂等拉取。

从 v0.3.1 升级 v0.3.2 同样禁止 clean start，并把现有配置中的
`public_connection_shards` 从 2 改为 4。v0.3.2 没有修改 107 pull/central 行为，107 已安装的
v0.3.1 可继续运行。服务重启后必须看到 transport recovery、snapshot ready、collector status，
并确认 `open-gaps` 为空。

从 v0.3.2 升级 v0.3.3 时同样禁止 clean start。升级前保存以下只读基线：

```bash
sudo cp -a /etc/ft-shadow-data-plane/edge.env /etc/ft-shadow-data-plane/edge.env.v0.3.2
sudo jq '{generation,core,boundary,probe,universe_hash}' \
  /srv/ft-data-rsync/control/universe/active.json
sudo sha256sum /srv/ft-data-rsync/control/formal-start.json \
  /srv/ft-data-rsync/control/universe/active.json
sudo find /srv/ft-data-rsync/control/open-gaps -maxdepth 1 -type f -print
```

只安装新的 `/opt/ft-shadow-data-plane/deploy/vultr` 并把 `EDGE_IMAGE` 改为 v0.3.3 immutable digest，
不要覆盖现有 `edge.yaml`。执行一次受控重启后，重新核对上述 hash、generation 和 60 个成员不变；
日志应出现各 route ready，`open-gaps` 最终为空，且不再出现 refresh timeout 导致的 collector 全局
退出。该版本将 refresh 限定到准确 subscription；控制状态不可信时只重建一个 route，并为 route
内所有受影响 stream 留下显式 gap。

从 v0.3.3 升级 v0.3.4 时禁止 clean start，并先保存 generation、formal-start、active universe 哈希
和 OPEN gap 列表。只替换 immutable edge image；不得覆盖 `edge.yaml`，不得删除
`control/open-gaps`。新容器完整 ready 后，遗留 `STORAGE_EXHAUSTED_GAP` 和
`COLLECTOR_STOPPED_GAP` 应自动关闭，`sources already running` 不得再次出现。继续观察至少两个
collector status 周期，并确认 ready chunk 和 107 ACK 均持续推进。107 不需要升级。

从 v0.3.4 升级 v0.3.5 不做状态迁移。按
[clean start 手册](../../docs/v0.3.5-structured-universe-clean-start.md) 先把所有 ready 拉到 107，
保留 107 旧 raw 原始字节，再归档旧 runtime 和 Vultr 旧 control/evidence/gap。只有归档验证完成后才清空 Vultr active
数据路径，以新配置直接启动 `7.0 / sequence 8`。代码不读取旧 generation 文件。

从 v0.3.5 升级 v0.3.6 不改变 edge 配置、raw 或 universe 合同。107 必须升级，以避免每分钟 pull
对字节完全一致、已经发布的历史 `SEALED.json` 重复哈希全部 chunk；某日第一次发布仍执行完整校验。

从 v0.3.6 升级 v0.3.7 必须同时升级 107 和 Vultr，但不删除 raw、ready、ACK、universe 或 gap。
新 edge 首先应用遗留 ACK，再启动 sources；新 central 自动创建 transfer ledger 和 status 目录。
升级后要求连续两次 107 `state=ok`，Vultr `transactions_pending=0`、`hash_mismatches=0`，并确认
`REMOTE_GC` 持续出现。磁盘保护线使用 `minimum_free_bytes: 2147483648`。

从 v0.3.7 升级 v0.3.8 不执行第 5 节 clean start。先备份 `edge.yaml`、`edge.env`，记录 active
universe、formal-start 哈希和 open gap；安装新部署脚本后，把示例中的 60 个
`public_symbol_load_weights` 及可靠性参数合入现有配置，不能覆盖现有正式名单。更新 immutable
image digest 后只执行一次受控重启。重启后要求 `7.0 / sequence 8`、60 币和哈希不变，所有 route
完成 snapshot ready，open gap 回到 0，107 ACK 继续推进。107 不需要升级。

从 v0.3.8 升级 v0.3.9 只升级 Vultr，107 协议和 central 处理不变。禁止删除 raw、ready、ACK、
universe、gap 或 formal start；保留 active `7.0 / sequence 8`。按第 5 节替换选择器配置字段，
更新 immutable image 后受控重启。首次 discovery 会补抓 35 日 Kline，因此耗时和 REST 请求数
高于日常增量；完成后 evaluation 必须包含 `mature_pool_count`、`market_context` 和
`decision_frozen_reason`，且不得仅因升级创建新的 pending decision。
