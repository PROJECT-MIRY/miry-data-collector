# Vultr 正式采集部署

本手册适用于 `167.179.115.243` 上的 v0.4.0 collector。数据根为
`/srv/miry-data-rsync`，collector 和受限传输账户都使用 UID/GID 10001。

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

在当前 release 仓库根目录执行：

```bash
sudo ./deploy/vultr/install.sh
```

安装器创建：

```text
/srv/miry-data-rsync/ready
/srv/miry-data-rsync/writing
/srv/miry-data-rsync/control/acks
/srv/miry-data-rsync/control/applying-acks
/srv/miry-data-rsync/control/rejected-acks
/srv/miry-data-rsync/control/transfer-ledger
/srv/miry-data-rsync/control/universe
/etc/miry-data-collector/edge.yaml
/etc/miry-data-collector/edge.env
/opt/miry-data-collector/deploy/vultr
```

它不会启动 collector，也不会改写已经存在的配置。

## 3. 配置受限 rsync

把 107 的 `~/.ssh/miry-data-puller.pub` 放到 Vultr 的临时管理路径，然后执行：

```bash
sudo /opt/miry-data-collector/deploy/vultr/configure-rsync.sh \
  /root/miry-data-puller.pub
```

脚本把 key 安装到 root 管理、`data-puller` 只读的 `AuthorizedKeysFile`，强制执行受限网关。
网关将两种操作分别限制为：

```text
读取 ready/         -> /usr/bin/rrsync -ro /srv/miry-data-rsync/ready
写入 control/acks/ -> /usr/bin/rrsync -wo -no-del /srv/miry-data-rsync/control/acks
```

该 key 没有交互 shell、TTY、端口转发或 X11 权限，不能写采集数据、读取其他目录或删除
ACK。不要为
该账户叠加其他 `ForceCommand` 或 chroot，它们会阻止 rsync 的远端进程。

显示并通过独立渠道发给 107 操作者核对 host key：

```bash
ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

## 4. 配置正式 60 币和镜像

`/etc/miry-data-collector/edge.yaml` 必须使用当前 checkout 的 schema。核对三个角色为 50/5/5、
`bootstrap_evidence_sha256` 与正式报告一致、`automation_enabled: true`、public shards 为 4，
queue 为 64MiB。`message_rates` 的单位是每分钟 public WebSocket 消息数，它是冷启动基准，不要求
与当前 60 币完全相同；没有观测值的新币使用已知速率中位数。旧的 load-weight 配置字段已删除，
不能与新字段同时保留。长期观测规则见
[public 流量均衡](../../docs/traffic-balancing.md)。

在 `/etc/miry-data-collector/edge.env` 中写 immutable digest：

```text
EDGE_IMAGE=ghcr.io/50829/miry-data-collector@sha256:<release-digest>
EDGE_DATA_ROOT=/srv/miry-data-rsync
EDGE_CONFIG=/etc/miry-data-collector/edge.yaml
```

拉取并检查架构：

```bash
set -a
. /etc/miry-data-collector/edge.env
set +a
docker pull "$EDGE_IMAGE"
docker image inspect "$EDGE_IMAGE" --format '{{json .RepoDigests}}'
```

Compose 已固定 1.00 CPU、768MiB RAM、256 PIDs、只读 rootfs 和日志轮换。

## 5. 保留状态上线

本版本禁止 clean start。必须保留 raw、ready、writing、ACK、transfer ledger、gap、
`formal-start.json` 和整个 `control/universe`。停止服务前记录权威状态：

```bash
sudo sha256sum \
  /srv/miry-data-rsync/control/formal-start.json \
  /srv/miry-data-rsync/control/universe/active.json
sudo jq '{core_generation,candidate_revision,decision_sequence,
          universe_version,universe_hash}' \
  /srv/miry-data-rsync/control/universe/active.json
```

安装 deploy 文件后，不要用示例覆盖现有 50/5/5 名单、版本号、bootstrap hash 和 universe 状态：

```bash
sudo ./deploy/vultr/install.sh
sudoedit /etc/miry-data-collector/edge.yaml
```

现有数据树必须完整保留在 `/srv/miry-data-rsync`。`universe` 配置应包含 `market_context_baseline_days: 28`、
`market_context_change_ratio: 1.25`、`market_context_breadth_ratio: 0.70`、
`market_context_minimum_instruments: 60`、`depth_mature_candidate_count: 200` 和
`mature_pool_warning_size: 65`。edge 配置还必须包含 `snapshot_request_interval_seconds: 0.75` 和
`snapshot_request_concurrency: 4`，以及 `open_interest_startup_spread_seconds: 5`。Pydantic 拒绝
未知字段，因此旧选择器字段必须删除干净。

## 6. 验证和启动

首次启动前，107 必须已能执行 `rsync --list-only`。然后：

```bash
sudo systemctl enable miry-data-collector.service
sudo systemctl start miry-data-collector.service
sudo /opt/miry-data-collector/deploy/vultr/verify.sh
```

观察启动：

```bash
journalctl -u miry-data-collector.service -f
```

只有出现以下日志后才进入正式时间范围：

```text
FORMAL_COLLECTION_STARTED ... universe_version=<major.revision> decision_sequence=<n> symbols=60
```

原地升级读取原有 `7.0 / sequence 8`，不会重新写 formal start。首次 discovery 补齐 35 个完整
UTC 日后才评估；缺证据或 market context pending 时保持当前名单。成交额、交易数、点差和 depth
仅参与横截面排名，不再触发绝对门槛拒绝。点差取 21 次、1 秒间隔 bookTicker 的 q95；3 次 depth
snapshot 只提供 10/50 bps 深度。

同时确认：

```bash
sudo test -s /srv/miry-data-rsync/control/formal-start.json
sudo jq '{core_generation,candidate_revision,decision_sequence,universe_version,
          core,boundary,probe,universe_hash}' \
  /srv/miry-data-rsync/control/universe/active.json
sudo find /srv/miry-data-rsync/ready -type f | head
```

## 7. 日常检查

```bash
systemctl status miry-data-collector.service
docker stats --no-stream
docker inspect miry-data-collector-collector-1 \
  --format 'oom={{.State.OOMKilled}} restarts={{.RestartCount}}'
df -h /srv/miry-data-rsync
find /srv/miry-data-rsync/ready -type f | wc -l
find /srv/miry-data-rsync/control/acks -type f | wc -l
sudo jq . /srv/miry-data-rsync/control/transfer-status.json
sudo find /srv/miry-data-rsync/control/rejected-acks -maxdepth 1 -type f -print
journalctl -u miry-data-collector.service --since '24 hours ago' \
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
sudo jq . /srv/miry-data-rsync/control/collector-lease.json
sudo find /srv/miry-data-rsync/control/open-gaps -type f -maxdepth 1 -print
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

## 9. 升级原则

每次升级只替换 deploy 文件和 immutable OCI digest，并执行一次受控重启。升级前后必须核对正式 60 币、`universe_hash`、formal-start 哈希、open gap、ready 和 ACK 状态。禁止 clean start，禁止删除或重写 `/srv/miry-data-rsync`；服务恢复后必须等待全部 realtime source ready、受控 stop gap 关闭，并确认 107 pull 与 `REMOTE_GC` 继续推进。
