# v0.5.6 端到端部署指南

正式链路为：

```text
Binance -> Vultr collector -> /srv/miry-data-rsync/ready
        -> restricted rsync over SSH -> 107 data/raw
        -> Slurm -> 107 data/derived
        -> ACK -> Vultr REMOTE_GC
```

## 数据保留原则

升级只替换程序、部署路径和服务名。以下状态必须原地保留：

- Vultr `/srv/miry-data-rsync` 下的 ready、writing、ACK、transfer ledger、gap、lease、formal-start
  和 universe；
- 107 `data/raw`、`data/derived`、`data/transfer-ledger`、runtime rsync staging 和 pull 状态；
- 正式 `7.0 / sequence 8` 的 50 core、5 boundary、5 probe 与 `universe_hash`。

禁止 clean start，禁止因为命名变化重写 raw、manifest、ACK 或 checkpoint。旧数据中的
`schema_version` 和 MIME 标识属于持久化合同，不是软件名称迁移目标。

## 上线顺序

1. 在 107 准备 `~/.ssh/miry-data-puller` 和经独立渠道核对的 known-hosts；
2. 暂停 107 pull cron，等待当前 pull/rsync 退出；
3. 安装并校验 release SIF，更新 `MIRY_*` processing 环境与 SSH 路径；
4. 运行一次前台 pull，确认 `state=ok`、`failures=0`、`acks_pushed=acks_queued`；
5. 在 Vultr 记录 universe、formal-start、open gap、ready、writing、ACK 和磁盘基线；
6. 旧 collector 保持运行，安装 deploy 文件；安装器不得覆盖现有 edge 配置或触发重启；
7. 拉取目标 immutable OCI image，并执行 `preflight-upgrade.sh`；
8. preflight 通过后停止 collector，将现有完整数据树放在 `/srv/miry-data-rsync`；
9. 写入 release 的 immutable OCI digest，只启动一次 collector；
10. 等待全部 realtime source ready 和受控 stop gap 关闭，再恢复 107 cron；
11. 启用 `miry-data-diagnostics.timer`，验证首条宿主机诊断 JSONL 无错误；
12. 验证 ACK/REMOTE_GC、raw 增长、transfer status、资源指标和 open gap。

## 验收

上线完成必须同时满足：

- `miry-data-collector.service` active，容器使用 release immutable digest；
- 日志模块名为 `miry.collector.*`，60 个 symbol 全部完成 realtime readiness；
- active universe 与 formal-start 的内容和哈希在升级前后不变；
- `/srv/miry-data-rsync/control/open-gaps` 没有遗留未关闭 gap；
- 107 `last-pull.json` 为 `state=ok`，ACK 数相等且 raw 文件持续增长；
- Vultr transfer status 没有 hash mismatch、invalid/unknown ACK 或 pending transaction；
- 1C1G 上 RSS、queue、event-loop lag 和磁盘余量仍在正式门限内。

具体命令见 [Vultr 手册](../deploy/vultr/README.md) 和
[107 手册](../deploy/campus-107/README.md)。
