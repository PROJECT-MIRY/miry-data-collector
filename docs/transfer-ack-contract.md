# ACK 传输审计与恢复合同

ACK 是删除 Vultr `ready/` 副本的授权，不是普通进度提示。端到端成功必须依次满足：

1. `LOCAL_DURABLE`：107 校验 size/SHA-256，fsync 并原子发布 raw 与 manifest；
2. `ACK_PUSHED`：107 已把 ACK 通过受限 rsync 写到 Vultr；
3. `ACK_VALIDATED`：Vultr 确认 ACK 的 `chunk_id` 和 SHA-256 与 ready manifest 一致；
4. `REMOTE_GC`：Vultr 删除 ready 数据副本并持久化审计事件。

107 与 Vultr 的目录同步和 GC 可以并发：107 可能在 GC 前列出 ready，随后对本地已持久化的同一
chunk 重发 ACK。Vultr 在未封日保留 acked manifest 墓碑；完全相同的 ACK 记录为
`ACK_REPLAYED`，`acks_replayed` 加一，不重复计算 GC，也不进入 rejected。相同 chunk ID 但不同
SHA-256 仍是 `ACK_HASH_MISMATCH`。

只有 `REMOTE_GC` 表示一个 chunk 完成闭环。`pull complete` 证明本轮 107 阶段成功，但 Vultr
可能要到下一个 5 秒 storage 周期才应用 ACK。

## 持久文件

107：

```text
runtime/logs/pull.log                         人类可读批次日志
runtime/status/last-pull.json                 原子覆盖的最新 pull 状态
data/transfer-ledger/date=YYYY-MM-DD/events.jsonl
```

Vultr：

```text
/srv/miry-data-rsync/control/transfer-status.json
/srv/miry-data-rsync/control/transfer-ledger/date=YYYY-MM-DD/events.jsonl
/srv/miry-data-rsync/control/applying-acks/      GC 崩溃恢复 transaction
/srv/miry-data-rsync/control/rejected-acks/      无效、未知或 hash 冲突 ACK
```

JSONL 是 append-only UTC 日志。每个事件包含 `event_id`、UTC `occurred_at`、`chunk_id`、
SHA-256 和批次 ID；重启恢复可能重复写同一个确定性 `event_id`，审计程序按 `event_id` 去重。
状态 JSON 使用临时文件、fsync 和原子 rename，不会暴露半写内容。

## 崩溃与异常语义

107 在 ACK rsync 成功并写入 `ACK_PUSHED` 后才删除本地 staging ACK。任一步崩溃都会保留 ACK，
下一轮可安全重传。Vultr 在删除 ready 前先持久化 `applying-acks` transaction；删除后必须写入
`REMOTE_GC` 才删除 transaction。若进程在中间退出，新进程启动时先恢复 transaction，再执行
日 seal 和采集启动。

单个损坏 ACK、文件名不匹配、真正未知的 chunk 或 hash mismatch 不再终止 collector，也不会删除 ready。
它们被原子移动到 `rejected-acks`，状态变为 `attention` 并写结构化错误事件。损坏 ready manifest
同样不会让 storage task 崩溃；对应数据保持在 spool，等待人工处理，磁盘保护线仍然生效。

## 日常检查

107：

```bash
jq . /home/scc/pb24000367/Projects/bn/runtime/status/last-pull.json
tail -n 20 /home/scc/pb24000367/Projects/bn/runtime/logs/pull.log
LEDGER_DATE=$(date -u +%F)
tail -n 20 "/home/scc/pb24000367/Projects/bn/data/transfer-ledger/date=$LEDGER_DATE/events.jsonl"
```

Vultr：

```bash
sudo jq . /srv/miry-data-rsync/control/transfer-status.json
LEDGER_DATE=$(date -u +%F)
sudo tail -n 20 "/srv/miry-data-rsync/control/transfer-ledger/date=$LEDGER_DATE/events.jsonl"
sudo find /srv/miry-data-rsync/control/rejected-acks -type f -maxdepth 1 -print
```

正常状态要求 107 `state=ok`、`acks_pushed` 等于 `acks_queued`、Vultr
`state=ok`、`hash_mismatches=0`、`invalid_acks=0`、`unknown_acks=0`、`transactions_pending=0`，
并且 ready backlog
持续收敛。两端批次时间不同，瞬时计数不要求相等。

以下任一条件需要处理：5 分钟没有成功 pull；10 分钟内 ready 文件和字节只增不减；任何
hash mismatch、invalid/unknown ACK 或 manifest error；transaction 超过一个 storage 周期仍未清理；
磁盘可用空间低于 2 GiB。结构化日志每批只执行一次 append/fsync，不逐 chunk 写 INFO，适用于
1 vCPU、1 GiB 的 Vultr。
