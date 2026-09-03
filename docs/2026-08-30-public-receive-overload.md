# Public 接收过载与 typed decode 优化（2026-08-30）

## 状态

本修复进入 v0.5.11。发布前的 Vultr 仍运行 v0.5.9；开发机结果不能替代相同市场负载下的 1C1G
cgroup replay 和生产验收。

## 事故证据

2026-08-28 的 `14:00--14:08 UTC` 和 `16:01--16:29 UTC` 共记录 26 次 public route failure：
19 次无 close frame、7 次 keepalive ping timeout。27 个 transport gap 的恢复中位数为 2.71 秒，
26 个 L2 reanchor gap 的总恢复中位数为 19.68 秒，最长 60.9 秒。

故障时 public 消息率从约 20--32 万/min 升到最高 66.4 万/min，容器 CPU 持续 90--96%，event-loop
lag 最高 281ms，socket `Recv-Q` 最高 34.6MiB。raw queue 最高仍只有 6.1%，没有 hard rejection、OOM、
显著 cgroup throttle 或 TCP timeout 增长。26 次失败涉及至少 22 个 Binance peer；market route 没有
断开，但 control RTT 一度达到 1.1 秒、Ping RTT 达到 1.78 秒。证据支持“上游消息突发触发、本机单核
接收处理饥饿放大”的结论，不支持单一 Binance backend、writer、磁盘或全局公网故障。

## 修改

Binance combined event 使用 `msgspec.Struct` 只解析在线控制面需要的字段：

```text
stream
data.e
data.s / data.o.s
data.U / data.u / data.pu
```

depth 的 bids/asks、成交价格数量等字段不再物化为 Python list/dict。原始 WebSocket bytes 仍未经修改
进入 `RawEvent.payload_bytes` 和 Parquet；L2 sequence 仍使用同一 `U/u/pu`，receive timestamp、
receive sequence、subscription audit、gap、snapshot bridge、writer 和 107 合同均不变。

非 combined 消息、control response、字段类型不匹配或缺少 depth update ID 时回退现有 `orjson`
解析。fallback 不是容错丢弃：原来会触发的 JSON、KeyError 或类型错误仍会在所属 route 显式失败。

后续完整热路径 profile 证明 typed decode 只覆盖一部分成本，并继续移除了以下逐事件开销：

- WebSocket 接收时取得的 realtime/monotonic 时间戳直接传给 route liveness 和 traffic recorder，避免
  同一事件重复读取系统时钟；
- ingest 在未执行 writer rotation 时使用无等待队列快路径，rotation 屏障与严格字节上限保持不变；
- queue 每次 admission 只解析一次 writer group，并复用 `RawEvent` 的 monotonic timestamp；
- traffic minute bucket、L2 `SnapshotBridgeTracker` 和 bridge `Event` 只在首次需要时创建，不再通过
  `setdefault(key, ExpensiveObject())` 为每条消息构造随后丢弃的对象；
- ASCII symbol 走等价的快速校验分支，非 ASCII/中文 symbol 继续执行 NFC、uppercase 和 alphanumeric
  校验。

这些修改不改变原始 payload、receive sequence、gap、snapshot bridge、writer 分组或 ACK 合同。

## 本地基准

同一 Python 3.12 环境、关闭 cyclic GC 以减少噪声，每组 7 次取中位数：

| 负载 | 修改前 | 修改后 | 提升 |
| --- | ---: | ---: | ---: |
| 50% 20档 depth + 25% bookTicker + 25% aggTrade | 351k events/s | 554k events/s | 58% |
| 100档 depth | 63.6k events/s | 210.6k events/s | 231% |

曾验证过手写 bytes 扫描方案；加入 event/symbol/JSON 语义校验后只有约 278k events/s，低于原实现，
已完整撤回。permessage-deflate + JSON 的独立基准约 241k events/s，关闭压缩会把测试语料网络字节放大
约 25 倍，因此没有通过放宽 buffer、timeout 或关闭压缩掩盖容量问题。

## 单核合成链路负载测试

仓库提供 `scripts/run-synthetic-public-load.sh`。它在独立 CPU 上运行本地合成 WebSocket sender，对
client 启用
真实 framing 与 permessage-deflate，并把 client 放入 `CPUQuota=100%`、单 CPU、`MemoryMax=768M` 的
systemd cgroup。client 继续走四条 route、60 个 symbol、typed decode、L2 sequence tracker、traffic
recorder、192 MiB strict queue 和真实 Parquet writer。负载比例为 50% 20 档 depth、25% bookTicker、
25% aggTrade。payload 是测试工具生成的，不是 Binance 历史 payload，因此这里只验证热路径和门禁
实现，不构成生产容量证据。

2026-09-03 在 Intel Core Ultra 5 125H 上每档回放 10 秒：

| 总消息率 | 接收率 | CPU p95 | event-loop lag p99 | queue high-water | 结果 |
| ---: | ---: | ---: | ---: | ---: | --- |
| 500k/min | 100.003% | 43.2% | 3.75ms | 5.72% | PASS |
| 600k/min | 100.004% | 51.1% | 2.75ms | 5.73% | PASS |
| 700k/min | 100.001% | 52.7% | 4.29ms | 5.84% | PASS |

三档均为 `hard_rejections=0`、`l2_sequence_gaps=0`，client memory peak 为 86--97 MiB。结果只适用于
该开发机和该合成 payload，不能外推到 Vultr 的 EPYC Rome，也不代表线上曾出现相同消息率。

运行示例：

```bash
uv sync --all-groups
scripts/run-synthetic-public-load.sh 500000 10 18801
scripts/run-synthetic-public-load.sh 600000 10 18802
scripts/run-synthetic-public-load.sh 700000 10 18803
```

这不是 Vultr 同机型验收。开发机 CPU 明显快于线上单核 AMD EPYC Rome；不能在正式 collector 所在
Vultr 上并行运行 synthetic load，否则测试本身可能制造 gap。生产容量必须在隔离的同规格 1C1G 实例
使用真实捕获 payload 验证，或在部署后仅用真实流量做无额外负载的观察验收。

## 发布门槛

候选已经在开发机受限单核环境完成 50/60/70 万 public msg/min 合成负载测试，并满足 CPU p95 小于 80%、
event-loop lag p99 小于 100ms、queue 无 hard rejection、无性能 gap。由于 CPU 型号不同，发布状态仍为
候选；同规格 Vultr 门禁通过前不得宣称 1C1G 生产容量已经证明。若同规格单核不通过，必须继续降低
逐事件成本或升级到至少 2 vCPU；增加 socket 数、放宽 ping timeout 或隐藏 gap 都不能增加总计算容量。
