# Public 接收过载与 typed decode 优化（2026-08-30）

## 状态

本修复是下一次集中发布的候选，尚未部署。Vultr 继续运行 v0.5.9；本地结果不能替代相同市场负载下的
1C1G cgroup replay 和生产验收。

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

## 本地基准

同一 Python 3.12 环境、关闭 cyclic GC 以减少噪声，每组 7 次取中位数：

| 负载 | 修改前 | 修改后 | 提升 |
| --- | ---: | ---: | ---: |
| 50% 20档 depth + 25% bookTicker + 25% aggTrade | 351k events/s | 554k events/s | 58% |
| 100档 depth | 63.6k events/s | 210.6k events/s | 231% |

曾验证过手写 bytes 扫描方案；加入 event/symbol/JSON 语义校验后只有约 278k events/s，低于原实现，
已完整撤回。permessage-deflate + JSON 的独立基准约 241k events/s，关闭压缩会把测试语料网络字节放大
约 25 倍，因此没有通过放宽 buffer、timeout 或关闭压缩掩盖容量问题。

## 发布门槛

候选仍必须在正式消息结构下依次回放 50/60/70 万 public msg/min，并满足：CPU p95 小于 80%、
event-loop lag p99 小于 100ms、socket `Recv-Q` 有界、queue 无 hard rejection、无性能 gap。若单核仍不
通过，必须升级到至少 2 vCPU；增加 socket 数或放宽 ping timeout 不能增加总计算容量。
