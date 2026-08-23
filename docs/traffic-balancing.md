# Public 流量均衡

## 目的

4 个 public WebSocket route 承担的实际流量差异很大。`message_rates` 是每个 symbol 每分钟成功
进入 ingest 的 public WebSocket 消息数，用于估算连接负载。它与 Binance REST API 的
`request weight` 无关，也不是流动性评分或选币门槛。

## 规则

采集器每分钟统计各 symbol 和 route 的消息数。启动所在的不完整分钟不参与证据；连续 60 个完整
分钟组成一个块，块内为每个 symbol 保存分钟峰值。状态最多保留最近 24 个完整块：

```text
/data/control/public-message-rates.json
```

少于 24 个块时，分片使用 `edge.yaml` 中的 `message_rates`。达到 24 个块后，使用最近 24 个块
的逐币峰值。没有历史的新币使用已知速率的中位数。

运行期间换币时保留所有仍在线 symbol 的 route，只把新币放到当前估算流量最低的 route。持续失衡
需要在线修正时，必须一次只迁移一个 symbol，并按 `target subscribe -> ACK/首事件/L2 snapshot ->
source unsubscribe` 的顺序交接。新 route 未就绪时旧 route 继续收包；交接失败则移除新订阅并保留
旧订阅。交接期重复事件由 central 去重，不能通过重启或先退订制造数据空窗。

## 故障边界

该文件是有界的性能状态，不是 raw 数据合同，也不会同步到 107。文件缺失、内容损坏或写入失败时，
采集器记录 warning 并继续收包；下一次启动回退到 `message_rates`。删除该文件只会失去自动校准
历史，不会删除市场数据，但正常部署没有理由删除它。

每分钟日志包含各 public route 的实际消息率：

```bash
docker logs ft-shadow-data-plane-edge 2>&1 | grep 'public traffic minute'
```

`route_rates` 长期明显失衡时，应先检查 symbol 的观测峰值和连接异常。配置中的基准只用于证据不足
或状态不可用时的启动，不需要随每次 50/5/5 换币同步修改。
