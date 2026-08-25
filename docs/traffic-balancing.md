# Public WebSocket 流量均衡

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

Binance canonical symbol 可能包含中文。流量状态和 YAML 均使用 UTF-8 原始 symbol 作为 key；新中文
合约没有历史 rate 时同样使用中位数，不能翻译或转写成另一个 ASCII 名称。

运行期间不追逐分钟级波动。采集器每小时评估最近 24 个完整块，UTC 日封存完成后也触发一次；少于
24 个完整块时不自动再均衡。评估继承当前 route assignment，只有最大 route 预计负载超过平均值
`1.25x` 且一次 pair swap 能减小最大最小差时才动作。

每个批次最多交换一对 symbol。批次成功后冷却 5 分钟，再从实际 assignment 继续评估，直到低于
触发线或没有可改善的 swap；因此持续失衡会自动收敛，但不会在一个时刻搬动几十个币。raw queue
达到 50% 或任一 public route 正在恢复时暂停，失败按 30/60/120 秒有界重试，之后等下一小时评估。

这意味着短时冲击可能暂时形成热点 route。该延迟是完整性约束：冲击期间立即跨 route 搬币会增加
订阅交接和 snapshot 重锚。小时块保存分钟峰值，最近 24 块再取逐币最大值，所以持续冲击会进入
下一次小时评估而不会被小时平均稀释。

配置允许 1--8 条 public route。正式基线仍为 4，8 只用于受控 A/B。分配同时限制单 route 的
币数为 `ceil(60 / routes) + 1`：4 条保持现有最大 16 币，8 条最大 9 币。热点币本身可能超过八等分
平均负载，因此 8 条的验收使用绝对 route rate、CPU 和 `symbol-gap-seconds`，不要求各 route 的
相对流量差固定小于 5%。

成员轮换和流量再均衡共用全局两阶段交接。成员变化先保留未变化币的现有 assignment，只放置新增币；
流量校准也只执行当前限幅 swap。第一阶段在所有目标 route 上增加订阅，并分别等待控制 ACK、所需
L2 snapshot 和新增币的第一条 `bookTicker`、`depth` 新事件；只有所有目标 route 都就绪
后，第二阶段才从旧 route 移除订阅。market route 和 OI poller 在成员变化时也先扩容并完成首事件/
首轮采样，再裁掉退出成员。任何扩容失败都不会进入裁剪阶段；已成功的扩容会回滚，状态不确定的
WebSocket route 会请求重连到旧分片。短暂交接期的重复事件由 pipeline 去重。

## 故障边界

该文件是有界的性能状态，不是 raw 数据合同，也不会同步到 107。文件缺失、内容损坏或写入失败时，
采集器记录 warning 并继续收包；证据不足时回退到 `message_rates`。删除该文件只会失去自动校准
历史，不会删除市场数据，但正常部署没有理由删除它。该文件与 raw、universe 和 formal-start 一样
应在原地升级时保留。

每分钟日志包含各 public route 的实际消息率：

```bash
journalctl -u miry-data-collector.service | grep 'public traffic minute'
```

`route_rates` 长期明显失衡时，应先检查 symbol 的观测峰值和连接异常。配置中的基准只用于证据不足
或状态不可用时的启动，不需要随每次 50/5/5 换币同步修改。
