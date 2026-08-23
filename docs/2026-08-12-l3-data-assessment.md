# Binance USD-M L3 数据评估（2026-08-12）

## 结论

当前项目**没有采集 true L3（market-by-order，逐订单盘口）**。项目采集的是可重建的
L2（market-by-price，按价格聚合盘口）、L1 最优报价和成交记录。Binance 官方公开 USD-M
Futures 行情接口也未提供全市场逐订单 L3 feed，因此不能只修改一个订阅名称就补上 L3。

当前正式实验不需要 L3。现有数据足以研究价差、价位深度、order-flow imbalance、冲击成本、
成交与盘口联动，并能在 sequence 连续时重建 L2。只有研究同价位排队顺序、订单寿命、精确撤单
行为或基于 queue position 的成交概率时，才必须引入 L3。那将是新的付费数据源、数据合同和
基础设施项目，不属于 v0.3 的 1 vCPU / 1 GiB 正式采集范围。

## 什么算 true L3

本文采用以下可验证的操作定义：true L3 必须能识别每一张 resting order，并表达其 order ID、
价格、剩余数量、方向以及新增、修改、撤销、成交等生命周期事件；还要保留同一价格内的队列
关系。只有价格档及该档总数量的是 L2，而只有最优买卖价量的是 L1。

这个区别不是命名问题。假设同一价格有两张各 5 张的挂单，公开 L2 只显示总量 10；总量随后
变为 5 时，无法判断是哪张订单撤销或成交，也无法恢复其排队顺序。成交记录也不能唯一补回这
些已经聚合掉的信息。

## Binance 官方公共数据能表达什么

以下判断来自 Binance 官方 USD-M Futures 文档的公开 payload 字段和官方本地盘口重建流程：

| 数据 | 官方字段所表达的内容 | 层级与限制 |
| --- | --- | --- |
| Diff Book Depth | `U/u/pu` 是更新序号；`b/a` 的每项仅为 `[price, quantity]`，数量为 0 时删除该价格档；可选 100/250/500 ms | L2 增量，不含 resting order ID、订单生命周期或同价位队列 |
| REST Order Book Snapshot | `lastUpdateId` 和 `bids/asks`，每档只有 price 与 quantity；最多返回 1,000 档 | L2 锚点；与连续 diff 合用可重建公开价位簿，但 1,000 档之外没有初始状态 |
| Book Ticker | 最优买价/量 `b/B` 和最优卖价/量 `a/A` | L1/BBO，不是完整盘口 |
| Aggregate Trade | 聚合成交 ID、价量、首末 trade ID、时间和 maker side；官方定义为同一 taker order 的聚合成交 | 成交打印，不描述仍在簿中的订单 |
| Trade | 单笔成交 ID、价量、时间和 maker side | 更细的成交打印；trade ID 不是 resting order ID |
| 私有 `ORDER_TRADE_UPDATE` | 包含当前账户自己的 client order ID 和 order ID | 仅是鉴权账户自己的订单事件，不是全市场公共 L3 |

一手来源：

- [Diff Book Depth Streams（Binance 官方）](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams)
- [How to manage a local order book correctly（Binance 官方）](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)
- [Order Book REST API（Binance 官方）](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book)
- [Partial Book Depth Streams（Binance 官方）](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams)
- [Individual Symbol Book Ticker Streams（Binance 官方）](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams)
- [Aggregate Trade Streams（Binance 官方）](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams)
- [Trade Streams（Binance 官方）](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Trade-Streams)
- [Event: Order Update（Binance 官方私有用户数据流）](https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Order-Update)

因此严谨结论是：**Binance 官方公开 USD-M Futures market-data 接口未提供
market-by-order/L3**。这不代表 Binance 内部没有逐订单数据，只代表其公开行情产品没有暴露
全市场逐订单 feed。

## 当前项目实际采集什么

正式合同默认 `d0_enabled: false`。60 个 symbol 当前订阅或抓取：

- 每个 symbol 的 `depth@100ms` 和最多 1,000 档 REST depth snapshot；
- 每个 symbol 的 `bookTicker`、`aggTrade`、`markPrice@1s` 和 `forceOrder`；
- 每 30 秒 open interest，以及 contract/exchange/universe/clock 元数据。

代码依据：

- [正式与 D0 stream 合同](../src/miry/contracts/collection.py)
- [WebSocket 订阅构造](../src/miry/collector/websocket.py)
- [L2 snapshot、diff sequence 与重锚逻辑](../src/miry/pipeline/l2.py)
- [Vultr 正式配置](../deploy/vultr/edge.yaml.example)

可选 D0 中的 individual `trade` 与 RPI depth 当前均关闭。即使打开，它们仍分别是成交记录和
按价格聚合的 RPI depth，不会变成 L3。日 Kline 的 `number of trades` 用于 universe 流动性
准入；正式 raw 流里的 `aggTrade` 还保存首末 trade ID。这些都不能恢复挂单队列。

## 60 个 symbol 的 L3 每日容量估算

Binance 没有公开 L3 产品，所以也没有可引用的官方 L3 字节率。下面是**容量敏感性估算，
不是 Binance 官方数字，也不是任何第三方供应商的报价**。实际值必须拿目标供应商的样本包
回放测量。

估算公式为：

```text
未压缩字节/日 = 60 symbols * 每 symbol 每秒订单事件数 * 86,400 秒 * 单事件字节数
压缩字节/日   = 未压缩字节/日 / 压缩比
```

“订单事件”指一张订单的 add/cancel/replace/fill 等 mutation。单事件字节假设已包含 symbol、
交易所时间、order ID、side、price、quantity、action 与记录 framing，但不包含索引、副本、
文件系统冗余、周期性全量 snapshot 和灾备开销。

| 情景 | 持续事件率（每 symbol） | 未压缩记录 | 假设压缩比 | 未压缩/日 | 压缩后/日 | 压缩流平均带宽 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 低负载 | 100 events/s | 160 B/event | 5:1 | 82.9 GB | 16.6 GB | 1.5 Mbit/s |
| 基准 | 500 events/s | 180 B/event | 4:1 | 466.6 GB | 116.6 GB | 10.8 Mbit/s |
| 高负载 | 2,000 events/s | 220 B/event | 3:1 | 2.28 TB | 760.3 GB | 70.4 Mbit/s |

这里用十进制 GB/TB。高流动性 symbol 的事件率远高于小币种，行情剧烈波动时又会突发，
所以不能把 60 个 symbol 当成 60 条均匀小流。较实际的工程预算应按**每天数十到数百 GB
压缩数据**设计，并单独预留 snapshot、索引、副本和峰值吞吐；不能用当前 Vultr 25 GB
磁盘承担持久 L3 spool。即使低负载假设也会在不足两天内消耗约 25 GB，仅剩很小安全余量。

## 是否应在本阶段采集 L3

不应。原因是：

1. 当前正式实验合同要求 L2 重建与 gap 可证明性，现有 snapshot + sequence-aware diff 已直接
   覆盖该目标；参见[正式采集实施合同](collection-contract.md)。
2. 研究价差、聚合深度、固定名义冲击成本、成交方向和 L2 order-flow imbalance 不要求知道
   同价位内每张订单的身份。
3. 官方公共源没有 L3；引入第三方会改变数据来源、时钟语义、许可、完整性校验和可复现性。
4. 上述容量与峰值明显超出当前 1 vCPU / 1 GiB / 25 GB Vultr 的性能合同，且会显著增加 107
   的传输与永久存储压力。

只有把研究问题明确改为以下任一项时，才应另立 L3 项目：同价位 FIFO 排队、逐订单寿命与撤单
风险、queue position、订单级 fill probability 或 spoofing/layering 的逐订单证据。届时应先取得
供应商 24 小时、覆盖相同 60 个 symbol 的样本，再实测事件率、压缩率、峰值、gap 语义和授权
条件，并据此重新设计采集机器与数据合同。
