# NautilusTrader 与 HftBacktest 接入评估（2026-08-24）

## 结论

当前数据**可以用于回测，也适合接入 NautilusTrader 和 HftBacktest**，但结论有三个边界：

1. 它是 Binance USD-M 的 **market-by-price L2（MBP）**，不是带全市场订单 ID 的
   market-by-order L3（MBO）。可以研究价差、深度、冲击成本、盘口失衡、成交与盘口联动，也可以做带
   假设的 maker fill；不能声称恢复了真实 FIFO 队列、逐订单撤单或精确 queue position。
2. 现有 typed Parquet **不能直接当作连续无缺口行情喂给回测器**。必须先生成一条 canonical replay
   stream：去重、选择 authoritative connection、只保留 `l2-validity` 中的区间，并在每个恢复点先输出
   `CLEAR + full snapshot`，再输出连续 diff。
3. `_PROCESSED.json` 目前证明的是 L2 覆盖合同通过，不等于所有 stream 都逐条完整。已知
   `2026-08-11` 的 60 个 symbol 全部 `_QUALITY_REJECTED`，不能作为整日正式回测样本；其他日期在各自
   quality marker 生成前也不能预先宣布可用。

框架选择建议：

- **NautilusTrader 作为主框架**：适合多品种策略、组合/账户/保证金、信号到执行的完整研究；其原生
  `L2_MBP`、`TradeTick`、mark/index/funding 类型和 Parquet catalog 与本项目覆盖面更吻合。官方说明
  L2 venue 由 `OrderBookDelta(s)` 更新，`TradeTick` 可触发撮合，而 Quote/Bar 不更新 L2 book：
  [Backtest Data and Venues](https://nautilustrader.io/docs/latest/concepts/backtesting/data-and-venues/)。
- **HftBacktest 作为微观结构复核框架**：适合 maker queue、feed latency、order latency 和不同 fill
  假设的敏感性分析。它原生以 exchange/local 双时间线重放，并提供 L2 queue model：
  [Data](https://hftbacktest.readthedocs.io/en/latest/data.html)、
  [Order Fill](https://hftbacktest.readthedocs.io/en/latest/order_fill.html)。


## 当前数据到底能证明什么

### 已经具备的完整性证据

采集合同包含 `depth@100ms`、1,000 档 REST snapshot、`bookTicker`、`aggTrade`、
`markPrice@1s`、`forceOrder`、`contractInfo`，以及每币 30 秒 OI；正式配置
`d0_enabled: false`，因此没有 individual trade 和 RPI depth。代码依据是
[models.py](../src/miry/contracts/models.py)、
[websocket.py](../src/miry/collector/websocket.py) 与
[edge.yaml.example](../deploy/vultr/edge.yaml.example)。

当前链路已有这些可审计证据：

- raw 保存 exchange symbol、stream、connection、connection 内 receive sequence、实时/单调接收时间、
  原始 payload 和 REST request 时间，见 [raw.py](../src/miry/contracts/raw.py)；
- chunk 按 manifest 的 size/SHA-256 拉取，UTC 日由 `SEALED.json` 固定输入集合；normalize 再校验 raw
  schema、chunk metadata 和 hash，见 [pull.py](../src/miry/pipeline/pull.py) 与
  [normalize.py](../src/miry/pipeline/normalize.py)；
- typed 保存 exchange event/transaction time、接收时间、`U/u/pu`、snapshot `lastUpdateId`、价位数组、
  aggregate trade ID/首末 trade ID 和 `is_duplicate`，见 [typed.py](../src/miry/contracts/typed.py)；
- L2 只有在 snapshot 与 diff 成功 bridge、`pu` 连续且没有 active transport gap 时才进入 `VALID`；
  日末 checkpoint 保存盘口和 update ID，见 [l2.py](../src/miry/pipeline/l2.py)；
- finalize 要求 60 币逐币 `valid_ratio >= 99.9%`、`unclassified_ns == 0`、
  `conflicting_ns == 0`，否则写 `_QUALITY_REJECTED.json`，见
  [quality.py](../src/miry/pipeline/quality.py)。

这些机制使 L2 缺口是显式的，并防止把 sequence 断裂后的错误盘口静默当真。

### 尚未被日质量 marker 证明的内容

`quality.py` 当前只对 depth/L2 计算 coverage。它不会单独证明以下数据逐事件完整：

- `aggTrade`；
- `bookTicker`；
- `markPrice@1s`；
- `forceOrder`；
- 30 秒 REST open interest。

因此实验应把完整性声明拆成两个等级：

| 研究内容 | 当前可用性 | 进入正式样本前的条件 |
| --- | --- | --- |
| bar、收益、成交量、方向性成交 | 可构建 | 过滤 `is_duplicate`；补做 aggTrade ID/stream gap coverage |
| spread、BBO、L2 depth、OFI、冲击成本 | 条件可用 | 只用 `l2-validity`；正式日优先要求 `_PROCESSED.json` |
| maker fill / queue sensitivity | 可做模型实验 | L2 queue model + 聚合成交的保守参数区间，不当作真实 FIFO |
| 真实订单队列、逐单撤单、订单寿命 | 不可用 | 需要外部 true MBO/L3 数据源 |

对于 `_QUALITY_REJECTED` 日，可以把每个 `VALID` 区间用于探索或模型开发，但不能把整日标成正式完整
样本。即使 `_PROCESSED` 日也允许最多 0.1% 的显式 L2 无效时间，所以回测导出仍应物理屏蔽 gap，不能只
看成功 marker 后把全天连续喂入。

## 为什么不能直接导入 typed Parquet

typed 层是规范化证据层，不是 canonical replay 层。它仍可能同时包含：

- overlap 期间的新旧两个 WebSocket connection；
- 标成 `is_duplicate=true` 的重放事件；
- snapshot 尚未 bridge 的 pending diff；
- transport/sequence gap 前后的事件；
- 不再 authoritative 的 connection 事件。

L2 reconstructer 会在这些候选之间选择 authority，但当前 derived 只输出
`connection-states.jsonl`、`l2-validity.jsonl` 和日末 `l2-checkpoint.json`，没有输出可直接交给外部
回测器的逐事件 canonical book stream。因此正确接入需要新增一个 exporter，而不是把 typed Parquet
改列名后直接导入。

## 推荐的 canonical replay exporter

每个 symbol 按以下顺序导出：

1. 读取 authoritative sealed/typed 输入，验证 `_NORMALIZED.json`、universe 和 quality marker。
2. 使用与 [l2.py](../src/miry/pipeline/l2.py) 相同的 snapshot bridge、`U/u/pu` continuity、gap 和
   connection authority 规则。overlap 中包括 `is_duplicate=true` 在内的完整 depth 证据仍应参与各
   connection 候选簿重建；只在**对外发射**时按 logical identity 去重，并且只发 authoritative connection。
3. 每次进入一个 `VALID` 区间时，先输出 `CLEAR`，再输出当时 authoritative book 的全量 price levels；
   随后只输出该 authority 的连续 diff。
4. 离开 `VALID` 时结束 segment。最保守实现是让每个 segment 成为独立回测区间；若同一 run 内恢复，
   必须再次 `CLEAR + snapshot`，不可让旧 book 穿过 gap。
5. 保留 sidecar audit 字段：collector/date/symbol、connection ID、receive sequence、`U/u/pu`、payload
   hash、validity segment ID、quality marker hash。目标引擎不认识的字段不能因此丢失审计链。
6. instrument metadata 从目标日的 exchange info 生成，包括 tick size、lot size、price/size precision、
   contract status；不同日期发生规则变化时不能拿今天的 precision 回写历史。

时间戳统一为：

```text
event/exchange timestamp = exchange_transaction_time_ms * 1_000_000
                         （没有 T 时使用 exchange_event_time_ms，再没有则显式 fallback）
local/initial timestamp  = app_receive_realtime_ns
tie breaker              = connection authority + receive_seq + batch内 level 顺序
```

对 segment 恢复时合成的 full snapshot，`ts_init` 应取 `valid_from_ns`，`ts_event` 应取 bridge 完成时最后
一条已应用 diff 的 exchange time；若原始 snapshot 和 diff 都没有 exchange time，才回退到 `ts_init`，并在
sidecar 记录 `timestamp_source=fallback_local`。任何 clock offset 校正也要写入版本化元数据，不能静默修改。

`app_receive_monotonic_ns` 只在同一 boot 内可比较，不能作为跨 boot 的 epoch timestamp。NautilusTrader
官方将 venue time 放在 `ts_event`、通常把本地接收/初始化时间放在 `ts_init`，并按 `ts_init` 稳定排序；
只有两端时钟同步时，二者之差才可解释为观测延迟：
[Nautilus timestamps](https://nautilustrader.io/docs/latest/concepts/data/#timestamps)。HftBacktest 对应字段是
`exch_ts` 与 `local_ts`，并要求 EXCH/LOCAL 两条时间线各自有序且 feed latency 非负：
[HftBacktest validation](https://hftbacktest.readthedocs.io/en/latest/data.html#validation)。

## NautilusTrader 映射

### 核心行情

| 本项目字段/事件 | NautilusTrader | 转换规则 |
| --- | --- | --- |
| symbol + exchange info | `CryptoPerpetual`/对应 instrument | 生成稳定 `InstrumentId`，保留历史 tick/lot/precision |
| depth snapshot | `OrderBookDeltas` | `CLEAR` + 每档 `ADD`；末条使用 `F_SNAPSHOT \| F_LAST` |
| depth diff `bids/asks` | `OrderBookDeltas` | qty=0 -> `DELETE`，否则 `UPDATE`；`sequence=final_update_id` |
| L2 `BookOrder.order_id` | `0` | 不伪造 venue order ID；以 `BookType.L2_MBP` 维护每价一档 |
| `agg_trade` | `TradeTick` | price/qty；`trade_id=aggregate_trade_id`；buyer-is-maker -> SELL aggressor，否则 BUY |
| `book_ticker` | `QuoteTick` | 可供策略订阅；L2 venue 不用它更新 matching book |
| mark/index/funding | `MarkPriceUpdate` / `IndexPriceUpdate` / `FundingRateUpdate` | 按 Binance native adapter 的对应语义构造 |
| OI / force order | Binance custom data 或项目 custom data | 用于因子，不参与 L2 matching |
| contract info / quality / validity | custom data 或 sidecar | 用于 universe、样本屏蔽和审计 |

这与 NautilusTrader 官方 Binance Futures adapter 的映射一致：aggregate trade 被转换为 `TradeTick`，
buyer-maker 被反转为 aggressor side；depth qty=0 为 `DELETE`、否则 `UPDATE`，`sequence` 使用 final update
ID，L2 order ID 为 0。固定源码：
[Futures WebSocket parser](https://github.com/nautechsystems/nautilus_trader/blob/21cc5497eed0e0fdbae0bc52323da96ce6c9e901/crates/adapters/binance/src/futures/websocket/streams/parse_data.rs)。

`OrderBookDelta` 的标准字段是 instrument/action/order/flags/sequence/`ts_event`/`ts_init`；logical batch
末条应设置 `F_LAST`：
[OrderBookDelta](https://nautilustrader.io/docs/latest/concepts/data/order_book_delta/)、
[OrderBookDeltas](https://nautilustrader.io/docs/latest/concepts/data/order_book_deltas/)。标准 delta 没有专门的
`U` 和 `pu` 字段，所以必须在 exporter 前验证连续性并保留 audit sidecar，不能期待引擎替本项目修 gap。

### 导入方式

现有 Parquet 不是 Nautilus catalog schema，推荐两阶段：

```text
miry typed + quality
    -> canonical replay exporter
    -> Nautilus model objects
    -> ParquetDataCatalog
    -> BacktestNode chunked replay
```

小样本可直接构造 model objects 后 `BacktestEngine.add_data()`；正式 60 币多日数据应写
`ParquetDataCatalog` 后由 `BacktestNode` 分块读取。Nautilus 官方区分这两种路径：
[Backtest APIs and Repeated Runs](https://nautilustrader.io/docs/latest/concepts/backtesting/apis-and-runs/)。catalog
要求 Nautilus Arrow schema，而 custom data schema 至少包含 `ts_init` 且按它升序：
[Data catalog and custom data](https://nautilustrader.io/docs/latest/concepts/data/#data-catalog)。

### 执行模型限制

venue 必须配置 `BookType.L2_MBP`。建议最初启用：

```text
trade_execution = true
queue_position = true
liquidity_consumption = true
```

Nautilus 的 L2 queue tracking 在订单接受时记录同价显示量，由正确 aggressor side 的成交减少前方数量；
L2 UPDATE 只能把 queue-ahead 上限收紧到新的显示量。隐藏单和 venue 特殊优先规则仍不可见：
[Trade execution and queue position](https://nautilustrader.io/docs/latest/concepts/backtesting/trade-execution/)。
历史盘口不会被模拟成交永久改写；`liquidity_consumption` 只能避免同一更新前重复消费显示量：
[Fill models](https://nautilustrader.io/docs/latest/concepts/backtesting/fill-models/)。因此 maker 结论必须对 queue/fill
参数做敏感性区间，taker size 也必须限制在显示深度和合理参与率内。

## HftBacktest 映射

### 核心格式

HftBacktest 接收 aligned NumPy structured array 或 `.npz`，标准八列为：

```text
ev:u64, exch_ts:i64, local_ts:i64, px:f64, qty:f64,
order_id:u64, ival:i64, fval:f64
```

`order_id` 只用于 L3 MBO。官方定义与 loader 见
[Data format](https://hftbacktest.readthedocs.io/en/latest/data.html#format) 和固定源码
[event_dtype](https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/py-hftbacktest/hftbacktest/types.py#L74-L86)。

| 本项目事件 | HftBacktest event |
| --- | --- |
| snapshot bid/ask | `DEPTH_CLEAR_EVENT` 后逐档 `DEPTH_SNAPSHOT_EVENT | BUY/SELL_EVENT` |
| depth diff bid/ask | `DEPTH_EVENT | BUY/SELL_EVENT`，`qty=0` 删除 price level |
| `agg_trade` | `TRADE_EVENT | BUY/SELL_EVENT`，side 为 aggressor side |
| timestamps | `exch_ts=T*1e6`，`local_ts=app_receive_realtime_ns` |
| L2 order ID | `order_id=0` |
| `u`、segment ID | 可审计地放 sidecar；`ival` 可携带 `u`，但引擎不会据此检查 `pu` |

HftBacktest 自带的 Binance Futures converter 只接受 gzip 文本
`local_timestamp + combined-stream JSON`，并识别 `trade`、depth、snapshot；当前项目是 Parquet 且正式只采集
`aggTrade`，所以必须写自定义 converter，不能直接调用该函数。固定源码：
[binancefutures.py](https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/py-hftbacktest/hftbacktest/data/utils/binancefutures.py)。

每个 replay segment 要有可信 initial snapshot。`BacktestAsset` 可接受 NumPy 数组或 `.npz`，也有
`initial_snapshot`；官方 Python API 见固定源码：
[BacktestAsset](https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/py-hftbacktest/hftbacktest/__init__.py#L118-L185)。

### 延迟和排队模型

本项目的 exchange/local 时间可以直接提供**行情延迟**。但它没有策略真实订单的 request/exchange-ack/
response 三时间戳，因此不能从市场数据反推出真实 order entry/response latency。HftBacktest 支持 constant
latency，以及从历史订单延迟样本插值的模型；插值数据是 `(req_ts, exch_ts, resp_ts)`：
[Latency Models](https://hftbacktest.readthedocs.io/en/latest/latency_models.html)。初期应使用多组 constant latency
做压力测试，等实盘/仿真下单采样后再换 measured latency model。

当前 L2 数据应选择 RiskAverse 或 probability queue models。没有 MBO order IDs 时 queue position 必须
估计；HftBacktest 官方明确说明 Market-By-Price 要用模型猜测，并给出不同 queue models：
[Queue Models](https://hftbacktest.readthedocs.io/en/latest/order_fill.html#queue-models)。不能启用 L3 FIFO 并用
伪造 ID 填数据。

HftBacktest 是 replay simulator：策略订单不会改变未来市场数据，也不建模 market impact；
`NoPartialFillExchange` 对 taker 的假设可能过度乐观，`PartialFillExchange` 虽按显示量限制部分成交，历史
depth 仍不会因策略订单改变。官方限制见
[Exchange Models](https://hftbacktest.readthedocs.io/en/latest/order_fill.html#exchange-models)。建议同时跑：

- conservative queue + PartialFill；
- probability queue 的多组参数；
- 不同 entry/response latency；
- 不同最大下单量/显示深度参与率。

## `aggTrade` 对成交回测的影响

Binance `aggTrade` 是按同一 taker order 聚合的成交，不是 individual trade；官方 payload 给出 aggregate
ID、首末 trade ID、总数量、价格和 maker side：
[Aggregate Trade Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams)。
Nautilus 的官方 Binance Futures adapter 本身也把它转换为一个 `TradeTick`，所以格式上完全可接入；但
一个 tick 可能代表多笔成交，无法恢复这些 fill 的细粒度先后。

影响是：

- bar、成交量、aggressor side 和价量研究基本保持所需语义；
- 依赖逐笔成交到达顺序的 queue depletion 会更粗；
- 同一毫秒内精确 maker fill 次序、亚 100ms 排队变化不能从 `aggTrade + depth@100ms` 恢复。

因此主实验可以做 L2 因子和低参与率执行回测；maker fill 结果必须报告模型区间，不能只报告一个“精确”
PnL。未来若 queue 研究成为主问题，再单独开启 individual `trade` 的新实验；它不能追补当前历史，也仍然
不会变成 MBO/L3。

## 最小实施顺序

1. 等所有日期产出 `_PROCESSED` 或 `_QUALITY_REJECTED`，生成 date/symbol/validity inventory。
2. 先实现单 symbol、单 `VALID` segment 的 canonical exporter，并与现有 L2 reconstructer 在每个事件边界
   对比 best bid/ask、档位数和 checksum。
3. 同一 canonical 中间层分别输出 Nautilus model/catalog 和 HftBacktest event array，避免维护两套 gap/
   authority 逻辑。
4. 用一个短 segment 做 golden test：事件数、首末 sequence、snapshot checksum、最终 book checksum、
   trade volume、aggressor volume、两套引擎的 BBO 路径必须一致。
5. Nautilus 先做 L2 + TradeTick 基线；HftBacktest 再做 latency/queue sensitivity matrix。
6. 正式结果必须附数据质量 marker、validity mask、converter version、fee/funding、latency、queue/fill model
   和最大参与率；否则回测不可复现。

## 最终判断

当前数据不是“不能回测”，而是**适合严谨的 L2 回测，不适合被包装成精确 L3 仿真**。只要先补 canonical
exporter、严格使用 validity mask，并把 maker fill/latency 作为需要校准的模型参数，NautilusTrader 与
HftBacktest 都能产生有研究价值的结果。主线推荐 NautilusTrader，HftBacktest 用作执行假设的第二套验证；
两者共享同一个经过质量门禁的 canonical replay 层。
