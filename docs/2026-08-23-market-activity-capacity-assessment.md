# 市场活跃度与采集容量评估（2026-08-23）

## 结论

截至 `2026-08-23T11:29:19.505Z`，市场没有回到 8 月 19 日冲击前的低位，但必须把持续基线与
事故尖峰分开比较：

- 同一 522 合约固定面板、同为 UTC `00:00--09:59:59` 的十小时中，8 月 23 日成交额是
  8 月 19 日同期的 `2.384` 倍，交易数是 `1.463` 倍；当前正式 60 币分别是 `2.665` 和
  `2.026` 倍。当前持续基线实际上更热。
- 8 月 19 日事故日最强的 `14:00--15:59:59` 两小时尚未重现。按 crypto perpetual 固定面板，
  今天已闭合两小时峰值只有当时峰值的 `34.6%` 成交额和 `76.7%` 交易数；按正式 60 币则为
  `33.2%` 和 `73.8%`。
- 522 合约滚动 24 小时成交额是 8 月 19 日完整日的 `60.0%`，交易数则是 `110.7%`；这进一步
  表明名义成交额与成交事件数已经分化，但滚动窗口不适合判断某个两小时尖峰是否重现。

所以，“现在是否低于 8 月 19 日”没有一个单一答案：当前基线更高，事故尖峰仍较低。对于采集负载，
交易数与实际 WebSocket 消息率比名义成交额更相关，但二者都不能代替实际消息率。

采集器当前能持续处理这一级别的流量，queue、event-loop lag、RSS 和 cgroup throttle 都很低；
但最近 10 分钟平均使用约 `0.84` 核，107 上一轮有效传输速率约 `206.7 KiB/s`，与最近 10 分钟
Vultr 生成速率 `209.8 KiB/s` 基本持平。结论应是：**当前链路可以继续正式采集，但今天没有真正
压到 8 月 19 日的两小时尖峰，不能据此宣称 1C1G 已经通过同级峰值验收**。瓶颈优先级是 1 vCPU
CPU 余量和 107 传输吞吐，而不是内存、writer queue 或磁盘 I/O。

## 数据来源与口径

只使用一手来源：

1. Vultr 冻结观测
   `/srv/miry-data-rsync/control/universe/observations/2026-08-23/daily-klines.json.gz`，压缩文件
   SHA-256 `c97566a39900664ee4ec0eb84a2195fa5a51449bc8421f3a7f60148b2d5eea81`，解压内容
   SHA-256 `9db73ef64df6756366f96269168aaecf01602d8c8d8a106c82fcda917d58e325`。其中 522 个合约
   各有完整 35 日数据，窗口在 `2026-08-23T00:00:00Z` 截止。
2. Binance 官方 [Kline/Candlestick Data](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data)
   `/fapi/v1/klines`。日 K 线索引 7 是 quote asset volume，索引 8 是 number of trades；
   仓库也按这个合同解析，见 [`universe/evidence.py`](../src/miry/universe/evidence.py) 与
   [`collector/polling.py`](../src/miry/collector/polling.py)。
3. Binance 官方 [24hr Ticker Price Change Statistics](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/24hr-Ticker-Price-Change-Statistics)
   `/fapi/v1/ticker/24hr`。响应在 `2026-08-23T11:27:24.272Z` 至 `11:27:25.223Z` 获取，
   原始响应 SHA-256 `a45f4e4d057334d8c165dd780ddc8a0885cd1b76330718e4954408fac5422458`。
4. Vultr collector status、public traffic 和 cgroup 指标，以及 107 的 `last-pull.json`，采样截止
   `2026-08-23T11:31:23Z`。

同小时和峰值比较另从 Binance 官方 `/fapi/v1/exchangeInfo` 冻结当前合约集合，并用 `interval=2h`
读取 `2026-08-19T00:00:00Z` 至 `2026-08-23T09:59:59.999Z`。`exchangeInfo` 原始响应
SHA-256 为 `b35ed44bec617c22688468a07d415130de01626f00b42ce94c1f58da2907a0a3`；871 份成功的
Kline 响应按 symbol 排序后的 canonical SHA-256 为
`8507f98ab7fc3d0d8332173e7c889a6b2a69fc739b89688a744e00d367253efd`。查询时 Binance
server time 为 `2026-08-23T11:29:19.505Z`，因此只使用 10:00 UTC 前已经完全闭合的 Kline。

固定面板只保留冻结证据中恰好有 35 个完整日的 522 个合约，避免挂牌集合变化制造假增长。
滚动 24 小时响应原有 744 行，计算只取这 522 个共同合约。
569 合约面板只保留当前为 `TRADING`、`contractType=PERPETUAL`、稳定币计价、8 月 19 日前已上线，
且两个十小时窗口各有五根完整 Kline 的合约；它用于排除周末几乎停盘的 `TRADIFI_PERPETUAL`。
正式 60 币面板直接来自生产 `/etc/miry-data-collector/edge.yaml`，不按事后活跃度重新选样。

滚动 24 小时之外，补充的当天日化值来自同一面板的 522 次官方 `1d` K 线查询，查询横跨
`2026-08-23T11:28:01.680Z` 至 `11:28:34.930Z`；按 symbol 排序的 canonical response
SHA-256 为 `27be1814b1419e00456daaef4adf344b33dd15cf3980bbfb0d4c95eca1e180de`。
用查询中点距 UTC 00:00 的 `41,298.305` 秒将累计量乘以 `2.09210`。这个线性外推忽略日内季节性，
只作为当前 run-rate；滚动 24 小时值更适合表示完整 24 小时实际活动。

## 同口径比较

### 相同的前十个 UTC 小时

这是判断“当前常态”是否高于 8 月 19 日同期的主口径；每边都只使用五根已经闭合的 `2h` Kline：

| 固定面板 | 08-19 成交额 | 08-23 成交额 | 比值 | 08-19 交易数 | 08-23 交易数 | 比值 |
|---|---:|---:|---:|---:|---:|---:|
| 522 个冻结合约 | 7.443B | 17.741B | 2.384 | 47.166M | 69.002M | 1.463 |
| 569 个 crypto perpetual | 8.824B | 20.625B | 2.338 | 48.912M | 72.874M | 1.490 |
| 正式采集 60 币 | 5.731B | 15.275B | 2.665 | 15.475M | 31.349M | 2.026 |

522 固定面板中，`78.5%` 的合约成交额上升、`71.5%` 至少上升 25%；`84.9%` 的合约交易数
上升、`79.1%` 至少上升 25%。正式 60 币对应 breadth 为 `78.3% / 71.7%` 和
`81.7% / 76.7%`。所以当前高基线是广泛变化，不是单纯由 BTC/ETH 总量制造。

### 已观察到的两小时峰值

| 固定面板 | 08-19 峰值（14:00） | 08-23 峰值（04:00） | 当前/旧峰值 |
|---|---:|---:|---:|
| 569 crypto：成交额 | 21.810B | 7.543B | 34.6% |
| 569 crypto：交易数 | 23.768M | 18.235M | 76.7% |
| 正式 60 币：成交额 | 17.698B | 5.881B | 33.2% |
| 正式 60 币：交易数 | 11.832M | 8.734M | 73.8% |

这说明当前已在承受更高的普通时段活动，但尚未经历 8 月 19 日同级的急剧两小时峰值。`2h` Kline
仍会隐藏分钟级尖峰，所以它是比整日更接近容量问题的代理，不是最坏秒级流量的上界。

### 完整日与滚动窗口

| 窗口 | 总 quote volume | 总交易数 | 相对 08-19 成交额 | 相对 08-19 交易数 |
|---|---:|---:|---:|---:|
| 08-18 完整 UTC 日 | 19.274B | 97.273M | 32.6% | 66.7% |
| 08-19 完整 UTC 日 | 59.161B | 145.744M | 100.0% | 100.0% |
| 08-20 完整 UTC 日 | 56.689B | 157.698M | 95.8% | 108.2% |
| 08-21 完整 UTC 日 | 84.155B | 216.443M | 142.2% | 148.5% |
| 08-22 完整 UTC 日 | 54.022B | 234.036M | 91.3% | 160.6% |
| 截止 11:27 UTC 的滚动 24 小时 | 35.472B | 161.384M | 60.0% | 110.7% |
| 08-23 截止 11:28 UTC 的线性日化 | 42.287B | 163.579M | 71.5% | 112.2% |

8 月 19 日相对 18 日的冲击仍被新冻结证据复现：成交额 `3.069` 倍、交易数 `1.498` 倍，
分别有 `78.4%` 和 `77.4%` 的合约上升。此后并非简单回落：8 月 22 日相对 19 日，虽然市场总成交额
下降 `8.7%`，仍有 `85.8%` 的单币成交额上升；总量下降主要由头部合约贡献变化造成。同期
`89.8%` 的合约交易数高于 19 日。

当前的横截面也说明同样问题：

| 当前窗口相对 08-19 | 成交额更高的合约 | 成交额高至少 25% | 交易数更高的合约 | 交易数高至少 25% |
|---|---:|---:|---:|---:|
| 滚动 24 小时 | 59.2% | 42.3% | 77.8% | 68.4% |
| 08-23 线性日化 | 61.5% | 42.7% | 75.5% | 63.2% |

正式采集的 60 币中，`GRVTUSDT` 没有完整 35 日历史；其余 59 币的同口径结果为：8 月 19 日
`54.186B / 61.901M trades`，当前滚动 24 小时 `29.564B / 70.239M`，当前线性日化
`36.216B / 73.230M`。因此当前实际成员的成交事件活动同样高于 8 月 19 日。

## 实际采集负载

市场成交额不是数据字节数。下面使用生产 collector 自身计数判断容量。

Vultr 在 `10:40:28.709Z` 至 `11:30:29.440Z` 的 50 分钟窗口中：

| 指标 | 结果 | 合同/解释 |
|---|---:|---|
| compressed raw 生成速率 | 平均 144.4 KiB/s | 最近 10 分钟 209.8 KiB/s |
| ingest events | 平均 2,775/s | 最近 10 分钟 4,187/s |
| CPU | 平均 0.657 core | 最近 10 分钟平均 0.840 core |
| public 消息/分钟 | P50 139,221；P95 250,533；峰值 293,892 | 4 个 route 合计 |
| queue ratio | P95 3.4%；峰值 3.6% | 合同上限关注 50%/70% |
| event-loop lag | P95 26.8ms；峰值 41.7ms | 合同 p99 小于 100ms |
| RSS | P95 232.9MiB；峰值 239.2MiB | 768MiB 容器上限 |
| cgroup throttle | 117/123,202 periods；累计 57.7ms | 当前不是主要瓶颈 |

队列、延迟、内存和 throttle 说明采集器并未处理不及；但 CPU 的 50 分钟均值已经略高于
[`collection-contract.md`](collection-contract.md) 的 65% 目标，最近 10 分钟也高于 80% 目标。
它仍在正常收包，不等于还有足够的 CPU 峰值余量。

107 在 `10:42:08.171Z` 至 `11:17:28.737Z` 成功验证并 ACK `448,917,230` bytes，
`failures=0`，折合约 `206.7 KiB/s`。下一轮 cron pull 在 `11:31Z` 仍在执行。因此：

- 对 50 分钟平均生成速率，传输吞吐约有 `43%` 余量，会排空 backlog；
- 对最近 10 分钟生成速率，传输基本持平，持续尖峰会缓慢积压；
- ACK 只在完整 chunk 落到 107 并校验后生效，所以 pull 运行期间 Vultr spool 上升是正常锯齿，
  不能仅凭单次 spool 读数判定传输失败。

`11:30Z` 的 spool 为 `444,930,159` bytes，可用磁盘 `13,226,864,640` bytes；10GiB spool
上限比 2GiB free-space 保护线更早触发，剩余有效 headroom 约 `9.59GiB`。若 107 完全停止 ACK，
按 50 分钟平均速率约可维持 `19.3` 小时，按最近 10 分钟速率约 `13.3` 小时。保护逻辑届时会
显式登记 storage gap 并停止 sources；它能防止静默损坏，但不能制造缺失期间的市场数据。

## 是否能应对另一次 08-19 冲击

分三层回答：

1. **采集进程：当前高基线正常，但旧峰值尚未被实测。** 生产 collector 仍保持低 queue、低
   event-loop lag、低 RSS、无重启；然而旧峰值的交易数约为今天两小时峰值的 `1.30--1.36` 倍，
   而最近 10 分钟 CPU 已达 `0.84` 核。不能把二者机械线性换算，但这足以说明 1 vCPU 峰值余量
   未经证明。8 月 19 日把短暂存储保护放大为 18 小时事故的 `sources already running` 恢复 bug
   已有单独的事故修复记录，见
   [`v0.3.4-storage-recovery-incident.md`](v0.3.4-storage-recovery-incident.md)。
2. **107 传输：平均态可以，持续峰值余量不足。** 当前有效吞吐略低于最近 10 分钟生成峰值。
   网络或 107 变慢数小时不会立即丢数据，但 backlog 会增长。
3. **长时间失联：不能保证。** 当前磁盘只为完全无 ACK 提供约 13--19 小时缓冲。类似冲击若叠加
   107 超过这个时长不可用，仍会触发显式 storage gap；与旧事故不同的是它不应再 crash loop，
   但对应市场原始数据仍不可补回。

因此现在无需暂停正式采集，但完成“可证明承受 8 月 19 日同级峰值”的最低补项是：连续保存至少
24 小时 CPU、event-loop、queue、raw bytes/s 与 pull bytes/s 的分位数；用保留原始事件结构的
受限 replay/load test 将 public 消息率逐步推到当前峰值的 `1.4` 倍；对 107 链路做持续吞吐基线；
将无 ACK 容量目标从当前约 13--19 小时提升到至少 24 小时。WebSocket 分片数测试应以
`symbol-gap-seconds` 和 CPU 为主指标，不能用成交额变化代替负载测试。

## Binance 官方快照与 v0.5.0 链路复核（2026-08-23 17:43 UTC）

### 当前市场不是 08-19 同级价格冲击，但活动仍处高位

Binance 官方 `/fapi/v1/time` 在本轮查询返回 `2026-08-23T17:33:39.399Z`；按同刻
[`exchangeInfo`][binance-exchange-info] 中 `TRADING + PERPETUAL + USDT` 筛选，官方
[`24hr ticker`][binance-24h-ticker] 的 527 个合约合计滚动 24 小时 quote volume 为
`40.230B USDT`、交易数为 `162.869M`。其中 299 涨、224 跌、4 平，价格变化中位数为
`+0.356%`；81 个合约绝对涨跌至少 5%，24 个至少 10%。这不是方向一致的全市场价格危机，
但尾部合约仍有明显活动。

以仓库冻结的 [`formal-universe-7.0-evidence.json`](formal-universe-7.0-evidence.json) 精确筛选正式
60 币，`2026-08-23T17:42:56Z` 的滚动 24 小时结果为 `33.956B USDT / 72.711M trades`，
33 涨、27 跌，价格变化中位数 `+0.335%`，14 个绝对涨跌至少 5%，5 个至少 10%。与本文前述
08-19 可比 59 币完整日的 `54.186B / 61.901M` 相比，当前名义成交额只有 `62.7%`，交易数却为
`117.5%`。两个窗口分别是滚动 24 小时和 UTC 自然日，适合判断量级，不应解释为严格日内因果比较。

BTC、ETH、SOL 提供了急性冲击的直观对照。Binance 官方 [`5m Kline`][binance-kline] 显示，
08-19 三币合计 `45.020B USDT / 17.530M trades`，分别是当前滚动 24 小时
`21.967B / 11.490M` 的 `2.05x / 1.53x`；当日三币涨幅为 `+7.13% / +17.48% / +10.83%`，
当前则为 `+0.009% / +0.614% / +0.977%`。因此当前不是 08-19 同级急性行情，但较高的交易事件
基线没有消失。

生产 evaluation 在 `2026-08-23T17:20:31.956Z` 仍报告 `ACTIVITY_SHOCK_PENDING`：最近 3 个
完整日相对 28 日基准的 quote volume/trade count 横截面因子为 `2.375/2.365`，breadth 为
`80.8%/79.6%`。这与当前价格温和并不矛盾：前者描述多日成交活动相对旧基线的广泛抬升，后者描述
滚动 24 小时价格方向。系统继续冻结普通成员轮换是正确行为。

### v0.5.0 生产容量

在 `17:17:40--17:42:40Z` 的稳态窗口和 27 个完整 public minute 中：

| 指标 | 结果 |
|---|---:|
| public 消息/分钟 | P50 `156,643`；P95 `238,487`；峰值 `279,092` |
| CPU | 平均 `0.492` core；峰值分钟 `0.627` core |
| ingest events | 平均 `2,971/s`；峰值 `4,622/s` |
| compressed raw | 平均 `155.4KiB/s`；峰值 `236.0KiB/s` |
| queue ratio | P50 `1.9%`；P95 `3.3%`；最大 `4.4%` |
| event-loop lag | P95 `18.7ms`；最大 `37.7ms` |
| audit / ping RTT | P95 `56.7ms / 101.0ms`；最大 `100.8ms / 169.1ms` |
| RSS / cgroup memory | 最大约 `297.0MiB / 304.9MiB` |

同一部署后的 53 个宿主机诊断样本覆盖 `17:16:05--17:43:30Z`：TCP retransmission 和 timeout
增量均为 0，collector socket `Recv-Q` P95 为 `628B`、最大 `33,676B`，真实数据连接 TCP RTT
P95/最大为 `13.11/18.13ms`，53 次 Binance HTTPS 探针全部成功。CPU PSI `avg10` P95/最大为
`14.82%/18.07%`，但容器只在 3 个 period 被 throttle，累计 `0.485ms`；OOM 为 0。启动初期两个
open-gap 诊断样本对应唯一一次 v0.5.0 受控部署 gap（`75.596s`），其后 connection failure 为 0，
当前 open gap 为 0。

107 在北京时间 `01:16:44--01:43:11` 完成 28 轮 pull，共校验并 ACK `274,502,097` bytes，
`failures=0`。实际 rsync 阶段平均约 `625KiB/s`，按整段墙钟折算约 `168.9KiB/s`，高于同期 raw
生成的 `155.4KiB/s`；Vultr `ready_manifests_remaining=0`，因此当前 backlog 正在及时清空。
Vultr 可用磁盘约 `12.37GiB`，但 10GiB spool 上限仍只提供约 12--18 小时无 ACK 缓冲，达不到
“冲击与 107 失联同时持续 24 小时”的目标。

### 协议容量与最终判断

Binance 官方 [WebSocket Connect][binance-ws-connect] 合同要求使用 `/public`、`/market` 或
`/private` 路由，单连接 24 小时强制断开、最多 1,024 streams、client-to-server 最多 10 条消息/秒，
并规定 3 分钟 Ping 和 10 分钟 Pong deadline。生产当前使用
`wss://fstream.binance.com/public/stream` 与 `.../market/stream`；4 条 public route 的订阅数为
`28/28/32/32`，market route 为 `181`，均远低于 1,024。配置中的
`connection_rotation_seconds=82800` 会在 23 小时提前轮换，见
[`edge.yaml.example`](../deploy/vultr/edge.yaml.example) 和
[`sources.py`](../src/miry/collector/sources.py)。所以当前风险不是 Binance 协议数量上限。

综合结论是：**v0.5.0 可以继续正式采集，也已经实测承受接近此前生产记录的单分钟流量峰值；但还不能
宣称可以无 gap 承受 08-19 同级、持续两小时的市场危机。** 当前峰值时 CPU、queue、event-loop、
socket backlog 和网络均有余量，说明热路径优化有效；剩余不确定性是 08-19 没有同口径真实 WebSocket
消息率、v0.5.0 观察窗口尚不足 24 小时，以及 107 完全失联时 spool 不足 24 小时。正式“危机通过”
仍应以 24 小时生产分位数和保留真实消息结构的 `1.4x` 受限 replay 为门禁，不能用 Kline 交易数的
线性外推替代。

[binance-exchange-info]: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information
[binance-24h-ticker]: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/24hr-Ticker-Price-Change-Statistics
[binance-kline]: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data
[binance-ws-connect]: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect

## 限制

- `number of trades` 是 Binance K 线返回的成交事件计数，不是独立交易者数量。
- quote volume 衡量名义成交活跃度，不代表盘口更新数、raw 字节数或可成交深度。
- 当天线性日化假设 UTC 日内活动均匀，可能高估或低估最终完整日；滚动 24 小时值没有这个外推误差。
- 固定 522 面板排除了期间新上市和已不具完整历史的合约，适合做同口径变化，不是当天全部挂牌市场总量。
- 本报告证明的是当前生产窗口，不替代 24 小时和 7 日稳定性验收。
