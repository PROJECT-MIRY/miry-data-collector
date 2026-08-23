# Public WebSocket 4/8 分片 A/B 评估（2026-08-23）

## 目标

比较 4 与 8 条 public WebSocket route 的故障半径、恢复时间和 1C1G 成本。A/B 不改变正式 60 币、
universe、raw、ACK 或 107 合同。

## 上线前结果

确定性拓扑门禁使用正式 60 币：

| 指标 | 4 shards | 8 shards |
|---|---:|---:|
| route 币数 | 14/14/16/16 | 1/7/8/9/8/9/9/9 |
| 最大单 route 币数 | 16 | 9 |
| 最大 snapshot 纯调度等待 | 11.25s | 6.00s |
| 最大静态估算 route rate | 23,695/min | 16,900/min |

满 4 个小时的生产峰值证据试算时，8 条 route 为 5--9 币且约 68.5k/min；证据不足 24 块，不能
用于正式切换。

本地 `1 CPU / 768MiB / 256 PIDs` Binance smoke 使用同一代码和正式 60 币：

| 指标 | 4 shards | 8 shards |
|---|---:|---:|
| WebSocket 总数（含 market） | 5 | 9 |
| PIDs | 9 | 12 |
| 短窗 RSS | 316MiB | 321MiB |
| 初始订阅 RTT 最大值 | 641ms | 1,952ms |
| 本地代理连接失败 | 5 | 0 |

本地代理随机断线，最后一行不能用于频率结论；该 smoke 只证明 8 条的资源、订阅和 snapshot 路径
可运行且无 OOM。

## 生产实验

1. 部署同一 v0.5.0 镜像并保持 `public_connection_shards: 4`，从诊断 timer 首条无错误记录开始计时。
2. 收集至少 24 小时，要求 24 个完整 message-rate block。
3. 记录每 100 connection-hours gap 数、transport/L2 `symbol-gap-seconds`、恢复 p50/p95/p99、CPU、
   throttle、event-loop、queue、RSS、TCP retrans/RTO、探针和 ACK 吞吐。
4. 在 UTC 边界受控改为 8，保留全部状态并只重启一次；再收集同样时长。
5. 8 条只有在 L2 symbol-gap-seconds 至少降低 30%、最大影响不超过 9 币，且 CPU/lag/queue/ACK
   不退化时才保留；否则恢复 4。

原始 gap 数不能直接比较：8 条的 connection-hours 是 4 条的两倍。成交额和交易数只描述市场状态，
不能替代实际 public message rate。

## v0.4.2 候选热路径优化

`2026-08-23 13:18 UTC` 的真实一分钟 raw 包含 `339,388` 条事件，约 `5,649/s`：depth
`12,629` 条，book ticker、成交和 market 事件 `326,759` 条。单核隔离计时得到：完整 WebSocket
JSON 解码约 `2.13s/min`、RawEvent 构造约 `1.01s/min`、现有 admission 约 `1.31s/min`，Arrow
转换和 Zstd 写入不足 `1s/min`。因此完整 JSON 和 writer 都不是生产 CPU 的单一主因。

生产 `py-spy --gil` 采样显示连接循环在每条消息上执行 `create_task(websocket.recv)`、
`asyncio.timeout` 和 `asyncio.wait`，把约 `5.6k/s` 的数据率放大为同量级的 task、future、timer 和
集合调度。回归测试在 200 条连续消息上记录到 203 个 task，证明 task 数随消息量线性增长。

候选实现改为每个连接固定的 receiver、receive watchdog、subscription update 和 audit task；
控制请求使用 request ID 到 Future 的 broker。receiver 直接等待下一帧，不再为每条消息创建 task
或 timeout。相同 100k 真实 payload 的单核回放从 `1.959s` 降到 `0.449s`，约 `4.36x`；启用真实
IngestCoordinator 后连续五次为 `0.811--0.870s`，约 `115k--123k events/s`。

该优化不改变 raw bytes、接收时间、receive sequence、writer group、manifest、gap、snapshot、
universe 或 107 合同。初始 SUBSCRIBE 必须在 receiver 启动前注册并发送；动态更新仍等待
UNSUBSCRIBE 与 SUBSCRIBE 双 ACK；depth sequence recovery task 的异常仍会终止所属连接。

本地 `1 CPU / 768MiB` live smoke 已通过真实 ACK、snapshot、audit 和 raw 写入路径，但本地代理
地址 `198.18.0.213` 持续产生 no-close-frame，不能用于 gap 频率对比。生产验收必须保持 4 shards，
比较部署前后相同市场流量下的 CPU、PSI、socket Recv-Q、audit/ping RTT、transport/L2
symbol-gap-seconds 和 ACK 吞吐，再决定是否进入 8 shards 阶段。

## 断联恢复与轻量工具调研

Binance 官方规定单连接最多存活 24 小时、每连接最多 1,024 streams，并由服务端每 3 分钟发送
Ping；10 分钟收不到 Pong 会断开。项目保持 23 小时 add-ready-remove 重建，提前于强制断开且旧连接
在新连接完成订阅、首事件和 snapshot 前不退出。来源：[Binance Connect][binance-connect]。

动态 SUBSCRIBE、UNSUBSCRIBE 和 LIST_SUBSCRIPTIONS 都以 request ID 匹配响应；成功响应为
`{"result": null, "id": ...}`。v0.4.2 候选的 control broker 直接保留该合同，不能用“已发送请求”
替代 ACK。来源：[Binance live subscriptions][binance-live-subscriptions]。

Binance 本地盘口规则要求第一批 diff 覆盖 REST snapshot 的 `lastUpdateId`，后续每条 diff 的 `pu`
必须等于前一条 `u`，否则必须重新从 snapshot 初始化。因此 sequence 检查和 snapshot 请求必须留在
Vultr；107 延迟发现后无法取得缺口当时的 snapshot。来源：[Binance local order book][binance-book]。

`websockets` 官方 keepalive 每 20 秒 Ping，并在 20 秒内无 Pong 时关闭连接；官方同时指出，当接收方
处理不过来时，数据在 buffer 中等待也会表现为 Ping 延迟。`max_queue` 是 frame queue 的高水位，队列
满后停止读 socket 形成 TCP backpressure；盲目扩大 buffer 会造成 bufferbloat 和更高延迟。因此保持
`ping_interval=20`、`ping_timeout=20`、`max_queue=16`，先降低每帧 CPU，不能靠放宽 timeout 或增大
queue 掩盖过载。来源：[keepalive][websockets-keepalive]、[memory and buffers][websockets-memory]、
[asyncio client API][websockets-client]。

候选工具实测和结论：

| 工具或选项 | 结论 | 依据 |
|---|---|---|
| `websockets` 新 asyncio client | 保留 | cancel-safe `recv`、Ping/Pong latency、frame flow control 和成熟异常类型与现有 gap 状态机吻合 |
| `uvloop` | 暂不加入 | 官方推荐用于 asyncio；本项目 100k 真实消息回放仅从 `0.843s` 降至 `0.824s`，约 2.2% |
| `msgspec` | 暂不加入 | 官方提供 typed fast decode，但生产 GIL profile 中现有 `orjson` decode 约 3%，不是主热点 |
| `compression=None` | 不采用 | permessage-deflate profile 占比较小，而官方语料显示压缩通常减少 80% 以上网络流量 |
| `picows` | 仅保留实验候选 | 官方宣称 C/Cython、zero-copy 和最高约 2x；core API 不支持 permessage-deflate，切换会重写成熟恢复合同 |
| `wsproto` | 不采用 | 只是纯 Python Sans-I/O 状态机，不提供网络、backpressure 或并发层，需要重新实现现有能力 |
| `aiohttp` WebSocket | 不迁移 | 已用于 REST，但更换 WebSocket client 没有本项目基准收益，反而扩大审计、latency 和异常语义变更面 |

`websockets` 官方也建议测试 `uvloop`，但通用 echo benchmark 不能替代本项目的生产 payload 与 gap
合同；本项目实测收益不足以承担新的 runtime 依赖。`picows` 与 `msgspec` 只有在 v0.4.2 生产 profile
显示对应路径成为新主热点时才重新评估。来源：[websockets performance][websockets-performance]、
[uvloop][uvloop]、[msgspec][msgspec]、[picows][picows]、[wsproto][wsproto]。

后续可独立评估 Binance 官方列出的第二 endpoint `wss://stream.binancefuture.com`：只在连接建立连续
失败时轮换 endpoint，不在健康连接运行中搬币，也不能缩短或删除 gap。该项尚无生产 A/B 证据，
不进入 v0.4.2。

[binance-connect]: https://developers.binance.com/legacy-docs/derivatives/usds-margined-futures/websocket-market-streams
[binance-live-subscriptions]: https://developers.binance.com/legacy-docs/derivatives/usds-margined-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams
[binance-book]: https://developers.binance.com/legacy-docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
[websockets-keepalive]: https://websockets.readthedocs.io/en/stable/topics/keepalive.html
[websockets-memory]: https://websockets.readthedocs.io/en/stable/topics/memory.html
[websockets-client]: https://websockets.readthedocs.io/en/stable/reference/asyncio/client.html
[websockets-performance]: https://websockets.readthedocs.io/en/stable/topics/performance.html
[uvloop]: https://github.com/MagicStack/uvloop
[msgspec]: https://github.com/jcrist/msgspec
[picows]: https://github.com/tarasko/picows
[wsproto]: https://github.com/python-hyper/wsproto

## Unicode Canonical Symbol

Binance 当前 `exchangeInfo` 包含 `币安人生USDT`、`我踏马来了USDT` 和 `龙虾USDT` 等真实中文
canonical symbol。A/B 的 eligible pool、message-rate key、WebSocket/REST 和 107 symbol 文件均保留
原始 UTF-8 identity。安全合同只允许 NFC 规范化的 Unicode 字母数字，禁止空白、路径分隔符、控制
字符和非 canonical 大小写。
