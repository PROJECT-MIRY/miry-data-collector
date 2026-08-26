# 2026-08-25 L2 snapshot bridge 卡死诊断与修复合同

## 事故边界

UTC `2026-08-25` 的派生质量报告识别出三个小时级 L2 无效区间：

- `APTUSDT`：`00:00:50--02:18:50`，约 2 小时 18 分；
- `CRVUSDT`：`02:18:17--07:50:13`，约 5 小时 32 分；
- `BSBUSDT`：`02:18:17--07:49:45`，约 5 小时 31 分。

它们不是同长度的网络中断。WebSocket raw 很快恢复，但旧 collector 在 HTTP 200 后立即把 snapshot
从 pending 移除，并把整条 route 记成 snapshot ready；它没有验证 snapshot 是否与缓存 diff overlap，
也没有在 non-overlap 时重抓。APT 的 snapshot 落在两条官方事件之间；CRV/BSB 的约 0.9--1.15 秒
REST 请求返回时，snapshot 已早于新连接最早缓存的 diff。三者都没有任何事件满足官方
`U <= lastUpdateId <= u`，只能等到下一次 route 重建碰巧取得可 overlap snapshot。

CRV/BSB 的捕获连接分别包含 921/894 条 diff，没有一条覆盖 stale snapshot，也没有一条的 `pu` 等于
snapshot ID。APT 的第一条 diff 虽有 `pu == lastUpdateId`，但官方初始化算法仍要求第一条保留事件
覆盖 snapshot ID；正式重建不能用本地扩展放宽该条件。

## 修复合同

`miry.orderbook.bridge` 是在线与离线共享的唯一 bridge 语义：

1. WebSocket 订阅后先缓存每币 `U/u/pu`；
2. snapshot fetched 只持久化 raw，不完成 readiness；
3. 丢弃 `u < lastUpdateId`，要求第一条保留事件满足 `U <= lastUpdateId <= u`；
4. snapshot 早于保留窗口时标记 stale 并按币重抓；snapshot 位于 stream 前方时继续等待 diff；
5. bridge 后要求每条 `pu == previous.u`，不连续则打开 `L2_SEQUENCE_GAP` 并重新 anchor；
6. 单币恢复最多尝试 5 个 snapshot，仍失败则重建所属 route，gap 始终保持 OPEN；
7. route transport 与 L2 readiness 分离：前者关闭 `CONNECTION_LOST_GAP`，全部 bridge 后才关闭
   `L2_REANCHOR_GAP`。

边缘只缓存有界 update-ID 元组，不复制完整盘口；snapshot HTTP 仍由全局 0.75 秒起点间隔和最多 4 个
在途请求限速。离线 pipeline 继续独立重放完整 book，但调用同一 overlap/continuity 规则。

## 历史数据

历史 raw、quality marker 和 gap 不得改写。CRV/BSB 在上述窗口没有可 bridge snapshot，无法从现有
raw 恢复绝对 L2 盘口。APT 也不应绕过官方 overlap 条件追认 VALID。修复只保证未来 stale snapshot
会立即重试，不把不可证明的历史区间伪装为完整数据。
