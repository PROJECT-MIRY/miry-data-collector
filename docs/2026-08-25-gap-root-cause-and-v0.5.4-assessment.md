# Gap 根因与 v0.5.4 稳定性修复（2026-08-25）

## 结论

从 v0.5.0 上线到 `2026-08-25 08:47 UTC` 共登记 34 个已闭合 gap：3 个
`COLLECTOR_STOPPED_GAP`、15 个 `INGEST_OVERLOAD_GAP` 和 16 个
`CONNECTION_LOST_GAP`。其中两个 stopped gap 是受控部署；另一个是日切缺陷。当前没有未闭合 gap。

这些 gap 不能统一归因于不可抗力网络。远端主动关闭和公网丢包本身不可控制，但主要事件同时出现
多 MiB socket `Recv-Q`、raw queue 上升和 1.6--3.7 秒 event-loop lag；TCP timeout 始终为 0，
多数窗口的 retransmission 增量为 0 或个位数。证据更支持单核接收调度和恢复风暴放大了外部抖动。

## 事件归因

| UTC 时间 | 现象 | 结论 |
| --- | --- | --- |
| 08-23 22:01--22:03 | 旧 64MiB queue 撞线，15 个 overload gap | 本机容量缺陷；v0.5.1 的 192MiB queue 后未再 hard reject |
| 08-24 14:24 / 15:44 / 17:23 | 约 567k--583k public msg/min，Recv-Q 16--25MiB，lag 2.2--3.7s | 以本机背压为主，网络可能助推 |
| 08-25 00:00 | 日切搬迁约 41 个 symbol，订阅 ACK 超时后全进程退出 | 确定性代码缺陷，不是网络故障 |
| 08-25 02:18 | retrans +7、HTTPS 探针约 0.8s，同时 Recv-Q 21.7MiB | 网络与本机积压的混合事件 |
| 08-25 07:49--07:52 | 四条 public route 同秒断开，恢复中二次断联 | 初始远端原因不能完全排除；retrans=0、TCP timeout=0，恢复风暴由本机积压明确放大 |

07:49 事件中 public-0 到最终有效 snapshot 约 159 秒，其他 route 约 80--104 秒。107 pull、rsync 和
ACK 在这些窗口保持正常，不是 gap 来源。

## 日切缺陷

旧实现即使 universe 成员不变，也会在 `00:00 UTC` 新建空 assignment 的 sharder。新的确定性目标
丢失当前 route 布局，导致约三分之二 symbol 同时执行 add-ready-remove 和 L2 snapshot。update 的
20 秒 ACK deadline 又从入队时开始，与 audit/control lock 竞争；任一 timeout 从 midnight task
逃逸后，collector 的 fail-fast 监控会停止全部 sources。

v0.5.4 将流量校准移到独立 background loop。UTC writer rotate、universe event、grace 和 day seal
先完成；可恢复的 `SourceUpdateError` 只进入局部重连和有界重试，不再终止 collector。

## v0.5.4 修复

- 继承当前 route assignment；成员轮换只处理真正退出和进入的 symbol。
- 每小时评估最近 24 个完整流量块，最大 route 超过平均值 `1.25x` 才尝试改善。
- 单批最多交换一对 symbol；成功后冷却 5 分钟并继续，直到持续失衡收敛。
- queue 达到 50% 或 public route 未完全 ready 时暂停；失败按 30/60/120 秒重试。
- 继续使用 add-ready-remove；裁剪失败时扩展覆盖保留，下一次 route-local recovery 收敛。
- ACK deadline 从 controller 取得锁并开始请求后计时；控制响应先交付 waiter，再等待 raw boundary。
- receiver 每 64 帧 cooperative yield，避免有 backlog 的连接长期独占事件循环。
- 正常 admission 去掉重复 condition/boundary lock；writer rotation 仍以关闭 admission 的 barrier 保证
  FIFO 合同边界。

这些修改不改变正式 60 币、raw schema、gap schema、universe identity、rsync/ACK 或 107 派生合同。
Vultr 和 107 均采用保留状态升级，历史 gap 仍作为研究有效区间的排除依据。

## v0.5.5 收敛补丁

上线复核补充了两个恢复不变量：裁剪阶段失败后，下一次维护先恢复最后提交的 route assignment，
不能把临时重复覆盖当成新的合法分片；正式成员更新失败时，日切不终止 collector，而是让变更 symbol
的 planned gap 保持 OPEN 并后台重试。该补丁不改变上面的根因归类或数据合同。
