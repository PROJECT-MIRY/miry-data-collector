# 2026-09-22 两端恢复 / v0.5.12

## 已验证的故障与修复

- WebSocket controller 取消共享 future，导致独立 liveness task 退出。修复见
  [取消传播记录](2026-09-22-liveness-cancellation.md)，包含锁、ACK、snapshot 三阶段回归。
- L2 将日切的 synthetic CLOSED 当作真实恢复，产生 1 ns VALID 尾段和错误 VALID checkpoint。
  现在日切 continuation 不解除 active gap；真正关闭且 snapshot bridge 成立后才恢复有效性。
- 旧 ACK 被新下载阻塞。现在先补发已持久化数据的 ACK，再下载；新增块仍需校验/fsync 后 ACK。
  失败状态记录阶段及已补发 ACK 数。SSH 显式单次连接、15 秒 alive、3 次无响应退出。

## 历史修复边界

`scripts/reconcile-liveness-gaps.py` 默认只生成方案，`--apply` 才修改派生文件。
仅处理起始日仍延续、单 symbol/stream、无 connection_id、明确由 liveness 超时产生的 gap。
从 typed 行查找检测之后的同币同流真实事件，记录其时间、connection/receive_seq/payload hash
及 typed 文件 SHA-256。证明的是 **stream 恢复活动**，不是 L2 立即有效；真正 L2 有效时间仍由
后续完整 replay 的 snapshot/sequence 决定。取到的证据时间是保守恢复界限，不宣称最早恢复点。

原始 raw、sealed manifest、collector gap 事实、generation 完全不改。派生 transport gap 的原文
保留在 `transport-gaps.unreconciled.jsonl`，证据保存于 `liveness-recovery.json`；旧 symbol 输出及
质量标记移动到 `before-liveness-recovery/`，可恢复。真实 route/sequence/reanchor gap 不动。
找不到全部候选的事件证据时拒绝修改；不得通过降低 99.9% 门槛或删除所有 gap 宣称质量恢复。

在 Slurm 计算节点执行，不能在登录节点扫描全部行情：

```bash
python scripts/reconcile-liveness-gaps.py \
  --derived-root /home/scc/pb24000367/Projects/bn/data/derived \
  --collector tokyo01 --start 2026-09-15 --through 2026-09-21
```

审核报告后相同命令加 `--apply`，并按日期顺序重建 L2 projection → L2 → finalize。
旧 normalized marker 缺少 source identity 时使用现有 `--bootstrap-legacy-identities` 显式回填，
不重写 raw、不重新抓取市场数据。每一天必须使用重建后的前一天 checkpoint。

## 部署与验收

构建正式 OCI/SIF 并校验 SHA；Vultr 在停机前完成无网络只读配置 preflight，保留状态后切换。
新 boot 等待全部 source ready，再关闭旧 open gap；这只证明当前恢复，不回填历史恢复时间。
107 暂停相关 cron，等待 pull/Slurm 排空，备份配置，安装 hash-named sandbox 后恢复 cron。
确认新版本、真实 pull complete、ACK apply/GC、无 hash mismatch，并区分质量拒绝与任务失败。
SSH 连接失败最多重试 3 次，达到上限停止访问 107。

网络断连根因不能仅凭 exit 255 推断。详细一手文档及取证方法见
[传输恢复调研](2026-09-22-transfer-recovery-research.md)。

部署实测结果在操作完成后追加；此节不代表已部署。
