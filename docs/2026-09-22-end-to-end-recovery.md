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
SSH 连接失败最多重试 20 次（用户本轮更新），达到上限停止访问 107，不并发重试。

网络断连根因不能仅凭 exit 255 推断。详细一手文档及取证方法见
[传输恢复调研](2026-09-22-transfer-recovery-research.md)。

部署实测见下文；历史重放与实时闭环分开验收。

## 处理端追加修复 / v0.5.13

实际历史重放发现去重的 `set_column(..., "is_duplicate", ...)` 丢掉非空字段约束，导致
不同 typed 文件出现 nullable/non-nullable 混合，projection writer 拒绝写入。
修复直接复用 canonical field；projection 使用 canonical Arrow schema 安全转换，
实际 null 不能伪造默认值。旧 typed 数据内容不改，不增加版本兼容层。

107 宿主的 `~/.local` PyArrow 被 Apptainer 自动导入，破坏锁定依赖。所有执行入口显式
`PYTHONNOUSERSITE=1`、清空 `PYTHONPATH`，verify 同时验证 miry/PyArrow 来自镜像 `/usr/local`。
v0.5.13 是中央处理/部署工具修复；Vultr 保留 v0.5.12 以免再次产生无必要的采集停机。

直连取证：SSH 在 KEX 阶段卡住，服务端未确认的报文不断重传；小报文路由和备用 443 端口
均未可靠恢复批量传输，已撤销。107 无可用 IPv6 路由。现有 127.0.0.1:26746 SOCKS 路径
两次完整目录读取成功，使用标准 OpenSSH ProxyCommand + netcat，不修改采集/ACK 合同。
模板为 `deploy/campus-107/ssh_config.example`，只匹配该 data-puller 目的端。
代理仍是运行依赖；不得将这项绕行措施描述为已经定位/修复校园公网设施。

历史任务以 Slurm 执行 `scripts/replay-recovered-days.py`，逐日使用前一日新 checkpoint。
9 月 22 日尚未封存，必须在该日正常处理完成后另跑 `--origin 2026-09-15 --start 2026-09-22
--through 2026-09-22`，不能用当前时刻推断该日后续质量。

## 实际部署和验收（截至 2026-09-22 06:34 UTC）

- Vultr：v0.5.12，OCI `sha256:e931eaf3c59106b50259309349850961a0cbdffcfab7209e3d516d4f1cddf0e6`。
  05:34:45 UTC 新容器启动，全部 source ready 后 OPEN 从 29 降到 0；后续抽样保持 0。
- 107：v0.5.13，SIF SHA-256
  `846f9cfd9a385412f26e9000ca74b4164b45014da971326668678a92986cf725`。
  SHA、install、verify 均通过，PyArrow 路径确认为 `/usr/local/lib/python3.12/site-packages/pyarrow`。
  原有 pull/submit cron 恢复；单 lane 拉取走现有 127.0.0.1:26746 SOCKS 代理。
- 实时闭环：原先 ready 积压 171 份 manifest、629,919,817 bytes；随后重新连续 ACK/GC。
  06:29、06:33 UTC 的 ACK apply 均正常；06:33:25 的待传清单为 1，hash/invalid/unknown/pending
  transaction 全部为 0。小报文路由、备用 443 listener、防火墙测试规则已撤销；没有弱化加密。
- 保持 formal-start SHA `3cc1d1d2d3c3ef24b56a94a32e3c133753fda6a165a18cbab65cec07d68edfce`，
  active universe SHA `07856438322852c43d9764954ce60d02bfc8144022f4e252217f0456457ba610` 不变。
  raw 未删除；服务器只按原 ACK 合同回收已持久化副本。
- 历史审计 job 75381：28 个 gap 均取得原始 typed 事件证据，时间范围
  `1789498553247362285..1789498554296733740` ns，即 09-15 18:55:53--54 UTC。
  v0.5.12 重放 job 75389 因 nullable schema 不一致退出，未发布错误结果。
  修复后 v0.5.13 重放 job **75410** 已启动，逐日重算 09-15--21；尚未验收全部完成。
  原质量结果在各日 `before-liveness-recovery/`，原 gap 文本在 `transport-gaps.unreconciled.jsonl`。

### 当前收尾阻塞

107 管理 SSH 随后要求二次验证：日志明确为 `Server accepts key` →
`Authenticated using publickey with partial success` → `keyboard-interactive`。
这不是密钥丢失或数据拉取再次停止；不应靠持续自动重试绕过验证。
用户允许的重试上限更新为 20 次，但在明确需要交互验证后停止认证尝试。

待用户验证后：核对 job 75410 的逐日结果；上传并安装已准备的
`/tmp/miry-recovery-v0.5.13/queue-recovery-day22.sh`（**尚未安装**）。它等待正常 09-22 finalize
job ID，再以 afterok 依赖排入修正任务，成功排队后移除自己的临时 cron，避免与正常处理并发。
还需同步 watchdog 的静默日志小改动。已部署的 watchdog 只在现有代理未监听时启动它，
不会重启当前共享代理；不依赖该账户的 systemd linger。

本次测试：244 passed，Ruff、mypy、冻结依赖锁检查通过。真实缺失的数据不能补造，
历史重算后的真实 gap 仍可能导致 `_QUALITY_REJECTED`，不得强制改成 `_PROCESSED`。
