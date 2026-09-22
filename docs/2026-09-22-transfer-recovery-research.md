# 107 → Vultr 传输恢复调研

日期：2026-09-22。代码基线：`3b91085`；比较起点：`v0.5.9`。
本调研只读本地代码与官方文档，没有连接 107/Vultr，没有改变线上参数。
线上断连次数和 SSH 服务端日志由部署操作单独取证，不能用以下机制分析代替根因证据。

## 代码确定的问题

### 1. 旧 ACK 的恢复被新下载阻塞

[`run_pull`](../src/miry/pipeline/pull.py) 顺序为 inventory → 所有下载 lane → 本地校验持久化 →
上传 ACK。若上轮数据已经持久化但 ACK 上传失败，本轮必须先完成新下载，才能重发旧 ACK。
因此，下载连续失败会使**已经持久化的数据也无法释放 Vultr ready 空间**。
这属于可从代码确定的恢复路径缺陷；不代表它已经解释本次所有 SSH 断连。

建议：先单独补发已有 staging ACK，再做新下载；新数据仍只能在完整校验、fsync、原子发布后
生成 ACK。补发成功后即使本轮下载失败，状态也应记录已完成的 ACK 数量，不能把整轮笼统描述为
“什么也没传”。旧 ACK 失败必须保留，下一轮重传，不能靠删除 ACK 掩盖问题。
如采用这一改动，应同步更新 [ACK 合同](transfer-ack-contract.md) 中“全部 lane 完成前不得 ACK”
的表述，明确该约束针对**本轮新增数据**，不是此前已经持久化的块。

### 2. 一轮最多启动六个 SSH 连接，但并发最多四个

[`RsyncTransport.pull_ready`](../src/miry/pipeline/pull.py) 一次 inventory 加最多四个互斥下载
lane，随后一次 ACK upload；空批次较少。除非用户环境已经配置 multiplexing，均是独立连接。
[`pull-once.sh`](../deploy/campus-107/pull-once.sh) 已用 `flock` 防止多个 cron 批次重叠，
不能把正常四 lane 误认为 cron 堆积。

独立握手较多可能放大坏链路下失败概率和 1c1g CPU 开销，但**尚无证据证明它就是当前根因**。
默认先保留已有吞吐参数，临时降为一或两 lane 必须观察 backlog 是否增长；不应未经测量将其
永久改成单路，也不应全局调大 sshd 连接限制。

### 3. 连接、静默传输已有超时，整个子进程没有总期限

[`_ssh_command` / `_run`](../src/miry/pipeline/pull.py) 已设置严格 host key、公钥 identity、
`BatchMode`、`ConnectTimeout=20`，rsync `--timeout=120`；未显式设置 SSH alive 检测，
`subprocess.run` 没有独立总时限。

官方语义：`ConnectTimeout` 覆盖 TCP 建立及初始 SSH 握手；`ServerAliveInterval` 与
`ServerAliveCountMax` 检测建立后无响应的 SSH。`--timeout` 是 rsync 无数据 I/O 时限，
不是整个下载总时长；`--contimeout` 仅用于 rsync daemon，不适用于本项目 SSH 模式。
[OpenSSH 客户端手册](https://man.openbsd.org/ssh_config#ConnectTimeout)、
[rsync 官方手册](https://download.samba.org/pub/rsync/rsync.1#opt--timeout)。

最小加固建议：显式 `ConnectionAttempts=1`、`ServerAliveInterval=30`、
`ServerAliveCountMax=3`，保留当前 rsync I/O 时限。它只能识别不响应的连接，不能保证连接不掉。
如果另加整个命令 deadline，必须覆盖健康积压下载所需时间，超时要回收 rsync/ssh 整个进程组，
保留 partial，不能把 120 秒误当作所有下载允许的总时长。

## 如何区分故障，而不是猜测公网问题

SSH `255` 只表示错误，不能单独区分密钥、远端主动关闭、网络丢包或服务端限流。
[OpenSSH ssh(1)](https://man.openbsd.org/ssh.1#EXIT_STATUS)。
每个自然发生的失败至少关联：UTC 时间、批次 ID、阶段（inventory/lane/ACK）、持续时间、
exit code、stderr，以及 Vultr 同一时间窗口的 sshd 认证/关闭记录。
只有必要时对一次受限 rsync 加 `ssh -vv` 诊断，并控制日志权限与体积。

`MaxStartups` 限制未认证连接，`MaxSessions` 限制同一已认证连接上的 session；两者不是一回事。
不能因客户端显示 connection closed 就断言命中限制。新版本文档中的 `PerSourcePenalties`
也不能直接套到线上旧 OpenSSH：先核对实际版本和有效配置。
[OpenSSH 服务端手册](https://man.openbsd.org/sshd_config#MaxStartups)。

用户要求：人工连接 107 失败时最多重试三次，之后停止；这不是授权额外探针、并发重试、无限
重连或从其他跳板绕过该限制。现有每分钟生产 pull 是另一条受锁保护的工作流，失败应如实留账。

## 是否启用 ControlMaster

不是本轮最小修复的前提。可选方案是**仅一轮 pull 内复用**一个连接：inventory 先建 master，
lane 和 ACK 复用；结束显式释放。socket 必须放在私有目录，身份包含 host/port/user；在 Apptainer
内部保证所有子进程看到同一路径。不要使用无限 `ControlPersist=yes`，也不要共享人工登录 socket。
OpenSSH 支持多 session 复用及有界 idle persist；socket 不可用时可能回退新连接，因此
不能仅凭设置选项声称“一定只有一条连接”。
[OpenSSH 复用配置](https://man.openbsd.org/ssh_config#ControlMaster)。

工程权衡：可减少握手，但一个 TCP 断开会同时影响全部 lane；socket 清理、孤儿 master、
server MaxSessions、不同 mount namespace 都增加维护边界。先修 ACK 饥饿和日志，再用实际
握手数、失败率、backlog、CPU 测量决定是否值得引入，不能把复用包装成网络根因修复。

## 107 v0.5.9 升级：不需要删除 raw

对比 `v0.5.9..3b91085`，`contracts/models.py`、`pipeline/pull.py` 和 `pipeline/config.py`
没有变化：raw chunk、manifest、ACK 合同未因此变更。升级不需要删 raw、清 staging、换密钥、
归档旧 generation 或重采数据。

派生处理不同：新版本增加按 symbol 的 L2 projection 和 typed source identity 校验，
[`build-l2-inputs.py`](../deploy/campus-107/build-l2-inputs.py) 会拒绝缺少源身份的旧 normalized
marker。不能把“raw 兼容”说成“任何旧派生产物都能直接复用”。旧 raw 保留；历史重算应在 Slurm
上重建相关派生数据，或明确使用仓库已有的显式历史 backfill 流程。不要改历史 raw 来迎合缓存。

[`install.sh`](../deploy/campus-107/install.sh) 已要求旧 Slurm pipeline 排空并持有提交锁，
保留存在的 `central.yaml` 和 `processing.env`，使用 hash-named SIF/sandbox 切换版本。
安装前暂停 pull/submit cron、等现有工作退出，保留配置和路径；不要直接用 example 覆盖站点
Slurm/account/partition 定制。验证新版本真实持久化及 ACK 后才恢复 cron。

## 本轮最小验收集

1. 已有合法 ACK + inventory/lane 失败：旧 ACK 可先送达，新块不能提前 ACK。
2. ACK 上传失败：文件仍在；下一轮重传正确，日志能区分 ACK 成功与下载失败。
3. 单 lane 超时：其余任务最终可退出、锁释放、partial 保留、raw 不受损。
4. SSH 参数仍强制 identity/host key，alive 和重试上限被测试覆盖。
5. 两端真实闭环出现 `LOCAL_DURABLE → ACK_PUSHED → ACK_VALIDATED → REMOTE_GC`，
   backlog 收敛，且无 hash mismatch。单看 `pull complete` 不足以证明服务端已 GC。
6. 更新后的正常日可处理；历史质量拒绝不自动变成成功，需另行做 gap/sequence 证据修复。

本文件只给出调查结论与建议，不声称上述变更已经实现或部署。
