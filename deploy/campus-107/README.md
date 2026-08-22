# 校园 107 正式拉取与处理

本机只运行每分钟一次的短时 rsync pull。CPU/内存较重的 normalize、L2 重建和 finalize
只能提交到 Slurm。固定目录为：

```text
/home/scc/pb24000367/Projects/bn/miry-data-collector  仓库 checkout
/home/scc/pb24000367/Projects/bn/runtime               程序、sandbox、日志、rsync 暂存
/home/scc/pb24000367/Projects/bn/data/raw              永久原始数据
/home/scc/pb24000367/Projects/bn/data/derived          Slurm 派生数据
```

`runtime/rsync` 只是远端镜像，可被后续同步清理。真正需要长期保留的是 `data/raw` 和
`data/derived`。

## 1. 前置条件

确认管理员允许 login node 每分钟运行一次短时任务，并检查：

```bash
module -t avail 2>&1 | grep apptainer
/public/app/apptainer/1.4.5/bin/apptainer --version
command -v crontab flock sbatch ssh
```

新装时使用 release 对应的仓库目录、`miry-data-collector.sif`、对应 SHA-256 文件，以及 Vultr 已授权的
`~/.ssh/ft-data-puller` 私钥。

v0.3.8 只支持当前结构化 universe 合同，不解析旧 generation。旧 raw 保持原始字节和日期分区，
不删除、不改写；旧 runtime/derived 移入 `data/archive/legacy-contract-*`。新 runtime/derived 从空路径
部署，并只处理新 formal-start 之后的日期。

## 2. 归档旧实验并 clean start

先在 Vultr 停止 collector，使 `writing` 完成封口。然后在 107 备份 crontab，并从
`crontab -e` 删除旧 pull 行：

```bash
BASE=/home/scc/pb24000367/Projects/bn
crontab -l > "$BASE/crontab.pre-v0.3.5"
crontab -e
pgrep -af ft-data-pull || true
```

等待现有 pull 退出后，手工运行旧 `pull-once.sh`，直到连续一次出现
`pull complete new_chunks=0 failures=0`。这一步必须在归档前完成，不能只看本地文件大小。

从 Vultr 导出的旧 control/evidence/gap tar 传到 107 后，为旧 runtime 创建时间戳 archive。以下
操作不移动旧 raw，只记录其 immutable inventory；runtime/derived 使用同一文件系统 rename：

```bash
BASE=/home/scc/pb24000367/Projects/bn
ARCHIVE="$BASE/data/archive/pre-v0.3.5-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$ARCHIVE"

find "$BASE/data/raw" -type f -printf '%s\n' | awk \
  '{files += 1; bytes += $1} END {printf "raw_files=%d raw_bytes=%.0f\n", files, bytes}' \
  > "$ARCHIVE/inventory.txt"
find "$BASE/data/raw" -type f -path '*/date=*/*' -printf '%p\n' \
  | sed -n 's#.*date=\([^/]*\)/.*#\1#p' | sort -u \
  >> "$ARCHIVE/inventory.txt"

mv "$BASE/data/derived" "$ARCHIVE/derived"
mv "$BASE/runtime" "$ARCHIVE/runtime"
```

把 `vultr-pre-v0.3.5-control.tar.gz` 及其 SHA-256 文件放入同一 `$ARCHIVE`，执行
`sha256sum --check`。再记录 release、文件数、字节数和日期范围，最后将 archive 设为只读：

```bash
(cd "$ARCHIVE" && sha256sum --check vultr-pre-v0.3.5-control.tar.gz.sha256)
chmod -R a-w "$ARCHIVE"
mkdir -p "$BASE/data/derived"
```

新代码不扫描 `$BASE/data/archive`。旧日期仍在 `$BASE/data/raw`，需要旧版离线环境时使用 archive
中的 runtime；不得把旧 control 状态链接回新的 runtime。新旧实验以 formal-start 时间边界区分。

## 3. 校验并安装 v0.3.8

```bash
cd /home/scc/pb24000367/Projects/bn/miry-data-collector
sha256sum --check miry-data-collector.sif.sha256

FT_CAMPUS_ROOT=/home/scc/pb24000367/Projects/bn/runtime \
FT_DATA_ROOT=/home/scc/pb24000367/Projects/bn/data \
FT_APPTAINER=/public/app/apptainer/1.4.5/bin/apptainer \
  ./deploy/campus-107/install.sh ./miry-data-collector.sif
```

安装器创建 hash-named SIF 和 sandbox，并令
`runtime/ft-shadow-data-plane.sandbox` 指向当前版本。构建约占 306MiB，只在新 hash 首次安装
时执行。`runtime/pull-once.sh` 安装为可执行文件。

归档后旧 runtime 已经不存在，并且 cron 尚未恢复，因此直接安装，不要对不存在的旧
`runtime/pull.lock` 加锁：

```bash
BASE=/home/scc/pb24000367/Projects/bn
cd "$BASE/miry-data-collector"
sha256sum --check miry-data-collector.sif.sha256

FT_CAMPUS_ROOT="$BASE/runtime" \
FT_DATA_ROOT="$BASE/data" \
FT_APPTAINER=/public/app/apptainer/1.4.5/bin/apptainer \
  ./deploy/campus-107/install.sh ./miry-data-collector.sif

FT_CAMPUS_ROOT="$BASE/runtime" \
  "$BASE/runtime/deploy/campus-107/verify.sh"
"$BASE/runtime/pull-once.sh"
```

详细归档、停 cron 和恢复顺序见
[`v0.3.5` clean start](../../docs/v0.3.5-structured-universe-clean-start.md)。安装完成后重新安装 cron，
不得复用 archive 中的 runtime 配置或 sandbox。

## 4. SSH host key 与 rsync

`known_hosts` 必须只接受通过独立渠道从 Vultr 管理员取得的 ED25519 指纹。第一次可执行：

```bash
ssh-keyscan -p 22 -t ed25519 167.179.115.243 \
  > /home/scc/pb24000367/.ssh/ft-shadow-data-plane.known_hosts.new
ssh-keygen -lf \
  /home/scc/pb24000367/.ssh/ft-shadow-data-plane.known_hosts.new
```

指纹完全匹配后再替换正式文件：

```bash
mv /home/scc/pb24000367/.ssh/ft-shadow-data-plane.known_hosts.new \
  /home/scc/pb24000367/.ssh/ft-shadow-data-plane.known_hosts
chmod 600 /home/scc/pb24000367/.ssh/ft-data-puller \
  /home/scc/pb24000367/.ssh/ft-shadow-data-plane.known_hosts
```

通过 sandbox 验证受限 rsync 只读列表：

```bash
/public/app/apptainer/1.4.5/bin/apptainer exec --writable \
  /home/scc/pb24000367/Projects/bn/runtime/ft-shadow-data-plane.sandbox \
  rsync --list-only \
  -e 'ssh -p 22 -i /home/scc/pb24000367/.ssh/ft-data-puller -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/home/scc/pb24000367/.ssh/ft-shadow-data-plane.known_hosts' \
  data-puller@167.179.115.243:ready/
```

这里不应出现 shell prompt；远端 key 只允许 rrsync 协议。

## 5. 配置文件

clean install 已生成 `runtime/central.yaml`。内容应与
`deploy/campus-107/central.yaml.example` 一致，尤其核对：

```yaml
local_raw_root: /home/scc/pb24000367/Projects/bn/data/raw
local_staging_root: /home/scc/pb24000367/Projects/bn/runtime/rsync
client_key: /home/scc/pb24000367/.ssh/ft-data-puller
known_hosts: /home/scc/pb24000367/.ssh/ft-shadow-data-plane.known_hosts
```

`runtime/deploy/campus-107/processing.env` 应使用绝对 Apptainer 路径、writable sandbox、上述
raw/derived 和 `tokyo01`。赋值两侧不能有空格，含空格的值必须加引号。

## 6. 前台验证和第一次拉取

```bash
FT_CAMPUS_ROOT=/home/scc/pb24000367/Projects/bn/runtime \
  /home/scc/pb24000367/Projects/bn/runtime/deploy/campus-107/verify.sh

/home/scc/pb24000367/Projects/bn/runtime/pull-once.sh
```

成功日志类似：

```text
pull complete run_id=<id> remote_manifests=<n> new_chunks=<n> existing_verified=<n> \
verified_bytes=<n> acks_queued=<n> acks_pushed=<n> failures=0 duration_seconds=<n>
```

检查永久数据而不是暂存目录：

```bash
du -sh /home/scc/pb24000367/Projects/bn/data/raw
find /home/scc/pb24000367/Projects/bn/data/raw -type f | wc -l
find /home/scc/pb24000367/Projects/bn/data/raw \
  -path '*/collector=tokyo01/*' -type f | head
```

Vultr 上对应 chunk 的 ACK 到达后才会删除 ready 副本。

检查 107 的持久传输状态和审计日志：

```bash
jq . /home/scc/pb24000367/Projects/bn/runtime/status/last-pull.json
LEDGER_DATE=$(date -u +%F)
tail -n 20 "/home/scc/pb24000367/Projects/bn/data/transfer-ledger/date=$LEDGER_DATE/events.jsonl"
```

`state=ok` 且 `acks_pushed=acks_queued` 证明 107 已完成本地持久化和 ACK 回传；端到端确认还要
在 Vultr 看到对应 `REMOTE_GC`。完整语义见
[ACK 传输审计合同](../../docs/transfer-ack-observability.md)。

## 7. 安装 cron

`crontab` 是当前用户的定时任务表。以下任务每分钟尝试一次；`flock -n` 保证上一次未结束时
不会再启动一个重叠进程。

运行 `crontab -e`，加入：

```cron
SHELL=/bin/bash
HOME=/home/scc/pb24000367
PATH=/usr/local/bin:/usr/bin:/bin
MAILTO=""
R=/home/scc/pb24000367/Projects/bn/runtime

* * * * * /usr/bin/flock -n "$R/pull.lock" "$R/pull-once.sh" >> "$R/logs/pull.log" 2>&1
```

保存后验证：

```bash
crontab -l | nl -ba
sleep 70
tail -n 50 /home/scc/pb24000367/Projects/bn/runtime/logs/pull.log
pgrep -af ft-data-pull || true
du -sh /home/scc/pb24000367/Projects/bn/data/raw
```

短时任务通常在检查时已经退出，所以 `pgrep` 没有输出不代表失败；以日志、raw 文件增长和
Vultr ACK 为准。

## 8. Slurm 处理

当某天的 `SEALED.json` 和其引用的全部 chunk 已拉取后，准备当天 60 币文件，每行一个大写
symbol，然后提交：

```bash
/home/scc/pb24000367/Projects/bn/runtime/deploy/campus-107/submit-day.sh \
  2026-08-12 \
  /home/scc/pb24000367/Projects/bn/runtime/symbols/2026-08-12.txt
```

脚本依次提交 normalize、受并发限制的 L2 array 和 finalize，并打印三个 job ID。检查：

```bash
squeue -u pb24000367
sacct -j <job-id> --format=JobID,State,Elapsed,MaxRSS,ExitCode
```

必须从 formal start 所在的首个 partial UTC day 开始逐日提交。每个 L2 task 会生成日末
`l2-checkpoint.json`，下一日用它继承连续盘口；如果本地已有前一天 `SEALED.json` 但尚无前一天
`_PROCESSED.json`，`submit-day.sh` 会拒绝乱序提交，L2 本身也会拒绝续日缺少前一日 checkpoint。
空 validity、损坏 checkpoint、区间重叠、越出目标 UTC 日、未分类时间、VALID/gap 冲突、任一币
有效率低于 99.9%，或输入名单不等于 raw 权威 60 币都会使 finalize 失败，并写
`_QUALITY_REJECTED.json`。成功后可检查：

```bash
jq '{core_generation,candidate_revision,decision_sequence,universe_version,
     universe_hash,quality_policy,minimum_l2_valid_ratio}' \
  /home/scc/pb24000367/Projects/bn/data/derived/quality/collector=tokyo01/date=2026-08-12/_PROCESSED.json
```

## 9. 常见故障

- `Permission denied (publickey)`：确认私钥名是 `ft-data-puller`，Vultr 已重新运行
  `configure-rsync.sh`，并且命令含 `IdentitiesOnly=yes`；
- host key 报错：不要关闭检查，重新从管理员渠道核对指纹；
- `rrsync` 拒绝命令：Vultr 仍有旧 SSH Match 配置，或客户端使用了服务端删除/覆盖参数；
- `apptainer: command not found`：只使用绝对路径，不依赖 cron 中的 module；
- overlay `invalid argument`：确认镜像是 `.sandbox` 且命令包含 `exec --writable`；
- `pull-once.sh: Permission denied`：重新运行当前 release installer，并检查 `stat -c '%A' runtime/pull-once.sh`；
- raw 不增长：先看 `pull.log`，再看 `runtime/rsync/ready` 是否有 manifest，最后在 Vultr 检查
  collector 是否仍写 `ready/`。
