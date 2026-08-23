# 部署入口

| 机器 | 职责 | 手册 |
|---|---|---|
| Vultr | 60 币采集、自动选币、spool、受限 rsync | [vultr/README.md](vultr/README.md) |
| 校园 107 | cron 拉取、永久 raw、ACK、Slurm | [campus-107/README.md](campus-107/README.md) |

先部署并验证 107 拉取端，再启动 Vultr 正式 collector。共同决策和验收标准见
[docs/collection-contract.md](../docs/collection-contract.md)。
