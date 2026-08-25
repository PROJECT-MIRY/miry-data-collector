# 文档索引

`docs/` 保持单层结构。当前合同使用稳定文件名；调研与评估以日期开头；版本背景以版本号开头。
历史文档用于解释决策来源，不自动覆盖当前合同。

## 当前文档

| 文档 | 用途 | 当前适用性 |
| --- | --- | --- |
| [系统架构与选币职责](architecture.md) | 模块边界、依赖方向和选币职责 | 当前 |
| [正式采集与处理合同](collection-contract.md) | 数据源、完整性、质量门槛和性能合同 | v0.5.4 |
| [端到端部署指南](deployment.md) | Vultr 与校园 107 的部署顺序和验收 | v0.5.4 |
| [Public WebSocket 流量均衡](traffic-balancing.md) | 路由负载观测和限幅在线再均衡规则 | 当前 |
| [ACK 传输审计与恢复合同](transfer-ack-contract.md) | raw 传输、ACK、远端 GC 和审计状态 | 当前 |

## 调研与评估

| 日期 | 文档 | 类型 |
| --- | --- | --- |
| 2026-08-12 | [Binance 重连与 L2 恢复](2026-08-12-binance-reconnect-l2-recovery-research.md) | 调研 |
| 2026-08-12 | [跨 UTC 日 L2 重建](2026-08-12-cross-day-l2-reconstruction-assessment.md) | 评估 |
| 2026-08-12 | [数据完整性风险与 v0.3.1 修复](2026-08-12-data-integrity-risk-assessment.md) | 评估 |
| 2026-08-12 | [Binance USD-M L3 数据](2026-08-12-l3-data-assessment.md) | 评估 |
| 2026-08-23 | [市场活跃度与采集容量](2026-08-23-market-activity-capacity-assessment.md) | 评估 |
| 2026-08-23 | [Binance USD-M 永续市场状态](2026-08-23-market-regime-research.md) | 调研 |
| 2026-08-23 | [Public WebSocket 4/8 分片 A/B](2026-08-23-public-shard-ab-assessment.md) | 评估 |
| 2026-08-24 | [NautilusTrader 与 HftBacktest 接入](2026-08-24-backtest-framework-integration-assessment.md) | 评估 |
| 2026-08-24 | [08-22 之后的数据完整性与分钟策略质量规则](2026-08-24-data-integrity-and-minute-policy-assessment.md) | 评估 |
| 2026-08-25 | [Gap 根因与 v0.5.4 稳定性修复](2026-08-25-gap-root-cause-and-v0.5.4-assessment.md) | 事故评估 |

## 版本与事故背景

| 版本 | 文档 | 类型 |
| --- | --- | --- |
| v0.3.3 | [数据缺口与订阅恢复](v0.3.3-gap-recovery.md) | 版本说明 |
| v0.3.4 | [存储恢复事故复盘](v0.3.4-storage-recovery-incident.md) | 事故复盘 |
| v0.3.5 | [结构化 Universe 正式起点](v0.3.5-structured-universe.md) | 版本说明 |
| v0.3.8 | [采集器可靠性优化](v0.3.8-collector-reliability.md) | 版本说明 |
| v0.3.10 | [L2 快照调度优化](v0.3.10-snapshot-scheduling.md) | 版本说明 |
| v0.4.0 | [架构重构发布说明](v0.4.0-architecture-refactor.md) | 发布说明 |

## 机器证据

- [正式 Universe 7.0 冻结证据](formal-universe-7.0-evidence.json)

## 命名约定

- 当前合同：`<topic>.md`，使用稳定文件名。
- 调研与评估：`YYYY-MM-DD-<topic>-research.md` 或
  `YYYY-MM-DD-<topic>-assessment.md`。
- 版本背景：`vX.Y.Z-<topic>.md`；事故文档以 `-incident.md` 结尾。
- H1 使用中文主题，保留 `L2`、`ACK`、`WebSocket`、`NautilusTrader` 等技术专名。
- 日期统一写为 `YYYY-MM-DD`；带日期的 H1 使用全角括号。
- 避免使用含义不明确的 `plan`、`record` 和 `notes` 作为新文档类型。
