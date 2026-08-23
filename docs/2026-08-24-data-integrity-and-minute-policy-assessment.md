# 08-22 之后的数据完整性与分钟策略质量规则评估（2026-08-24）

## 结论

- `2026-08-22` 已完成 seal、normalize、L2 和 finalize。60 币均有非空 validity 和 checkpoint，
  但全部低于 `99.9%` L2 有效率，因此是正式 `_QUALITY_REJECTED` 日。
- `2026-08-23` 截至 `23:05 UTC` 尚未 seal，不能提前给出最终 L2 coverage。当前没有 open gap，
  raw、pull 和 ACK 正常，但当天已记录 182 个显式 gap，确定不能达到 `99.5%` 全日标准。
- 继续保留采集层 `99.9%` Gold 门槛。分钟策略不修改该合同，而是在下游按逐 symbol、逐分钟
  validity mask 建立 `99.8% / 99.5% / 99.0%` 研究层级。

## 2026-08-22 正式结果

正式窗口从 `2026-08-22T02:08:08.683705851Z` 开始，到 UTC 日末结束。raw Parquet 共约
`15.96 GB`。

| 指标 | 结果 |
| --- | ---: |
| L2 checkpoint | 60/60 |
| 非空 validity | 60/60 |
| 达到 99.9% | 0/60 |
| 最低有效率 | 68.9193% |
| 中位有效率 | 91.5626% |
| 最高有效率 | 97.5269% |
| VALID/gap 冲突 | 0 秒 |
| 未分类时间 | 合计 18.009 symbol-seconds |
| Clock quality | 1,289 VALID / 24 DEGRADED / 1 INVALID |

当天 21,746 条 gap 事件组成 10,873 个完整 OPEN/CLOSED 对，没有未关闭 gap：

| 原因 | 唯一 gap | 说明 |
| --- | ---: | --- |
| `CONNECTION_LOST_GAP` | 10,871 | 其中 10,187 个 OPEN 涉及 `mark_price`；多数为单币 scoped recovery |
| `COLLECTOR_STOPPED_GAP` | 2 | 累计约 226.946 秒；影响全部 60 币 |

connection gap 的持续时间合计会在不同 symbol、stream 和 route 间重叠，不能解释为单一全市场
停机时长。该日不能作为完整 60 币回测日，只能使用逐 symbol 的 `l2-validity.jsonl` 有效区间。

## 2026-08-23 临时结果

截至 `2026-08-23T23:05Z`：

- 107 已持久化约 `14.60 GB` raw Parquet；该日仍未 seal；
- Vultr collector 为 active，当前 open gap 为 0；
- 最新 pull 为 `state=ok`，`acks_queued=acks_pushed=3`，`failures=0`；
- 当前 queue、spool 和磁盘处于正常范围。

已落盘的 364 条 gap 事件组成 182 个完整 OPEN/CLOSED 对：

| 原因 | 唯一 gap | 配对状态 | 累计持续时间 |
| --- | ---: | --- | ---: |
| `CONNECTION_LOST_GAP` | 162 | 全部关闭 | 3,264.887 秒，跨 symbol/stream 可重叠 |
| `INGEST_OVERLOAD_GAP` | 15 | 全部关闭 | 232.223 秒，跨 route/OI 可重叠 |
| `COLLECTOR_STOPPED_GAP` | 5 | 全部关闭 | 469.980 秒，影响全部 60 币 |

五次全局停机为：

| UTC 区间 | 持续时间 |
| --- | ---: |
| 06:20:21–06:23:41 | 200.174 秒 |
| 08:06:27–08:07:24 | 57.755 秒 |
| 13:07:26–13:08:24 | 58.333 秒 |
| 16:30:51–16:32:09 | 78.121 秒 |
| 17:15:24–17:16:40 | 75.597 秒 |

仅这五次全局停机已经达到约 7.833 分钟，超过 `99.5%` 的 7.2 分钟日预算。因此即使之后没有
其他无效时间，08-23 也不可能成为 60 币全部通过 `99.5%` 的完整日。

`22:01` 和 `22:03 UTC` 还发生两次 64 MiB raw queue 硬上限：四条 public route 和一条 market
route 均登记 gap，transport 约 0.4–2.1 秒恢复；OI scoped overload 最长约 23.3 秒。当前已经恢复，
但 L2 仍必须在各币完成 snapshot bridge 后才重新进入 VALID。

最终结论必须等待 UTC 日末、150 秒 seal grace、107 拉取、L2 重建和 finalize。

## 分钟策略质量规则

采集层继续使用 `l2-coverage-v1 / 99.9%`，用于标识最高质量的完整数据日。研究层不覆盖该 marker，
而是基于同一 validity 生成独立、版本化的分钟样本政策。

### 硬门禁

以下条件不得因研究阈值放宽：

```text
unclassified_ns == 0
conflicting_ns == 0
checkpoint、sequence 和 universe identity 一致
特征窗口、执行分钟和标签窗口均不得跨 gap
```

### 决策点

设因子回看为 `L` 分钟、标签或持有周期为 `H` 分钟：

```text
eligible(t, symbol) =
    [t-L+1, t] 全部有效
    AND 执行分钟有效
    AND [t+1, t+H] 全部有效
```

日质量应报告 `decision_coverage = eligible decision points / theoretical decision points`。原始无效
bar 数只能作为辅助指标，因为一个孤立 gap 最多影响约 `L + H` 个决策点。

### 研究层级

| 层级 | symbol-day 有效率 | 整分钟 mask 后的日预算 | 用途 |
| --- | ---: | ---: | --- |
| Gold | 99.9% | 最多 1 个无效分钟 | 事件级、执行和 HFT 复核 |
| Minute-Strict | 99.8% | 最多 2 个无效分钟 | 分钟策略最终验证 |
| Minute-Research | 99.5% | 最多 7 个无效分钟 | 因子选择、训练和演化 |
| Exploratory | 99.0% | 最多 14 个无效分钟 | 初筛和敏感性分析 |

横截面分钟还要求至少 `54/60` 个 symbol 有效，单次连续 gap 默认不超过 3 分钟。最终阈值不能根据
现有 PnL 事后选择；候选策略必须同时报告 99.8%、99.5% 和 99.0% 下的 IC、Sharpe、回撤、换手和
因子排名稳定性。

## 后续动作

1. 08-23 seal 后按相同流程生成正式质量 marker，并以正式结果替换本节临时数字。
2. 分钟 feature store 保存逐 symbol validity mask、gap reason、snapshot age 和 universe version。
3. 在策略的 `L/H` 确定后计算 decision coverage，不再仅凭整日 valid ratio 决定训练样本。
4. 单独增加 `aggTrade`、bookTicker、mark price 和 OI 的逐流完整性报告；当前 marker 严格证明的是 L2。
