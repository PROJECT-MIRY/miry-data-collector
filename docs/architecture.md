# 系统架构与选币职责

## 源码布局

`src` 是 Python 构建布局的源码根，不是业务包。把模块直接平铺在 `src/` 会产生 `import config`、
`import service` 这类容易与依赖冲突的顶级名称，因此项目使用短命名空间 `miry`：

```text
src/miry/
  collector/  实时采集运行时
  universe/   选币领域规则
  pipeline/   107 数据流水线
  contracts/  跨节点合同
  cli/        进程入口
```

旧 `edge` 和 `central` 以部署位置命名，同一目录混入了领域规则、进程入口和数据处理。新目录按职责
命名，部署位置只存在于部署文件中。

## 各层职责

`collector` 在 Vultr 运行，负责 WebSocket 路由、REST 轮询、接收时间戳、gap journal、chunk writer、
spool/ACK GC，以及在线应用已经形成的 universe 决策。内部进一步分为：

- `routes.py`：WebSocket route 生命周期、重连、存活检测和 add-ready-remove 更新；
- `websocket.py`：高频数据帧接收、边缘时间戳、raw admission 和 depth sequence 恢复；
- `ws_control.py`：低频订阅 ACK、LIST audit 和动态订阅控制；
- `polling.py`：OI、时钟、交易所目录和 universe evidence 的 REST 轮询；
- `sources.py`：装配 routes 与 pollers，不实现协议细节；
- `membership.py`：持久化 active/pending universe，并在受控边界应用决策。

`pipeline` 在 107 运行，负责 rsync pull、size/SHA-256 校验、ACK、raw 标准化、gap 有效性、L2 重建、
审计和 retention。`pull.py` 持有完整传输事务，`day.py` 编排单日作业，`quality.py` 持有完成门槛和
coverage 合同。它不排名、不生成候选名单，也不改变正式 60 币。

`cli` 只解析参数、配置日志并调用一个运行层函数；质量算法、transfer ledger、ACK 状态和业务决策
不得放回 CLI。正式命令使用动作或精确对象名称：`collect`、`pull`、`process`、`override`、`select`、
`pin`、`retain` 和 `symbols`。

`universe` 是纯领域层：输入完整 evidence 和当前名单，输出排名、market regime 与下一份名单。它不
打开 WebSocket、不访问 107 文件系统，也不操作 spool。`models.py` 定义输入、策略与结果，
`evidence.py` 解析并计算 evidence 指标，`selection.py` 只编排名单决策。

## 选币放在哪里

把选币放在 107 的优点是计算资源更充足、便于离线研究；缺点是正式采集会依赖 107 可用性、校园
网络和一条反向控制链路。107 一旦离线，Vultr 无法按时完成日常决策；同时 pull/重建代码会变成
采集控制中心，扩大故障范围。

把全部选币代码塞进 Vultr 采集实现，部署简单且证据就地可用，但会让排名规则依赖连接、writer 和
进程生命周期，难以独立回放与测试。

本项目采用第三种边界：**规则属于 `universe`，执行属于 `collector`**。Vultr 的 collector 收集并
持久化 evidence，调用纯 `universe` 规则形成 pending decision，再在线应用；同一规则也可由
`miry-data-select` 对离线 evidence 重放。107 只接收包含 universe identity 的 raw 数据。

依赖方向为：

```text
collector --> universe --> contracts
    |                         ^
    +-------------------------+

pipeline --------------------> contracts
cli ------> collector / universe / pipeline
```

禁止 `universe -> collector`、`universe -> pipeline` 和 `collector -> pipeline`。这保证更换采集连接
实现不会改变选币规则，也保证 107 的重建逻辑不能反向控制正式采集。
