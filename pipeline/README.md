# Pipeline 通用消息消费执行引擎

基于泛型的「队列消费 → 步骤链执行 → 重试/终态处置」引擎。消息类型、执行上下文完全泛型化，
引擎本身不感知任何业务领域；成功回写、死信落库、重试计数等领域语义全部经钩子注入。

## 特性

- **双泛型参数**：`Engine[M any, C Context]`，M 为消息类型，C 为业务执行上下文
- **密封上下文接口**：`Context` 含未导出 `setContext/setLogger`，业务结构体只需
  embed 内置 `ExecContext` 即可跨包零实现满足约束
- **引擎统一注入**：步骤执行前引擎自动注入消息级 context（停机即取消）与统一日志出口，
  `Resolve` 只填业务领域字段，使用方零处理
- **全链路 panic 免疫**：Resolve/上下文注入/步骤（含组合子深层）/任意钩子的 panic 均被 recover：
  步骤级转 error 走重试/终态，其余转 OnFatal（需幂等），不击穿进程，堆栈记入日志；
  BestEffort 内部 panic 走忽略分支，不中断主流程
- **退避溢出安全 + 封顶**：指数退避 `base×2^attempt` 移位前预检溢出，负轮次/超大轮次/大基数
  回绕均饱和；`MaxDelay` 限制单次退避上限；饱和且未设封顶时视同额度耗尽直接终态，
  杜绝负 delay 热循环与「永不入队也永不死信」的僵尸消息
- **生命周期安全**：`Start` 幂等防重入（三段状态机），并发 `Stop` 安全，停机后可重启；
  **停机中断 ≠ 业务失败**：在途消息不重投、不落终态、不计重试（OnInterrupted 观测）
- **组合子**：`ConditionalStep`（条件跳过分枝）、`BestEffortStep`（非关键步骤失败/panic 均忽略，不中断主流程）
- **类型安全数据键**：`DataKey[T]` 编译期检查步骤间传值，`errors.Is` 可精确区分
  键不存在与类型不匹配两类失败
- **错误分级**：步骤错误链包装 `ErrNonRetryable`（参数非法/永久失败）时跳过重试直达终态，
  不再为注定失败的校验浪费退避额度
- **重试策略抽象**：`Retryer[M]` 接口是唯一重试入口——是否重试/等待多久/重投消息变换
  均由使用方实现裁决（重试状态外置、按错误类别分档、动态额度）；内置 `PolicyRetryer`
  组合 `Strategy` 退避接口裁决（默认实现 `RetryPolicy`：额度在其 `Delay` 内自检，
  返回 0 即不重试；指数退避 + 溢出防护），开箱即用亦可委托复用；未配置即不重试
- **步骤级超时**：`WithTimeout(step, d)`（或自实现 `TimeoutDecliner`）为单步骤限定从消息级
  context 派生的超时上限，前序超时不影响后续步骤；`DeadlineExceeded` 按普通失败分派，
  不会被误判为停机中断
- **有界停机与暂停**：`StopWithTimeout(d)` 发布流程不被挂死操作拖死（超时返回
  `ErrStopTimeout`，后台继续排空，排空完成前 Start 被拒）；`Pause`/`Resume` 止血式停领新消息
  不影响在途处理，暂停态亦可直接 `Stop`
- **同 Key 保序**：`MsgKeyFunc` 启用「单接收器 + 哈希分片」模式，同一 Key 的消息严格
  路由到同一 worker 串行执行，不同 Key 保持并发；无缓冲分片通道对上游形成天然背压

## 安装

```bash
go get github.com/xm-utils/tools/pipeline
```

## 核心概念

| 类型 | 职责 |
|---|---|
| `Queue[M]` | 消息队列抽象：`Submit(msg, delay)` 入队（可延迟，须线程安全且不得无限阻塞），`Receive(ctx)` 阻塞领取 |
| `Step[C]` | 执行步骤：`Name()` + `Execute(ctx C) error`，C 自带注入好的 context/logger |
| `Context`（接口） | 密封的执行上下文约束，业务 embed `ExecContext` 即满足；**C 必须用指针类型实例化（`*BizCtx`）** |
| `ExecContext`（结构体） | 内置实现：消息级 context + 步骤间 Data + 原始 RawPayload |
| `Config[M, C]` | 装配配置：队列、worker 数、Resolve 与生命周期钩子、重试策略 |
| `Engine[M, C]` | 引擎本体：`New` 构造（配置错误返回 error）→ `Start`/`Stop` |

### 处理流程

```
Queue.Receive → Resolve(msg) → [引擎注入 msgCtx/logger] → Step 链顺序执行
    ├─ 全部成功          → OnSuccess
    ├─ Resolve 失败/链路 panic → OnFatal（不重试；panic 后状态未知，OnFatal 需幂等）
    ├─ 错误含 ErrNonRetryable → OnExhausted（短路裁决，直达终态）
    ├─ 步骤失败            → Retryer.Retry 裁决：retry=true → Queue.Submit(延迟重投) → OnRetried
    ├─ 未配 Retryer/裁决不重试/退避饱和 → OnExhausted（终态回写/落死信）
    └─ 停机中断（Stop 取消） → OnInterrupted（不重投/不终态，留待上游重投）
```

## Config 钩子一览

| 钩子 | 触发时机 |
|---|---|
| `Resolve(msg)` | 每条消息开始；返回 Step 链 + 上下文（只填业务字段） |
| `OnFatal(msg, reason)` | Resolve 失败 / 空上下文 / 全链路任意 panic，不可恢复，不重试 |
| `OnSuccess(msg, ctx)` | 全部步骤成功（含空 Step 链的「空成功」，引擎告警） |
| `OnRetried(msg, next, delay)` | 步骤失败且 Retryer 裁决重投、入队成功 |
| `OnExhausted(msg, failedStep, err)` | 步骤失败且重试裁决不重试（未配 Retryer 首次即触发；退避饱和封顶同样命中） |
| `OnInterrupted(msg, err)` | 步骤因 Stop 停机取消而失败；仅观测用途，不改变消息终态 |
| `Retryer` | 重试策略抽象接口（唯一重试入口）：`Retry(msg, failedStep, err) (next, delay, retry)`；nil 即不重试；内置默认实现 `PolicyRetryer{Policy Strategy, Attempt, Next}`，额度由 `Strategy.Delay` 自检（返回 0 = 不重试） |
| `MsgKeyFunc(msg)` | 返回有序键（可选）：非 nil 即启用同 Key 串行分片模式，空键降级轮询分发 |
| `Log` | `*logrus.Entry`，缺省自动初始化为 `module=pipeline` |

## 错误语义

- 装配期配置错误：`New` 返回 error（不 panic），调用方可优雅处理
- 引擎处理 at-least-once：**停机中断不改变消息终态**（不重投不终态化），由 MQ 重投或补偿通道兜底
- panic 后消息状态未知，统一转 `OnFatal` 处置——**业务侧 OnFatal 必须幂等**（可能重复触发）
- 上下文硬性契约：`C` 必须用指针类型（`Config[M, *BizCtx]`）；Workers>1 时 `Resolve` 必须为每条消息
  返回独立 C 实例（共享单例/池化对象会导致注入覆盖与 Data map 并发写竞争）
- `Queue.Submit` 契约：线程安全且**不得无限阻塞**（停机中仍可能被调用，阻塞会卡死 Stop）；
  停机期间允许返回 error，引擎按「留待外部兜底」处理
- `RawPayload` 由业务在 `Resolve` 中自行填充（引擎不触达消息体字节）；`GetPayload` 不在 Context 接口内
- 失败分派优先级：**停机中断 > `ErrNonRetryable` > `Retryer` 裁决重投 > 耗尽终态**；
  轮次状态与退避形状由 `Retryer` 实现自管，引擎不介入；未配置 `Retryer` 即不重试；
  `WithTimeout` 超时的 `DeadlineExceeded` 走普通失败分派（非停机场景不会误判为中断）
- 分片模式下停机时已路由/尚未交付的消息、`StopWithTimeout` 超时后未处理完的消息，
  一律经 `OnInterrupted` 上报，绝不静默丢弃（语义统一为 at-least-once）
- 步骤失败错误链完整：组合子内部失败经 `conditional[外层]: step[内层]: %w` 包装，
  `errors.Is/As` 可穿透定位
- `DataKey.Get` 返回 `(T, error)`：`errors.Is(err, ErrKeyNotFound)` /
  `errors.Is(err, ErrKeyTypeMismatch)` 区分两类失败，错误信息含键名与实际存储类型

## 快速上手

完整可运行示例见 [QUICKSTART.md](./QUICKSTART.md)。

## 测试

```bash
go test -race ./...
```

覆盖：注入链路、停机中断不终态化、全链路 panic 恢复（Resolve/钩子/步骤）、退避溢出与封顶、
饱和转耗尽、生命周期幂等、错误包装、DataKey 三态语义、空 Step 链、错误分级直达终态、
自定义/内置 Retryer 裁决链路、步骤级超时、有界停机/暂停恢复、同 Key 串行保序，以及外部包（`package pipeline_test`）
接入形态的编译期验证。
