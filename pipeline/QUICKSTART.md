# Pipeline 快速上手

以「支付到账回调处理」为例，五分钟接入完整消费链路。以下代码合并后可保存为 `main.go` 运行
（需 import：`context`、`errors`、`fmt`、`time`、`github.com/xm-utils/tools/pipeline`）。

## 五步接入

### ① 定义消息类型与业务上下文

业务上下文只需 **embed `pipeline.ExecContext`**——密封接口 `Context` 的全部方法
（含未导出的注入钩子）自动继承，零实现：

```go
// 消息类型（示例：带重试计数的回调消息）
type PayMsg struct {
	OrderID string
	Retries int    // 当前重试轮次
}

// 业务执行上下文：embed 内置 ExecContext 即可
type PayContext struct {
	pipeline.ExecContext // 引擎执行前自动注入消息级 context 与统一 logger
	OrderID string
}
```

### ② 实现 Step

`ctx C` 本身就是 `context.Context`（停机/超时感知）并携带统一日志出口：

```go
// 步骤间数据键：编译期类型检查，避免字符串 map 误用
var keyAmount = pipeline.DataKey[int64]{Name: "amount"}

type loadOrderStep struct{}

func (loadOrderStep) Name() string { return "loadOrder" }

func (loadOrderStep) Execute(ctx *PayContext) error {
	// 长操作务必 select ctx.Done()，Stop 时可提前退出
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	keyAmount.Set(ctx, 9900) // 模拟加载订单金额
	ctx.Logger().Infof("订单 %s 金额已加载", ctx.OrderID)
	return nil
}

type chargeStep struct{}

func (chargeStep) Name() string { return "charge" }

func (chargeStep) Execute(ctx *PayContext) error {
	amount, err := keyAmount.Get(ctx)
	if errors.Is(err, pipeline.ErrKeyNotFound) {
		return fmt.Errorf("上游步骤未写入金额: %w", err)
	}
	if errors.Is(err, pipeline.ErrKeyTypeMismatch) {
		return fmt.Errorf("金额类型污染: %w", err) // 错误信息含实际存储类型
	}
	if amount <= 0 {
		return fmt.Errorf("非法金额 %d", amount) // 失败将触发退避重试
	}
	return nil
}
```

### ③ 适配消息队列

内存 channel 演示；生产可对接 Kafka / Redis Stream / NSQ 等（实现两方法即可）：

```go
type memQueue struct {
	ch chan PayMsg
}

// Submit 契约：线程安全且不得无限阻塞——停机流程中引擎仍可能调用 Submit，
// 阻塞会卡死 worker 导致 Stop() 永不返回；停机期间允许返回 error（引擎按外部兜底处理）
func (q *memQueue) Submit(msg PayMsg, delay time.Duration) error {
	if delay > 0 {
		// 演示简化：延迟后非阻塞入队（生产交给 MQ 原生延时消息能力）
		time.AfterFunc(delay, func() {
			select {
			case q.ch <- msg:
			default:
			}
		})
		return nil
	}
	select {
	case q.ch <- msg:
		return nil
	default:
		return fmt.Errorf("memQueue 缓冲已满，留待外部兜底重投")
	}
}

func (q *memQueue) Receive(ctx context.Context) (PayMsg, bool) {
	select {
	case m := <-q.ch:
		return m, true
	case <-ctx.Done():
		return PayMsg{}, false // 仅停机时返回，保证未领取任何消息
	}
}
```

### ④ 组合可选步骤（条件分枝 / 非关键容错）

```go
// 积分步骤（完整最小实现，供 VIP 分支引用）
type pointsStep struct{}

func (pointsStep) Name() string { return "points" }

func (pointsStep) Execute(ctx *PayContext) error {
	ctx.Logger().Infof("为 %s 加积分", ctx.OrderID)
	return nil
}

// 短信步骤（完整最小实现，供 BestEffort 引用；内部失败/panic 均不会中断主流程）
type smsStep struct{}

func (smsStep) Name() string { return "sms" }

func (smsStep) Execute(ctx *PayContext) error {
	return fmt.Errorf("短信网关抱残（示例：失败被忽略）")
}

// VIP 分支：条件不满足静默跳过，视为成功
vipBranch := pipeline.NewConditionalStep[*PayContext]("vipBranch",
	func(ctx *PayContext) bool { return ctx.OrderID != "" },
	pointsStep{},
)

// 短信通知：失败/panic 都只记日志，不中断主流程、不触发重试、不让消息落死信
notify := pipeline.NewBestEffortStep[*PayContext]("notify", nil, smsStep{})
```

### ⑤ 装配并启动引擎

```go
func main() {
	q := &memQueue{ch: make(chan PayMsg, 64)}

	e, err := pipeline.New(pipeline.Config[PayMsg, *PayContext]{
		Queue:   q,
		Workers: 4,
		Resolve: func(msg PayMsg) ([]pipeline.Step[*PayContext], *PayContext, error) {
			if msg.OrderID == "" {
				// 数据缺失类错误不重试，直接进 OnFatal
				return nil, nil, fmt.Errorf("OrderID 缺失")
			}
			return []pipeline.Step[*PayContext]{
				loadOrderStep{}, chargeStep{}, vipBranch, notify,
			}, &PayContext{OrderID: msg.OrderID}, nil
		},
		OnSuccess: func(msg PayMsg, ctx *PayContext) {
			fmt.Println("处理成功:", msg.OrderID)
		},
		OnFatal: func(msg PayMsg, reason string) {
			fmt.Println("不可恢复:", msg.OrderID, reason) // 如落死信表
		},
		OnRetried: func(msg, next PayMsg, delay time.Duration) {
			// 重试计数 +1（next 已入队）
			fmt.Printf("将在 %s 后重试: %s\n", delay, msg.OrderID)
		},
		OnExhausted: func(msg PayMsg, failedStep string, err error) {
			fmt.Println("重试耗尽转终态:", msg.OrderID, failedStep, err)
		},
		// 重试策略：内置默认实现 PolicyRetryer——额度与退避均由 Strategy（典型为 RetryPolicy）
		// 的 Delay 自检裁决：超额/未配退避返回 0 即不重试；指数退避 base×2^attempt
		// （MaxDelay 封顶、内部防移位溢出，饱和未封顶同样转终态）；Next 生成重投消息
		Retryer: &pipeline.PolicyRetryer[PayMsg]{
			Policy:  &pipeline.RetryPolicy{MaxAttempt: 3, BaseDelay: time.Second, MaxDelay: time.Minute},
			Attempt: func(msg PayMsg) int { return msg.Retries },
			Next: func(msg PayMsg) PayMsg {
				msg.Retries++
				return msg
			},
		},
	})
	if err != nil {
		panic(err) // 装配期配置错误，优雅处理而非崩溃
	}

	if err := e.Start(); err != nil {
		panic(err) // 重复 Start 会被拒绝
	}

	q.ch <- PayMsg{OrderID: "ORD-1"}

	// 收到 SIGTERM 时：e.Stop() 取消在途步骤并等待 worker 全部退出。
	// 停机中断 ≠ 业务失败：在途消息不重投、不落终态、不计重试（可选 OnInterrupted 观测），
	// 由 MQ 重投/补偿通道兜底——引擎语义为 at-least-once。
	time.Sleep(3 * time.Second)
	e.Stop()
	// Stop 完成后可再次 Start（重启），并发 Stop 安全
}
```

## 进阶能力（可选装配）

```go
// 保序：同一 OrderID 的消息钉定同一 worker 串行，不同订单照常并发；
// 加到上面 Config 即可：
//   MsgKeyFunc: func(msg PayMsg) string { return msg.OrderID },

// 步骤级超时：网关调用 2 秒封顶，超时以 DeadlineExceeded 进入重试/终态分派；
// 只约束遵守 ctx.Done() 的操作，后续步骤不受前序超时影响
callGateway := pipeline.WithTimeout(gatewayStep{}, 2*time.Second)

// 永久错误：错误链包装哨兵，跳过重试直达终态（适合参数校验/数据不一致）
return fmt.Errorf("金额与订单不符: %w", pipeline.ErrNonRetryable)

// 自定义重试策略：实现 Retryer 接口（唯一重试入口）接管「是否重试/等多久/重投什么」，
// 重试状态由实现自管（外置存储计数等）；未配置 Retryer 即不重试，
// 也可在自定义实现内委托 PolicyRetryer 复用默认退避语义
var ErrRefundClosed = errors.New("退款单已关闭") // 业务自定义永久错误哨兵

type orderRetryer struct{ base *pipeline.PolicyRetryer[PayMsg] }

func (r orderRetryer) Retry(msg PayMsg, failedStep string, err error) (PayMsg, time.Duration, bool) {
	if errors.Is(err, ErrRefundClosed) {
		return msg, 0, false // 按业务错误类别直接裁决终态
	}
	return r.base.Retry(msg, failedStep, err) // 其余回落默认「额度+指数退避」
}

// 止血与有界发布：
e.Pause()  // 停止领取新消息，在途处理继续；上游故障时隔离用，e.Resume() 恢复
if err := e.StopWithTimeout(10 * time.Second); errors.Is(err, pipeline.ErrStopTimeout) {
	// 挂死操作未排空：引擎后台继续停机（期间 Start 被拒），
	// 未处理完的消息由 MQ 重投兜底（at-least-once）
}
```

## 常用场景速查

| 场景 | 做法 |
|---|---|
| 步骤需要传值 | `DataKey[T].Set/Get`，勿自设 context value（会被引擎注入覆盖） |
| 失败后不重试、直接死信 | `Resolve` 返回 error（进 `OnFatal`） |
| 失败可重试 | 任意 Step 返回 error，配 `Retryer`（内置默认实现 `PolicyRetryer`） |
| 步骤/Resolve/钩子内可能 panic | 无需处理：全链路 recover——步骤级转 error 走重试/终态，其余转 OnFatal（需幂等） |
| 非关键副作用（打点/通知） | `NewBestEffortStep`，失败与 panic 均忽略不中断 |
| 停机时不想误落死信 | 无需处理：停机中断不重投/不终态；可选 `OnInterrupted` 打点观测 |
| 退避时长不想失控 | `RetryPolicy.MaxDelay` 封顶；额度耗尽/饱和未封顶时 `Delay` 返 0 或饱和，均自动转终态不留僵尸消息 |
| 按条件执行分枝 | `NewConditionalStep`，内部失败错误含内外两层步骤名 |
| 长时间操作响应停机 | `select ctx.Done()` 或把 `ctx` 直接传给支持 context 的 SDK |
| 区分数据读取失败 | `errors.Is(err, ErrKeyNotFound / ErrKeyTypeMismatch)` |
| 参数非法等永久错误 | 步骤返回 `fmt.Errorf("...: %w", pipeline.ErrNonRetryable)`，免重试直达终态 |
| 重试策略自定义 | `Config.Retryer` 实现 `Retry(msg, failedStep, err) (next, delay, retry)`；自定义实现可委托 `PolicyRetryer` 复用默认额度/退避 |
| 慢步骤需要截止 | `pipeline.WithTimeout(step, d)`，仅约束遵守 ctx 的操作 |
| 同业务对象必须串行 | `Config.MsgKeyFunc` 返回业务键，同 Key 钉定同 worker 串行 |
| 挂死调用阻塞发布/上游故障止血 | `StopWithTimeout(d)`；`Pause()`/`Resume()` |

## API 契约摘要

- `New(cfg) (*Engine, error)` — 配置错误返回 error，不 panic
- `Start() error` / `Stop()` — 幂等防重入；`Stop` 返回即全部 worker 已退出；停机中断不改变消息终态
- `C` 必须用指针类型（`Config[M, *BizCtx]`）；`Resolve` 每条消息返回独立 C 实例（Workers>1 硬性要求）
- `Queue.Submit` 必须线程安全且不得无限阻塞；停机期间可返回 error（引擎留待外部兜底）
- panic 后消息状态未知，统一转 `OnFatal`——业务侧 `OnFatal` 必须幂等
- 失败分派优先级：停机中断 > `ErrNonRetryable` > `Retryer` 裁决重投 > 耗尽终态；
  `Retryer` 是唯一重试入口（nil 即不重试），裁决实现必须线程安全
- `StopWithTimeout(d) error` / `Pause()` / `Resume() error` — 有界停机（超时后台继续排空，
  排空前 Start 被拒）；Pause 仅停领取，暂停态可直接 Stop
- `MsgKeyFunc` 非 nil ⇒ 分片模式：同 Key 严格串行、不同 Key 并发；停机时已路由消息经
  `OnInterrupted` 上报不静默丢弃
- `Engine` 不触达任何业务表：状态回写/死信/计数全部经钩子由业务侧实现
