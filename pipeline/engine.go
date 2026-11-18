package pipeline

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"runtime/debug"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// ErrNonRetryable 不可重试错误哨兵：步骤返回的错误链（fmt.Errorf %w 包装）包含该错误时，
// 表明参数非法/数据不一致等永久性失败——引擎不消耗重试额度，直接 OnExhausted 终态处置。
// 判定顺序位于停机中断之后、额度检查之前，与退避饱和转耗尽互不冲突。
var ErrNonRetryable = errors.New("pipeline: 不可重试错误")

// 引擎生命周期状态：Start 仅允许在 stopped 态执行；Stop 走 running/paused→stopping→stopped 迁移，
// 确保停机等待期间不会重入 Start 造成 WaitGroup Add/Wait 并发竞争；
// paused 为 running 的子态（停止领取新消息、在途处理继续），可 Resume 也可直接 Stop。
const (
	engineStopped = iota
	engineRunning
	engineStopping
	enginePaused
)

// Engine 通用消费执行引擎：worker 池从 Queue 领取消息 → Resolve 得 Step 链顺序执行 →
// 按结果回调成功/重试/终态钩子。上下文与消息类型完全泛型化，不感知任何业务领域。
type Engine[M any, C Context] struct {
	cfg    Config[M, C]
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	receiveCtx context.Context    // 领取专用 context：Pause 时取消以立即结束 worker 的 Receive 等待，随运行 ctx 级联取消（Stop）
	rcCancel   context.CancelFunc // 旧值已在 Pause 中取消，Resume 重建时覆盖不构成泄漏
	resumeCh   chan struct{}      // Resume 时关闭，唤醒滞留在暂停闸口的 worker/dispatcher
	shards     []chan M           // 同 Key 串行模式（MsgKeyFunc 非 nil）下非空：每个 worker 独占一个分片通道

	mu    sync.Mutex // 保护 state 与 ctx/receiveCtx/shards 等生命周期字段，Start/Stop 可并发调用
	state int
}

// New 创建引擎实例（不启动）。配置错误返回 error 而非 panic，
// 与项目「装配期配置错误应返回 error」的初始化规范一致，调用方可优雅处理。
func New[M any, C Context](cfg Config[M, C]) (*Engine[M, C], error) {
	if cfg.Queue == nil {
		return nil, fmt.Errorf("pipeline: Config.Queue 不能为空")
	}
	if cfg.Resolve == nil {
		return nil, fmt.Errorf("pipeline: Config.Resolve 不能为空")
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}

	if cfg.Log == nil {
		cfg.Log = logrus.WithField("module", "pipeline")
	}
	return &Engine[M, C]{cfg: cfg}, nil
}

// Start 启动 worker 池消费循环。幂等保护：重复 Start（含 running/stopping/paused 态）返回 error，
// 不会泄漏第一批 worker；Stop 完成后的 stopped 态允许重新 Start（重启）。
// MsgKeyFunc 非 nil 时启用同 Key 串行模式：额外拉起一个 dispatcher 接收器。
func (e *Engine[M, C]) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != engineStopped {
		return fmt.Errorf("pipeline: 当前状态不允许 Start（已启动或停机中）")
	}
	e.state = engineRunning
	e.ctx, e.cancel = context.WithCancel(context.Background())
	// 领取专用 ctx 从运行 ctx 派生：Stop 级联取消，Pause 单独取消
	e.receiveCtx, e.rcCancel = context.WithCancel(e.ctx)
	e.resumeCh = make(chan struct{})
	if e.cfg.MsgKeyFunc != nil {
		e.shards = make([]chan M, e.cfg.Workers)
		for i := range e.shards {
			e.shards[i] = make(chan M) // 无缓冲：阻塞转发即背压，消息不滞留引擎内部
		}
		e.wg.Add(1)
		go e.dispatcher()
		for i := 0; i < e.cfg.Workers; i++ {
			e.wg.Add(1)
			go e.shardWorker(i)
		}
		e.cfg.Log.Infof("引擎已启动(同Key串行模式), workerCount=%d", e.cfg.Workers)
		return nil
	}
	for i := 0; i < e.cfg.Workers; i++ {
		e.wg.Add(1)
		go e.worker(i)
	}
	e.cfg.Log.Infof("引擎已启动，workerCount=%d", e.cfg.Workers)
	return nil
}

// Stop 停止消费并等待在途 worker 退出。幂等：停机完成/未启动时直接返回；
// 并发重复 Stop 不重复等待，但仅首个调用保证返回时 worker 已全部退出。
// Pause 态可直接 Stop（按停机处理，暂停闸口 worker 经 e.ctx 取消退出）。
func (e *Engine[M, C]) Stop() {
	e.mu.Lock()
	if e.state != engineRunning && e.state != enginePaused {
		e.mu.Unlock()
		return
	}
	e.state = engineStopping
	cancel := e.cancel
	e.mu.Unlock()

	cancel()
	e.wg.Wait()

	e.mu.Lock()
	e.state = engineStopped
	e.mu.Unlock()
	e.cfg.Log.Info("引擎已停止")
}

// ErrStopTimeout 表示 StopWithTimeout 限时内未排空，workers 仍在后台继续退出。
var ErrStopTimeout = errors.New("pipeline: 停机超时,在途 worker 仍在排空")

// StopWithTimeout 有界停机：超时返回 ErrStopTimeout 而非阻塞发布流程。
// 超时后引擎仍在后台完成停机（状态保持 stopping，期间任何 Start 被拒），旧 worker
// 全部自然退出后才回到 stopped；未处理完的消息由上游 MQ 重投/补偿兜底（at-least-once）。
func (e *Engine[M, C]) StopWithTimeout(d time.Duration) error {
	finished := make(chan struct{})
	go func() {
		e.Stop()
		close(finished)
	}()
	select {
	case <-finished:
		return nil
	case <-time.After(d):
		e.cfg.Log.Errorf("Stop 超时(%s): workers 仍在后台排空，全部退出前 Start 将被拒绝", d)
		return ErrStopTimeout
	}
}

// Pause 暂停领取新消息（不影响已领取的在途处理）；可 Resume，也可直接 Stop。
// worker 正阻塞在 Receive 时，receiveCtx 取消使其立即结束等待（未领取任何消息）并挂起。
func (e *Engine[M, C]) Pause() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != engineRunning {
		return fmt.Errorf("pipeline: 仅 running 态允许 Pause")
	}
	e.state = enginePaused
	e.rcCancel()
	e.cfg.Log.Info("引擎已暂停: 停止领取新消息, 在途处理继续")
	return nil
}

// Resume 从暂停态恢复领取消息。
func (e *Engine[M, C]) Resume() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != enginePaused {
		return fmt.Errorf("pipeline: 仅 paused 态允许 Resume")
	}
	e.receiveCtx, e.rcCancel = context.WithCancel(e.ctx)
	close(e.resumeCh) // 唤醒滞留在暂停闸口的 worker/dispatcher
	e.resumeCh = make(chan struct{})
	e.state = engineRunning
	e.cfg.Log.Info("引擎已恢复领取消息")
	return nil
}

// snapshotReceiveCtx 锁内读取当前领取上下文（Resume 会更换它）。
func (e *Engine[M, C]) snapshotReceiveCtx() context.Context {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.receiveCtx
}

// awaitResume 暂停闸口：返回 true 表示可继续领取，false 表示暂停期间发生停机应退出。
func (e *Engine[M, C]) awaitResume() bool {
	e.mu.Lock()
	state := e.state
	ch := e.resumeCh
	e.mu.Unlock()
	if state == engineRunning {
		return true // Pause 后已立即 Resume，无需等待
	}
	select {
	case <-ch:
		return true
	case <-e.ctx.Done():
		return false
	}
}

// interrupt 停机中断上报（不终态、不重投）：统一 handle 在途步骤与分片模式已路由/
// 尚未交付的消息两条路径，均走 OnInterrupted 观测链路。
func (e *Engine[M, C]) interrupt(msg M, err error) {
	e.cfg.Log.Warnf("停机中断在途消息(不终态化/不重投): err=%v", err)
	if e.cfg.OnInterrupted != nil {
		e.cfg.OnInterrupted(msg, err)
	}
}

// hashKey 将有序键稳定哈希为分片索引（fnv-1a）。
func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key)) // hash.Hash64.Write 恒不报错
	return h.Sum64()
}

func (e *Engine[M, C]) worker(id int) {
	defer e.wg.Done()
	e.cfg.Log.Infof("Worker-%d 已启动", id)
	for {
		msg, ok := e.cfg.Queue.Receive(e.snapshotReceiveCtx())
		if !ok {
			if e.ctx.Err() != nil {
				return // 停机：消费上下文已结束
			}
			if !e.awaitResume() {
				return // 暂停期间停机
			}
			continue // 仅暂停：恢复后继续领取
		}
		e.handle(msg)
	}
}

// dispatcher 同 Key 串行模式的接收器：按 Key 哈希路由到分片通道，同 Key 严格同 worker。
// 停机/暂停语义与 worker 同源：Receive 结束先辨停机还是暂停，退出前关闭全部分片通道。
func (e *Engine[M, C]) dispatcher() {
	defer e.wg.Done()
	rr := 0
dispatchLoop:
	for {
		msg, ok := e.cfg.Queue.Receive(e.snapshotReceiveCtx())
		if !ok {
			if e.ctx.Err() != nil {
				break dispatchLoop // 停机
			}
			if !e.awaitResume() {
				break dispatchLoop // 暂停期间停机
			}
			continue
		}
		idx := rr % e.cfg.Workers // 空键：无有序要求，轮询分摊
		if key := e.cfg.MsgKeyFunc(msg); key != "" {
			idx = int(hashKey(key) % uint64(e.cfg.Workers))
		}
		rr++
		select {
		case e.shards[idx] <- msg: // 无缓冲同步交付：worker 忙则阻塞形成背压
		case <-e.ctx.Done():
			// 尚未交付的手上消息：按停机中断上报后退出
			e.interrupt(msg, context.Canceled)
			break dispatchLoop
		}
	}
	for _, ch := range e.shards {
		close(ch) // 分片 worker 收流结束后退出
	}
}

// shardWorker 分片消费循环：消息已在通道中（已离队），停机时不再执行处理，
// 仅排空本分片并逐条上报中断——与 handle 的停机中断语义一致。
func (e *Engine[M, C]) shardWorker(i int) {
	defer e.wg.Done()
	for {
		select {
		case msg, ok := <-e.shards[i]:
			if !ok {
				return // dispatcher 已退出且分片排空
			}
			if e.ctx.Err() != nil {
				e.interrupt(msg, context.Canceled)
				continue
			}
			e.handle(msg)
		case <-e.ctx.Done():
			// 停机：非阻塞排空本分片剩余消息，全部上报中断
			for {
				select {
				case msg, ok := <-e.shards[i]:
					if !ok {
						return
					}
					e.interrupt(msg, context.Canceled)
				default:
					return
				}
			}
		}
	}
}

// handle worker 处理单消息入口：全链路最后一道 panic 防线。
// Resolve/注入/步骤/任意钩子内的 panic 均被恢复并转 OnFatal 不可恢复处置
// （panic 后消息状态未知，业务侧 OnFatal 应保证幂等）；
// 配合 runStep 的步骤级 recover，保证 worker 不因任何业务 panic 崩溃。
func (e *Engine[M, C]) handle(msg M) {
	defer func() {
		if r := recover(); r != nil {
			e.cfg.Log.Errorf("消息处理 panic 已恢复: panic=%v\n%s", r, debug.Stack())
			e.safeFatal(msg, fmt.Sprintf("panic: %v", r))
		}
	}()
	e.process(msg)
}

// safeFatal 回调 OnFatal 并兜住回调自身的 panic，保证终态处置阶段也不击穿 worker。
func (e *Engine[M, C]) safeFatal(msg M, reason string) {
	defer func() {
		if r := recover(); r != nil {
			e.cfg.Log.Errorf("OnFatal 回调 panic 已恢复: panic=%v\n%s", r, debug.Stack())
		}
	}()
	if e.cfg.OnFatal != nil {
		e.cfg.OnFatal(msg, reason)
	}
}

// process 单消息处理：解析→注入消息级 context/日志→顺序执行步骤→按结果分派钩子
func (e *Engine[M, C]) process(msg M) {
	steps, ctx, err := e.cfg.Resolve(msg)
	if err != nil {
		e.cfg.Log.Errorf("消息不可恢复，转终态处置: err=%v", err)
		e.safeFatal(msg, err.Error())
		return
	}
	if cv := reflect.ValueOf(any(ctx)); !cv.IsValid() || (cv.Kind() == reflect.Ptr && cv.IsNil()) {
		// Resolve 成功但返回空上下文（nil 接口或 typed-nil 指针）：注入即 panic，按不可恢复处置
		e.cfg.Log.Error("Resolve 返回 nil 上下文，转终态处置")
		e.safeFatal(msg, "Resolve 返回 nil 上下文")
		return
	}

	// 引擎统一注入（使用方零处理）：
	// 消息级 context 从运行上下文派生，Stop 或 handle 返回时取消，
	// 步骤内长操作（HTTP/DB）经 ctx.Done() 可感知停机提前退出；
	// 日志出口统一为 cfg.Log，业务自定义 logger 请在步骤内自行持有。
	msgCtx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	ctx.setContext(msgCtx)
	ctx.setLogger(e.cfg.Log)

	if len(steps) == 0 {
		// 契约成文：空 Step 链视为空成功（多为配置漏网，告警提醒）
		e.cfg.Log.Warn("Resolve 返回空 Step 链，按空成功处理")
	}

	var failedStep string
	var execErr error
	for _, step := range steps {
		e.cfg.Log.Infof("步骤开始: step=%s", step.Name())
		if execErr = e.runStep(step, ctx, msgCtx); execErr != nil {
			failedStep = step.Name()
			e.cfg.Log.Errorf("步骤失败: step=%s, err=%v", failedStep, execErr)
			break
		}
	}

	if execErr == nil {
		if e.cfg.OnSuccess != nil {
			e.cfg.OnSuccess(msg, ctx)
		}
		return
	}

	// 失败分派优先级：停机中断 > 不可重试错误 > Retryer 裁决重投 > 耗尽终态。
	// 停机中断 ≠ 业务失败：不重投、不落终态、不计重试额度，消息由上游重投/补偿通道兜底。
	// msgCtx 仅可能由引擎运行 ctx 取消而来，e.ctx.Err() 是精确信号，不会误判业务超时/步骤超时。
	if e.ctx.Err() != nil {
		e.interrupt(msg, execErr)
		return
	}

	// 错误分级：错误链包装 ErrNonRetryable（参数非法/业务永久错误）时不浪费重试额度，直接终态
	if errors.Is(execErr, ErrNonRetryable) {
		if e.cfg.OnExhausted != nil {
			e.cfg.OnExhausted(msg, failedStep, fmt.Errorf("step=%s: %w", failedStep, execErr))
		}
		return
	}

	// 重试裁决（唯一重试入口）：轮次状态、退避形状、是否重投均由使用方 Retryer
	// 实现给出；未配置 Config.Retryer 即不重试，失败直接转终态
	if e.cfg.Retryer != nil {
		next, delay, retry := e.cfg.Retryer.Retry(msg, failedStep, execErr)
		if !retry {
			if e.cfg.OnExhausted != nil {
				e.cfg.OnExhausted(msg, failedStep, fmt.Errorf("step=%s: %w", failedStep, execErr))
			}
			return
		}
		if serr := e.cfg.Queue.Submit(next, delay); serr != nil {
			e.cfg.Log.Errorf("Retryer 裁决重投入队失败(留待外部兜底): err=%v", serr)
			return
		}
		if e.cfg.OnRetried != nil {
			e.cfg.OnRetried(msg, next, delay)
		}
		e.cfg.Log.Warnf("Retryer 裁决重投: delay=%s, err=%v", delay, execErr)
		return
	}

	if e.cfg.OnExhausted != nil {
		e.cfg.OnExhausted(msg, failedStep, fmt.Errorf("step=%s: %w", failedStep, execErr))
	}
}

// runStep 执行单个步骤：支持步骤级超时，并捕获步骤内 panic（含组合子内部任意层级步骤）。
// 业务 panic（nil 解引用、nil map 写入、并发竞争等）转为 error 沿既有「重试退避/终态处置」
// 链路分派，避免击穿进程；堆栈记入日志供排查。
// 步骤级超时：实现 TimeoutDecliner（或用 WithTimeout 包装）的步骤获得从 msgCtx 派生的超时
// context——执行前经 setContext 替换注入，执行后先恢复消息级 ctx 再 cancel，后续步骤不受
// 前序超时影响。超时仅对遵守 ctx.Done() 的长操作生效；不遵守的代码只能自然跑完
// （引擎不强杀，与停机语义一致）。超时错误为 DeadlineExceeded，非停机场景不会被误判为中断。
func (e *Engine[M, C]) runStep(step Step[C], ctx C, msgCtx context.Context) (err error) {
	if td, ok := step.(TimeoutDecliner); ok && td.Timeout() > 0 {
		timeoutCtx, cancel := context.WithTimeout(msgCtx, td.Timeout())
		ctx.setContext(timeoutCtx)
		defer func() {
			ctx.setContext(msgCtx) // 先恢复消息级 ctx，再释放超时资源
			cancel()
		}()
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("step panic: %v", r)
			e.cfg.Log.Errorf("步骤 panic 已恢复转错误: step=%s, panic=%v\n%s", step.Name(), r, debug.Stack())
		}
	}()
	return step.Execute(ctx)
}
