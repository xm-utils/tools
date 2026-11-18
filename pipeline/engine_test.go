package pipeline

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// BorrowOpContext 业务上下文样例：embed 内置 ExecContext 即满足密封接口 Context，
// 零方法实现；context/logger 由引擎在步骤执行前自动注入。
type BorrowOpContext struct {
	ExecContext
	OpNo    string
	entered chan struct{} // blockingStep 进场信号（仅测试用）
}

// chanQueue 内存测试队列：Receive 阻塞直到有消息或 ctx 结束
type chanQueue struct {
	msgs chan string
}

func newChanQueue() *chanQueue {
	return &chanQueue{msgs: make(chan string, 16)}
}

func (q *chanQueue) Submit(msg string, delay time.Duration) error {
	if delay > 0 {
		tm := time.NewTimer(delay)
		defer tm.Stop()
		<-tm.C
	}
	q.msgs <- msg
	return nil
}

func (q *chanQueue) Receive(ctx context.Context) (string, bool) {
	select {
	case m := <-q.msgs:
		return m, true
	case <-ctx.Done():
		return "", false
	}
}

// 成功链路：Resolve 只填领域字段，引擎自动注入消息级 context 与统一 logger，
// 全部步骤成功触发 OnSuccess
func TestHandleSuccess(t *testing.T) {
	q := newChanQueue()
	var mu sync.Mutex
	var gotCtx, gotLog bool
	var success []string
	wantLog := logrus.WithField("test", t.Name())

	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 1,
		Log:     wantLog,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			// 使用方零处理：不碰 context、不设 logger
			return []Step[*BorrowOpContext]{noOpStep{}}, &BorrowOpContext{OpNo: msg}, nil
		},
		OnSuccess: func(msg string, ctx *BorrowOpContext) {
			mu.Lock()
			gotCtx = ctx.Done() != nil
			gotLog = ctx.Logger() == wantLog
			success = append(success, ctx.OpNo)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	q.msgs <- "OP-1"
	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(success) == 1
	})
	e.Stop()
	if !gotCtx {
		t.Fatal("引擎未在步骤执行前注入消息级 context")
	}
	if !gotLog {
		t.Fatal("引擎未注入统一 logger（应为 cfg.Log）")
	}
	if len(success) != 1 || success[0] != "OP-1" {
		t.Fatalf("OnSuccess 消息不符: %v", success)
	}
}

// 停机中断链路：步骤阻塞在 ctx.Done()，Stop 后步骤收到 context.Canceled，
// 引擎按「停机中断 ≠ 业务失败」处理：仅 OnInterrupted，不终态化/不重投/不计重试（即使配了 Retry）
func TestStopCancelsStep(t *testing.T) {
	q := newChanQueue()
	stepEntered := make(chan struct{})
	var mu sync.Mutex
	var interruptedErr error
	var exhaustedCalled, retriedCalled int

	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 1,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			return []Step[*BorrowOpContext]{blockingStep{}}, &BorrowOpContext{entered: stepEntered}, nil
		},
		OnInterrupted: func(msg string, err error) {
			mu.Lock()
			interruptedErr = err
			mu.Unlock()
		},
		OnExhausted: func(msg string, failedStep string, err error) {
			mu.Lock()
			exhaustedCalled++
			mu.Unlock()
		},
		OnRetried: func(msg, next string, delay time.Duration) {
			mu.Lock()
			retriedCalled++
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	q.msgs <- "OP-2"
	<-stepEntered

	e.Stop() // cancel 引擎 ctx → msgCtx 取消 → blockingStep 返回 context.Canceled
	// 钩子在 worker 退出前完成，Stop 返回后状态确定可检
	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(interruptedErr, context.Canceled) {
		t.Fatalf("期望 OnInterrupted(context.Canceled), got %v", interruptedErr)
	}
	if exhaustedCalled != 0 || retriedCalled != 0 {
		t.Fatalf("停机中断不得触发终态化/重投: exhausted=%d retried=%d", exhaustedCalled, retriedCalled)
	}
}

// Resolve 返回 typed-nil 上下文：按不可恢复转 OnFatal，不能让 panic 击穿
func TestNilContextFatal(t *testing.T) {
	q := newChanQueue()
	var reason string
	done := make(chan struct{})

	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 1,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			return []Step[*BorrowOpContext]{noOpStep{}}, (*BorrowOpContext)(nil), nil
		},
		OnFatal: func(msg string, r string) {
			reason = r
			close(done)
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	q.msgs <- "OP-3"

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("nil 上下文未转 OnFatal")
	}
	e.Stop()
	if reason == "" {
		t.Fatal("OnFatal reason 为空")
	}
}

// BestEffortStep：配置 OnIgnoredError 时只走钩子，不再重复打默认日志
func TestBestEffortSingleLogPath(t *testing.T) {
	var hookCalled int
	best := NewBestEffortStep[*BorrowOpContext]("best", nil, failingStep{})
	best.OnIgnoredError = func(ctx *BorrowOpContext, stepName string, err error) {
		hookCalled++
	}
	if err := best.Execute(&BorrowOpContext{ExecContext: *NewExecContext(context.Background(), nil)}); err != nil {
		t.Fatalf("BestEffort 不应向上传播错误: %v", err)
	}
	if hookCalled != 1 {
		t.Fatalf("OnIgnoredError 应恰好回调一次, got %d", hookCalled)
	}
}

// #1 步骤 panic：恢复转 error 进入 OnExhausted（未配重试首次失败即终态），
// 进程不崩且 worker 继续消费后续消息
func TestStepPanicRecovered(t *testing.T) {
	q := newChanQueue()
	var mu sync.Mutex
	var panicMsgs []string
	var successMsgs []string
	secondArrived := make(chan struct{})

	var once sync.Once
	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 1,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			if msg == "P1" {
				return []Step[*BorrowOpContext]{panickingStep{}}, &BorrowOpContext{OpNo: msg}, nil
			}
			return []Step[*BorrowOpContext]{noOpStep{}}, &BorrowOpContext{OpNo: msg}, nil
		},
		OnExhausted: func(msg string, failedStep string, err error) {
			mu.Lock()
			panicMsgs = append(panicMsgs, msg+"/"+failedStep)
			mu.Unlock()
		},
		OnSuccess: func(msg string, ctx *BorrowOpContext) {
			mu.Lock()
			successMsgs = append(successMsgs, msg)
			mu.Unlock()
			if msg == "S1" {
				once.Do(func() { close(secondArrived) })
			}
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	q.msgs <- "P1" // 首轮：步骤内运行时 panic（nil map 写入）
	q.msgs <- "S1" // 二轮：验证 worker 未被击穿，仍能正常消费
	select {
	case <-secondArrived:
	case <-time.After(2 * time.Second):
		t.Fatal("panic 后 worker 未恢复消费（超时）")
	}
	e.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(panicMsgs) != 1 || panicMsgs[0] != "P1/panicking" {
		t.Fatalf("panic 消息应转入 OnExhausted 并记录失败步骤名, got %v", panicMsgs)
	}
	if len(successMsgs) != 1 || successMsgs[0] != "S1" {
		t.Fatalf("worker 应继续消费后续消息, got %v", successMsgs)
	}
}

// #3 Delay 溢出封顶：负轮次/超大轮次/大基数回绕均不得产生负值或错误小值 delay
// （额度耗尽返回 0 是 Strategy 契约的合法值，另有专项断言）
func TestBackoffOverflowCapped(t *testing.T) {
	p := &RetryPolicy{MaxAttempt: math.MaxInt32, BaseDelay: time.Second}

	if d := p.Delay(-5); d != time.Second {
		t.Fatalf("负轮次应按首次处理, got %v", d)
	}
	if d := p.Delay(0); d != time.Second {
		t.Fatalf("attempt=0 应为 BaseDelay, got %v", d)
	}
	if d := p.Delay(30); d != time.Second<<30 {
		t.Fatalf("正常区间不应被封顶, got %v", d)
	}
	for _, attempt := range []int{62, 63, 100, 1 << 20} {
		if d := p.Delay(attempt); d != math.MaxInt64 {
			t.Fatalf("attempt=%d 应饱和为 MaxInt64, got %v", attempt, d)
		}
	}
	// 大基数中间轮次回绕为负：同样饱和而非负 delay
	huge := &RetryPolicy{MaxAttempt: 5, BaseDelay: time.Duration(math.MaxInt64 / 3)}
	if d := huge.Delay(2); d != math.MaxInt64 {
		t.Fatalf("移位回绕应饱和为 MaxInt64, got %v", d)
	}
	// 单调非减且恒正，排除负 delay 入队引发热循环
	for attempt := 0; attempt < 70; attempt++ {
		if d := p.Delay(attempt); d <= 0 {
			t.Fatalf("attempt=%d 产生非正 delay=%v", attempt, d)
		}
		if attempt > 0 && p.Delay(attempt) < p.Delay(attempt-1) {
			t.Fatalf("attempt=%d 退避时长非单调", attempt)
		}
	}

	// MaxDelay 封顶：常规退避与饱和路径均被截断到封顶值
	capped := &RetryPolicy{MaxAttempt: 20, BaseDelay: time.Second, MaxDelay: 5 * time.Minute}
	if d := capped.Delay(0); d != time.Second {
		t.Fatalf("未超封顶时应为原始退避值, got %v", d)
	}
	if d := capped.Delay(10); d != 5*time.Minute { // 1s<<10=17min > 5min 封顶
		t.Fatalf("超封顶应截断为 MaxDelay, got %v", d)
	}
	hugeCap := &RetryPolicy{MaxAttempt: 100, BaseDelay: time.Duration(math.MaxInt64), MaxDelay: time.Minute}
	if d := hugeCap.Delay(62); d != time.Minute {
		t.Fatalf("饱和值也应受 MaxDelay 截断, got %v", d)
	}

	// 额度自检（Strategy 的 0 契约）：attempt+1 > MaxAttempt 或 BaseDelay<=0 时返回 0
	quota := &RetryPolicy{MaxAttempt: 2, BaseDelay: time.Second}
	if d := quota.Delay(1); d != 2*time.Second {
		t.Fatalf("额度内应返回退避值, got %v", d)
	}
	if d := quota.Delay(2); d != 0 {
		t.Fatalf("额度耗尽应返回 0(不需重试), got %v", d)
	}
	if d := (&RetryPolicy{MaxAttempt: 5}).Delay(0); d != 0 {
		t.Fatalf("BaseDelay<=0 应返回 0(不需重试), got %v", d)
	}
}

// #5 配置校验：装配期错误返回 error 而非 panic，调用方可优雅处理
func TestNewConfigValidation(t *testing.T) {
	validResolve := func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
		return nil, &BorrowOpContext{OpNo: msg}, nil
	}
	q := newChanQueue()

	cases := []struct {
		name string
		cfg  Config[string, *BorrowOpContext]
	}{
		{"Queue 缺失", Config[string, *BorrowOpContext]{Resolve: validResolve}},
		{"Resolve 缺失", Config[string, *BorrowOpContext]{Queue: q}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("配置错误不应 panic: %v", r)
				}
			}()
			e, err := New(tc.cfg)
			if err == nil || e != nil {
				t.Fatalf("期望返回 error, got e=%v, err=%v", e, err)
			}
		})
	}

	// 合法配置（含 Workers<=0 默认化、未配 Retryer=不重试）正常构造
	e, err := New(Config[string, *BorrowOpContext]{Queue: q, Resolve: validResolve, Workers: -1})
	if err != nil || e == nil {
		t.Fatalf("合法配置应构造成功: e=%v, err=%v", e, err)
	}
	if e.cfg.Workers != 1 {
		t.Fatalf("Workers<=0 应默认为 1, got %d", e.cfg.Workers)
	}
}

// #6 Start/Stop 幂等与防重入：重复 Start 报错不泄漏 worker；
// 并发 Stop 安全；停机后可重启且重启后 worker 真实消费
func TestStartStopIdempotent(t *testing.T) {
	q := newChanQueue()
	restarted := make(chan string, 1)

	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 2,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			return []Step[*BorrowOpContext]{noOpStep{}}, &BorrowOpContext{OpNo: msg}, nil
		},
		OnSuccess: func(msg string, ctx *BorrowOpContext) {
			select {
			case restarted <- ctx.OpNo:
			default:
			}
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := e.Start(); err != nil {
		t.Fatalf("首次 Start: %v", err)
	}
	if err := e.Start(); err == nil {
		t.Fatal("重复 Start 应返回 error")
	}

	// 并发 Stop：不得 panic/死锁，全部返回后进入 stopped 态
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Stop()
		}()
	}
	wg.Wait()

	// 停机后重启，重启后 worker 真实消费
	if err := e.Start(); err != nil {
		t.Fatalf("Stop 后重新 Start: %v", err)
	}
	q.msgs <- "OP-R"
	select {
	case got := <-restarted:
		if got != "OP-R" {
			t.Fatalf("重启后消费消息不符: %s", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("重启后 worker 未消费消息")
	}
	e.Stop()
}

// #7 ConditionalStep 错误包装：同时携带组合步骤名与内部失败步骤名，%w 保留原始 error 链
func TestConditionalStepErrorWrap(t *testing.T) {
	errBoom := errors.New("boom")
	inner := &namedErrStep{name: "chargeFee", err: errBoom}
	cond := NewConditionalStep[*BorrowOpContext]("riskCheck", nil, inner)

	execErr := cond.Execute(&BorrowOpContext{ExecContext: *NewExecContext(context.Background(), nil)})
	if execErr == nil {
		t.Fatal("内部步骤失败应向上传播")
	}
	if !errors.Is(execErr, errBoom) {
		t.Fatal("包装丢失原始 error 链（%w）")
	}
	for _, want := range []string{"riskCheck", "chargeFee"} {
		if !strings.Contains(execErr.Error(), want) {
			t.Fatalf("错误信息缺少 %s: %s", want, execErr.Error())
		}
	}
}

// DataKey.Get 三态语义：键不存在/读取成功/类型不匹配可用 errors.Is 精确区分
func TestDataKeyGetSemantics(t *testing.T) {
	ctx := NewExecContext(context.Background(), nil)

	// 1. 从未写入：ErrKeyNotFound
	got, err := DataKey[string]{"orderId"}.Get(ctx)
	if !errors.Is(err, ErrKeyNotFound) || got != "" {
		t.Fatalf("期望 ErrKeyNotFound, got v=%q err=%v", got, err)
	}
	if !strings.Contains(err.Error(), `key="orderId"`) {
		t.Fatalf("错误信息应含键名便于排查: %s", err)
	}

	// 2. 写入后读取成功
	DataKey[string]{"orderId"}.Set(ctx, "ORD-1")
	if got, err := (DataKey[string]{"orderId"}).Get(ctx); err != nil || got != "ORD-1" {
		t.Fatalf("写入后读取失败: v=%q err=%v", got, err)
	}

	// 3. 存在但类型不符：ErrKeyTypeMismatch（与不存在明确区分）
	mismatchVal, mismatchErr := (DataKey[int64]{"orderId"}).Get(ctx)
	if !errors.Is(mismatchErr, ErrKeyTypeMismatch) || mismatchVal != 0 {
		t.Fatalf("期望 ErrKeyTypeMismatch, got v=%d err=%v", mismatchVal, mismatchErr)
	}
	if !strings.Contains(mismatchErr.Error(), "got string") {
		t.Fatalf("类型不匹配错误应含实际存储类型: %s", mismatchErr)
	}
}

// Blocker#2：Resolve 内 panic 被 handle 最后一道防线兜住，转 OnFatal 且 worker 继续消费
func TestResolvePanicRecovered(t *testing.T) {
	q := newChanQueue()
	var mu sync.Mutex
	var fatalReason string
	secondOK := make(chan struct{})

	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 1,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			if msg == "P1" {
				var m map[string]string
				m["x"] = "1" // Resolve 内 panic（写 nil map）
			}
			return []Step[*BorrowOpContext]{noOpStep{}}, &BorrowOpContext{OpNo: msg}, nil
		},
		OnFatal: func(msg string, reason string) {
			mu.Lock()
			fatalReason = reason
			mu.Unlock()
		},
		OnSuccess: func(msg string, ctx *BorrowOpContext) {
			if msg == "S1" {
				close(secondOK)
			}
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	q.msgs <- "P1"
	q.msgs <- "S1" // panic 后 worker 不死，第二条消息正常消费
	select {
	case <-secondOK:
	case <-time.After(3 * time.Second):
		t.Fatal("panic 后 worker 未能继续消费")
	}
	e.Stop()
	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(fatalReason, "panic") {
		t.Fatalf("期望 OnFatal 携带 panic 原因, got %q", fatalReason)
	}
}

// Blocker#2：钩子（OnExhausted）自身 panic 同样被兜住，降级转 OnFatal
func TestHookPanicRecovered(t *testing.T) {
	q := newChanQueue()
	fatal := make(chan string, 1)

	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 1,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			return []Step[*BorrowOpContext]{failingStep{}}, &BorrowOpContext{OpNo: msg}, nil
		},
		OnExhausted: func(msg string, failedStep string, err error) { panic("hook bug") },
		OnFatal:     func(msg string, reason string) { fatal <- reason },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	q.msgs <- "H1"
	var reason string
	select {
	case reason = <-fatal:
	case <-time.After(3 * time.Second):
		t.Fatal("OnExhausted panic 未被兜底，OnFatal 未触发")
	}
	e.Stop()
	if !strings.Contains(reason, "panic") {
		t.Fatalf("期望 OnFatal 携带 panic 原因, got %q", reason)
	}
}

// Major#3：BestEffortStep 内部 panic 与非关键失败同路——忽略并继续主流程，消息不落死信
func TestBestEffortPanicNotBreakingMainFlow(t *testing.T) {
	q := newChanQueue()
	success := make(chan string, 1)
	var mu sync.Mutex
	var ignoredErr error

	// 组合器无状态，构建一次共享安全（内部仅 noOp/panic 步骤）
	best := NewBestEffortStep[*BorrowOpContext]("notify", nil, panickingStep{})
	best.OnIgnoredError = func(ctx *BorrowOpContext, stepName string, err error) {
		mu.Lock()
		ignoredErr = err
		mu.Unlock()
	}

	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 1,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			return []Step[*BorrowOpContext]{best, noOpStep{}}, &BorrowOpContext{OpNo: msg}, nil
		},
		OnSuccess: func(msg string, ctx *BorrowOpContext) { success <- msg },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	q.msgs <- "B1"
	select {
	case <-success:
	case <-time.After(3 * time.Second):
		t.Fatal("BestEffort 内部 panic 中断了主流程（期望被忽略后继续执行后续步骤）")
	}
	e.Stop()
	mu.Lock()
	defer mu.Unlock()
	if ignoredErr == nil || !strings.Contains(ignoredErr.Error(), "panic") {
		t.Fatalf("期望 OnIgnoredError 收到 panic 转译错误, got %v", ignoredErr)
	}
}

// Major#4：退避饱和封顶（未设 MaxDelay）视同额度耗尽直接终态，不留僵尸消息
func TestBackoffSaturatedMeansExhausted(t *testing.T) {
	// 未配 Retryer：首次失败直接终态，不重投
	t.Run("未配Retryer首败即耗尽", func(t *testing.T) {
		q := newChanQueue()
		exhausted := make(chan string, 1)
		var mu sync.Mutex
		var retried int

		e, err := New(Config[string, *BorrowOpContext]{
			Queue:   q,
			Workers: 1,
			Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
				return []Step[*BorrowOpContext]{failingStep{}}, &BorrowOpContext{OpNo: msg}, nil
			},
			OnRetried:   func(msg, next string, delay time.Duration) { mu.Lock(); retried++; mu.Unlock() },
			OnExhausted: func(msg string, failedStep string, err error) { exhausted <- msg },
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if err := e.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		q.msgs <- "Z1"
		select {
		case <-exhausted:
		case <-time.After(3 * time.Second):
			t.Fatal("未配 Retryer 时首次失败应落入终态处置")
		}
		e.Stop()
		mu.Lock()
		defer mu.Unlock()
		if retried != 0 || len(q.msgs) != 0 {
			t.Fatalf("不应重投/入队: retried=%d queue_len=%d", retried, len(q.msgs))
		}
	})

	// 端到端饱和转耗尽：Strategy.Delay 饱和为 MaxInt64（未设 MaxDelay）时
	// PolicyRetryer 裁决不重试，消息直达终态而非僵尸滞留
	t.Run("退避饱和转耗尽", func(t *testing.T) {
		q := newChanQueue()
		exhausted := make(chan string, 1)
		var mu sync.Mutex
		var retried int

		e, err := New(Config[string, *BorrowOpContext]{
			Queue:   q,
			Workers: 1,
			Retryer: &PolicyRetryer[string]{
				Policy: &RetryPolicy{MaxAttempt: 10, BaseDelay: time.Duration(math.MaxInt64)},
				Next:   func(msg string) string { return msg },
			},
			Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
				return []Step[*BorrowOpContext]{failingStep{}}, &BorrowOpContext{OpNo: msg}, nil
			},
			OnRetried:   func(msg, next string, delay time.Duration) { mu.Lock(); retried++; mu.Unlock() },
			OnExhausted: func(msg string, failedStep string, err error) { exhausted <- msg },
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if err := e.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		q.msgs <- "Z2"
		select {
		case <-exhausted:
		case <-time.After(3 * time.Second):
			t.Fatal("退避饱和后未落入终态处置（僵尸消息）")
		}
		e.Stop()
		mu.Lock()
		defer mu.Unlock()
		if retried != 0 || len(q.msgs) != 0 {
			t.Fatalf("饱和不应触发重投/入队: retried=%d queue_len=%d", retried, len(q.msgs))
		}
	})
}

// Minor#9：空 Step 链语义成文——视为空成功（引擎告警）并走 OnSuccess
func TestEmptyStepsChainSuccess(t *testing.T) {
	q := newChanQueue()
	success := make(chan string, 1)

	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 1,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			return nil, &BorrowOpContext{OpNo: msg}, nil // 空链
		},
		OnSuccess: func(msg string, ctx *BorrowOpContext) { success <- msg },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	q.msgs <- "E1"
	select {
	case <-success:
	case <-time.After(3 * time.Second):
		t.Fatal("空 Step 链未按空成功语义走 OnSuccess")
	}
	e.Stop()
}

// #5：错误链包装 ErrNonRetryable 时不消耗重试额度，首次失败直达 OnExhausted
func TestNonRetryableErrorDirectExhausted(t *testing.T) {
	q := newChanQueue()
	exhausted := make(chan error, 1)
	var mu sync.Mutex
	var retried int

	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 1,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			return []Step[*BorrowOpContext]{nonRetryableStep{}}, &BorrowOpContext{OpNo: msg}, nil
		},
		OnRetried:   func(msg, next string, delay time.Duration) { mu.Lock(); retried++; mu.Unlock() },
		OnExhausted: func(msg string, failedStep string, err error) { exhausted <- err },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	q.msgs <- "N1"
	var got error
	select {
	case got = <-exhausted:
	case <-time.After(3 * time.Second):
		t.Fatal("ErrNonRetryable 未直达终态处置")
	}
	e.Stop()
	if !errors.Is(got, ErrNonRetryable) {
		t.Fatalf("OnExhausted 错误链应可 errors.Is(ErrNonRetryable), got %v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if retried != 0 {
		t.Fatalf("不可重试错误不得消耗额度: retried=%d", retried)
	}
}

// #6：WithTimeout 步骤超时转 DeadlineExceeded 失败，不影响后续消息正常处理
func TestStepTimeout(t *testing.T) {
	q := newChanQueue()
	exhausted := make(chan error, 1)
	success := make(chan string, 1)

	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 1,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			if msg == "TO1" {
				// hangingStep 永久阻塞在 ctx.Done()，30ms 超时后以 DeadlineExceeded 失败
				return []Step[*BorrowOpContext]{WithTimeout(hangingStep{}, 30*time.Millisecond)}, &BorrowOpContext{OpNo: msg}, nil
			}
			return []Step[*BorrowOpContext]{noOpStep{}}, &BorrowOpContext{OpNo: msg}, nil
		},
		OnSuccess:   func(msg string, ctx *BorrowOpContext) { success <- msg },
		OnExhausted: func(msg string, failedStep string, err error) { exhausted <- err },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	q.msgs <- "TO1"
	var got error
	select {
	case got = <-exhausted:
	case <-time.After(3 * time.Second):
		t.Fatal("步骤超时未按失败分派")
	}
	if !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("期望 DeadlineExceeded, got %v", got)
	}
	q.msgs <- "OK2" // 超时不影响后续消息（runStep 已恢复消息级 ctx）
	select {
	case <-success:
	case <-time.After(3 * time.Second):
		t.Fatal("超时后引擎未能继续正常消费")
	}
	e.Stop()
}

// #12：Pause 停止领取新消息、Resume 恢复；在途处理不受影响
func TestPauseResume(t *testing.T) {
	q := newChanQueue()
	var mu sync.Mutex
	var count int

	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 1,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			return []Step[*BorrowOpContext]{noOpStep{}}, &BorrowOpContext{OpNo: msg}, nil
		},
		OnSuccess: func(msg string, ctx *BorrowOpContext) { mu.Lock(); count++; mu.Unlock() },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	q.msgs <- "P1"
	waitFor(t, 2*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return count == 1 })

	if err := e.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if err := e.Pause(); err == nil {
		t.Fatal("重复 Pause 应报错")
	}
	q.msgs <- "P2" // 暂停中不得被领取
	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	pausedCount := count
	mu.Unlock()
	if pausedCount != 1 {
		t.Fatalf("Pause 期间仍在领取消息: count=%d", pausedCount)
	}

	if err := e.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return count == 2 })
	e.Stop()
}

// #12：暂停态可直接 Stop（闸口 worker 经 e.ctx 取消退出，不卡死）
func TestStopWhilePaused(t *testing.T) {
	q := newChanQueue()
	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 2,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			return []Step[*BorrowOpContext]{noOpStep{}}, &BorrowOpContext{OpNo: msg}, nil
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := e.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	done := make(chan struct{})
	go func() { e.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("暂停态 Stop 未能退出（闸口 worker 未响应停机取消）")
	}
	if err := e.Resume(); err == nil {
		t.Fatal("停机后 Resume 应报错")
	}
}

// #12：不配合 ctx 的挂死步骤——StopWithTimeout 限时返回，后台排空后可重启
func TestStopWithTimeout(t *testing.T) {
	q := newChanQueue()
	entered := make(chan struct{})
	release := make(chan struct{})

	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 1,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			return []Step[*BorrowOpContext]{stuckStep{entered: entered, release: release}}, &BorrowOpContext{OpNo: msg}, nil
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	q.msgs <- "S1"
	<-entered

	if err := e.StopWithTimeout(30 * time.Millisecond); !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("期望 ErrStopTimeout, got %v", err)
	}
	close(release) // 模拟挂死操作最终完成
	// 后台排空后回到 stopped，允许重启
	waitFor(t, 3*time.Second, func() bool { return e.Start() == nil })
	e.Stop()
}

// #8：MsgKeyFunc 同 Key 严格串行有序，不同 Key 互不阻塞
func TestSameKeySerial(t *testing.T) {
	q := newChanQueue()
	order := &keyOrderStep{orders: map[string][]int{}, inFlight: map[string]int{}}
	var mu sync.Mutex
	var total int

	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 3,
		MsgKeyFunc: func(msg string) string {
			if msg == "" {
				return ""
			}
			return msg[:1]
		},
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			return []Step[*BorrowOpContext]{order}, &BorrowOpContext{OpNo: msg}, nil
		},
		OnSuccess: func(msg string, ctx *BorrowOpContext) { mu.Lock(); total++; mu.Unlock() },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// A/B 两键交错入队：各键内部必须串行且保序
	for i := 0; i < 4; i++ {
		q.msgs <- fmt.Sprintf("A%d", i)
		q.msgs <- fmt.Sprintf("B%d", i)
	}
	waitFor(t, 5*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return total == 8 })
	e.Stop()

	mu.Lock()
	defer mu.Unlock()
	if order.violated {
		t.Fatal("同 Key 消息被并发执行")
	}
	wantA := []int{0, 1, 2, 3}
	if fmt.Sprint(order.orders["A"]) != fmt.Sprint(wantA) {
		t.Fatalf("A 键乱序: got %v, want %v", order.orders["A"], wantA)
	}
	if fmt.Sprint(order.orders["B"]) != fmt.Sprint(wantA) {
		t.Fatalf("B 键乱序: got %v", order.orders["B"])
	}
}

// countRetryer 使用方自定义 Retryer 样例：重试状态外置在实现内部（按消息键计数），
// 固定短延迟——完全脱离内置「计数编码进消息体 + 指数退避」形态
type countRetryer struct {
	mu     sync.Mutex
	counts map[string]int
	max    int
}

func (r *countRetryer) Retry(msg string, failedStep string, err error) (string, time.Duration, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counts[msg] >= r.max {
		return "", 0, false
	}
	r.counts[msg]++
	return msg, 5 * time.Millisecond, true
}

// 端到端：Config.Retryer 接管重试裁决——重投/终态均由实现裁决，不再触碰 Retry 路径
func TestCustomRetryer(t *testing.T) {
	q := newChanQueue()
	rier := &countRetryer{counts: map[string]int{}, max: 2}
	var mu sync.Mutex
	var retried int
	exhausted := make(chan string, 1)

	e, err := New(Config[string, *BorrowOpContext]{
		Queue:   q,
		Workers: 1,
		Retryer: rier,
		Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
			return []Step[*BorrowOpContext]{failingStep{}}, &BorrowOpContext{OpNo: msg}, nil
		},
		OnRetried: func(msg, next string, delay time.Duration) { mu.Lock(); retried++; mu.Unlock() },
		OnExhausted: func(msg string, failedStep string, err error) {
			select {
			case exhausted <- msg:
			default:
			}
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	q.msgs <- "C1"
	select {
	case <-exhausted:
	case <-time.After(3 * time.Second):
		t.Fatal("Retryer 裁决耗尽后未转终态")
	}
	e.Stop()
	mu.Lock()
	defer mu.Unlock()
	if retried != 2 {
		t.Fatalf("期望 Retryer 裁决重投 2 次, got %d", retried)
	}
}

// PolicyRetryer 内置默认实现：额度内退避重投、额度外/饱和/缺 Next 均裁决不重试
func TestPolicyRetryer(t *testing.T) {
	policy := &RetryPolicy{MaxAttempt: 2, BaseDelay: 10 * time.Millisecond}
	next := func(msg string) string { return msg + "!" }

	got, delay, retry := (&PolicyRetryer[string]{Policy: policy, Next: next}).Retry("m1", "s", errors.New("x"))
	if !retry || got != "m1!" || delay != 10*time.Millisecond {
		t.Fatalf("额度内应退避重投: got=%q delay=%v retry=%v", got, delay, retry)
	}
	if _, _, retry := (&PolicyRetryer[string]{Policy: policy, Next: next, Attempt: func(string) int { return 2 }}).Retry("m1", "s", errors.New("x")); retry {
		t.Fatal("额度耗尽应裁决不重试")
	}
	saturated := &PolicyRetryer[string]{Policy: &RetryPolicy{MaxAttempt: 5, BaseDelay: time.Duration(math.MaxInt64)}, Next: next}
	if _, _, retry := saturated.Retry("m1", "s", errors.New("x")); retry {
		t.Fatal("退避饱和应裁决不重试(转终态不留僵尸消息)")
	}
	if _, _, retry := (&PolicyRetryer[string]{Policy: policy}).Retry("m1", "s", errors.New("x")); retry {
		t.Fatal("缺 Next 应裁决不重试而非 panic")
	}
	if _, _, retry := (&PolicyRetryer[string]{Policy: &RetryPolicy{MaxAttempt: 5}, Next: next}).Retry("m1", "s", errors.New("x")); retry {
		t.Fatal("BaseDelay<=0 时 Delay 返回 0, 应裁决不重试(Strategy 的 0 契约)")
	}
}

// enteredStep 进入执行即通知已领取，随后挂起直到 ctx 取消（停机时序配合用）
type enteredStep struct{ entered chan struct{} }

func (enteredStep) Name() string { return "entered" }
func (s enteredStep) Execute(ctx *BorrowOpContext) error {
	close(s.entered)
	<-ctx.Done()
	return ctx.Err()
}

// panic 防线覆盖分片/中断路径：OnInterrupted panic 被 interrupt 兜住；
// MsgKeyFunc panic 被 safeKey 降级为空键（轮询），进程不崩且消息正常处理
func TestInterruptAndKeyPanicRecovered(t *testing.T) {
	t.Run("OnInterrupted panic 被兜住", func(t *testing.T) {
		q := newChanQueue()
		entered := make(chan struct{})
		interrupted := make(chan struct{}, 1)
		e, err := New(Config[string, *BorrowOpContext]{
			Queue:   q,
			Workers: 1,
			Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
				return []Step[*BorrowOpContext]{enteredStep{entered: entered}}, &BorrowOpContext{OpNo: msg}, nil
			},
			OnInterrupted: func(msg string, err error) {
				select {
				case interrupted <- struct{}{}:
				default:
				}
				panic("OnInterrupted crash")
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if err := e.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		q.msgs <- "I1"
		<-entered
		e.Stop() // 未兜住 panic 则测试进程直接崩溃
		select {
		case <-interrupted:
		case <-time.After(2 * time.Second):
			t.Fatal("停机未触发 OnInterrupted")
		}
	})

	t.Run("MsgKeyFunc panic 降级轮询", func(t *testing.T) {
		q2 := newChanQueue()
		successes := make(chan string, 2)
		e2, err := New(Config[string, *BorrowOpContext]{
			Queue:   q2,
			Workers: 2,
			MsgKeyFunc: func(msg string) string {
				if msg == "P1" {
					panic("key extraction crash")
				}
				return msg
			},
			Resolve: func(msg string) ([]Step[*BorrowOpContext], *BorrowOpContext, error) {
				return []Step[*BorrowOpContext]{noOpStep{}}, &BorrowOpContext{OpNo: msg}, nil
			},
			OnSuccess: func(msg string, ctx *BorrowOpContext) { successes <- msg },
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if err := e2.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		q2.msgs <- "P1"
		q2.msgs <- "K1"
		got := map[string]bool{}
		for i := 0; i < 2; i++ {
			select {
			case m := <-successes:
				got[m] = true
			case <-time.After(3 * time.Second):
				t.Fatal("MsgKeyFunc panic 后消息未正常处理")
			}
		}
		e2.Stop()
		if !got["P1"] || !got["K1"] {
			t.Fatalf("两条消息均应成功, got %v", got)
		}
	})
}

// ---- 测试步骤 ----

type noOpStep struct{}

func (noOpStep) Name() string { return "noOp" }
func (noOpStep) Execute(ctx *BorrowOpContext) error {
	if ctx.Done() == nil {
		return errors.New("ctx 未携带 Done 通道")
	}
	return nil
}

type panickingStep struct{}

func (panickingStep) Name() string { return "panicking" }
func (panickingStep) Execute(ctx *BorrowOpContext) error {
	var m map[string]string
	m["boom"] = "1" // 运行时 panic：赋值给 nil map
	return nil
}

type blockingStep struct{}

func (blockingStep) Name() string { return "blocking" }
func (blockingStep) Execute(ctx *BorrowOpContext) error {
	if ctx.entered != nil {
		close(ctx.entered)
	}
	<-ctx.Done()
	return ctx.Err()
}

type failingStep struct{}

func (failingStep) Name() string                       { return "failing" }
func (failingStep) Execute(ctx *BorrowOpContext) error { return errors.New("boom") }

type namedErrStep struct {
	name string
	err  error
}

func (s *namedErrStep) Name() string                       { return s.name }
func (s *namedErrStep) Execute(ctx *BorrowOpContext) error { return s.err }

// hangingStep 无限阻塞在 ctx.Done() 并返回取消错误（步骤级超时场景：被 WithTimeout 以超时 ctx 包裹）
type hangingStep struct{}

func (hangingStep) Name() string                       { return "hanging" }
func (hangingStep) Execute(ctx *BorrowOpContext) error { <-ctx.Done(); return ctx.Err() }

// nonRetryableStep 返回包装 ErrNonRetryable 的业务永久错误
type nonRetryableStep struct{}

func (nonRetryableStep) Name() string { return "badParam" }
func (nonRetryableStep) Execute(ctx *BorrowOpContext) error {
	return fmt.Errorf("参数非法: %w", ErrNonRetryable)
}

// stuckStep 完全不配合 ctx，模拟挂死的三方调用（StopWithTimeout 场景），经 release 外部释放
type stuckStep struct {
	entered chan struct{}
	release chan struct{}
}

func (stuckStep) Name() string { return "stuck" }
func (s stuckStep) Execute(ctx *BorrowOpContext) error {
	close(s.entered)
	<-s.release // 故意不监听 ctx.Done，验证有界停机
	return nil
}

// keyOrderStep 记录各 Key 的处理序列并监测同 Key 并发（有序消费验证）
type keyOrderStep struct {
	mu       sync.Mutex
	orders   map[string][]int
	inFlight map[string]int
	violated bool
}

func (s *keyOrderStep) Name() string { return "keyOrder" }
func (s *keyOrderStep) Execute(ctx *BorrowOpContext) error {
	key := ctx.OpNo[:1]
	seq := int(ctx.OpNo[1] - '0')
	s.mu.Lock()
	s.inFlight[key]++
	if s.inFlight[key] > 1 {
		s.violated = true
	}
	s.mu.Unlock()

	time.Sleep(5 * time.Millisecond) // 拉开处理窗口，使乱序/并发可被捕获

	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight[key]--
	s.orders[key] = append(s.orders[key], seq)
	return nil
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("等待条件超时")
}
