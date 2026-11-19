package pipequeue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xm-utils/tools/mq"
	xmpipeline "github.com/xm-utils/tools/pipeline"
)

// ==================== 测试替身 ====================

// fakeConsumer 模拟 mq.Consumer 适配器的确认分派行为：
// Subscribe 阻塞至 ctx 结束；push 按 Handler 返回值分派 commit/inflight/fail，
// 与 mq/kafka、mq/redis/* 的契约一致。
type fakeConsumer struct {
	mu       sync.Mutex
	handler  mq.Handler
	started  chan struct{}
	startOne sync.Once

	commits  []string
	rejects  []string
	inflight map[string]bool
	fails    []string
}

func newFakeConsumer() *fakeConsumer {
	return &fakeConsumer{started: make(chan struct{}), inflight: make(map[string]bool)}
}

func (f *fakeConsumer) Subscribe(ctx context.Context, group, topic string, handler mq.Handler) error {
	f.mu.Lock()
	f.handler = handler
	f.mu.Unlock()
	f.startOne.Do(func() { close(f.started) })
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeConsumer) Commit(_ context.Context, msg *mq.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.inflight[msg.ID] {
		return mq.ErrNotSubscribed
	}
	f.commits = append(f.commits, msg.ID)
	delete(f.inflight, msg.ID)
	return nil
}

func (f *fakeConsumer) Reject(_ context.Context, msg *mq.Message, requeue bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.inflight[msg.ID] {
		return mq.ErrNotSubscribed
	}
	tag := msg.ID
	if requeue {
		tag += ":requeue"
	}
	f.rejects = append(f.rejects, tag)
	delete(f.inflight, msg.ID)
	return nil
}

func (f *fakeConsumer) Close() error { return nil }

// push 模拟适配器消费循环对单消息的分派（同步阻塞至桥接 handle 返回）。
func (f *fakeConsumer) push(ctx context.Context, msg *mq.Message) error {
	f.mu.Lock()
	handler := f.handler
	f.mu.Unlock()
	if handler == nil {
		return errors.New("fakeConsumer: 未订阅")
	}
	err := mq.InvokeHandler(ctx, handler, msg)
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case err == nil:
		f.commits = append(f.commits, msg.ID)
	case errors.Is(err, mq.ErrManualAck):
		f.inflight[msg.ID] = true
	default:
		f.fails = append(f.fails, msg.ID)
	}
	return err
}

func (f *fakeConsumer) snapshot() (commits, fails int, inflight map[string]bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make(map[string]bool, len(f.inflight))
	for k, v := range f.inflight {
		cp[k] = v
	}
	return len(f.commits), len(f.fails), cp
}

// fakeProducer 记录回投消息，可配置固定错误。
type fakeProducer struct {
	mu       sync.Mutex
	produced []producedItem
	err      error
}

type producedItem struct {
	topic string
	msg   *mq.Message
}

func (p *fakeProducer) Produce(_ context.Context, topic string, msg *mq.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.produced = append(p.produced, producedItem{topic: topic, msg: msg})
	return nil
}

func (p *fakeProducer) ProduceBatch(ctx context.Context, topic string, msgs []*mq.Message) error {
	for _, m := range msgs {
		if err := p.Produce(ctx, topic, m); err != nil {
			return err
		}
	}
	return nil
}

func (p *fakeProducer) Close() error { return nil }

func (p *fakeProducer) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.produced)
}

// waitFor 轮询等待条件成立（2s 上限）。
func waitFor(t *testing.T, cond func() bool, msgAndArgs ...any) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待超时: %v", msgAndArgs)
}

// ==================== 装配校验 ====================

func TestNewValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("nil Consumer 应报错")
	}
	if _, err := New(Config{Consumer: newFakeConsumer()}); err == nil {
		t.Fatal("空 Group/Topic 应报错")
	}
	if _, err := New(Config{
		Consumer: newFakeConsumer(), Group: "g", Topic: "t", DurableRetry: true,
	}); err == nil {
		t.Fatal("DurableRetry 缺 Producer 应报错")
	}
	if _, err := New(Config{Consumer: newFakeConsumer(), Group: "g", Topic: "t"}); err != nil {
		t.Fatalf("合法配置不应报错: %v", err)
	}
}

// ==================== 交接与确认模式 ====================

func TestAutoAckHandoff(t *testing.T) {
	fc := newFakeConsumer()
	q, err := New(Config{Consumer: fc, Group: "g", Topic: "t", BufferSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	<-fc.started

	msg := &mq.Message{ID: "m1", Topic: "t"}
	go func() { _ = fc.push(ctx, msg) }()

	got, ok := q.Receive(ctx)
	if !ok || got != msg {
		t.Fatalf("Receive 应取回交接消息: ok=%v got=%v", ok, got)
	}
	waitFor(t, func() bool {
		commits, _, _ := fc.snapshot()
		return commits == 1
	}, "自动确认模式应在交接成功后 Commit")
	q.Stop()
}

func TestManualAckDefersCommit(t *testing.T) {
	fc := newFakeConsumer()
	q, err := New(Config{Consumer: fc, Group: "g", Topic: "t", BufferSize: 4, ManualAck: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	<-fc.started

	msg := &mq.Message{ID: "m1", Topic: "t"}
	go func() { _ = fc.push(ctx, msg) }()

	if _, ok := q.Receive(ctx); !ok {
		t.Fatal("Receive 失败")
	}
	waitFor(t, func() bool {
		commits, _, inflight := fc.snapshot()
		return commits == 0 && inflight["m1"]
	}, "ManualAck 模式交接后应挂起确认")

	if err := q.Commit(ctx, msg); err != nil {
		t.Fatalf("Commit 失败: %v", err)
	}
	waitFor(t, func() bool {
		commits, _, inflight := fc.snapshot()
		return commits == 1 && !inflight["m1"]
	}, "显式 Commit 应转达底层消费者")

	// 未知消息确认应报错而非静默
	if err := q.Commit(ctx, &mq.Message{ID: "unknown"}); !errors.Is(err, mq.ErrNotSubscribed) {
		t.Fatalf("未知消息 Commit 应返回 ErrNotSubscribed, got %v", err)
	}
	q.Stop()
}

func TestStopUnblocksPendingHandle(t *testing.T) {
	fc := newFakeConsumer()
	q, err := New(Config{Consumer: fc, Group: "g", Topic: "t", BufferSize: 1, ManualAck: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	<-fc.started

	// 先占满缓冲（无人领取），再推送：handle 阻塞在通道写入
	if err := q.Submit(&mq.Message{ID: "filler"}, 0); err != nil {
		t.Fatal(err)
	}
	pushErr := make(chan error, 1)
	blocked := &mq.Message{ID: "blocked"}
	go func() { pushErr <- fc.push(ctx, blocked) }()

	time.Sleep(50 * time.Millisecond) // 让 push 阻塞在通道写入
	q.Stop()

	select {
	case err := <-pushErr:
		if !errors.Is(err, mq.ErrManualAck) {
			t.Fatalf("停机时 handle 应返回 ErrManualAck 保持在途, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop 后阻塞交接未退出")
	}
	if commits, fails, _ := fc.snapshot(); commits != 0 || fails != 0 {
		t.Fatalf("停机消息不得被确认或判失败: commits=%d fails=%d", commits, fails)
	}
	if err := q.Submit(blocked, 0); !errors.Is(err, ErrQueueStopped) {
		t.Fatalf("停止后 Submit 应返回 ErrQueueStopped, got %v", err)
	}
}

// ==================== Submit / Receive ====================

func TestReceiveCtxCancelReturnsFalse(t *testing.T) {
	fc := newFakeConsumer()
	q, err := New(Config{Consumer: fc, Group: "g", Topic: "t"})
	if err != nil {
		t.Fatal(err)
	}
	rctx, rcancel := context.WithCancel(context.Background())
	rcancel()
	if msg, ok := q.Receive(rctx); ok || msg != nil {
		t.Fatal("ctx 结束时 Receive 应返回 ok=false 且未领取消息")
	}
	_ = q.Start(context.Background())
	q.Stop()
}

func TestSubmitLocalImmediateAndDelayed(t *testing.T) {
	fc := newFakeConsumer()
	q, err := New(Config{Consumer: fc, Group: "g", Topic: "t", BufferSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := q.Submit(&mq.Message{ID: "now"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := q.Submit(&mq.Message{ID: "later"}, 60*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	msg, ok := q.Receive(ctx)
	if !ok || msg.ID != "now" {
		t.Fatalf("delay=0 应立即可领取: %#v ok=%v", msg, ok)
	}

	// 延迟消息在到期前不可领取（短暂探测），到期后可领取
	probeCtx, probeCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer probeCancel()
	if _, ok := q.Receive(probeCtx); ok {
		t.Fatal("延迟消息不应提前可领取")
	}
	recvCtx, recvCancel := context.WithTimeout(ctx, 2*time.Second)
	defer recvCancel()
	msg, ok = q.Receive(recvCtx)
	if !ok || msg.ID != "later" {
		t.Fatalf("延迟消息到期后应可领取: %#v ok=%v", msg, ok)
	}
	q.Stop()
}

func TestSubmitNonBlockingWhenBufferFull(t *testing.T) {
	fc := newFakeConsumer()
	q, err := New(Config{Consumer: fc, Group: "g", Topic: "t", BufferSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Submit(&mq.Message{ID: "a"}, 0); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- q.Submit(&mq.Message{ID: "b"}, 0) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("缓冲满时 Submit 应转定时回灌而非报错: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("缓冲满时 Submit 不得阻塞（引擎契约）")
	}

	ctx := context.Background()
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		msg, ok := q.Receive(recvCtx)
		cancel()
		if !ok {
			t.Fatalf("第 %d 条回灌消息未领取到", i)
		}
		seen[msg.ID] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("两条消息均应回灌: %#v", seen)
	}
	q.Stop()
}

func TestDurableRetrySubmit(t *testing.T) {
	fc := newFakeConsumer()
	fp := &fakeProducer{}
	q, err := New(Config{
		Consumer: fc, Group: "g", Topic: "fallback-topic",
		Producer: fp, DurableRetry: true, ProduceTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	// delay=0 同步回投；msg.Topic 缺省回落配置 Topic
	if err := q.Submit(&mq.Message{ID: "m1"}, 0); err != nil {
		t.Fatalf("同步回投失败: %v", err)
	}
	if fp.count() != 1 {
		t.Fatalf("应回投 1 条, got %d", fp.count())
	}
	// delay>0 异步回投，Submit 立即返回
	if err := q.Submit(&mq.Message{ID: "m2", Topic: "explicit"}, 30*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return fp.count() == 2 }, "延迟持久回投未执行")
	q.Stop()

	// 生产者失败：delay=0 时如实返回 error 交引擎兜底
	fpErr := &fakeProducer{err: errors.New("mq down")}
	q2, err := New(Config{
		Consumer: fc, Group: "g", Topic: "t",
		Producer: fpErr, DurableRetry: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := q2.Submit(&mq.Message{ID: "m3"}, 0); err == nil {
		t.Fatal("回投失败应透传 error")
	}
	q2.Stop()
}

// ==================== 与真实 pipeline 引擎端到端 ====================

type e2eCtx struct {
	xmpipeline.ExecContext
}

type countStep struct {
	name   string
	hits   *atomic.Int64
	failUp int64 // 前 failUp-1 次执行失败，之后成功（0 表示始终成功）
}

func (s *countStep) Name() string { return s.name }

func (s *countStep) Execute(ctx *e2eCtx) error {
	n := s.hits.Add(1)
	if s.failUp > 0 && n < s.failUp {
		return errors.New("step transient failure")
	}
	return nil
}

func TestEngineEndToEndWithRetryAndCommit(t *testing.T) {
	fc := newFakeConsumer()
	q, err := New(Config{Consumer: fc, Group: "g", Topic: "t", BufferSize: 2, ManualAck: true})
	if err != nil {
		t.Fatal(err)
	}

	var hits, successes atomic.Int64
	step := &countStep{name: "do", hits: &hits, failUp: 2} // 首次失败，重投后成功

	cfg := xmpipeline.Config[*mq.Message, *e2eCtx]{
		Queue:   q,
		Workers: 1,
		Resolve: func(msg *mq.Message) ([]xmpipeline.Step[*e2eCtx], *e2eCtx, error) {
			return []xmpipeline.Step[*e2eCtx]{step}, &e2eCtx{}, nil
		},
		OnSuccess: func(msg *mq.Message, ctx *e2eCtx) {
			successes.Add(1)
			_ = q.Commit(context.Background(), msg) // 终态确认：MQ 门面逐消息 ack
		},
		OnExhausted: func(msg *mq.Message, failedStep string, err error) {
			_ = q.Reject(context.Background(), msg, false) // 耗尽终态：不重投
		},
		Retryer: &xmpipeline.PolicyRetryer[*mq.Message]{
			Policy:  &xmpipeline.RetryPolicy{MaxAttempt: 3, BaseDelay: 10 * time.Millisecond},
			Attempt: func(msg *mq.Message) int { return int(hits.Load()) - 1 },
			Next:    func(msg *mq.Message) *mq.Message { return msg },
		},
	}
	eng, err := xmpipeline.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	<-fc.started
	if err := eng.Start(); err != nil {
		t.Fatal(err)
	}

	msg := &mq.Message{ID: "order-1", Topic: "t", Body: []byte("payload")}
	go func() { _ = fc.push(ctx, msg) }()

	waitFor(t, func() bool { return hits.Load() == 2 }, "重投链路未闭环：步骤应执行两次")
	waitFor(t, func() bool { return successes.Load() == 1 }, "OnSuccess 未触发")
	waitFor(t, func() bool {
		commits, _, _ := fc.snapshot()
		return commits == 1
	}, "终态未确认到 MQ 门面")

	eng.Stop()
	q.Stop()
}
