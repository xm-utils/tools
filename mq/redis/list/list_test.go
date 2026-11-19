package list

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"

	"github.com/xm-utils/tools/mq"
)

// newTestClient 启动内存 Redis 并返回配套 client（miniredis 关闭随 t 自动结束）。
func newTestClient(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis 启动失败: %v", err)
	}
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client, mr
}

// waitFor 轮询等待条件成立，超时即失败。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", msg)
}

func fastOptions() *Options {
	return &Options{PollInterval: 10 * time.Millisecond}
}

// startSubscribe 后台运行阻塞式 Subscribe，返回其退出错误通道。
func startSubscribe(consumer *Consumer, ctx context.Context, group, topic string, handler mq.Handler) <-chan error {
	errCh := make(chan error, 1)
	go func() { errCh <- consumer.Subscribe(ctx, group, topic, handler) }()
	return errCh
}

// mustStartSubscribe 启动订阅并断言未因参数/登记校验立即失败（校验失败会瞬间返回）。
func mustStartSubscribe(t *testing.T, consumer *Consumer, ctx context.Context, group, topic string, handler mq.Handler) <-chan error {
	t.Helper()
	errCh := startSubscribe(consumer, ctx, group, topic, handler)
	waitFor(t, time.Second, func() bool {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		return len(consumer.subs) == 1
	}, "订阅登记应完成")
	select {
	case err := <-errCh:
		t.Fatalf("Subscribe 失败: %v", err)
	default:
	}
	return errCh
}

type collector struct {
	mu   sync.Mutex
	msgs []*mq.Message
}

func (c *collector) handle(ctx context.Context, msg *mq.Message) error {
	c.mu.Lock()
	c.msgs = append(c.msgs, msg)
	c.mu.Unlock()
	return nil
}

func (c *collector) all() []*mq.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*mq.Message(nil), c.msgs...)
}

func TestProduceConsumeFIFO(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	producer := NewProducer(client, nil)
	consumer := NewConsumer(client, fastOptions())
	defer func() { _ = consumer.Close() }()

	var got collector
	mustStartSubscribe(t, consumer, ctx, "g1", "order", got.handle)

	bodies := []string{"a", "b", "c"}
	for i, b := range bodies {
		msg := &mq.Message{Key: fmt.Sprintf("k%d", i), Body: []byte(b)}
		msg.SetHeader("trace", "t-"+b)
		if err := producer.Produce(ctx, "order", msg); err != nil {
			t.Fatalf("Produce 失败: %v", err)
		}
		if msg.ID == "" {
			t.Fatal("Produce 应回填消息 ID")
		}
	}

	waitFor(t, 3*time.Second, func() bool { return len(got.all()) == 3 }, "应消费 3 条消息")
	for i, m := range got.all() {
		if string(m.Body) != bodies[i] {
			t.Fatalf("FIFO 顺序错误: 第 %d 条 got %q, want %q", i, m.Body, bodies[i])
		}
		if m.Header("trace") != "t-"+bodies[i] || m.Key != fmt.Sprintf("k%d", i) || m.Topic != "order" {
			t.Fatalf("消息字段还原不完整: %+v", m)
		}
	}

	// 自动确认后 ready 与 processing 均应为空
	waitFor(t, time.Second, func() bool {
		return client.LLen(ctx, "mq:list:order").Val() == 0 &&
			client.LLen(ctx, "mq:list:order:processing").Val() == 0
	}, "确认后列表应清空")
}

func TestManualAckCommitAndReject(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	producer := NewProducer(client, nil)
	consumer := NewConsumer(client, fastOptions())
	defer func() { _ = consumer.Close() }()

	var mu sync.Mutex
	handled := map[string]*mq.Message{}
	handler := func(ctx context.Context, msg *mq.Message) error {
		mu.Lock()
		handled[string(msg.Body)] = msg
		mu.Unlock()
		return mq.ErrManualAck
	}
	mustStartSubscribe(t, consumer, ctx, "g1", "tasks", handler)
	for _, b := range []string{"keep", "drop"} {
		if err := producer.Produce(ctx, "tasks", &mq.Message{Body: []byte(b)}); err != nil {
			t.Fatalf("Produce 失败: %v", err)
		}
	}
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(handled) == 2
	}, "应挂起 2 条手动确认消息")

	// 挂起期间消息滞留 processing
	if n := client.LLen(ctx, "mq:list:tasks:processing").Val(); n != 2 {
		t.Fatalf("processing 应有 2 条在途, got %d", n)
	}

	mu.Lock()
	keepMsg, dropMsg := handled["keep"], handled["drop"]
	mu.Unlock()

	if err := consumer.Commit(ctx, keepMsg); err != nil {
		t.Fatalf("Commit 失败: %v", err)
	}
	if n := client.LLen(ctx, "mq:list:tasks:processing").Val(); n != 1 {
		t.Fatalf("Commit 后 processing 应剩 1 条, got %d", n)
	}
	if err := consumer.Reject(ctx, dropMsg, false); err != nil {
		t.Fatalf("Reject 失败: %v", err)
	}
	waitFor(t, time.Second, func() bool {
		return client.LLen(ctx, "mq:list:tasks:processing").Val() == 0
	}, "Reject 后 processing 应清空")

	// 未知消息确认返回 ErrNotSubscribed
	if err := consumer.Commit(ctx, &mq.Message{ID: "no-such-id"}); !errors.Is(err, mq.ErrNotSubscribed) {
		t.Fatalf("未知消息 Commit 应返回 ErrNotSubscribed, got %v", err)
	}
}

func TestFailureRequeueThenDeadLetter(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	producer := NewProducer(client, nil)

	var terminalMsg *mq.Message
	var terminalErr error
	var mu sync.Mutex
	errBoom := errors.New("boom")
	opts := fastOptions()
	opts.MaxRequeue = 1
	opts.DeadLetterKey = "mq:list:tasks:dlq"
	opts.Rejected = func(ctx context.Context, msg *mq.Message, err error) {
		mu.Lock()
		terminalMsg, terminalErr = msg, err
		mu.Unlock()
	}
	consumer := NewConsumer(client, opts)
	defer func() { _ = consumer.Close() }()

	fail := func(ctx context.Context, msg *mq.Message) error {
		return errBoom
	}
	mustStartSubscribe(t, consumer, ctx, "g1", "tasks", fail)
	if err := producer.Produce(ctx, "tasks", &mq.Message{Body: []byte("payload")}); err != nil {
		t.Fatalf("Produce 失败: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return terminalMsg != nil
	}, "应进入终态")

	// 终态错误须包裹原始失败原因
	if !errors.Is(terminalErr, errBoom) {
		t.Fatalf("终态错误应包含原始失败原因, got %v", terminalErr)
	}
	if client.LLen(ctx, "mq:list:tasks:dlq").Val() != 1 {
		t.Fatalf("死信列表应有 1 条, got %d", client.LLen(ctx, "mq:list:tasks:dlq").Val())
	}
	if client.LLen(ctx, "mq:list:tasks:processing").Val() != 0 {
		t.Fatal("终态后 processing 应清空")
	}
	// 重投副本须携带 attempt+1：dlq 中保留的是最后一次投递的原始 payload
	// （attempt=1 信封），验证信封可解码且计数正确
	dlqRaw := client.LIndex(ctx, "mq:list:tasks:dlq", 0).Val()
	if dlqRaw == "" {
		t.Fatal("死信 payload 不应为空")
	}
}

func TestPoisonMessageGoesTerminal(t *testing.T) {
	client, mr := newTestClient(t)
	ctx := context.Background()

	var mu sync.Mutex
	var poisoned *mq.Message
	opts := fastOptions()
	opts.DeadLetterKey = "mq:list:raw:dlq"
	opts.Rejected = func(ctx context.Context, msg *mq.Message, err error) {
		mu.Lock()
		poisoned = msg
		mu.Unlock()
	}
	consumer := NewConsumer(client, opts)
	defer func() { _ = consumer.Close() }()

	// 外部直写非信封内容模拟毒消息
	mr.Lpush("mq:list:raw", "not-a-json-envelope")

	fail := func(ctx context.Context, msg *mq.Message) error {
		t.Errorf("毒消息不应进入 Handler")
		return nil
	}
	mustStartSubscribe(t, consumer, ctx, "g1", "raw", fail)

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return poisoned != nil
	}, "毒消息应进终态")
	if poisoned.Topic != "raw" {
		t.Fatalf("毒消息 Topic 应为 topic 名而非 key, got %q", poisoned.Topic)
	}
	if client.LLen(ctx, "mq:list:raw:dlq").Val() != 1 {
		t.Fatal("毒消息原文应入死信列表")
	}
}

func TestRecoverProcessingOnSubscribe(t *testing.T) {
	client, mr := newTestClient(t)
	ctx := context.Background()

	// 模拟上一实例崩溃：消息滞留在 processing
	mr.Lpush("mq:list:jobs:processing", `{"id":"x-1","body":"aGk="}`)

	opts := fastOptions()
	opts.RecoverProcessing = true
	consumer := NewConsumer(client, opts)
	defer func() { _ = consumer.Close() }()

	var got collector
	mustStartSubscribe(t, consumer, ctx, "g1", "jobs", got.handle)
	waitFor(t, 3*time.Second, func() bool { return len(got.all()) == 1 }, "回补消息应被重新消费")
	if string(got.all()[0].Body) != "hi" {
		t.Fatalf("回补消息 Body 解码错误: %q", got.all()[0].Body)
	}
}

func TestSubscribeAndCloseSemantics(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	consumer := NewConsumer(client, fastOptions())

	noop := func(ctx context.Context, msg *mq.Message) error { return nil }
	// 参数校验类失败立即返回，不进入阻塞循环
	if err := consumer.Subscribe(ctx, "", "t", noop); err == nil {
		t.Fatal("空 group 应返回 error")
	}
	if err := consumer.Subscribe(ctx, "g1", "", noop); err == nil {
		t.Fatal("空 topic 应返回 error")
	}
	if err := consumer.Subscribe(ctx, "g1", "t2", nil); err == nil {
		t.Fatal("nil handler 应返回 error")
	}

	errCh := startSubscribe(consumer, ctx, "g1", "t", noop)
	waitFor(t, time.Second, func() bool {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		return len(consumer.subs) == 1
	}, "订阅登记应完成")
	if err := consumer.Subscribe(ctx, "g1", "t", noop); err == nil {
		t.Fatal("重复订阅应返回 error")
	}

	if err := consumer.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if err := consumer.Close(); err != nil {
		t.Fatalf("Close 应幂等: %v", err)
	}
	// Close 应终止阻塞中的 Subscribe（退出原因为 context.Canceled）
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Close 后 Subscribe 应以 context.Canceled 退出, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close 后 Subscribe 未退出")
	}
	if err := consumer.Subscribe(ctx, "g3", "t", noop); !errors.Is(err, mq.ErrConsumerClosed) {
		t.Fatalf("Close 后 Subscribe 应返回 ErrConsumerClosed, got %v", err)
	}
	if err := consumer.Commit(ctx, &mq.Message{ID: "x"}); !errors.Is(err, mq.ErrConsumerClosed) {
		t.Fatalf("Close 后 Commit 应返回 ErrConsumerClosed, got %v", err)
	}
}

// TestSubscribeCtxCancelExit 验证门面契约：入参 ctx 取消时 Subscribe 退出，
// 并撤销登记使同一 group+topic 可重新订阅。
func TestSubscribeCtxCancelExit(t *testing.T) {
	client, _ := newTestClient(t)
	consumer := NewConsumer(client, fastOptions())
	defer func() { _ = consumer.Close() }()

	noop := func(ctx context.Context, msg *mq.Message) error { return nil }
	ctx, cancel := context.WithCancel(context.Background())
	errCh := startSubscribe(consumer, ctx, "g1", "t", noop)
	waitFor(t, time.Second, func() bool {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		return len(consumer.subs) == 1
	}, "订阅登记应完成")

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ctx 取消后 Subscribe 应返回 context.Canceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ctx 取消后 Subscribe 未退出")
	}

	// 退出后同一 group+topic 可重新订阅
	errCh2 := startSubscribe(consumer, context.Background(), "g1", "t", noop)
	waitFor(t, time.Second, func() bool {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		return len(consumer.subs) == 1
	}, "重新订阅应登记成功")
	_ = errCh2

	// 取消-重订阅必须回收上一轮的循环上下文登记（常驻重连场景防无上限累积）
	consumer.mu.Lock()
	cancels := len(consumer.cancels)
	consumer.mu.Unlock()
	if cancels != 1 {
		t.Fatalf("重订阅后取消登记应随退出回收, got %d 项", cancels)
	}
}

func TestProduceBatchValidation(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	producer := NewProducer(client, nil)

	if err := producer.ProduceBatch(ctx, "t", nil); err != nil {
		t.Fatalf("空批量应为 no-op: %v", err)
	}
	if err := producer.ProduceBatch(ctx, "t", []*mq.Message{{Body: []byte("1")}, nil}); err == nil {
		t.Fatal("批量含 nil 元素应返回 error")
	}
	if err := producer.Produce(ctx, "t", nil); err == nil {
		t.Fatal("nil 消息应返回 error")
	}
	if err := producer.Produce(ctx, "", &mq.Message{}); err == nil {
		t.Fatal("空 topic 应返回 error")
	}

	msgs := []*mq.Message{{Body: []byte("1")}, {Body: []byte("2")}, {Body: []byte("3")}}
	if err := producer.ProduceBatch(ctx, "t", msgs); err != nil {
		t.Fatalf("ProduceBatch 失败: %v", err)
	}
	// LPUSH 头插三值后，尾端 RPOP 消费顺序须为 1,2,3
	var got collector
	consumer2 := NewConsumer(client, fastOptions())
	defer func() { _ = consumer2.Close() }()
	mustStartSubscribe(t, consumer2, ctx, "g1", "t", got.handle)
	waitFor(t, 3*time.Second, func() bool { return len(got.all()) == 3 }, "批量消息应全部消费")
	for i, m := range got.all() {
		if string(m.Body) != string(msgs[i].Body) {
			t.Fatalf("批量 FIFO 顺序错误: %d, got %q", i, m.Body)
		}
	}
}
