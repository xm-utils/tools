package zset

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"

	"github.com/xm-utils/tools/mq/core"
)

// newTestClient 启动内存 Redis 并返回配套 client。
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

// mustStartSubscribe 启动订阅并断言未因参数/登记校验立即失败。
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

func TestProduceConsumeRoundTrip(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	producer := NewProducer(client, nil)
	consumer := NewConsumer(client, fastOptions())
	defer func() { _ = consumer.Close() }()

	var mu sync.Mutex
	var bodies []string
	handler := func(ctx context.Context, msg *mq.Message) error {
		mu.Lock()
		bodies = append(bodies, string(msg.Body))
		mu.Unlock()
		return nil
	}
	mustStartSubscribe(t, consumer, ctx, "g1", "evt", handler)

	// 相同 Body 两条：依赖持久 ID 保证成员唯一，不被 ZSET 去重合并
	m1 := &mq.Message{Body: []byte("same")}
	m2 := &mq.Message{Body: []byte("same")}
	if err := producer.Produce(ctx, "evt", m1); err != nil {
		t.Fatalf("Produce 失败: %v", err)
	}
	if err := producer.ProduceBatch(ctx, "evt", []*mq.Message{m2}); err != nil {
		t.Fatalf("ProduceBatch 失败: %v", err)
	}
	if m1.ID == "" || m1.ID == m2.ID {
		t.Fatalf("Produce 应回填互不相同的持久 ID: %q / %q", m1.ID, m2.ID)
	}

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(bodies) == 2
	}, "相同内容两条消息均应投递（成员不被去重）")

	waitFor(t, time.Second, func() bool {
		return client.ZCard(ctx, "mq:zset:evt").Val() == 0 &&
			client.ZCard(ctx, "mq:zset:evt:processing").Val() == 0
	}, "自动确认后集合应清空")
}

func TestDelayedMessage(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	producer := NewProducer(client, nil)
	consumer := NewConsumer(client, fastOptions())
	defer func() { _ = consumer.Close() }()

	var mu sync.Mutex
	delivered := 0
	handler := func(ctx context.Context, msg *mq.Message) error {
		mu.Lock()
		delivered++
		mu.Unlock()
		return nil
	}
	mustStartSubscribe(t, consumer, ctx, "g1", "delayed", handler)

	msg := &mq.Message{Body: []byte("later")}
	msg.SetHeader(HeaderDelay, "300")
	if err := producer.Produce(ctx, "delayed", msg); err != nil {
		t.Fatalf("Produce 失败: %v", err)
	}

	time.Sleep(120 * time.Millisecond)
	mu.Lock()
	early := delivered
	mu.Unlock()
	if early != 0 {
		t.Fatalf("延迟窗口内不应投递, got %d 条", early)
	}

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return delivered == 1
	}, "延迟到期后应投递")
}

func TestFailureRequeueThenDeadLetter(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	producer := NewProducer(client, nil)

	errBoom := errors.New("boom")
	var mu sync.Mutex
	var attempts []string
	var terminalMsg *mq.Message
	var terminalErr error
	opts := fastOptions()
	opts.MaxRequeue = 1
	opts.DeadLetterKey = "mq:zset:jobs:dlq"
	opts.Rejected = func(ctx context.Context, msg *mq.Message, err error) {
		mu.Lock()
		terminalMsg, terminalErr = msg, err
		mu.Unlock()
	}
	consumer := NewConsumer(client, opts)
	defer func() { _ = consumer.Close() }()

	handler := func(ctx context.Context, msg *mq.Message) error {
		mu.Lock()
		attempts = append(attempts, msg.Header(HeaderAttempt))
		mu.Unlock()
		return errBoom
	}
	mustStartSubscribe(t, consumer, ctx, "g1", "jobs", handler)
	if err := producer.Produce(ctx, "jobs", &mq.Message{Body: []byte("payload")}); err != nil {
		t.Fatalf("Produce 失败: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return terminalMsg != nil
	}, "应进入终态")

	// 第一次投递 attempt 头为空，重投后为 "1"
	mu.Lock()
	got := append([]string(nil), attempts...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "" || got[1] != "1" {
		t.Fatalf("重投计数错误: %q", got)
	}
	if !errors.Is(terminalErr, errBoom) {
		t.Fatalf("终态错误应包含原始失败原因, got %v", terminalErr)
	}
	if client.ZCard(ctx, "mq:zset:jobs:dlq").Val() != 1 {
		t.Fatalf("死信集合应有 1 条, got %d", client.ZCard(ctx, "mq:zset:jobs:dlq").Val())
	}
	if client.ZCard(ctx, "mq:zset:jobs:processing").Val() != 0 {
		t.Fatal("终态后 processing 应清空")
	}
}

func TestManualAckCommitAndReclaim(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	producer := NewProducer(client, nil)

	var mu sync.Mutex
	handled := 0
	var lastMsg *mq.Message
	handler := func(ctx context.Context, msg *mq.Message) error {
		mu.Lock()
		handled++
		lastMsg = msg
		mu.Unlock()
		return mq.ErrManualAck
	}

	opts := fastOptions()
	opts.VisibilityTimeout = 100 * time.Millisecond
	consumer := NewConsumer(client, opts)
	defer func() { _ = consumer.Close() }()

	mustStartSubscribe(t, consumer, ctx, "g1", "manual", handler)
	msg := &mq.Message{Body: []byte("task")}
	if err := producer.Produce(ctx, "manual", msg); err != nil {
		t.Fatalf("Produce 失败: %v", err)
	}

	// 可见性回收超时后消息应被重新领取投递（at-least-once）
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return handled >= 2
	}, "超时未确认应触发可见性回收重投")

	mu.Lock()
	target := lastMsg
	mu.Unlock()
	if target.ID != msg.ID {
		t.Fatalf("重投消息应保留持久 ID: want %q, got %q", msg.ID, target.ID)
	}
	if err := consumer.Commit(ctx, target); err != nil {
		t.Fatalf("Commit 失败: %v", err)
	}
	if err := consumer.Commit(ctx, &mq.Message{ID: "no-such"}); !errors.Is(err, mq.ErrNotSubscribed) {
		t.Fatalf("未知消息 Commit 应返回 ErrNotSubscribed, got %v", err)
	}

	waitFor(t, 2*time.Second, func() bool {
		return client.ZCard(ctx, "mq:zset:manual").Val() == 0 &&
			client.ZCard(ctx, "mq:zset:manual:processing").Val() == 0
	}, "确认后两个集合应清空（回收重投的副本也已消费）")
}

func TestPoisonAndCloseSemantics(t *testing.T) {
	client, mr := newTestClient(t)
	ctx := context.Background()

	var mu sync.Mutex
	var poisoned *mq.Message
	opts := fastOptions()
	opts.Rejected = func(ctx context.Context, msg *mq.Message, err error) {
		mu.Lock()
		poisoned = msg
		mu.Unlock()
	}
	consumer := NewConsumer(client, opts)

	mr.ZAdd("mq:zset:raw", 0, "not-a-json-envelope")
	noCall := func(ctx context.Context, msg *mq.Message) error {
		t.Errorf("毒消息不应进入 Handler")
		return nil
	}
	errCh := startSubscribe(consumer, ctx, "g1", "raw", noCall)
	waitFor(t, time.Second, func() bool {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		return len(consumer.subs) == 1
	}, "订阅登记应完成")
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return poisoned != nil
	}, "毒消息应进终态")
	if poisoned.Topic != "raw" {
		t.Fatalf("毒消息 Topic 应为 topic 名, got %q", poisoned.Topic)
	}

	noop := func(ctx context.Context, msg *mq.Message) error { return nil }
	if err := consumer.Subscribe(ctx, "g1", "raw", noop); err == nil {
		t.Fatal("重复订阅应返回 error")
	}
	if err := consumer.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
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
	if err := consumer.Subscribe(ctx, "g2", "raw", noop); !errors.Is(err, mq.ErrConsumerClosed) {
		t.Fatalf("Close 后 Subscribe 应返回 ErrConsumerClosed, got %v", err)
	}
}
