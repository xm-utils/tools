package pubsub

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

// startSubscribe 后台运行阻塞式 Subscribe，返回其退出错误通道。
func startSubscribe(consumer *Consumer, ctx context.Context, group, topic string, handler mq.Handler) <-chan error {
	errCh := make(chan error, 1)
	go func() { errCh <- consumer.Subscribe(ctx, group, topic, handler) }()
	return errCh
}

// waitSubscribed 等待频道在服务端注册完成（模式订阅以 NumPat 计数判定）——广播语义下
// 发布早于订阅的消息会丢失，测试必须显式建立「订阅已就绪」的顺序。
func waitSubscribed(t *testing.T, client *redis.Client, name string, pattern bool) {
	t.Helper()
	waitFor(t, 3*time.Second, func() bool {
		ctx := context.Background()
		if pattern {
			return client.PubSubNumPat(ctx).Val() > 0
		}
		for _, c := range client.PubSubChannels(ctx, name).Val() {
			if c == name {
				return true
			}
		}
		return false
	}, "订阅尚未在服务端注册: "+name)
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

func TestProduceConsumeRoundTrip(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	producer := NewProducer(client, nil)
	consumer := NewConsumer(client, nil)
	defer func() { _ = consumer.Close() }()

	var got collector
	errCh := startSubscribe(consumer, ctx, "g1", "notice", got.handle)
	waitSubscribed(t, client, "mq:ps:notice", false)
	_ = errCh

	messages := []*mq.Message{
		{Key: "k1", Body: []byte("first")},
		{Key: "k2", Body: []byte("second")},
	}
	for _, m := range messages {
		m.SetHeader("trace", "t-"+string(m.Body))
	}
	if err := producer.Produce(ctx, "notice", messages[0]); err != nil {
		t.Fatalf("Produce 失败: %v", err)
	}
	if err := producer.ProduceBatch(ctx, "notice", messages[1:]); err != nil {
		t.Fatalf("ProduceBatch 失败: %v", err)
	}
	if messages[0].ID == "" || messages[1].ID == "" {
		t.Fatal("Produce 应回填消息 ID")
	}

	waitFor(t, 3*time.Second, func() bool { return len(got.all()) == 2 }, "两条广播消息均应收到")
	for _, m := range got.all() {
		if m.Topic != "notice" || m.Header("trace") == "" {
			t.Fatalf("消息字段还原不完整: %+v", m)
		}
	}

	// 广播无确认通道：Commit/Reject 均为 no-op，且对 nil / 未知消息不报错、不 panic
	if err := consumer.Commit(ctx, got.all()[0]); err != nil {
		t.Fatalf("Commit 应为 no-op: %v", err)
	}
	if err := consumer.Commit(ctx, nil); err != nil {
		t.Fatalf("Commit(nil) 应为 no-op: %v", err)
	}
	if err := consumer.Reject(ctx, &mq.Message{ID: "no-such"}, true); err != nil {
		t.Fatalf("Reject 应为 no-op: %v", err)
	}
}

func TestBroadcastReachesEverySubscriber(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	producer := NewProducer(client, nil)

	var a, b collector
	ca := NewConsumer(client, &Options{ConsumerName: "a"})
	cb := NewConsumer(client, &Options{ConsumerName: "b"})
	defer func() { _ = ca.Close(); _ = cb.Close() }()

	startSubscribe(ca, ctx, "g1", "sync", a.handle)
	startSubscribe(cb, ctx, "g1", "sync", b.handle)
	waitSubscribed(t, client, "mq:ps:sync", false)

	if err := producer.Produce(ctx, "sync", &mq.Message{Body: []byte("invalidate")}); err != nil {
		t.Fatalf("Produce 失败: %v", err)
	}

	// 与队列型后端的关键差异：同一消息被每个在线实例各投递一次（不分流）
	waitFor(t, 3*time.Second, func() bool {
		return len(a.all()) == 1 && len(b.all()) == 1
	}, "两个订阅者均应各收到一条")
	if a.all()[0].ID != b.all()[0].ID {
		t.Fatalf("同一条广播消息 ID 应一致: %q / %q", a.all()[0].ID, b.all()[0].ID)
	}
}

func TestPatternSubscribe(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	producer := NewProducer(client, nil)
	consumer := NewConsumer(client, &Options{Pattern: true})
	defer func() { _ = consumer.Close() }()

	var got collector
	startSubscribe(consumer, ctx, "g1", "evt.*", got.handle)
	waitSubscribed(t, client, "mq:ps:evt.*", true)

	for _, name := range []string{"evt.created", "evt.paid"} {
		if err := producer.Produce(ctx, name, &mq.Message{Body: []byte(name)}); err != nil {
			t.Fatalf("Produce 失败: %v", err)
		}
	}
	// 不匹配模式的频道不应命中
	if err := producer.Produce(ctx, "other.channel", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Produce 失败: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool { return len(got.all()) == 2 }, "通配订阅应收到 2 条")
	for _, m := range got.all() {
		// 通配模式下 Topic 还原为实际命中频道名（去前缀）
		if m.Topic != "evt.created" && m.Topic != "evt.paid" {
			t.Fatalf("通配订阅 topic 还原错误: %q", m.Topic)
		}
		if string(m.Body) != m.Topic {
			t.Fatalf("通配订阅消息内容错位: got %q, want %q", m.Body, m.Topic)
		}
	}
}

func TestPoisonMessageInvokesRejected(t *testing.T) {
	client, mr := newTestClient(t)
	ctx := context.Background()

	var mu sync.Mutex
	var poisoned *mq.Message
	var handled int
	opts := &Options{Rejected: func(ctx context.Context, msg *mq.Message, err error) {
		mu.Lock()
		poisoned = msg
		mu.Unlock()
	}}
	consumer := NewConsumer(client, opts)
	defer func() { _ = consumer.Close() }()

	startSubscribe(consumer, ctx, "g1", "raw", func(ctx context.Context, msg *mq.Message) error {
		mu.Lock()
		handled++
		mu.Unlock()
		return nil
	})
	waitSubscribed(t, client, "mq:ps:raw", false)

	mr.Publish("mq:ps:raw", "not-a-json-envelope")

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return poisoned != nil
	}, "毒消息应触发 Rejected 钩子")
	if poisoned.Topic != "raw" || poisoned.ID == "" {
		t.Fatalf("毒消息观测字段不完整: %+v", poisoned)
	}
	mu.Lock()
	defer mu.Unlock()
	if handled != 0 {
		t.Fatalf("毒消息不应进入 Handler, got %d 次", handled)
	}
}

func TestHandlerFailureNotRetried(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	producer := NewProducer(client, nil)

	errBoom := errors.New("boom")
	var mu sync.Mutex
	var deliveries int
	var rejectedErr error
	opts := &Options{Rejected: func(ctx context.Context, msg *mq.Message, err error) {
		mu.Lock()
		rejectedErr = err
		mu.Unlock()
	}}
	consumer := NewConsumer(client, opts)
	defer func() { _ = consumer.Close() }()

	startSubscribe(consumer, ctx, "g1", "fail", func(ctx context.Context, msg *mq.Message) error {
		mu.Lock()
		deliveries++
		mu.Unlock()
		return errBoom
	})
	waitSubscribed(t, client, "mq:ps:fail", false)

	if err := producer.Produce(ctx, "fail", &mq.Message{Body: []byte("payload")}); err != nil {
		t.Fatalf("Produce 失败: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return rejectedErr != nil
	}, "处理失败应触发 Rejected 钩子")

	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if deliveries != 1 {
		t.Fatalf("广播语义下失败不得重投, got %d 次投递", deliveries)
	}
	if !errors.Is(rejectedErr, errBoom) {
		t.Fatalf("Rejected 错误应为原始失败原因, got %v", rejectedErr)
	}
}

func TestSubscribeValidationAndExit(t *testing.T) {
	client, _ := newTestClient(t)
	consumer := NewConsumer(client, nil)

	noop := func(ctx context.Context, msg *mq.Message) error { return nil }
	// 参数校验类失败立即返回，不进入阻塞循环
	if err := consumer.Subscribe(context.Background(), "", "t", noop); err == nil {
		t.Fatal("空 group 应返回 error")
	}
	if err := consumer.Subscribe(context.Background(), "g1", "", noop); err == nil {
		t.Fatal("空 topic 应返回 error")
	}
	if err := consumer.Subscribe(context.Background(), "g1", "t", nil); err == nil {
		t.Fatal("nil handler 应返回 error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := startSubscribe(consumer, ctx, "g1", "t", noop)
	waitSubscribed(t, client, "mq:ps:t", false)
	if err := consumer.Subscribe(context.Background(), "g1", "t", noop); err == nil {
		t.Fatal("重复订阅应返回 error")
	}

	// ctx 取消 → Subscribe 退出并撤销登记
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ctx 取消后 Subscribe 应返回 context.Canceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ctx 取消后 Subscribe 未退出")
	}
	waitFor(t, 3*time.Second, func() bool {
		return len(client.PubSubChannels(context.Background(), "mq:ps:t").Val()) == 0
	}, "退出后频道订阅应释放")

	// 退出后可重新订阅同一 group+topic
	errCh2 := startSubscribe(consumer, context.Background(), "g1", "t", noop)
	waitSubscribed(t, client, "mq:ps:t", false)

	if err := consumer.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if err := consumer.Close(); err != nil {
		t.Fatalf("Close 应幂等: %v", err)
	}
	select {
	case err := <-errCh2:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Close 后 Subscribe 应以 context.Canceled 退出, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close 后 Subscribe 未退出")
	}
	if err := consumer.Subscribe(context.Background(), "g2", "t", noop); !errors.Is(err, mq.ErrConsumerClosed) {
		t.Fatalf("Close 后 Subscribe 应返回 ErrConsumerClosed, got %v", err)
	}
}

// TestSubscribeFailsFastWhenRedisUnreachable v8 的 SUBSCRIBE 会吞掉初始拨号错误导致
// 静默空转，Subscribe 须先 Ping 预检，把「服务不可达」作为初始化失败返回。
func TestSubscribeFailsFastWhenRedisUnreachable(t *testing.T) {
	client, mr := newTestClient(t)
	consumer := NewConsumer(client, nil)
	defer func() { _ = consumer.Close() }()

	noop := func(ctx context.Context, msg *mq.Message) error { return nil }
	mr.Close() // 服务端下线
	if err := consumer.Subscribe(context.Background(), "g1", "t", noop); err == nil {
		t.Fatal("Redis 不可达时 Subscribe 应返回 error，不得静默空转")
	}

	// 失败路径不得留下半启动订阅登记
	consumer.mu.Lock()
	registered := len(consumer.subs) + len(consumer.cancels)
	consumer.mu.Unlock()
	if registered != 0 {
		t.Fatalf("初始化失败不得留下登记, got %d", registered)
	}
}

func TestProduceValidation(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	producer := NewProducer(client, nil)

	if err := producer.ProduceBatch(ctx, "t", nil); err != nil {
		t.Fatalf("空批量应为 no-op: %v", err)
	}
	if err := producer.Produce(ctx, "t", nil); err == nil {
		t.Fatal("nil 消息应返回 error")
	}
	if err := producer.Produce(ctx, "", &mq.Message{}); err == nil {
		t.Fatal("空 topic 应返回 error")
	}
	if err := producer.ProduceBatch(ctx, "", []*mq.Message{{Body: []byte("1")}}); err == nil {
		t.Fatal("空 topic 批量应返回 error")
	}
	if err := producer.ProduceBatch(ctx, "t", []*mq.Message{{Body: []byte("1")}, nil}); err == nil {
		t.Fatal("批量含 nil 元素应返回 error")
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("Producer Close 应幂等返回 nil: %v", err)
	}

	// 无在线订阅者时发布不报错（at-most-once 语义）
	if err := producer.Produce(ctx, "nobody", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("无订阅者发布应为 no-op 成功: %v", err)
	}
}
