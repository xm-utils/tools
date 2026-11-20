package stream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	rs "github.com/xm-utils/tools/mq/redis/stream/queue"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"

	"github.com/xm-utils/tools/mq/core"
	envpkg "github.com/xm-utils/tools/mq/redis/internal/envelope"
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

// TestRequeueKeepsPersistentEnvelopeID 失败重投的新 entry 必须沿用信封内的业务持久 ID：
// 投递令牌（group|entryID）只存在于 mq.Message.ID，不得回写信封，否则跨实例/跨后端的
// 同一消息 ID 会漂移，破坏信封二进制兼容约定。
func TestRequeueKeepsPersistentEnvelopeID(t *testing.T) {
	client, _ := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const bizID = "biz-stable-id"
	producer := NewProducer(client)
	src := &mq.Message{ID: bizID, Key: "req-1", Body: []byte("payload")}
	if err := producer.Produce(ctx, "orders", src); err != nil {
		t.Fatalf("Produce 失败: %v", err)
	}

	consumer, err := NewConsumer(client, WithMaxRequeue(2))
	if err != nil {
		t.Fatalf("NewConsumer 失败: %v", err)
	}
	defer func() { _ = consumer.Close() }()

	var mu sync.Mutex
	var attempts, envIDs []string
	go func() {
		_ = consumer.Subscribe(ctx, "g1", "orders", func(ctx context.Context, msg *mq.Message) error {
			// 直接取重投后的 entry 原文，验信封内 ID 未被投递令牌污染
			var envID string
			if sm, ok := msg.Metadata.(*rs.StreamMessage); ok {
				if env, err := envpkg.Decode(sm.Payload); err == nil {
					envID = env.ID
				}
			}
			mu.Lock()
			attempts = append(attempts, msg.Header(HeaderAttempt))
			envIDs = append(envIDs, envID)
			mu.Unlock()
			return errors.New("boom")
		})
	}()

	waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) >= 2
	}, "失败消息应被重投再次投递")

	mu.Lock()
	secondAttempt, secondEnvID := attempts[1], envIDs[1]
	mu.Unlock()
	if secondAttempt != "1" {
		t.Fatalf("重投后 attempt 头应递增为 1, got %q", secondAttempt)
	}
	// 重投 entry 的信封 ID 须仍为业务持久 ID（污染后形如 "g1|1795122858020-0"）
	if secondEnvID != bizID {
		t.Fatalf("重投信封 ID 被投递令牌污染: got %q, want %q", secondEnvID, bizID)
	}
}
