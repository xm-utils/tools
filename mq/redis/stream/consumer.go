package stream

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	rs "github.com/xm-utils/tools/redis_stream_queue"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"

	"github.com/xm-utils/tools/mq"
	envpkg "github.com/xm-utils/tools/mq/internal/envelope"
)

// 默认失败重投额度：与 mq/kafka 语义对齐，超限进终态，防「重投-再消费」热循环。
const defaultMaxRequeue = 3

// readFailBackoff 拉取失败后的最小间歇，防止 Redis 不可用时热循环刷屏。
const readFailBackoff = 500 * time.Millisecond

// Consumer Redis Stream 消费者适配器，实现 mq.Consumer。
//
// 以消费者组模式（XREADGROUP）逐条消费，确认粒度为单消息（XACK）：
//   - Handler 返回 nil → XACK；
//   - 返回 mq.ErrManualAck → 留在 PEL 在途，等待业务显式 Commit/Reject；
//   - 返回其他 error → 额度内 attempt+1 重投（XADD 新 entry）后 XACK 原消息；
//     额度耗尽进终态：配置了死信 Stream 则转入死信后 XACK，否则仅回调钩子后 XACK 丢弃。
type Consumer struct {
	client       *redis.Client
	consumerName string
	log          *logrus.Entry

	maxRequeue    int
	deadLetterKey string
	rejected      func(ctx context.Context, msg *mq.Message, err error)

	mu        sync.Mutex
	subs      map[string]*subscription      // key: group+"\x00"+topic
	inFlight  map[string]*inFlightRef       // key: group|entryID（多 group 订阅同 stream 时登记不互撞）
	cancels   map[string]context.CancelFunc // key 同 subs，随订阅退出回收，防常驻重订阅累积
	closeOnce sync.Once
	closed    bool
}

var _ mq.Consumer = (*Consumer)(nil)

// subscription 一次订阅建立的组件队列句柄及其 group/topic。
type subscription struct {
	sq    *rs.StreamQueue
	key   string
	group string
	topic string
}

// inFlightRef ErrManualAck 模式下登记的在途消息（id 为原始 entry ID，供 XACK）。
type inFlightRef struct {
	sub *subscription
	id  string
}

// Option 消费者可选配置。
type Option func(*Consumer)

// WithConsumerName 覆盖消费者实例名（默认 hostname-pid-序号，同一组内必须唯一）。
func WithConsumerName(name string) Option {
	return func(c *Consumer) {
		if name != "" {
			c.consumerName = name
		}
	}
}

// WithMaxRequeue 覆盖默认重投额度（<=0 视为不重投，失败直接终态）。
func WithMaxRequeue(n int) Option {
	return func(c *Consumer) { c.maxRequeue = n }
}

// WithDeadLetterKey 指定终态失败消息转入的死信 Stream key（空则仅回调钩子后丢弃）。
func WithDeadLetterKey(key string) Option {
	return func(c *Consumer) { c.deadLetterKey = key }
}

// WithRejected 终态失败钩子：消息进终态（无论是否转死信）时调用，panic 会被 recover。
func WithRejected(fn func(ctx context.Context, msg *mq.Message, err error)) Option {
	return func(c *Consumer) { c.rejected = fn }
}

// WithLogger 覆盖日志出口（默认 module=mq/redis/stream Consumer）。
func WithLogger(entry *logrus.Entry) Option {
	return func(c *Consumer) {
		if entry != nil {
			c.log = entry
		}
	}
}

// instanceSeq 同进程多消费者实例编号，参与默认消费者名生成。
var instanceSeq atomic.Int64

// NewConsumer 创建消费者适配器，client 由调用方持有并负责关闭。
func NewConsumer(client *redis.Client, opts ...Option) (*Consumer, error) {
	if client == nil {
		return nil, fmt.Errorf("mq/redis/stream: redis client 不能为 nil")
	}
	host, _ := os.Hostname()
	c := &Consumer{
		client:       client,
		consumerName: fmt.Sprintf("%s-%d-%d", host, os.Getpid(), instanceSeq.Add(1)),
		log:          logrus.WithField("module", "mq/redis/stream Consumer"),
		maxRequeue:   defaultMaxRequeue,
		subs:         make(map[string]*subscription),
		inFlight:     make(map[string]*inFlightRef),
		cancels:      make(map[string]context.CancelFunc),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Subscribe 订阅 group-topic（XREADGROUP）并阻塞消费，直到 ctx 结束或 Close。
// 退出返回值：ctx 结束为 ctx.Err()，Close 触发为 context.Canceled，其余非 nil
// error 仅为初始化失败。同一 group-topic 重复订阅返回 error。
// 首次订阅自动完成 XGroupCreateMkStream 初始化。
func (c *Consumer) Subscribe(ctx context.Context, group, topic string, handler mq.Handler) error {
	if group == "" || topic == "" {
		return fmt.Errorf("mq/redis/stream: group 与 topic 不能为空")
	}
	if handler == nil {
		return fmt.Errorf("mq/redis/stream: handler 不能为 nil")
	}

	runCtx, cancel := context.WithCancel(ctx)

	sq, err := rs.NewStreamQueue(rs.DefaultQueueConfig(topic, group), c.client)
	if err != nil {
		cancel()
		return fmt.Errorf("mq/redis/stream: 创建队列句柄失败: %w", err)
	}
	if err := sq.InitStream(runCtx); err != nil {
		cancel()
		return fmt.Errorf("mq/redis/stream: 初始化消费者组失败: %w", err)
	}

	key := subKey(group, topic)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		return mq.ErrConsumerClosed
	}
	if _, exists := c.subs[key]; exists {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("mq/redis/stream: group-topic 已订阅: %s/%s", group, topic)
	}
	sub := &subscription{sq: sq, key: key, group: group, topic: topic}
	c.subs[key] = sub
	c.cancels[key] = cancel
	c.mu.Unlock()

	c.log.Infof("开始订阅: group=%s, topic=%s, consumer=%s", group, topic, c.consumerName)
	c.consumeLoop(runCtx, sub, handler)
	// 循环已退出：撤销登记使同一 group-topic 可重新订阅（StreamQueue 无额外资源需释放）
	c.unsub(key)
	return runCtx.Err()
}

// unsub 撤销本次订阅的全部登记（subs/cancels），使同一 group-topic 可原地重新订阅；
// Close 路径由 closed 标志拒绝新订阅，不依赖此处清理。
func (c *Consumer) unsub(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subs, key)
	if cancel, ok := c.cancels[key]; ok {
		delete(c.cancels, key)
		cancel() // 释放订阅派生 ctx，重复 cancel 幂等
	}
}

// consumeLoop 阻塞拉取主循环：ReadMessages 内部经 XREADGROUP BLOCK 实现长轮询。
func (c *Consumer) consumeLoop(ctx context.Context, sub *subscription, handler mq.Handler) {
	for {
		if ctx.Err() != nil {
			return
		}
		streamMsgs, err := sub.sq.ReadMessages(ctx, c.consumerName)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Errorf("拉取消息失败: sub=%s, err=%v", sub.key, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(readFailBackoff):
			}
			continue
		}
		for i := range streamMsgs {
			if ctx.Err() != nil {
				return
			}
			c.dispatch(ctx, sub, &streamMsgs[i], handler)
		}
	}
}

// dispatch 单消息解码 + 确认分派。解码失败视为毒消息直接终态，不进 Handler。
func (c *Consumer) dispatch(ctx context.Context, sub *subscription, sm *rs.StreamMessage, handler mq.Handler) {
	env, decErr := decodeEnvelope(sm.Payload)
	if decErr != nil {
		c.log.Errorf("消息解码失败(毒消息终态): topic=%s, id=%s, err=%v", sub.topic, sm.MessageID, decErr)
		c.terminal(ctx, &mq.Message{ID: flightID(sub.group, sm.MessageID), Topic: sub.topic, Metadata: sm}, decErr)
		return
	}
	msg := toMessage(sub.topic, sm, env)
	// 投递 ID 纳入 group 作用域：同一实例多 group 订阅同 stream 时
	// 相同 entry 的在途登记不互撞，Commit 能 XACK 到正确消费者组
	msg.ID = flightID(sub.group, msg.ID)

	err := mq.InvokeHandler(ctx, handler, msg)
	switch {
	case err == nil:
		c.ack(ctx, sub, sm.MessageID)
	case errors.Is(err, mq.ErrManualAck):
		c.mu.Lock()
		if c.closed { // Close 与挂起登记竞态：不留悬挂条目，消息留在 PEL 由巡检兜底
			c.mu.Unlock()
			c.log.Warnf("消费者已关闭，放弃挂起在途登记: topic=%s, id=%s", msg.Topic, msg.ID)
			return
		}
		c.inFlight[msg.ID] = &inFlightRef{sub: sub, id: sm.MessageID}
		c.mu.Unlock()
	default:
		c.handleFailure(ctx, sub, sm, msg, env, err)
	}
}

// handleFailure 失败处置：额度内重投（新 entry 携带 attempt+1 信封）后 XACK 原消息；
// 额度耗尽进终态。重投信封基于解码出的原信封 Clone，以保留持久 ID 与 Headers
// （msg.ID 是含 group 前缀的不透明投递令牌，不得写入信封污染跨实例 ID）。
func (c *Consumer) handleFailure(ctx context.Context, sub *subscription, sm *rs.StreamMessage, msg *mq.Message, env *envpkg.Envelope, handleErr error) {
	if env.Attempt < c.maxRequeue {
		retryEnv := env.Clone()
		retryEnv.Attempt = env.Attempt + 1
		payload, err := retryEnv.Encode()
		if err != nil {
			c.terminal(ctx, msg, fmt.Errorf("重投信封序列化失败: %w（原始错误: %v）", err, handleErr))
			return
		}
		if _, err := sub.sq.EnqueueMessage(ctx, &rs.StreamMessage{RequestID: sm.RequestID, Payload: payload}); err != nil {
			c.log.Errorf("失败消息重投失败: topic=%s, id=%s, err=%v", msg.Topic, msg.ID, err)
			c.terminal(ctx, msg, fmt.Errorf("重投失败: %w（原始错误: %v）", err, handleErr))
			return
		}
		c.ack(ctx, sub, sm.MessageID)
		c.log.Warnf("消息处理失败已重投: topic=%s, id=%s, attempt=%d, err=%v", msg.Topic, msg.ID, retryEnv.Attempt, handleErr)
		return
	}
	c.terminal(ctx, msg, handleErr)
	// 终态消息同样 XACK，避免 PEL 无限堆积后被 PendingChecker 反复巡检
	c.ack(ctx, sub, sm.MessageID)
}

// terminal 终态失败：可选转投死信 Stream + 观测钩子（含 panic 防线）。
func (c *Consumer) terminal(ctx context.Context, msg *mq.Message, handleErr error) {
	if c.deadLetterKey != "" {
		if sm, ok := msg.Metadata.(*rs.StreamMessage); ok {
			dlArgs := &redis.XAddArgs{
				Stream: c.deadLetterKey,
				Values: map[string]interface{}{
					"data":       sm.Payload,
					"requestId":  sm.RequestID,
					"reason":     handleErr.Error(),
					"failedTime": strconv.FormatInt(time.Now().UnixMilli(), 10),
				},
			}
			if _, err := c.client.XAdd(ctx, dlArgs).Result(); err != nil {
				c.log.Errorf("终态消息转死信失败: dlq=%s, id=%s, err=%v", c.deadLetterKey, msg.ID, err)
			}
		}
	}
	c.log.Errorf("消息终态失败: topic=%s, id=%s, err=%v", msg.Topic, msg.ID, handleErr)
	if c.rejected == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			c.log.Errorf("Rejected 钩子 panic: %v", r)
		}
	}()
	c.rejected(ctx, msg, handleErr)
}

// Commit 显式确认 ErrManualAck 模式下的在途消息（XACK）。
// XACK 失败时恢复在途登记，允许业务重试确认。
func (c *Consumer) Commit(ctx context.Context, msg *mq.Message) error {
	ref, err := c.takeInFlight(msg)
	if err != nil {
		return err
	}
	if err := ref.sub.sq.AckMessage(ctx, ref.id); err != nil {
		c.restoreInFlight(msg.ID, ref)
		return fmt.Errorf("mq/redis/stream: 确认消息失败: %w", err)
	}
	return nil
}

// Reject 显式否定 ErrManualAck 模式下的在途消息：
// requeue=true 以 attempt+1 重投新 entry；requeue=false 直接 XACK 终态丢弃
// （死信留存请在调用前自行完成）。两种分支最终都会 XACK 原消息。
func (c *Consumer) Reject(ctx context.Context, msg *mq.Message, requeue bool) error {
	ref, err := c.takeInFlight(msg)
	if err != nil {
		return err
	}
	if requeue {
		sm, ok := msg.Metadata.(*rs.StreamMessage)
		if !ok {
			c.restoreInFlight(msg.ID, ref) // 恢复登记，业务可改走非重投分支或重试
			return fmt.Errorf("mq/redis/stream: 消息 Metadata 非组件 StreamMessage，无法重投")
		}
		// 重投信封从原始 payload 反解，保留持久 ID（不从 msg.ID 反推）
		retryEnv, decErr := decodeEnvelope(sm.Payload)
		if decErr != nil {
			c.restoreInFlight(msg.ID, ref)
			return fmt.Errorf("mq/redis/stream: 重投需原始信封，解码失败: %w", decErr)
		}
		retryEnv.Attempt++
		payload, err := retryEnv.Encode()
		if err != nil {
			c.restoreInFlight(msg.ID, ref)
			return err
		}
		if _, err := ref.sub.sq.EnqueueMessage(ctx, &rs.StreamMessage{RequestID: sm.RequestID, Payload: payload}); err != nil {
			c.restoreInFlight(msg.ID, ref)
			return fmt.Errorf("mq/redis/stream: requeue 重投失败: %w", err)
		}
	}
	if err := ref.sub.sq.AckMessage(ctx, ref.id); err != nil {
		// XACK 失败恢复登记供重试；requeue 分支已重投成功时重试会形成重复投递，
		// 由 at-least-once 语义与业务幂等性吸收
		c.restoreInFlight(msg.ID, ref)
		return fmt.Errorf("mq/redis/stream: 确认消息失败: %w", err)
	}
	return nil
}

// Close 停止全部订阅循环并释放登记，重复调用安全。client 归调用方所有，不在此关闭。
// ErrManualAck 模式下未确认的在途消息留在 PEL，可由同组其他实例经 XCLAIM 接管
// 或组件 PendingChecker 巡检兜底（at-least-once）。
func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.closed = true
		for _, cancel := range c.cancels {
			cancel()
		}
		c.cancels = make(map[string]context.CancelFunc)
		for key := range c.subs {
			delete(c.subs, key)
		}
		for id := range c.inFlight {
			delete(c.inFlight, id)
		}
	})
	return nil
}

// ack XACK 单消息（失败仅记日志：PEL 残留由巡检兜底）并清理对应在途登记。
func (c *Consumer) ack(ctx context.Context, sub *subscription, id string) {
	if err := sub.sq.AckMessage(ctx, id); err != nil {
		c.log.Errorf("确认消息失败: sub=%s, id=%s, err=%v", sub.key, id, err)
	}
	c.mu.Lock()
	delete(c.inFlight, flightID(sub.group, id))
	c.mu.Unlock()
}

// takeInFlight 摘出在途登记；未登记（已确认/未知消息）返回 ErrNotSubscribed。
func (c *Consumer) takeInFlight(msg *mq.Message) (*inFlightRef, error) {
	if msg == nil || msg.ID == "" {
		return nil, mq.ErrNotSubscribed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, mq.ErrConsumerClosed
	}
	ref, ok := c.inFlight[msg.ID]
	if !ok {
		return nil, mq.ErrNotSubscribed
	}
	delete(c.inFlight, msg.ID)
	return ref, nil
}

// restoreInFlight 重投失败时回滚在途登记，保留业务重试窗口。
func (c *Consumer) restoreInFlight(id string, ref *inFlightRef) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.inFlight[id] = ref
	}
}

func subKey(group, topic string) string {
	return group + "\x00" + topic
}

// flightID 投递 ID：group 作用域前缀，避免多 group 订阅同 stream 时
// 相同 entry 的在途登记互撞（mq.Message.ID 对业务为不透明令牌）。
func flightID(group, id string) string {
	return group + "|" + id
}
