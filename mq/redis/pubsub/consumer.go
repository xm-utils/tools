package pubsub

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"

	"github.com/xm-utils/tools/mq/core"
	envpkg "github.com/xm-utils/tools/mq/redis/internal/envelope"
	"github.com/xm-utils/tools/trace"
)

// defaultHealthCheck 订阅连接健康检查间隔，取 go-redis Channel 模式默认值；
// Options.HealthCheckInterval 置负数可关闭（关闭后连接异常将静默失去消息，不建议）。
const defaultHealthCheck = 3 * time.Second

// subscription 单条 group+topic 订阅的运行态。
type subscription struct {
	channel string // 频道全名或 glob 模式
	key     string // 登记键 group\x00topic
	pattern bool   // 是否按 PSUBSCRIBE 通配订阅
}

// Consumer Redis 发布/订阅消费者适配器，实现 mq.Consumer。
//
// Subscribe 同步持有订阅连接并分发消息，直到 ctx 结束或 Close（阻塞式，与其他
// 适配器语义一致）。广播语义下无在途状态：Commit/Reject 为 no-op 恒返回 nil，
// Handler 返回 mq.ErrManualAck 等价于「投递已完成」，返回 error 仅观测不重投。
// 每个消费者实例都会收到同一频道的全部消息（不像队列型后端多实例分流）。
type Consumer struct {
	client   *redis.Client
	cfg      Options
	consumer string // 实际生效的订阅者标识，参与投递 ID 生成与日志

	mu          sync.Mutex
	subs        map[string]*subscription
	cancels     map[string]context.CancelFunc // key 同 subs，随订阅退出回收，防常驻重订阅累积
	closed      bool
	deliverySeq atomic.Int64
}

var _ mq.Consumer = (*Consumer)(nil)

// NewConsumer 基于已建立的 Redis 连接创建订阅者适配器。
// client 由调用方持有并负责关闭；opts 为 nil 时使用默认装配。
func NewConsumer(client *redis.Client, opts *Options) *Consumer {
	cfg := opts.normalized()
	if cfg.ConsumerName == "" {
		cfg.ConsumerName = defaultConsumerName()
	}
	return &Consumer{
		client:   client,
		cfg:      cfg,
		consumer: cfg.ConsumerName,
		subs:     make(map[string]*subscription),
		cancels:  make(map[string]context.CancelFunc),
	}
}

// defaultConsumerName 未显式指定订阅者时以 "redis-pubsub-<hostname>-<pid>" 生成。
func defaultConsumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("redis-pubsub-%s-%d", host, os.Getpid())
}

// Subscribe 订阅频道（SUBSCRIBE，或 Options.Pattern 时 PSUBSCRIBE）并阻塞分发，
// 直到 ctx 结束或 Close。退出返回值：ctx 结束为 ctx.Err()，Close 触发为
// context.Canceled，其余非 nil error 仅为初始化失败（参数不合法、建链预检失败）。
// 同一 group+topic 重复订阅返回 error；退出后同一 group+topic 可重新订阅。
// 注意：广播无消费组概念，group 仅作登记与日志维度；发布时刻不在线的订阅者
// 收不到该消息（at-most-once），常驻消费请放入独立 goroutine。
func (c *Consumer) Subscribe(ctx context.Context, group, topic string, handler mq.Handler) error {
	if group == "" {
		return fmt.Errorf("mq/redis/pubsub: group 不能为空")
	}
	if topic == "" {
		return fmt.Errorf("mq/redis/pubsub: topic(频道) 不能为空")
	}
	if handler == nil {
		return fmt.Errorf("mq/redis/pubsub: handler 不能为 nil")
	}
	// 建连预检：go-redis v8 的 Subscribe/PSubscribe 会丢弃初始拨号错误，Redis 不可达时
	// 只会静默空转重拨（无任何消息也无任何日志），与门面「初始化失败返回 error」契约相背，
	// 故先显式 Ping 一次让装配失败可感知
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("mq/redis/pubsub: 建立订阅连接失败: %w", err)
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return mq.ErrConsumerClosed
	}
	key := subKey(group, topic)
	if _, dup := c.subs[key]; dup {
		c.mu.Unlock()
		return fmt.Errorf("mq/redis/pubsub: 订阅已存在: group=%q, topic=%q", group, topic)
	}
	sub := &subscription{
		channel: c.cfg.ChannelPrefix + topic,
		key:     key,
		pattern: c.cfg.Pattern,
	}
	c.subs[key] = sub

	// 循环 ctx 派生自入参 ctx：订阅生命周期随调用方 ctx（门面契约），
	// Close 另行统一终止全部循环，两者先到先退
	runCtx, cancel := context.WithCancel(ctx)
	c.cancels[key] = cancel
	c.mu.Unlock()

	ps := c.newPubSub(runCtx, sub)
	defer func() {
		// 释放订阅专用连接（v8 的 Channel 读取协程随 Close 退出），并撤销登记
		_ = ps.Close()
		c.unsub(key)
	}()

	msgCh := ps.Channel(
		redis.WithChannelSize(c.cfg.ChannelBuffer),
		redis.WithChannelHealthCheckInterval(c.healthCheckInterval()),
	)

	c.logInfof("开始订阅: group=%s, channel=%s, pattern=%v", group, sub.channel, sub.pattern)
	c.consumeLoop(runCtx, sub, msgCh, handler)
	return runCtx.Err()
}

// newPubSub 按订阅模式建立底层 PubSub 连接（conn 归本订阅独占，随 Subscribe 退出关闭）。
func (c *Consumer) newPubSub(ctx context.Context, sub *subscription) *redis.PubSub {
	if sub.pattern {
		return c.client.PSubscribe(ctx, sub.channel)
	}
	return c.client.Subscribe(ctx, sub.channel)
}

// healthCheckInterval 归一化健康检查间隔：0 取默认 3s，负数关闭。
func (c *Consumer) healthCheckInterval() time.Duration {
	switch v := c.cfg.HealthCheckInterval; {
	case v < 0:
		return 0
	case v == 0:
		return defaultHealthCheck
	default:
		return v
	}
}

// consumeLoop 分发循环：ctx 与消息通道双 select，任一结束即退出。
func (c *Consumer) consumeLoop(ctx context.Context, sub *subscription, msgCh <-chan *redis.Message, handler mq.Handler) {
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-msgCh:
			if !ok {
				// 通道关闭仅出现在订阅连接被释放时（健康检查重连不会关闭通道），
				// 此处保守退出，由调用方重新订阅
				return
			}
			c.dispatch(ctx, sub, m, handler)
		}
	}
}

// dispatch 解码并分发单条广播消息。
func (c *Consumer) dispatch(ctx context.Context, sub *subscription, m *redis.Message, handler mq.Handler) {
	topic := c.topicOf(sub, m)

	e, decErr := envpkg.Decode(m.Payload)
	if decErr != nil {
		// 毒消息：无死信队列可投（广播本身即不可靠通道），仅回调观测钩子
		poison := &mq.Message{ID: c.nextDeliveryID(), Topic: topic, Metadata: m}
		c.logErrorf("消息信封反序列化失败(毒消息丢弃): channel=%s, err=%v", m.Channel, decErr)
		c.invokeRejected(ctx, poison, fmt.Errorf("消息信封反序列化失败: %w", decErr))
		return
	}
	// 外部直接 PUBLISH 的信封可能无 ID，兜底生成本实例内唯一投递 ID
	if e.ID == "" {
		e.ID = c.nextDeliveryID()
	}
	msg := e.ToMessage(topic, "", m)

	// 从消息头还原上游链路并开 CONSUMER span，handler 拿到贯通后的 ctx。
	hctx, end := trace.StartConsumerSpan(ctx, traceName, "pubsub receive "+topic, msg.Headers)
	defer end()
	err := mq.InvokeHandler(hctx, handler, msg)
	switch {
	case err == nil:
		// 投递完成，无确认动作
	case errors.Is(err, mq.ErrManualAck):
		// 广播无确认通道：等价于投递完成，Commit/Reject 为 no-op
		c.logWarnf("pub/sub 无确认通道，ErrManualAck 按投递完成处理: channel=%s, id=%s", m.Channel, msg.ID)
	default:
		// 失败无重投能力：仅观测，避免业务误以为可恢复
		c.logErrorf("消息处理失败(广播不可重投): channel=%s, id=%s, err=%v", m.Channel, msg.ID, err)
		c.invokeRejected(ctx, msg, err)
	}
}

// invokeRejected 终态观测钩子（含 panic 防线）。
func (c *Consumer) invokeRejected(ctx context.Context, msg *mq.Message, cause error) {
	if c.cfg.Rejected == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			c.logErrorf("Rejected 钩子 panic: id=%v, panic=%v", msg.ID, r)
		}
	}()
	c.cfg.Rejected(ctx, msg, cause)
}

// Commit 恒返回 nil：Redis pub/sub 无逐消息确认原语，广播消息一经交付 Handler 即为终态。
// 传入 nil 或未知消息同样返回 nil（无在途状态可校验，不 panic、不报错）。
func (c *Consumer) Commit(ctx context.Context, msg *mq.Message) error { return nil }

// Reject 恒返回 nil：广播语义下无法回收已投递消息，requeue 更无从实现
// （需要重投请改用 redis/list、redis/zset 或 redis/stream 后端）。
func (c *Consumer) Reject(ctx context.Context, msg *mq.Message, requeue bool) error { return nil }

// Close 终止全部订阅并释放各订阅独占的 PubSub 连接，幂等。
// client 归调用方所有，不在此关闭。Close 后重新订阅返回 mq.ErrConsumerClosed。
func (c *Consumer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	snapshot := c.cancels
	c.cancels = make(map[string]context.CancelFunc)
	c.subs = make(map[string]*subscription)
	c.mu.Unlock()

	for _, cancel := range snapshot {
		cancel()
	}
	return nil
}

// ---------- 内部辅助 ----------

// unsub 撤销本次订阅的全部登记（subs/cancels）并释放派生 ctx，使同一 group+topic
// 可原地重新订阅；Close 后由 closed 标志拒绝新订阅。
func (c *Consumer) unsub(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subs, key)
	if cancel, ok := c.cancels[key]; ok {
		delete(c.cancels, key)
		cancel() // 重复 cancel 幂等，Close 路径无副作用
	}
}

// nextDeliveryID 生成本实例内唯一投递 ID：<订阅者>-<纳秒>-<序号>。
func (c *Consumer) nextDeliveryID() string {
	return fmt.Sprintf("%s-%d-%d", c.consumer, time.Now().UnixNano(), c.deliverySeq.Add(1))
}

// topicOf 推导消息的 topic 名：通配订阅取实际命中频道，精确订阅取登记时的 topic。
func (c *Consumer) topicOf(sub *subscription, m *redis.Message) string {
	if !sub.pattern {
		return strings.TrimPrefix(sub.channel, c.cfg.ChannelPrefix)
	}
	return strings.TrimPrefix(m.Channel, c.cfg.ChannelPrefix)
}

func subKey(group, topic string) string { return group + "\x00" + topic }

func (c *Consumer) logInfof(format string, args ...any) {
	c.logger().Infof(format, args...)
}

func (c *Consumer) logWarnf(format string, args ...any) {
	c.logger().Warnf(format, args...)
}

func (c *Consumer) logErrorf(format string, args ...any) {
	c.logger().Errorf(format, args...)
}

// logger 惰性解析日志入口，logrus standard logger 配置变更即时生效。
func (c *Consumer) logger() *logrus.Entry {
	return logrus.WithField("component", "mq/redis/pubsub")
}
