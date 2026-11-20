package zset

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"

	"github.com/xm-utils/tools/mq/core"
	envpkg "github.com/xm-utils/tools/mq/redis/internal/envelope"
)

// emptyBackoff 领取失败（非空队列）后的退避间隔，防持续错误热循环。
const emptyBackoff = 500 * time.Millisecond

// subscription 单条 group+topic 订阅的运行态。
type subscription struct {
	ready      string // 就绪集合 key
	processing string // 处理中集合 key
	key        string // 登记键 group\x00topic
}

// flightRef 手动确认模式下挂起的在途消息。
type flightRef struct {
	sub    *subscription
	member string // 集合成员（信封 JSON 原文），ZREM 精确移除依赖原文匹配
	msg    *mq.Message
}

// Consumer Redis 有序集合消费者适配器，实现 mq.Consumer。
// Subscribe 同步执行领取循环直到 ctx 结束或 Close（阻塞式，与 kafka/redis/stream
// 语义一致），可见性回收在后台协程运行；Commit/Reject 仅作用于本实例经 Handler
// 返回 mq.ErrManualAck 挂起的在途消息，跨实例无效。
type Consumer struct {
	client   *redis.Client
	cfg      Options
	consumer string // 实际生效的领取者标识，参与日志与 ID 兜底生成

	mu          sync.Mutex
	subs        map[string]*subscription
	inFlight    map[string]*flightRef         // key: 消息 ID（信封持久 ID，缺失时兜底投递 ID）
	cancels     map[string]context.CancelFunc // key 同 subs，随订阅退出回收，防常驻重订阅累积
	closed      bool
	deliverySeq atomic.Int64
}

var _ mq.Consumer = (*Consumer)(nil)

// NewConsumer 基于已建立的 Redis 连接创建消费者适配器。
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
		inFlight: make(map[string]*flightRef),
		cancels:  make(map[string]context.CancelFunc),
	}
}

// defaultConsumerName 未显式指定领取者时以 "redis-zset-<hostname>-<pid>" 生成。
func defaultConsumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("redis-zset-%s-%d", host, os.Getpid())
}

// Subscribe 订阅 topic 并同步执行领取循环（按 score 升序领取到期消息，阻塞直到
// ctx 结束或 Close）。退出返回值：ctx 结束为 ctx.Err()，Close 触发为 context.Canceled，
// 其余非 nil error 仅为初始化失败。业务常驻消费请放入独立 goroutine。
// 可见性回收启用时由后台协程承担，不占用本调用。
// 注意：有序集合队列无消费者组隔离，group 仅作登记与日志维度；
// 同一 group+topic 重复订阅返回 error，不同 group 订阅同一 topic 共享队列。
func (c *Consumer) Subscribe(ctx context.Context, group, topic string, handler mq.Handler) error {
	if group == "" {
		return fmt.Errorf("mq/redis/zset: group 不能为空")
	}
	if topic == "" {
		return fmt.Errorf("mq/redis/zset: topic 不能为空")
	}
	if handler == nil {
		return fmt.Errorf("mq/redis/zset: handler 不能为 nil")
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return mq.ErrConsumerClosed
	}
	key := subKey(group, topic)
	if _, dup := c.subs[key]; dup {
		c.mu.Unlock()
		return fmt.Errorf("mq/redis/zset: 订阅已存在: group=%q, topic=%q", group, topic)
	}
	sub := &subscription{
		ready:      c.cfg.KeyPrefix + topic,
		processing: c.cfg.KeyPrefix + topic + processingSuffix,
		key:        key,
	}
	c.subs[key] = sub

	// 领取循环 ctx 派生自入参 ctx：订阅生命周期随调用方 ctx（门面契约），
	// Close 另行统一终止全部循环，两者先到先退；回收循环同生共死
	runCtx, cancel := context.WithCancel(ctx)
	c.cancels[key] = cancel
	c.mu.Unlock()

	if c.cfg.VisibilityTimeout > 0 {
		go c.reclaimLoop(runCtx, sub)
	}

	c.consumeLoop(runCtx, sub, handler)
	// 循环已退出：撤销登记使同一 group+topic 可重新订阅（回收循环随 runCtx 一同结束）
	c.unsub(key)
	return runCtx.Err()
}

// unsub 撤销本次订阅的全部登记（subs/cancels），使同一 group+topic 可原地重新订阅
// （回收循环随 runCtx 取消一同结束）；Close 后由 closed 标志拒绝新订阅。
func (c *Consumer) unsub(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subs, key)
	if cancel, ok := c.cancels[key]; ok {
		delete(c.cancels, key)
		cancel() // 释放领取/回收循环派生 ctx，重复 cancel 幂等
	}
}

// consumeLoop 领取循环：Lua 原子领取到期消息 → 分发；无可领消息按 PollInterval 轮询。
func (c *Consumer) consumeLoop(ctx context.Context, sub *subscription, handler mq.Handler) {
	for {
		if ctx.Err() != nil {
			return
		}

		member, err := c.claim(ctx, sub)
		if errors.Is(err, redis.Nil) {
			// 无到期消息：等待轮询间隔，期间 ctx 取消可立即退出
			timer := time.NewTimer(c.cfg.PollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logErrorf("领取消息失败: sub=%s, err=%v", sub.key, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(emptyBackoff):
			}
			continue
		}

		c.dispatch(ctx, sub, member, handler)
	}
}

// claim 原子领取一条到期消息，无消息时返回 redis.Nil。
func (c *Consumer) claim(ctx context.Context, sub *subscription) (string, error) {
	now := float64(time.Now().UnixMilli())
	v, err := c.client.Eval(ctx, claimZScript, []string{sub.ready, sub.processing}, now).Result()
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return "", ctx.Err()
		}
		return "", err
	}
	member, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("mq/redis/zset: 领取返回值类型异常: %T", v)
	}
	return member, nil
}

// dispatch 解码并分发单条消息，按 Handler 返回值完成确认分派。
func (c *Consumer) dispatch(ctx context.Context, sub *subscription, member string, handler mq.Handler) {
	topic := c.topicOf(sub)

	e, decErr := envpkg.Decode(member)
	if decErr != nil {
		// 毒成员：无法解码信封不重试（重投也必然失败），直接终态处置
		poison := &mq.Message{ID: c.nextDeliveryID(), Topic: topic, Metadata: member}
		c.terminal(ctx, sub, poison, member, fmt.Errorf("消息信封反序列化失败: %w", decErr))
		return
	}
	// 信封 ID 为持久消息 ID；外部直写成员无 ID 时兜底投递 ID
	if e.ID == "" {
		e.ID = c.nextDeliveryID()
	}
	msg := e.ToMessage(topic, "", member)

	err := mq.InvokeHandler(ctx, handler, msg)
	switch {
	case err == nil:
		if ackErr := c.ack(ctx, sub, member); ackErr != nil {
			c.logErrorf("提交失败: sub=%s, id=%s, err=%v", sub.key, msg.ID, ackErr)
		}
	case errors.Is(err, mq.ErrManualAck):
		c.mu.Lock()
		if c.closed { // Close 与挂起登记竞态：不留悬挂条目，消息滞留 processing 待回收
			c.mu.Unlock()
			c.logWarnf("消费者已关闭，放弃挂起在途登记: topic=%s, id=%s", msg.Topic, msg.ID)
			return
		}
		// 同一消息 ID 重复投递（可见性回收后重领）时覆盖登记，成员内容一致
		c.inFlight[msg.ID] = &flightRef{sub: sub, member: member, msg: msg}
		c.mu.Unlock()
	default:
		c.handleFailure(ctx, sub, e, msg, member, err)
	}
}

// handleFailure 失败策略：额度内信封 attempt+1 立即回队重投（可见时间=现在），
// 耗尽进终态。
func (c *Consumer) handleFailure(ctx context.Context, sub *subscription, e *envpkg.Envelope, msg *mq.Message, member string, handleErr error) {
	attempt := e.Attempt
	if attempt < c.cfg.MaxRequeue {
		retryEnv := e.Clone()
		retryEnv.Attempt = attempt + 1
		payload, err := retryEnv.Encode()
		if err != nil {
			c.terminal(ctx, sub, msg, member, fmt.Errorf("重投信封序列化失败: %w（原始错误: %v）", err, handleErr))
			return
		}
		now := float64(time.Now().UnixMilli())
		if _, err := c.client.Eval(ctx, requeueZScript,
			[]string{sub.processing, sub.ready}, member, payload, now).Result(); err != nil {
			if ctx.Err() == nil {
				c.logErrorf("重投回队失败: sub=%s, id=%s, err=%v", sub.key, msg.ID, err)
				return // 回队失败：成员留在 processing，由可见性回收兜底，不重复终态化
			}
		}
		return
	}
	c.terminal(ctx, sub, msg, member, fmt.Errorf("重试额度耗尽（attempt=%d）: %w", attempt, handleErr))
}

// terminal 终态处置：可选入死信集合（score=失败时刻）→ 回调 Rejected 钩子。
// 无论死信与钩子结果如何，最后都从 processing 移除，防止残留反复回收。
func (c *Consumer) terminal(ctx context.Context, sub *subscription, msg *mq.Message, member string, cause error) {
	if c.cfg.DeadLetterKey != "" {
		now := float64(time.Now().UnixMilli())
		if err := c.client.ZAdd(ctx, c.cfg.DeadLetterKey, &redis.Z{Score: now, Member: member}).Err(); err != nil && ctx.Err() == nil {
			c.logErrorf("死信投递失败: topic=%s, id=%s, err=%v", msg.Topic, msg.ID, err)
		}
	}
	if c.cfg.Rejected != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					c.logErrorf("Rejected 钩子 panic: id=%v, panic=%v", msg.ID, r)
				}
			}()
			c.cfg.Rejected(ctx, msg, cause)
		}()
	}
	if err := c.ack(ctx, sub, member); err != nil && ctx.Err() == nil {
		c.logErrorf("终态清理 processing 失败: id=%s, err=%v", msg.ID, err)
	}
	c.forget(msg.ID)
}

// reclaimLoop 可见性回收循环：周期性把 processing 中超时未确认的成员挪回 ready。
// 间隔取超时的 1/2（下限 10ms 防高频空转）；回收不计数 attempt（崩溃恢复语义，
// 与业务失败重投分离）。
func (c *Consumer) reclaimLoop(ctx context.Context, sub *subscription) {
	interval := c.cfg.VisibilityTimeout / 2
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			moved, err := c.reclaimOnce(ctx, sub)
			if err != nil {
				if ctx.Err() == nil {
					c.logErrorf("可见性回收失败: sub=%s, err=%v", sub.key, err)
				}
				continue
			}
			if moved > 0 {
				c.logInfof("可见性回收: sub=%s, moved=%d", sub.key, moved)
			}
		}
	}
}

// reclaimOnce 执行一轮回收，返回移动条数。
func (c *Consumer) reclaimOnce(ctx context.Context, sub *subscription) (int64, error) {
	now := time.Now().UnixMilli()
	deadline := float64(now - c.cfg.VisibilityTimeout.Milliseconds())
	return c.client.Eval(ctx, reclaimZScript,
		[]string{sub.processing, sub.ready}, deadline, float64(now), int64(c.cfg.ReclaimBatch)).Int64()
}

// Commit 确认在途消息处理完成（从 processing 移除），msg 必须来自本实例 Handler。
// 成员已被可见性回收挪回 ready 等场景下 ZREM 为空操作，幂等返回 nil。
func (c *Consumer) Commit(ctx context.Context, msg *mq.Message) error {
	ref, err := c.takeInFlight(msg)
	if err != nil {
		return err
	}
	if err := c.ack(ctx, ref.sub, ref.member); err != nil {
		c.restoreInFlight(ref) // 恢复登记，允许业务重试确认
		return err
	}
	return nil
}

// Reject 显式判定在途消息失败。requeue=true 时信封 attempt+1 后立即回队
// （重投失败返回 error 且条目恢复登记）；requeue=false 时仅移除 processing 成员，
// 需要先入死信/暂存集合的请业务自行完成（或使用 Options.DeadLetterKey 走自动终态路径）。
func (c *Consumer) Reject(ctx context.Context, msg *mq.Message, requeue bool) error {
	if !requeue {
		ref, err := c.takeInFlight(msg)
		if err != nil {
			return err
		}
		if err := c.ack(ctx, ref.sub, ref.member); err != nil {
			c.restoreInFlight(ref)
			return err
		}
		return nil
	}

	ref, err := c.takeInFlight(msg)
	if err != nil {
		return err
	}
	e, decErr := envpkg.Decode(ref.member)
	if decErr != nil {
		c.restoreInFlight(ref)
		return fmt.Errorf("mq/redis/zset: 在途消息信封不可重投: %w", decErr)
	}
	retryEnv := e.Clone()
	retryEnv.Attempt = e.Attempt + 1
	payload, err := retryEnv.Encode()
	if err != nil {
		c.restoreInFlight(ref)
		return err
	}
	now := float64(time.Now().UnixMilli())
	if _, err := c.client.Eval(ctx, requeueZScript,
		[]string{ref.sub.processing, ref.sub.ready}, ref.member, payload, now).Result(); err != nil {
		c.restoreInFlight(ref)
		return err
	}
	return nil
}

// Close 终止全部领取/回收循环并释放在途登记，幂等。client 归调用方，不在此关闭。
// Close 后 processing 中的未确认消息滞留，到期后由启用可见性回收的实例挪回 ready。
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
	c.inFlight = make(map[string]*flightRef)
	c.mu.Unlock()

	for _, cancel := range snapshot {
		cancel()
	}
	return nil
}

// ---------- 内部辅助 ----------

// ack 从 processing 精确移除成员。
func (c *Consumer) ack(ctx context.Context, sub *subscription, member string) error {
	if sub == nil {
		return fmt.Errorf("mq/redis/zset: 无法定位消息所属订阅")
	}
	return c.client.ZRem(ctx, sub.processing, member).Err()
}

// takeInFlight 校验并摘除在途登记条目，未知消息返回 mq.ErrNotSubscribed。
func (c *Consumer) takeInFlight(msg *mq.Message) (*flightRef, error) {
	if msg == nil {
		return nil, fmt.Errorf("mq/redis/zset: msg 不能为 nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, mq.ErrConsumerClosed
	}
	ref, ok := c.inFlight[msg.ID]
	if !ok {
		return nil, fmt.Errorf("mq/redis/zset: 消息 %q 不在本消费者在途登记中（须来自本实例 Handler）: %w", msg.ID, mq.ErrNotSubscribed)
	}
	delete(c.inFlight, msg.ID)
	return ref, nil
}

// restoreInFlight 重投失败时恢复在途登记。
func (c *Consumer) restoreInFlight(ref *flightRef) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.inFlight[ref.msg.ID] = ref
}

// forget 释放在途登记（终态路径，登记不存在时为空操作）。
func (c *Consumer) forget(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inFlight, id)
}

// nextDeliveryID 生成本实例内唯一投递 ID（外部直写无 ID 成员兜底用）。
func (c *Consumer) nextDeliveryID() string {
	return fmt.Sprintf("%s-%d-%d", c.consumer, time.Now().UnixNano(), c.deliverySeq.Add(1))
}

// topicOf 由 ready key 反推 topic 名。
func (c *Consumer) topicOf(sub *subscription) string {
	return sub.ready[len(c.cfg.KeyPrefix):]
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
	return logrus.WithField("component", "mq/redis/zset")
}
