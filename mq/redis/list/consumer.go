package list

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
	"github.com/xm-utils/tools/trace"
)

// emptyBackoff 拉取失败（非空队列）后的退避间隔，防持续错误热循环。
const emptyBackoff = 500 * time.Millisecond

// recoverBatchLimit 单次回补脚本搬运的消息条数上限（超长脚本会阻塞 Redis）。
const recoverBatchLimit = 500

// subscription 单条 group+topic 订阅的运行态。
type subscription struct {
	ready      string // 就绪列表 key
	processing string // 处理中列表 key
	key        string // 登记键 group\x00topic
}

// flightRef 手动确认模式下挂起的在途消息。
type flightRef struct {
	sub *subscription
	raw string // 消息 payload（Commit/Reject 以 LREM 精确移除依赖原文匹配）
	msg *mq.Message
}

// Consumer Redis List 消费者适配器，实现 mq.Consumer。
// Subscribe 同步执行领取循环直到 ctx 结束或 Close（阻塞式，与 kafka/redis/stream
// 语义一致）；Commit/Reject 仅作用于本实例经 Handler 返回 mq.ErrManualAck 挂起
// 的在途消息，跨实例无效。
type Consumer struct {
	client   *redis.Client
	cfg      Options
	consumer string // 实际生效的领取者标识，参与投递 ID 生成

	mu          sync.Mutex
	subs        map[string]*subscription
	inFlight    map[string]*flightRef         // key: 投递 ID（含领取者标识与进程内序号）
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

// defaultConsumerName 未显式指定领取者时以 "redis-list-<hostname>-<pid>" 生成，
// 保证同机多实例的投递 ID 不冲突。
func defaultConsumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("redis-list-%s-%d", host, os.Getpid())
}

// Subscribe 订阅 topic 并同步执行领取循环（按 FIFO 处理，阻塞直到 ctx 结束或
// Close）。退出返回值：ctx 结束为 ctx.Err()，Close 触发为 context.Canceled，
// 其余非 nil error 仅为初始化失败。业务常驻消费请放入独立 goroutine。
// 注意：List 队列无消费者组隔离，group 仅作登记与日志维度；
// 同一 group+topic 重复订阅返回 error，不同 group 订阅同一 topic 共享队列。
func (c *Consumer) Subscribe(ctx context.Context, group, topic string, handler mq.Handler) error {
	if group == "" {
		return fmt.Errorf("mq/redis/list: group 不能为空")
	}
	if topic == "" {
		return fmt.Errorf("mq/redis/list: topic 不能为空")
	}
	if handler == nil {
		return fmt.Errorf("mq/redis/list: handler 不能为 nil")
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return mq.ErrConsumerClosed
	}
	key := subKey(group, topic)
	if _, dup := c.subs[key]; dup {
		c.mu.Unlock()
		return fmt.Errorf("mq/redis/list: 订阅已存在: group=%q, topic=%q", group, topic)
	}
	sub := &subscription{
		ready:      c.cfg.KeyPrefix + topic,
		processing: c.cfg.KeyPrefix + topic + processingSuffix,
		key:        key,
	}
	c.subs[key] = sub

	// 循环 ctx 派生自入参 ctx：订阅生命周期随调用方 ctx（门面契约），
	// Close 另行统一终止全部循环，两者先到先退
	runCtx, cancel := context.WithCancel(ctx)
	c.cancels[key] = cancel
	c.mu.Unlock()

	if c.cfg.RecoverProcessing {
		n, err := c.recoverProcessing(runCtx, sub)
		if err != nil {
			// 回补失败不留半启动订阅：撤销登记后退出，调用方可重试
			cancel()
			c.unsub(key)
			return fmt.Errorf("mq/redis/list: processing 回补失败: %w", err)
		}
		if n > 0 {
			c.logInfof("processing 回补完成: sub=%s, count=%d", sub.key, n)
		}
	}

	c.consumeLoop(runCtx, sub, handler)
	// 循环已退出：撤销登记使同一 group+topic 可重新订阅（在途登记不随之丢失，
	// 仍可由 Commit/Reject 确认直到 Close）
	c.unsub(key)
	return runCtx.Err()
}

// unsub 撤销本次订阅的全部登记（subs/cancels，含初始化失败回滚路径），
// 使同一 group+topic 可原地重新订阅；Close 后由 closed 标志拒绝新订阅。
func (c *Consumer) unsub(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subs, key)
	if cancel, ok := c.cancels[key]; ok {
		delete(c.cancels, key)
		cancel() // 释放订阅派生 ctx，重复 cancel 幂等
	}
}

// recoverProcessing 分批把 processing 残留回补 ready：单脚本条数有上限
// （防超长脚本阻塞 Redis），循环至搬空。
func (c *Consumer) recoverProcessing(ctx context.Context, sub *subscription) (int64, error) {
	var total int64
	for {
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
		moved, err := c.client.Eval(ctx, recoverScript, []string{sub.processing, sub.ready}, recoverBatchLimit).Int64()
		if err != nil {
			return total, err
		}
		total += moved
		if moved < recoverBatchLimit {
			return total, nil
		}
	}
}

// consumeLoop 领取循环：Lua 原子领取 → 分发；队列空按 PollInterval 轮询。
// （List 无消费者模型下的阻塞读取原语，短轮询换取 processing 语义的完整性）
func (c *Consumer) consumeLoop(ctx context.Context, sub *subscription, handler mq.Handler) {
	for {
		if ctx.Err() != nil {
			return
		}

		raw, err := c.claim(ctx, sub)
		if errors.Is(err, redis.Nil) {
			// 队列空：等待轮询间隔，期间 ctx 取消可立即退出
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

		c.dispatch(ctx, sub, raw, handler)
	}
}

// claim 原子领取一条消息，无消息时返回 redis.Nil。
func (c *Consumer) claim(ctx context.Context, sub *subscription) (string, error) {
	v, err := c.client.Eval(ctx, claimScript, []string{sub.ready, sub.processing}).Result()
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return "", ctx.Err()
		}
		return "", err
	}
	raw, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("mq/redis/list: 领取返回值类型异常: %T", v)
	}
	return raw, nil
}

// dispatch 解码并分发单条消息，按 Handler 返回值完成确认分派。
func (c *Consumer) dispatch(ctx context.Context, sub *subscription, raw string, handler mq.Handler) {
	deliveryID := c.nextDeliveryID()
	topic := c.topicOf(sub)

	e, decErr := envpkg.Decode(raw)
	if decErr != nil {
		// 毒消息：无法解码信封不重试（重投也必然失败），直接终态处置
		poison := &mq.Message{ID: deliveryID, Topic: topic, Metadata: raw}
		c.terminal(ctx, sub, poison, raw, fmt.Errorf("消息信封反序列化失败: %w", decErr))
		return
	}
	msg := e.ToMessage(topic, deliveryID, raw)

	// 从消息头还原上游链路并开 CONSUMER span，handler 拿到贯通后的 ctx。
	hctx, end := trace.StartConsumerSpan(ctx, traceName, "list receive "+topic, msg.Headers)
	defer end()
	err := mq.InvokeHandler(hctx, handler, msg)
	switch {
	case err == nil:
		if ackErr := c.ack(ctx, sub, raw); ackErr != nil {
			c.logErrorf("提交失败: sub=%s, id=%s, err=%v", sub.key, msg.ID, ackErr)
		}
	case errors.Is(err, mq.ErrManualAck):
		c.mu.Lock()
		if c.closed { // Close 与挂起登记竞态：不留悬挂条目，消息滞留 processing 供回补
			c.mu.Unlock()
			c.logWarnf("消费者已关闭，放弃挂起在途登记: topic=%s, id=%s", msg.Topic, msg.ID)
			return
		}
		c.inFlight[msg.ID] = &flightRef{sub: sub, raw: raw, msg: msg}
		c.mu.Unlock()
	default:
		c.handleFailure(ctx, sub, e, msg, raw, err)
	}
}

// handleFailure 失败策略：额度内信封 attempt+1 原子回队重投，耗尽进终态。
func (c *Consumer) handleFailure(ctx context.Context, sub *subscription, e *envpkg.Envelope, msg *mq.Message, raw string, handleErr error) {
	attempt := e.Attempt
	if attempt < c.cfg.MaxRequeue {
		retryEnv := e.Clone()
		retryEnv.Attempt = attempt + 1
		payload, err := retryEnv.Encode()
		if err != nil {
			c.terminal(ctx, sub, msg, raw, fmt.Errorf("重投信封序列化失败: %w（原始错误: %v）", err, handleErr))
			return
		}
		if _, err := c.client.Eval(ctx, requeueScript,
			[]string{sub.processing, sub.ready}, raw, payload).Result(); err != nil {
			if ctx.Err() == nil {
				c.logErrorf("重投入队失败: sub=%s, id=%s, err=%v", sub.key, msg.ID, err)
			}
			// 回队失败：消息留在 processing，不重复消费也不丢确认登记
		}
		return
	}
	c.terminal(ctx, sub, msg, raw, fmt.Errorf("重试额度耗尽（attempt=%d）: %w", attempt, handleErr))
}

// terminal 终态处置：可选入死信 List（payload 原文）→ 回调 Rejected 钩子。
// 无论死信与钩子结果如何，最后都从 processing 移除，防止残留反复回收。
func (c *Consumer) terminal(ctx context.Context, sub *subscription, msg *mq.Message, raw string, cause error) {
	if c.cfg.DeadLetterKey != "" {
		if err := c.client.LPush(ctx, c.cfg.DeadLetterKey, raw).Err(); err != nil && ctx.Err() == nil {
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
	if err := c.ack(ctx, sub, raw); err != nil && ctx.Err() == nil {
		c.logErrorf("终态清理 processing 失败: id=%s, err=%v", msg.ID, err)
	}
	c.forget(msg.ID)
}

// Commit 确认在途消息处理完成（从 processing 移除），msg 必须来自本实例 Handler。
// 条目已被 recover 回补等场景移除时幂等返回 nil。
func (c *Consumer) Commit(ctx context.Context, msg *mq.Message) error {
	ref, err := c.takeInFlight(msg)
	if err != nil {
		return err
	}
	if err := c.ack(ctx, ref.sub, ref.raw); err != nil {
		// 恢复登记，允许业务重试确认
		c.restoreInFlight(ref)
		return err
	}
	return nil
}

// Reject 显式判定在途消息失败。requeue=true 时信封 attempt+1 后重新入队
// （重投失败返回 error 且条目恢复登记）；requeue=false 时仅移除 processing 条目，
// 需要先入死信/暂存队列的请业务自行完成（或使用 Options.DeadLetterKey 走自动终态路径）。
func (c *Consumer) Reject(ctx context.Context, msg *mq.Message, requeue bool) error {
	if !requeue {
		ref, err := c.takeInFlight(msg)
		if err != nil {
			return err
		}
		if err := c.ack(ctx, ref.sub, ref.raw); err != nil {
			c.restoreInFlight(ref)
			return err
		}
		return nil
	}

	ref, err := c.takeInFlight(msg)
	if err != nil {
		return err
	}
	e, decErr := envpkg.Decode(ref.raw)
	if decErr != nil {
		c.restoreInFlight(ref)
		return fmt.Errorf("mq/redis/list: 在途消息信封不可重投: %w", decErr)
	}
	retryEnv := e.Clone()
	retryEnv.Attempt = e.Attempt + 1
	payload, err := retryEnv.Encode()
	if err != nil {
		c.restoreInFlight(ref)
		return err
	}
	if _, err := c.client.Eval(ctx, requeueScript,
		[]string{ref.sub.processing, ref.sub.ready}, ref.raw, payload).Result(); err != nil {
		c.restoreInFlight(ref)
		return err
	}
	return nil
}

// Close 终止全部订阅循环并释放在途登记，幂等。client 归调用方，不在此关闭。
// Close 后 processing 中的未确认消息留存于列表，可由新实例 RecoverProcessing 回补。
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

// ack 从 processing 精确移除 payload 原文（LREM count=1）。
func (c *Consumer) ack(ctx context.Context, sub *subscription, raw string) error {
	if sub == nil {
		return fmt.Errorf("mq/redis/list: 无法定位消息所属订阅")
	}
	return c.client.LRem(ctx, sub.processing, 1, raw).Err()
}

// takeInFlight 校验并摘除在途登记条目，未知消息返回 mq.ErrNotSubscribed。
func (c *Consumer) takeInFlight(msg *mq.Message) (*flightRef, error) {
	if msg == nil {
		return nil, fmt.Errorf("mq/redis/list: msg 不能为 nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, mq.ErrConsumerClosed
	}
	ref, ok := c.inFlight[msg.ID]
	if !ok {
		return nil, fmt.Errorf("mq/redis/list: 消息 %q 不在本消费者在途登记中（须来自本实例 Handler）: %w", msg.ID, mq.ErrNotSubscribed)
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

// nextDeliveryID 生成本实例内唯一投递 ID：<领取者>-<纳秒>-<序号>。
func (c *Consumer) nextDeliveryID() string {
	return fmt.Sprintf("%s-%d-%d", c.consumer, time.Now().UnixNano(), c.deliverySeq.Add(1))
}

// subOfMessage 已下沉至 terminal 参数；此处仅保留 topicOf。

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
	return logrus.WithField("component", "mq/redis/list")
}
