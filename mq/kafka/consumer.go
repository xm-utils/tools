package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	segkafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	xmkafka "github.com/xm-utils/tools/kafka"

	"github.com/xm-utils/tools/mq"
)

// 默认失败重投上限：Kafka 无原生单消息重投，重投经生产者回环实现，
// 必须有额度上限防止「生产-消费-再生产」热循环；逐消息精细重试语义
// 建议交给上层 pipeline 引擎（mq/pipequeue + Retryer）承载。
const defaultMaxRequeue = 3

// readFailBackoff 拉取失败后的退避间隔，防底层持续报错时热循环刷日志。
const readFailBackoff = 500 * time.Millisecond

// Consumer Kafka 消费者适配器，实现 mq.Consumer。
//
// 以手动 offset 模式（FetchMessage + CommitMessages）逐条消费：
//   - Handler 返回 nil → 提交 offset；
//   - 返回 mq.ErrManualAck → 不提交，等待业务显式 Commit/Reject；
//   - 返回其他 error → 若配置了重投生产者且未超额度，attempt+1 回环重投后提交；
//     否则视为终态失败：不提交前移、回调 Rejected 钩子后跳过
//     （Kafka offset 分区级前移，跳过消息会被后续提交隐性确认，重投/死信请在钩子内完成）。
type Consumer struct {
	template xmkafka.ConsumerConfig
	log      *logrus.Entry

	requeueProducer mq.Producer // 失败重投通道（可选）
	rejected        func(ctx context.Context, msg *mq.Message, err error)
	maxRequeue      int

	mu        sync.Mutex
	subs      map[string]*subscription      // key: group+"\x00"+topic
	inFlight  map[string]*inFlightRef       // key: mq.Message.ID
	cancels   map[string]context.CancelFunc // key 同 subs，随订阅退出回收，防常驻重订阅累积
	closeOnce sync.Once
	closed    bool
}

var _ mq.Consumer = (*Consumer)(nil)

// subscription 一次 Subscribe 建立的底层消费者及其派生键。
type subscription struct {
	cons  *xmkafka.Consumer
	key   string
	group string
}

// inFlightRef ErrManualAck 模式下登记的在途消息，供 Commit/Reject 定位 offset 归属。
type inFlightRef struct {
	sub    *subscription
	native segkafka.Message
}

// Option 消费者可选配置。
type Option func(*Consumer)

// WithRequeueProducer 指定失败重投用的生产者（推荐 *Producer，也可用其他 mq.Producer）。
// 未配置时失败消息直接进入终态处置。
func WithRequeueProducer(p mq.Producer) Option {
	return func(c *Consumer) { c.requeueProducer = p }
}

// WithMaxRequeue 覆盖默认重投额度（<=0 视为不重投）。
func WithMaxRequeue(n int) Option {
	return func(c *Consumer) { c.maxRequeue = n }
}

// WithRejected 终态失败钩子：消息被判定放弃时（额度耗尽或未配置重投生产者）调用，
// 典型用途：落死信（deadletter）或告警。钩子内 panic 会被 recover 并记日志。
func WithRejected(fn func(ctx context.Context, msg *mq.Message, err error)) Option {
	return func(c *Consumer) { c.rejected = fn }
}

// WithLogger 覆盖日志出口（默认 module=mq/kafka）。
func WithLogger(entry *logrus.Entry) Option {
	return func(c *Consumer) {
		if entry != nil {
			c.log = entry
		}
	}
}

// NewConsumer 创建消费者适配器。cfg 作为模板拷贝保存（Brokers 等连接参数必填，
// GroupID/Topic 由 Subscribe 调用时给定）；配置非法返回 error 而非 panic。
func NewConsumer(cfg *xmkafka.ConsumerConfig, opts ...Option) (*Consumer, error) {
	if cfg == nil {
		return nil, fmt.Errorf("mq/kafka: 消费者配置不能为 nil")
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("mq/kafka: 消费者配置缺少 brokers")
	}
	c := &Consumer{
		template:   *cfg,
		log:        logrus.WithField("module", "mq/kafka Consumer"),
		maxRequeue: defaultMaxRequeue,
		subs:       make(map[string]*subscription),
		inFlight:   make(map[string]*inFlightRef),
		cancels:    make(map[string]context.CancelFunc),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Subscribe 订阅 group-topic 并阻塞消费，直到 ctx 结束或 Close。退出返回值：
// ctx 结束为 ctx.Err()，Close 触发为 context.Canceled，其余非 nil error 仅为
// 初始化失败。同一 group-topic 重复订阅返回 error，退出后可重新订阅。
func (c *Consumer) Subscribe(ctx context.Context, group, topic string, handler mq.Handler) error {
	if group == "" || topic == "" {
		return fmt.Errorf("mq/kafka: group 与 topic 不能为空")
	}
	if handler == nil {
		return fmt.Errorf("mq/kafka: handler 不能为 nil")
	}

	runCtx, cancel := context.WithCancel(ctx)

	cfg := c.template
	cfg.GroupID = group
	cfg.Topic = topic
	cfg.Topics = nil
	// 门面语义：手动逐消息确认。CommitInterval 同步归零——非零时底层 Reader 会
	// 后台周期提交已读 offset，静默击穿 ErrManualAck 挂起与失败重投语义
	// （at-least-once 退化为 at-most-once）。
	cfg.AutoCommit = false
	cfg.CommitInterval = 0

	cons, err := xmkafka.NewConsumer(&cfg)
	if err != nil {
		cancel()
		return fmt.Errorf("mq/kafka: 创建消费者失败: %w", err)
	}

	key := subKey(group, topic)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		cons.Close()
		return mq.ErrConsumerClosed
	}
	if _, exists := c.subs[key]; exists {
		c.mu.Unlock()
		cancel()
		cons.Close()
		return fmt.Errorf("mq/kafka: group-topic 已订阅: %s/%s", group, topic)
	}
	sub := &subscription{cons: cons, key: key, group: group}
	c.subs[key] = sub
	c.cancels[key] = cancel
	c.mu.Unlock()

	c.log.Infof("开始订阅: group=%s, topic=%s", group, topic)
	defer func() {
		// 循环退出即释放本次建立的底层消费者（不等 Close，防调用方只取消 ctx 时泄漏），
		// 同时撤销登记使同一 group-topic 可重新订阅
		cancel()
		c.unsub(key)
		cons.Close()
	}()
	c.consumeLoop(runCtx, sub, handler)
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

// isClosed 消费者是否已关闭。
func (c *Consumer) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// consumeLoop 顺序拉取-处理-确认主循环。
func (c *Consumer) consumeLoop(ctx context.Context, sub *subscription, handler mq.Handler) {
	reader := sub.cons.GetReader()
	for {
		if ctx.Err() != nil {
			return
		}
		km, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Close 后底层 Reader 已关，再拉取只会永久报错：及时退出循环
			if c.isClosed() {
				return
			}
			// 拉取失败不视为终态：组件 Reader 内部已带重试退避，短暂错误记录后继续
			c.log.Errorf("拉取消息失败: sub=%s, err=%v", sub.key, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(readFailBackoff):
			}
			continue
		}
		msg := fromKafkaMessage(km)
		// 投递 ID 纳入 group 作用域：同一实例多 group 订阅同 topic 时
		// 同一 offset 的在途登记不互撞，Commit/Reject 能找到正确 reader
		msg.ID = flightID(sub.group, msg.ID)
		c.dispatch(ctx, sub, km, msg, handler)
	}
}

// dispatch 处理单条消息的确认分派：成功提交 / 手动确认登记 / 失败重投或终态。
func (c *Consumer) dispatch(ctx context.Context, sub *subscription, km segkafka.Message, msg *mq.Message, handler mq.Handler) {
	err := mq.InvokeHandler(ctx, handler, msg)
	switch {
	case err == nil:
		c.commitNative(ctx, sub, km, msg.ID)
	case errors.Is(err, mq.ErrManualAck):
		c.mu.Lock()
		if c.closed { // Close 与挂起登记竞态：不留悬挂条目，未提交 offset 由重平衡重投
			c.mu.Unlock()
			c.log.Warnf("消费者已关闭，放弃挂起在途登记: topic=%s, id=%s", msg.Topic, msg.ID)
			return
		}
		c.inFlight[msg.ID] = &inFlightRef{sub: sub, native: km}
		c.mu.Unlock()
	default:
		c.handleFailure(ctx, sub, km, msg, err)
	}
}

// handleFailure 失败处置：额度内且配置了重投生产者 → attempt+1 回环重投后提交 offset；
// 否则终态：回调 Rejected 钩子并登记放弃（不阻止后续消息前进）。
func (c *Consumer) handleFailure(ctx context.Context, sub *subscription, km segkafka.Message, msg *mq.Message, handleErr error) {
	attempt := attemptOf(msg)
	if c.requeueProducer != nil && attempt < c.maxRequeue {
		retry := cloneForRequeue(msg, attempt+1)
		if err := c.requeueProducer.Produce(ctx, msg.Topic, retry); err != nil {
			c.log.Errorf("失败消息重投失败: topic=%s, id=%s, err=%v", msg.Topic, msg.ID, err)
			c.terminal(ctx, msg, fmt.Errorf("重投失败: %w（原始错误: %v）", err, handleErr))
			return
		}
		// 重投成功后提交原消息 offset，避免双份在途
		c.commitNative(ctx, sub, km, msg.ID)
		c.log.Warnf("消息处理失败已重投: topic=%s, id=%s, attempt=%d, err=%v", msg.Topic, msg.ID, attempt+1, handleErr)
		return
	}
	c.terminal(ctx, msg, handleErr)
}

// terminal 终态失败：观测钩子（含 panic 防线），offset 不前移提交。
func (c *Consumer) terminal(ctx context.Context, msg *mq.Message, handleErr error) {
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

// Commit 显式确认 ErrManualAck 模式下的在途消息（提交其 offset）。
// 提交失败时恢复在途登记，允许业务重试确认。
func (c *Consumer) Commit(ctx context.Context, msg *mq.Message) error {
	ref, err := c.takeInFlight(msg)
	if err != nil {
		return err
	}
	if err := c.commitNative(ctx, ref.sub, ref.native, msg.ID); err != nil {
		c.restoreInFlight(msg.ID, ref)
		return err
	}
	return nil
}

// Reject 显式否定 ErrManualAck 模式下的在途消息：
// requeue=true 经重投生产者回环投递（需配置 WithRequeueProducer，attempt+1）；
// requeue=false 直接提交 offset 终态丢弃（死信留存请在调用前完成）。
func (c *Consumer) Reject(ctx context.Context, msg *mq.Message, requeue bool) error {
	ref, err := c.takeInFlight(msg)
	if err != nil {
		return err
	}
	if requeue {
		if c.requeueProducer == nil {
			// 未配置重投通道：把在途登记放回，让调用方感知配置缺失后自行处置
			c.restoreInFlight(msg.ID, ref)
			return fmt.Errorf("mq/kafka: 未配置 WithRequeueProducer，无法 requeue")
		}
		attempt := attemptOf(msg)
		retry := cloneForRequeue(msg, attempt+1)
		if err := c.requeueProducer.Produce(ctx, msg.Topic, retry); err != nil {
			c.restoreInFlight(msg.ID, ref)
			return fmt.Errorf("mq/kafka: requeue 重投失败: %w", err)
		}
	}
	if err := c.commitNative(ctx, ref.sub, ref.native, msg.ID); err != nil {
		// 提交失败恢复登记供重试；requeue 已成功的消息重试会形成重复投递，
		// 由 at-least-once 语义与业务幂等性吸收
		c.restoreInFlight(msg.ID, ref)
		return err
	}
	return nil
}

// Close 停止全部订阅并关闭底层消费者，重复调用安全。
// ErrManualAck 模式下未确认的在途消息随订阅关闭失去确认通道，
// 由 Kafka 消费组重平衡后重新投递（at-least-once）。
func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.closed = true
		for _, cancel := range c.cancels {
			cancel()
		}
		c.cancels = make(map[string]context.CancelFunc)
		for key, sub := range c.subs {
			sub.cons.Close()
			delete(c.subs, key)
		}
		for id := range c.inFlight {
			delete(c.inFlight, id)
		}
	})
	return nil
}

// takeInFlight 摘出在途消息登记；未登记（已确认/未知消息）返回 ErrNotSubscribed。
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

// commitNative 提交单条消息 offset 并清理对应在途登记。
func (c *Consumer) commitNative(ctx context.Context, sub *subscription, km segkafka.Message, id string) error {
	if err := sub.cons.GetReader().CommitMessages(ctx, km); err != nil {
		c.log.Errorf("提交 offset 失败: topic=%s, partition=%d, offset=%d, err=%v",
			km.Topic, km.Partition, km.Offset, err)
		return fmt.Errorf("mq/kafka: 提交 offset 失败: %w", err)
	}
	c.mu.Lock()
	delete(c.inFlight, id)
	c.mu.Unlock()
	return nil
}

// subKey 订阅唯一键。
func subKey(group, topic string) string {
	return group + "\x00" + topic
}

// flightID 投递 ID：group 作用域前缀，避免同一实例多 group 订阅同 topic 时
// 相同 offset 的在途登记互撞（mq.Message.ID 对业务为不透明令牌）。
func flightID(group, id string) string {
	return group + "|" + id
}

// cloneForRequeue 生成重投副本：深拷贝 Headers 与 Body，避免 SetHeader 或
// Handler 原地改写 Body 后污染调用方持有的原消息/后续重投内容。
func cloneForRequeue(msg *mq.Message, attempt int) *mq.Message {
	cp := *msg
	if msg.Headers != nil {
		cp.Headers = make(map[string]string, len(msg.Headers)+1)
		for k, v := range msg.Headers {
			cp.Headers[k] = v
		}
	}
	if msg.Body != nil {
		cp.Body = append([]byte(nil), msg.Body...)
	}
	cp.SetHeader(HeaderAttempt, strconv.Itoa(attempt))
	return &cp
}

// fromKafkaMessage kafka.Message → mq.Message。
func fromKafkaMessage(km segkafka.Message) *mq.Message {
	return &mq.Message{
		ID:       fmt.Sprintf("%s-%d-%d", km.Topic, km.Partition, km.Offset),
		Key:      string(km.Key),
		Topic:    km.Topic,
		Body:     km.Value,
		Headers:  fromSegHeaders(km.Headers),
		Metadata: km,
	}
}
