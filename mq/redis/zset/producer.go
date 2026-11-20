// Package zset（mq/redis/zset）提供基于 Redis Sorted Set 的 mq 门面适配器（延迟/优先级队列）。
//
// 队列模型：score 即「可见时间」（unix 毫秒），ZADD 入队；消费侧 Lua 原子领取
// 到期消息（ready → processing，processing 中 score 重置为领取时刻作为认领时间），
// Commit/Reject 以 ZREM 从 processing 移除完成确认。延迟消息通过消息头
// X-Mq-Delay / X-Mq-Ready-At 指定，无需额外定时任务。
//
// 与 List 后端的关键差异：集合成员唯一，信封必须携带持久消息 ID（Produce 时
// 为空自动生成），相同内容两条消息不会因 member 相同而互相覆盖；
// 认领时间使 processing 具备可见性超时可回收——超时未确认的消息由后台协程
// 挪回 ready 重新投递（at-least-once，需业务幂等）。
package zset

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/xm-utils/tools/mq/core"
	envpkg "github.com/xm-utils/tools/mq/redis/internal/envelope"
	"github.com/xm-utils/tools/trace"
)

// traceName 链路追踪 tracer 名（与错误前缀口径一致）。
const traceName = "mq/redis/zset"

const (
	// HeaderAttempt 重投次数消息头（门面单点定义，与 mq.HeaderAttempt 同值）。
	HeaderAttempt = mq.HeaderAttempt
	// HeaderDelay 相对延迟消息头，值为毫秒数；与 X-Mq-Ready-At 同存时后者优先。
	HeaderDelay = "X-Mq-Delay"
	// HeaderReadyAt 绝对可见时间消息头，值为 unix 毫秒；解析失败按立即投递。
	HeaderReadyAt = "X-Mq-Ready-At"
)

const (
	defaultKeyPrefix    = "mq:zset:"
	processingSuffix    = ":processing"
	defaultMaxRequeue   = 3
	defaultPollInterval = 100 * time.Millisecond
	// defaultReclaimBatch 单轮可见性回收的移动条数上限。
	defaultReclaimBatch = 50
)

// idSeq 进程内消息 ID 序号，参与生成 z-<unixnano>-<seq> 形式的唯一标识。
var idSeq atomic.Int64

// Options 生产者/消费者共用装配项，nil 表示全默认。
type Options struct {
	// KeyPrefix ready/processing 有序集合 key 前缀，默认 "mq:zset:"。
	KeyPrefix string

	// ConsumerName 领取者标识，参与日志与 ID 兜底生成；空则自动生成（仅消费者使用）。
	ConsumerName string

	// MaxRequeue Handler 失败后的重投额度（信封 attempt 计数）；0 取默认 3，负数关闭重投。
	MaxRequeue int

	// DeadLetterKey 终态失败消息转入的死信有序集合 key（score=失败时刻）；空则不转死信。
	DeadLetterKey string

	// Rejected 终态失败钩子（含毒消息），入死信之后调用；panic 由内部 recover。
	Rejected func(ctx context.Context, msg *mq.Message, err error)

	// PollInterval 无可领取消息时轮询间隔，默认 100ms。
	PollInterval time.Duration

	// VisibilityTimeout processing 中消息的认领可见性超时；超过该时长未确认的
	// 消息由后台协程挪回 ready 重新投递。<=0 关闭回收（崩溃残留须业务自行处理）。
	VisibilityTimeout time.Duration

	// ReclaimBatch 单轮可见性回收的移动条数上限，默认 50。
	ReclaimBatch int
}

func (o *Options) normalized() Options {
	n := Options{
		KeyPrefix:    defaultKeyPrefix,
		MaxRequeue:   defaultMaxRequeue,
		PollInterval: defaultPollInterval,
		ReclaimBatch: defaultReclaimBatch,
	}
	if o == nil {
		return n
	}
	if o.KeyPrefix != "" {
		n.KeyPrefix = o.KeyPrefix
	}
	n.ConsumerName = o.ConsumerName
	if o.MaxRequeue != 0 {
		n.MaxRequeue = o.MaxRequeue
	}
	n.DeadLetterKey = o.DeadLetterKey
	n.Rejected = o.Rejected
	if o.PollInterval > 0 {
		n.PollInterval = o.PollInterval
	}
	n.VisibilityTimeout = o.VisibilityTimeout
	if o.ReclaimBatch > 0 {
		n.ReclaimBatch = o.ReclaimBatch
	}
	return n
}

// Producer Redis 有序集合生产者适配器，实现 mq.Producer。
type Producer struct {
	client *redis.Client
	prefix string
}

var _ mq.Producer = (*Producer)(nil)

// NewProducer 基于已建立的 Redis 连接创建生产者适配器。
// client 由调用方持有并负责关闭（Close 不会关闭它）；opts 为 nil 时使用默认装配。
func NewProducer(client *redis.Client, opts *Options) *Producer {
	return &Producer{client: client, prefix: opts.normalized().KeyPrefix}
}

// Produce 入队一条消息（ZADD，score=可见时间），msg.ID 为空时回填生成的消息 ID。
// 延迟经消息头 X-Mq-Delay（毫秒）或 X-Mq-Ready-At（unix 毫秒）指定。
func (p *Producer) Produce(ctx context.Context, topic string, msg *mq.Message) error {
	if msg == nil {
		return fmt.Errorf("mq/redis/zset: 消息不能为 nil")
	}
	if topic == "" {
		return fmt.Errorf("mq/redis/zset: topic 不能为空")
	}
	// 开启 PRODUCER span 并把 traceparent 写入消息头（经信封 Headers 传播）。
	if msg.Headers == nil {
		msg.Headers = map[string]string{}
	}
	_, end := trace.StartProducerSpan(ctx, traceName, "zset produce "+topic, msg.Headers)
	defer end()
	e, payload, err := p.pack(msg)
	if err != nil {
		return err
	}
	score := readyAtMillis(msg)
	if err := p.client.ZAdd(ctx, p.readyKey(topic), &redis.Z{Score: score, Member: payload}).Err(); err != nil {
		return fmt.Errorf("mq/redis/zset: 消息入队失败: %w", err)
	}
	msg.ID = e.ID
	return nil
}

// ProduceBatch 批量入队：单命令 ZADD 多变体。
func (p *Producer) ProduceBatch(ctx context.Context, topic string, msgs []*mq.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if topic == "" {
		return fmt.Errorf("mq/redis/zset: topic 不能为空")
	}
	// 整批共用一个 PRODUCER span，逐个注入同一 traceparent。
	batchCtx, end := trace.StartProducerSpan(ctx, traceName, "zset produce "+topic, nil)
	defer end()
	zs := make([]*redis.Z, 0, len(msgs))
	for _, m := range msgs {
		if m == nil {
			return fmt.Errorf("mq/redis/zset: 批量消息中包含 nil 元素")
		}
		if m.Headers == nil {
			m.Headers = map[string]string{}
		}
		trace.InjectMap(batchCtx, m.Headers)
		e, payload, err := p.pack(m)
		if err != nil {
			return err
		}
		m.ID = e.ID
		zs = append(zs, &redis.Z{Score: readyAtMillis(m), Member: payload})
	}
	if err := p.client.ZAdd(ctx, p.readyKey(topic), zs...).Err(); err != nil {
		return fmt.Errorf("mq/redis/zset: 批量消息入队失败: %w", err)
	}
	return nil
}

// Close 无共享资源可释放（client 归调用方所有），重复调用安全。
func (p *Producer) Close() error { return nil }

// pack 信封编码；集合成员必须唯一，ID 为空时生成持久消息 ID 写回信封。
func (p *Producer) pack(msg *mq.Message) (*envpkg.Envelope, string, error) {
	e := envpkg.FromMessage(msg)
	if e.ID == "" {
		e.ID = newMessageID()
	}
	payload, err := e.Encode()
	if err != nil {
		return nil, "", err
	}
	return e, payload, nil
}

// readyKey 就绪队列 key。
func (p *Producer) readyKey(topic string) string { return p.prefix + topic }

// readyAtMillis 解析消息可见时间（unix 毫秒）：X-Mq-Ready-At 优先，其次
// X-Mq-Delay（相对延迟毫秒），均缺失/非法按当前时刻（立即投递）。
func readyAtMillis(msg *mq.Message) float64 {
	now := float64(time.Now().UnixMilli())
	if v := msg.Header(HeaderReadyAt); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms > 0 {
			return float64(ms)
		}
	}
	if v := msg.Header(HeaderDelay); v != "" {
		if d, err := strconv.ParseInt(v, 10, 64); err == nil && d > 0 {
			return now + float64(d)
		}
	}
	return now
}

// newMessageID 生成 z-<unixnano>-<seq> 形式的进程内唯一 ID。
func newMessageID() string {
	return "z-" + strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.FormatInt(idSeq.Add(1), 10)
}
