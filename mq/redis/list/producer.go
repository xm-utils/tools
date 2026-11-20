// Package list（mq/redis/list）提供基于 Redis List 的 mq 门面适配器（安全队列模式）。
//
// 队列模型：LPUSH 入队 + Lua 原子「RPOP ready → LPUSH processing」领取，
// Commit/Reject 以 LREM 从 processing 移除完成确认——消息在被确认前始终留存于
// processing 列表，进程崩溃后可由 RecoverProcessing 回补，实现 at-least-once。
//
// 与 Stream/ZSet 后端的差异：List 无消费者组与 entry ID 语义，
// group 参数仅作订阅登记与日志维度，不做队列隔离；多实例并发消费同一 topic
// 共享同一 ready/processing 列表（互斥领取由 Lua 原子性保证，不会双领，
// 但 RecoverProcessing 回补假设为单活动消费者实例）。
package list

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/xm-utils/tools/mq/core"
	envpkg "github.com/xm-utils/tools/mq/redis/internal/envelope"
)

// HeaderAttempt 重投次数消息头（门面单点定义，与 mq.HeaderAttempt 同值）。
const HeaderAttempt = mq.HeaderAttempt

const (
	defaultKeyPrefix = "mq:list:"
	processingSuffix = ":processing"

	// defaultMaxRequeue Handler 失败后信封重投额度，防持续失败消息热循环。
	defaultMaxRequeue = 3
	// defaultPollInterval 队列空时轮询间隔（List 无阻塞领取命令可用，采用短周期 polling）。
	defaultPollInterval = 100 * time.Millisecond
)

// idSeq 进程内消息 ID 序号，参与生成 l-<unixnano>-<seq> 形式的唯一标识。
var idSeq atomic.Int64

// Options 生产者/消费者共用装配项，nil 表示全默认。
type Options struct {
	// KeyPrefix ready/processing 列表 key 前缀，默认 "mq:list:"。
	KeyPrefix string

	// ConsumerName 领取者标识，参与投递 ID 生成与日志；空则自动生成（仅消费者使用）。
	ConsumerName string

	// MaxRequeue Handler 失败后的重投额度（信封 attempt 计数）；0 取默认 3，负数关闭重投。
	MaxRequeue int

	// DeadLetterKey 终态失败消息转入的死信 List key；空则不转死信（仅钩子观测）。
	DeadLetterKey string

	// Rejected 终态失败钩子（含毒消息），入死信之后调用；panic 由内部 recover。
	Rejected func(ctx context.Context, msg *mq.Message, err error)

	// PollInterval 队列空时轮询间隔，默认 100ms。
	PollInterval time.Duration

	// RecoverProcessing 订阅启动时把 processing 残留回补 ready（单活动消费者假设），默认关闭。
	RecoverProcessing bool
}

func (o *Options) normalized() Options {
	n := Options{
		KeyPrefix:    defaultKeyPrefix,
		MaxRequeue:   defaultMaxRequeue,
		PollInterval: defaultPollInterval,
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
	n.RecoverProcessing = o.RecoverProcessing
	return n
}

// Producer Redis List 生产者适配器，实现 mq.Producer。
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

// Produce 入队一条消息（LPUSH 到 ready 列表头），msg.ID 为空时回填生成的消息 ID。
// 消费端从尾端 RPOP 领取，天然 FIFO。
func (p *Producer) Produce(ctx context.Context, topic string, msg *mq.Message) error {
	if msg == nil {
		return fmt.Errorf("mq/redis/list: 消息不能为 nil")
	}
	if topic == "" {
		return fmt.Errorf("mq/redis/list: topic 不能为空")
	}
	e := envpkg.FromMessage(msg)
	if e.ID == "" {
		e.ID = newMessageID()
	}
	payload, err := e.Encode()
	if err != nil {
		return err
	}
	if err := p.client.LPush(ctx, p.readyKey(topic), payload).Err(); err != nil {
		return fmt.Errorf("mq/redis/list: 消息入队失败: %w", err)
	}
	msg.ID = e.ID
	return nil
}

// ProduceBatch 批量入队：单命令 LPUSH 多变体，尾端依次可消费（FIFO 保序）。
func (p *Producer) ProduceBatch(ctx context.Context, topic string, msgs []*mq.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if topic == "" {
		return fmt.Errorf("mq/redis/list: topic 不能为空")
	}
	payloads := make([]interface{}, 0, len(msgs))
	for _, m := range msgs {
		if m == nil {
			return fmt.Errorf("mq/redis/list: 批量消息中包含 nil 元素")
		}
		e := envpkg.FromMessage(m)
		if e.ID == "" {
			e.ID = newMessageID()
		}
		payload, err := e.Encode()
		if err != nil {
			return err
		}
		m.ID = e.ID
		payloads = append(payloads, payload)
	}
	if err := p.client.LPush(ctx, p.readyKey(topic), payloads...).Err(); err != nil {
		return fmt.Errorf("mq/redis/list: 批量消息入队失败: %w", err)
	}
	return nil
}

// Close 无共享资源可释放（client 归调用方所有），重复调用安全。
func (p *Producer) Close() error { return nil }

// readyKey 就绪队列 key。
func (p *Producer) readyKey(topic string) string { return p.prefix + topic }

// newMessageID 生成 l-<unixnano>-<seq> 形式的进程内唯一 ID。
func newMessageID() string {
	return "l-" + strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.FormatInt(idSeq.Add(1), 10)
}
