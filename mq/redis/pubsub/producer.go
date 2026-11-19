// Package pubsub（mq/redis/pubsub）提供基于 Redis Publish / Subscribe 的 mq 门面适配器（广播通道）。
//
// 与队列型后端的根本差异：Redis pub/sub 无持久化、无消费者组、无逐消息确认原语，
// 消息只投递给「发布瞬间在线」的订阅者（离线即丢，不补投），因此本适配器的投递保证
// 为 at-most-once，落到门面上的具体表现：
//   - Handler 返回 nil 或 mq.ErrManualAck 均视为投递完成，Commit/Reject 为 no-op 返回 nil；
//   - Handler 返回其他 error 只能观测（Options.Rejected 钩子 + 日志），无法重投；
//   - 需要重投额度、死信、崩溃恢复等可靠性语义请改用 redis/list、redis/zset 或 redis/stream。
//
// 适用场景：配置刷新、缓存失效通知、在线事件广播等「丢一条无所谓、下游随时上下线」的
// 一次性消息。同一消息需要被多个实例各自处理时，本后端是唯一「每实例都收到」的适配器
// （队列型后端多实例分流，一条消息只投一个消费者）。
//
// 消息体沿用共享 JSON 信封（mq/internal/envelope），与其余 Redis 后端字节级兼容。
// topic 映射为频道名（前缀由 Options.ChannelPrefix 控制），group 参数仅作订阅登记
// 与日志维度，无消费组语义。
package pubsub

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/xm-utils/tools/mq"
	envpkg "github.com/xm-utils/tools/mq/internal/envelope"
)

// HeaderAttempt 重投次数消息头（共享信封单点定义，各后端适配器同名）。
// pub/sub 无重投能力，该计数恒为 0，仅为跨后端 payload 结构一致性保留。
const HeaderAttempt = envpkg.HeaderAttempt

const (
	defaultChannelPrefix = "mq:ps:"

	// defaultChannelBuffer 订阅侧内部缓冲的消息条数。go-redis v8 缓冲满时接收协程
	// 最长阻塞 chanSendTimeout（1 分钟，库层不可配）后丢弃当时那条新消息，
	// 调大可平滑短时突发，但不能消除慢 Handler 带来的投递延迟。
	defaultChannelBuffer = 100
)

// idSeq 进程内消息 ID 序号，参与生成 p-<unixnano>-<seq> 形式的唯一标识。
var idSeq atomic.Int64

// Options 生产者/消费者共用装配项，nil 表示全默认。
type Options struct {
	// ChannelPrefix 频道名前缀，默认 "mq:ps:"。
	ChannelPrefix string

	// ConsumerName 订阅者标识，参与投递 ID 生成与日志；空则自动生成（仅消费者使用）。
	ConsumerName string

	// Pattern topic 按 PSUBSCRIBE 通配匹配（支持 "event.*" 形式 glob），默认 false。
	// 开启后 topic 即 glob 模式，Produce 仍须使用具体频道名。
	Pattern bool

	// ChannelBuffer 订阅内部缓冲条数，0 取默认 100；缓冲满时接收协程会阻塞至多 1 分钟，
	// 超时后丢弃该条新消息（go-redis v8 行为，非「丢弃最旧」）。
	ChannelBuffer int

	// Rejected Handler 失败/毒消息观测钩子（无法重投，仅告知）；panic 由内部 recover。
	Rejected func(ctx context.Context, msg *mq.Message, err error)

	// HealthCheckInterval 订阅连接探活间隔（发现断链则自动重连重订阅），
	// 0 取默认 3s，负数关闭（关闭后连接异常会静默失去消息，不建议）。
	HealthCheckInterval time.Duration
}

func (o *Options) normalized() Options {
	n := Options{
		ChannelPrefix: defaultChannelPrefix,
		ChannelBuffer: defaultChannelBuffer,
	}
	if o == nil {
		return n
	}
	if o.ChannelPrefix != "" {
		n.ChannelPrefix = o.ChannelPrefix
	}
	n.ConsumerName = o.ConsumerName
	n.Pattern = o.Pattern
	if o.ChannelBuffer > 0 {
		n.ChannelBuffer = o.ChannelBuffer
	}
	n.Rejected = o.Rejected
	n.HealthCheckInterval = o.HealthCheckInterval
	return n
}

// Producer Redis 发布适配器，实现 mq.Producer。
type Producer struct {
	client *redis.Client
	prefix string
}

var _ mq.Producer = (*Producer)(nil)

// NewProducer 基于已建立的 Redis 连接创建发布者适配器。
// client 由调用方持有并负责关闭（Close 不会关闭它）；opts 为 nil 时使用默认装配。
func NewProducer(client *redis.Client, opts *Options) *Producer {
	return &Producer{client: client, prefix: opts.normalized().ChannelPrefix}
}

// Produce 向频道发布一条消息（PUBLISH），msg.ID 为空时回填生成的消息 ID。
// 无在线订阅者时消息直接丢失（Redis pub/sub 语义），不构成错误。
func (p *Producer) Produce(ctx context.Context, topic string, msg *mq.Message) error {
	if msg == nil {
		return fmt.Errorf("mq/redis/pubsub: 消息不能为 nil")
	}
	if topic == "" {
		return fmt.Errorf("mq/redis/pubsub: topic(频道) 不能为空")
	}
	e := envpkg.FromMessage(msg)
	if e.ID == "" {
		e.ID = newMessageID()
	}
	payload, err := e.Encode()
	if err != nil {
		return err
	}
	if err := p.client.Publish(ctx, p.channelKey(topic), payload).Err(); err != nil {
		return fmt.Errorf("mq/redis/pubsub: 消息发布失败: %w", err)
	}
	msg.ID = e.ID
	return nil
}

// ProduceBatch 逐条发布（管道批量提交，一次往返）。任一命令失败即返回 error，
// 已成功的发布无法回滚——pub/sub 无事务语义，部分成功属预期结果。
func (p *Producer) ProduceBatch(ctx context.Context, topic string, msgs []*mq.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if topic == "" {
		return fmt.Errorf("mq/redis/pubsub: topic(频道) 不能为空")
	}
	channel := p.channelKey(topic)
	pipe := p.client.Pipeline()
	for _, m := range msgs {
		if m == nil {
			return fmt.Errorf("mq/redis/pubsub: 批量消息中包含 nil 元素")
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
		pipe.Publish(ctx, channel, payload)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("mq/redis/pubsub: 批量消息发布失败: %w", err)
	}
	return nil
}

// Close 无共享资源可释放（client 归调用方所有），重复调用安全。
func (p *Producer) Close() error { return nil }

// channelKey 频道全名。
func (p *Producer) channelKey(topic string) string { return p.prefix + topic }

// newMessageID 生成 p-<unixnano>-<seq> 形式的进程内唯一 ID。
func newMessageID() string {
	return "p-" + strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.FormatInt(idSeq.Add(1), 10)
}
