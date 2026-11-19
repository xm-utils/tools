// Package stream（mq/redis/stream）提供基于 github.com/xm-utils/tools/redis_stream_queue 的
// mq 门面适配器。topic 映射为 Stream key，group 映射为消费者组。
//
// 消息体封装：mq.Message 的 Key/Body/Headers/重投计数序列化为共享 JSON 信封
// （mq/internal/envelope，与 redis/list、redis/zset 后端字节级兼容），
// 承载于组件 StreamMessage.Payload 字段（组件负责 XADD/XREADGROUP/XACK，
// 信封内容对组件透明）。Metadata 保留组件 *StreamMessage 以便取回 idle 时间等专有信息。
package stream

import (
	"context"
	"fmt"

	rs "github.com/xm-utils/tools/redis_stream_queue"

	"github.com/go-redis/redis/v8"
	"github.com/xm-utils/tools/mq"
	envpkg "github.com/xm-utils/tools/mq/internal/envelope"
)

// HeaderAttempt 重投次数消息头（共享信封单点定义，各后端适配器同名）。
const HeaderAttempt = envpkg.HeaderAttempt

// decodeEnvelope 反序列化信封，非法 JSON 返回 error（消费侧按毒消息进终态处置，不 panic）。
func decodeEnvelope(raw string) (*envpkg.Envelope, error) {
	return envpkg.Decode(raw)
}

// toEnvelope mq.Message → 共享信封（attempt 取自消息头，缺失/非法按 0）。
func toEnvelope(msg *mq.Message) *envpkg.Envelope {
	return envpkg.FromMessage(msg)
}

// toMessage 信封 + 组件消息 → mq.Message（entry ID 为投递主键，覆盖信封 ID）。
func toMessage(topic string, sm *rs.StreamMessage, e *envpkg.Envelope) *mq.Message {
	return e.ToMessage(topic, sm.MessageID, sm)
}

// Producer Redis Stream 生产者适配器，实现 mq.Producer。
type Producer struct {
	client *redis.Client
}

var _ mq.Producer = (*Producer)(nil)

// NewProducer 基于已建立的 Redis 连接创建生产者适配器。
// client 由调用方持有并负责关闭（Close 不会关闭它）。
func NewProducer(client *redis.Client) *Producer {
	return &Producer{client: client}
}

// NewProducerWithConfig 按组件 RedisConfig 建立连接并创建生产者适配器；
// 返回的 client 与适配器共享，调用方需在不再使用后自行关闭 client。
func NewProducerWithConfig(cfg *rs.RedisConfig) (*Producer, *redis.Client, error) {
	client, err := rs.CreateRedisClient(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("mq/redis/stream: 创建 Redis 连接失败: %w", err)
	}
	return &Producer{client: client}, client, nil
}

// Produce 向指定 Stream 发送一条消息，返回后 msg.ID 回填为 entry ID。
func (p *Producer) Produce(ctx context.Context, topic string, msg *mq.Message) error {
	if msg == nil {
		return fmt.Errorf("mq/redis/stream: 消息不能为 nil")
	}
	if topic == "" {
		return fmt.Errorf("mq/redis/stream: topic(stream key) 不能为空")
	}
	env := toEnvelope(msg)
	payload, err := env.Encode()
	if err != nil {
		return err
	}
	sq, err := rs.NewStreamQueue(rs.DefaultQueueConfig(topic, "mq-producer"), p.client)
	if err != nil {
		return fmt.Errorf("mq/redis/stream: 创建队列句柄失败: %w", err)
	}
	id, err := sq.EnqueueMessage(ctx, &rs.StreamMessage{RequestID: msg.Key, Payload: payload})
	if err != nil {
		return fmt.Errorf("mq/redis/stream: 消息入队失败: %w", err)
	}
	msg.ID = id
	return nil
}

// ProduceBatch 批量发送消息，所有消息归属同一 Stream。
func (p *Producer) ProduceBatch(ctx context.Context, topic string, msgs []*mq.Message) error {
	for _, m := range msgs {
		if err := p.Produce(ctx, topic, m); err != nil {
			return err
		}
	}
	return nil
}

// Close 无共享资源可释放（client 归调用方所有），重复调用安全。
func (p *Producer) Close() error { return nil }
