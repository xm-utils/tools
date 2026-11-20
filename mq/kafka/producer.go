// Package kafka 提供基于 github.com/xm-utils/tools/kafka 的 mq 门面适配器。
//
// Producer 复用组件 Producer（底层 kafka.Writer）发送消息，mq.Message 的
// Headers 映射为 Kafka 消息头；Consumer 以 FetchMessage/CommitMessages 手动
// offset 模式实现逐消息确认语义。切换 Kafka ↔ Redis Stream 时业务代码零改动。
package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	segkafka "github.com/segmentio/kafka-go"
	xmkafka "github.com/xm-utils/tools/kafka"

	"github.com/xm-utils/tools/mq"
)

// HeaderAttempt 重投次数消息头（门面单点定义，与 mq.HeaderAttempt 同值）；
// Consumer 失败重投与 Reject(requeue=true) 均自增该值。
const HeaderAttempt = mq.HeaderAttempt

// Producer Kafka 生产者适配器，实现 mq.Producer。
type Producer struct {
	inner *xmkafka.Producer
}

var _ mq.Producer = (*Producer)(nil)

// NewProducer 基于组件 ProducerConfig 创建生产者适配器。
func NewProducer(cfg *xmkafka.ProducerConfig) *Producer {
	return &Producer{inner: xmkafka.NewProducer(cfg)}
}

// Produce 发送一条消息。msg.Topic 字段被忽略，以 topic 参数为准。
func (p *Producer) Produce(ctx context.Context, topic string, msg *mq.Message) error {
	if msg == nil {
		return fmt.Errorf("mq/kafka: 消息不能为 nil")
	}
	km, err := toKafkaMessage(topic, msg)
	if err != nil {
		return err
	}
	// 统一走 PublishBatch（底层 WriteMessages），保留 Headers 映射能力
	return p.inner.PublishBatch(ctx, []segkafka.Message{km})
}

// ProduceBatch 批量发送消息，所有消息归属 topic。
func (p *Producer) ProduceBatch(ctx context.Context, topic string, msgs []*mq.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	kms := make([]segkafka.Message, 0, len(msgs))
	for _, m := range msgs {
		if m == nil {
			return fmt.Errorf("mq/kafka: 批量消息中包含 nil 元素")
		}
		km, err := toKafkaMessage(topic, m)
		if err != nil {
			return err
		}
		kms = append(kms, km)
	}
	return p.inner.PublishBatch(ctx, kms)
}

// Close 关闭生产者，重复调用安全。
func (p *Producer) Close() error {
	p.inner.Close()
	return nil
}

// toKafkaMessage mq.Message → kafka.Message。
func toKafkaMessage(topic string, msg *mq.Message) (segkafka.Message, error) {
	if topic == "" {
		return segkafka.Message{}, fmt.Errorf("mq/kafka: topic 不能为空")
	}
	return segkafka.Message{
		Topic:   topic,
		Key:     []byte(msg.Key),
		Value:   msg.Body,
		Headers: toSegHeaders(msg.Headers),
		Time:    time.Now(),
	}, nil
}

// toSegHeaders map → Kafka 消息头（nil/空值跳过，保持与 segmentio 行为一致）。
func toSegHeaders(headers map[string]string) []segkafka.Header {
	if len(headers) == 0 {
		return nil
	}
	out := make([]segkafka.Header, 0, len(headers))
	for k, v := range headers {
		out = append(out, segkafka.Header{Key: k, Value: []byte(v)})
	}
	return out
}

// fromSegHeaders Kafka 消息头 → map（同名多值取最后一个）。
func fromSegHeaders(headers []segkafka.Header) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	out := make(map[string]string, len(headers))
	for _, h := range headers {
		out[h.Key] = string(h.Value)
	}
	return out
}

// attemptOf 读取消息中的重投次数，缺失或非法按 0 计。
func attemptOf(msg *mq.Message) int {
	n, err := strconv.Atoi(msg.Header(HeaderAttempt))
	if err != nil || n < 0 {
		return 0
	}
	return n
}
