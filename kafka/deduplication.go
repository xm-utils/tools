package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// DeduplicationStore 去重存储接口
type DeduplicationStore interface {
	// IsDuplicate 检查消息是否已处理
	IsDuplicate(key string) (bool, error)
	// MarkProcessed 标记消息已处理
	MarkProcessed(key string, ttl time.Duration) error
	// Close 关闭存储
	Close() error
}

// MessageDeduplicator 消息去重器
type MessageDeduplicator struct {
	store  DeduplicationStore
	ttl    time.Duration // 去重记录存活时间
	prefix string        // key前缀
}

// NewMessageDeduplicator 创建消息去重器
func NewMessageDeduplicator(store DeduplicationStore, ttl time.Duration) *MessageDeduplicator {
	if ttl == 0 {
		ttl = 24 * time.Hour // 默认24小时
	}
	return &MessageDeduplicator{
		store:  store,
		ttl:    ttl,
		prefix: "kafka:duplicate:",
	}
}

// GenerateKey 生成去重key（基于消息唯一标识）
func (d *MessageDeduplicator) GenerateKey(msg kafka.Message) string {
	// 优先使用消息的 Key 作为唯一标识
	if len(msg.Key) > 0 {
		return fmt.Sprintf("%s%s:%s", d.prefix, msg.Topic, string(msg.Key))
	}
	// 否则使用 topic+partition+offset 作为唯一标识
	return fmt.Sprintf("%s%s:%d:%d", d.prefix, msg.Topic, msg.Partition, msg.Offset)
}

// IsDuplicate 检查消息是否重复
func (d *MessageDeduplicator) IsDuplicate(msg kafka.Message) (bool, error) {
	key := d.GenerateKey(msg)
	return d.store.IsDuplicate(key)
}

// MarkProcessed 标记消息已处理
func (d *MessageDeduplicator) MarkProcessed(msg kafka.Message) error {
	key := d.GenerateKey(msg)
	return d.store.MarkProcessed(key, d.ttl)
}

// WrapHandlerWithDeduplication 包装处理器，添加去重功能
func (d *MessageDeduplicator) WrapHandlerWithDeduplication(handler TopicHandler) TopicHandler {
	return func(ctx context.Context, topic string, msg kafka.Message) error {
		// 检查是否重复
		isDup, err := d.IsDuplicate(msg)
		if err != nil {
			logrus.Warnf("检查消息重复失败: topic=%s, err=%v，继续处理", topic, err)
			// 如果检查失败，继续处理（宁可重复也不丢失）
		} else if isDup {
			logrus.Debugf("跳过重复消息: topic=%s, key=%s", topic, string(msg.Key))
			return nil // 直接返回成功，不重复处理
		}

		// 处理消息
		err = handler(ctx, topic, msg)
		if err != nil {
			return err
		}

		// 标记为已处理
		if markErr := d.MarkProcessed(msg); markErr != nil {
			logrus.Errorf("标记消息已处理失败: topic=%s, err=%v", topic, markErr)
			// 不影响业务逻辑，只记录日志
		}

		return nil
	}
}
