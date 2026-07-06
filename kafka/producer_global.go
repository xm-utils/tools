package kafka

import (
	"context"
	"fmt"
)

var defaultProducer *Producer

// InitGlobalProducer 初始化生产者
func InitGlobalProducer(config *ProducerConfig) error {
	if config == nil {
		return fmt.Errorf("kafka config is nil")
	}

	if len(config.Brokers) == 0 {
		return fmt.Errorf("kafka brokers is empty")
	}

	defaultProducer = NewProducer(config)

	return nil
}

// GetGlobalProducer 获取默认生产者
func GetGlobalProducer() *Producer {
	return defaultProducer
}

// Publish 发布消息（使用默认生产者）
func Publish(ctx context.Context, topic string, key string, value []byte) error {
	if defaultProducer == nil {
		return fmt.Errorf("kafka producer not initialized")
	}

	return defaultProducer.Publish(ctx, topic, key, value)
}

func Close() {
	defaultProducer.Close()
}
