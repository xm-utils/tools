package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

var (
	defaultProducer *Producer
)

// TopicHandler 主题特定的消息处理函数
type TopicHandler func(ctx context.Context, topic string, msg kafka.Message) error

// Producer Kafka生产者
type Producer struct {
	writer *kafka.Writer
	log    *logrus.Entry
	config *Config
}

// InitProducer 初始化生产者
func InitProducer(config *Config) error {
	if config == nil {
		return fmt.Errorf("kafka config is nil")
	}

	if len(config.Brokers) == 0 {
		return fmt.Errorf("kafka brokers is empty")
	}

	log := logrus.WithField("module", "Kafka Producer")
	// 设置默认值
	setProducerDefaults(config)

	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Balancer:     &kafka.LeastBytes{},
		MaxAttempts:  config.MaxAttempts,
		BatchSize:    config.BatchSize,
		BatchBytes:   config.BatchBytes,
		BatchTimeout: 10 * time.Millisecond,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				log.Errorf("Kafka消息发送失败: %v", err)
			}
		},
	}

	defaultProducer = &Producer{
		writer: writer,
		log:    log,
		config: config,
	}

	log.Infof("Kafka生产者初始化成功: brokers=%v, topic=%s", config.Brokers, config.Topic)
	return nil
}

// GetProducer 获取默认生产者
func GetProducer() *Producer {
	return defaultProducer
}

// Publish 发布消息（使用默认生产者）
func Publish(ctx context.Context, topic string, key string, value []byte) error {
	if defaultProducer == nil {
		return fmt.Errorf("kafka producer not initialized")
	}

	return defaultProducer.Publish(ctx, topic, key, value)
}

// Publish 发布消息
func (p *Producer) Publish(ctx context.Context, topic string, key string, value []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
		Time:  time.Now(),
	}
	err := p.writer.WriteMessages(ctx, msg)
	if err != nil {
		p.log.Errorf("Kafka发送消息失败: topic=%s, key=%s, err=%v", topic, key, err)
		return fmt.Errorf("publish message failed: %w", err)
	}

	p.log.Debugf("Kafka消息发送成功: topic=%s, key=%s", topic, key)
	return nil
}

// PublishBatch 批量发布消息
func (p *Producer) PublishBatch(ctx context.Context, messages []kafka.Message) error {
	err := p.writer.WriteMessages(ctx, messages...)
	if err != nil {
		p.log.Errorf("Kafka批量发送消息失败: count=%d, err=%v", len(messages), err)
		return fmt.Errorf("publish batch messages failed: %w", err)
	}

	p.log.Debugf("Kafka批量消息发送成功: count=%d", len(messages))
	return nil
}

// Close 关闭生产者
func (p *Producer) Close() {
	if p.writer != nil {
		if err := p.writer.Close(); err != nil {
			p.log.Errorf("关闭Kafka生产者失败: %v", err)
		} else {
			p.log.Info("Kafka生产者已关闭")
		}
	}
}

// SetProducerDefaults 设置生产者默认值
func setProducerDefaults(config *Config) {
	if config.MaxAttempts == 0 {
		config.MaxAttempts = 10
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = 10 * time.Second
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = 10 * time.Second
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 10 * time.Second
	}
	if config.BatchSize == 0 {
		config.BatchSize = 1000
	}
	if config.BatchBytes == 0 {
		config.BatchBytes = 1048576 // 1MB
	}
}
