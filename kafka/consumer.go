package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

var defaultConsumer *Consumer

// Consumer Kafka消费者
type Consumer struct {
	reader *kafka.Reader
	log    *logrus.Entry
	config *Config
}

// InitConsumer 初始化消费者（支持单主题和多主题）
func InitConsumer(config *Config) error {
	if config == nil {
		return fmt.Errorf("kafka config is nil")
	}

	if len(config.Brokers) == 0 {
		return fmt.Errorf("kafka brokers is empty")
	}

	if config.GroupID == "" {
		return fmt.Errorf("kafka group_id is empty")
	}

	// 验证主题配置：至少需要配置 Topic 或 Topics
	if config.Topic == "" && len(config.Topics) == 0 {
		return fmt.Errorf("kafka topic or topics must be configured")
	}

	log := logrus.WithField("module", "Kafka Consumer")
	// 设置默认值
	setConsumerDefaults(config)

	// 构建 ReaderConfig
	readerConfig := kafka.ReaderConfig{
		Brokers:           config.Brokers,
		GroupID:           config.GroupID,
		MinBytes:          config.MinBytes,
		MaxBytes:          config.MaxBytes,
		MaxWait:           1 * time.Second,
		ReadLagInterval:   -1,
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
		RebalanceTimeout:  30 * time.Second,
		StartOffset:       kafka.FirstOffset,
		ReadBackoffMin:    100 * time.Millisecond,
		ReadBackoffMax:    1 * time.Second,
	}

	// 根据配置选择单主题或多主题模式
	if len(config.Topics) > 0 {
		// 多主题模式（需要使用 GroupTopics，且必须配合 GroupID）
		readerConfig.GroupTopics = config.Topics
		log.Infof("Kafka消费者初始化成功（多主题模式）: brokers=%v, topics=%v, group=%s", config.Brokers, config.Topics, config.GroupID)
	} else {
		// 单主题模式
		readerConfig.Topic = config.Topic
		log.Infof("Kafka消费者初始化成功（单主题模式）: brokers=%v, topic=%s, group=%s", config.Brokers, config.Topic, config.GroupID)
	}

	reader := kafka.NewReader(readerConfig)

	defaultConsumer = &Consumer{
		reader: reader,
		log:    log,
		config: config,
	}

	return nil
}

// GetConsumer 获取默认消费者
func GetConsumer() *Consumer {
	return defaultConsumer
}

// Subscribe 订阅消息（使用默认消费者）
func Subscribe(ctx context.Context, handler TopicHandler) error {
	if defaultConsumer == nil {
		return fmt.Errorf("kafka consumer not initialized")
	}
	return defaultConsumer.Subscribe(ctx, handler)
}

// SubscribeWithTopicHandler 订阅消息并支持按主题分发处理（使用默认消费者）
func SubscribeWithTopicHandler(ctx context.Context, handler TopicHandler) error {
	if defaultConsumer == nil {
		return fmt.Errorf("kafka consumer not initialized")
	}
	return defaultConsumer.SubscribeWithTopicHandler(ctx, handler)
}

// Subscribe 订阅消息
func (c *Consumer) Subscribe(ctx context.Context, handler TopicHandler) error {
	c.log.Info("开始订阅Kafka消息...")

	for {
		select {
		case <-ctx.Done():
			c.log.Info("Kafka消费者停止")
			return ctx.Err()
		default:
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				c.log.Errorf("Kafka读取消息失败: %v", err)
				continue
			}

			// 异步处理消息
			go func(m kafka.Message) {
				if err := handler(ctx, m.Topic, m); err != nil {
					c.log.Errorf("Kafka消息处理失败: topic=%s, partition=%d, offset=%d, err=%v", m.Topic, m.Partition, m.Offset, err)
				}
			}(msg)
		}
	}
}

// SubscribeWithTopicHandler 订阅消息并支持按主题分发处理
func (c *Consumer) SubscribeWithTopicHandler(ctx context.Context, handler TopicHandler) error {
	c.log.Info("开始订阅Kafka消息（支持主题分发）...")

	for {
		select {
		case <-ctx.Done():
			c.log.Info("Kafka消费者停止")
			return ctx.Err()
		default:
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				c.log.Errorf("Kafka读取消息失败: %v", err)
				continue
			}

			// 异步处理消息，传入主题信息
			go func(m kafka.Message) {
				if err := handler(ctx, m.Topic, m); err != nil {
					c.log.Errorf("Kafka消息处理失败: topic=%s, partition=%d, offset=%d, err=%v", m.Topic, m.Partition, m.Offset, err)
				}
			}(msg)
		}
	}
}

// Close 关闭消费者
func (c *Consumer) Close() {
	if c.reader != nil {
		if err := c.reader.Close(); err != nil {
			c.log.Errorf("关闭Kafka消费者失败: %v", err)
		} else {
			c.log.Info("Kafka消费者已关闭")
		}
	}
}

// setConsumerDefaults 设置消费者默认值
func setConsumerDefaults(config *Config) {
	if config.MinBytes == 0 {
		config.MinBytes = 1
	}
	if config.MaxBytes == 0 {
		config.MaxBytes = 1048576 // 1MB
	}
	if config.QueueCapacity == 0 {
		config.QueueCapacity = 1000
	}
}
