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
	config *ConsumerConfig
}

// MessageContext 消息上下文，包含消息和确认方法
type MessageContext struct {
	Message   kafka.Message
	Commit    func() error // 提交offset（确认消息）
	ShouldAck bool         // 是否应该确认（手动模式下由业务代码设置）
}

// InitConsumer 初始化消费者（支持单主题和多主题）
func InitConsumer(config *ConsumerConfig) error {
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
		StartOffset:       getStartOffset(config.StartOffset),
		ReadBackoffMin:    100 * time.Millisecond,
		ReadBackoffMax:    1 * time.Second,
		CommitInterval:    config.CommitInterval,
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

			// 创建消息上下文
			msgCtx := &MessageContext{
				Message:   msg,
				ShouldAck: !c.config.AutoCommit, // 非自动提交模式下需要手动确认
			}

			// 异步处理消息
			go c.processMessage(ctx, msgCtx, handler)
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

// processMessage 带重试的消息处理
func (c *Consumer) processMessage(ctx context.Context, msgCtx *MessageContext, handler TopicHandler) {

	// 创建带超时的上下文
	processCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	// 执行消息处理
	err := handler(processCtx, msgCtx.Message.Topic, msgCtx.Message)
	cancel()

	if err != nil {
		c.log.Errorf("消息处理失败: topic=%s, partition=%d, offset=%d, last_err=%v", msgCtx.Message.Topic, msgCtx.Message.Partition, msgCtx.Message.Offset, err)
	}

	// 处理成功，提交offset
	if msgCtx.ShouldAck {
		if commitErr := c.commitMessage(msgCtx.Message); commitErr != nil {
			c.log.Errorf("提交offset失败: topic=%s, partition=%d, offset=%d, err=%v", msgCtx.Message.Topic, msgCtx.Message.Partition, msgCtx.Message.Offset, commitErr)
			return
		}
		c.log.Debugf("消息处理成功并已确认: topic=%s, partition=%d, offset=%d", msgCtx.Message.Topic, msgCtx.Message.Partition, msgCtx.Message.Offset)
	}
}

// commitMessage 提交消息的offset（确认消息）
func (c *Consumer) commitMessage(msg kafka.Message) error {
	// 设置消息的offset为当前offset+1，表示该offset及之前的消息都已处理
	return c.reader.CommitMessages(context.Background(), msg)
}

// CommitOffset 手动提交指定消息的offset（供业务代码调用）
func (c *Consumer) CommitOffset(msg kafka.Message) error {
	if c.config.AutoCommit {
		c.log.Warn("自动提交模式下不需要手动调用CommitOffset")
		return nil
	}
	return c.commitMessage(msg)
}

// getRetryBackoff 计算重试退避时间（指数退避）
func (c *Consumer) getRetryBackoff(attempt int) time.Duration {
	if c.config.RetryBackoff > 0 {
		// 使用配置的退避时间，每次重试翻倍
		backoff := c.config.RetryBackoff * time.Duration(1<<uint(attempt))
		// 最大不超过5分钟
		if backoff > 5*time.Minute {
			backoff = 5 * time.Minute
		}
		return backoff
	}
	// 默认退避策略：1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s...
	defaultBackoffs := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
		60 * time.Second,
	}

	if attempt < len(defaultBackoffs) {
		return defaultBackoffs[attempt]
	}
	return 60 * time.Second
}

// GetReader 获取底层的kafka.Reader（用于高级操作）
func (c *Consumer) GetReader() *kafka.Reader {
	return c.reader
}

// getStartOffset 获取起始offset
func getStartOffset(offset int64) int64 {
	if offset == -2 {
		return kafka.FirstOffset
	}
	if offset == -1 {
		return kafka.LastOffset
	}
	// 默认从最早开始
	return kafka.FirstOffset
}
