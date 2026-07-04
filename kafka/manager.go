package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// ConsumerManager 消费者管理器，管理多个业务消费者实例
type ConsumerManager struct {
	consumers map[string]*Consumer // key: business name
	mu        sync.RWMutex
	log       *logrus.Entry
}

var (
	defaultManager *ConsumerManager
	managerOnce    sync.Once
)

// GetConsumerManager 获取默认的消费者管理器（单例）
func GetConsumerManager() *ConsumerManager {
	managerOnce.Do(func() {
		defaultManager = &ConsumerManager{
			consumers: make(map[string]*Consumer),
			log:       logrus.WithField("module", "Kafka Consumer Manager"),
		}
	})
	return defaultManager
}

// NewConsumerManager 创建新的消费者管理器
func NewConsumerManager() *ConsumerManager {
	return &ConsumerManager{
		consumers: make(map[string]*Consumer),
		log:       logrus.WithField("module", "Kafka Consumer Manager"),
	}
}

// RegisterConsumer 注册一个业务消费者
// business: 业务名称（唯一标识）
// config: 消费者配置
func (m *ConsumerManager) RegisterConsumer(business string, config *ConsumerConfig) error {
	if business == "" {
		return fmt.Errorf("business name cannot be empty")
	}

	if config == nil {
		return fmt.Errorf("consumer config for business '%s' cannot be nil", business)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查是否已存在
	if _, exists := m.consumers[business]; exists {
		return fmt.Errorf("consumer for business '%s' already exists", business)
	}

	// 验证配置
	if err := m.validateConfig(config); err != nil {
		return fmt.Errorf("invalid config for business '%s': %w", business, err)
	}

	// 应用默认值
	m.applyDefaults(config)

	// 创建消费者实例
	consumer, err := m.createConsumer(config)
	if err != nil {
		return fmt.Errorf("failed to create consumer for business '%s': %w", business, err)
	}

	// 注册到管理器
	m.consumers[business] = consumer
	m.log.Infof("成功注册业务消费者: business=%s, topic=%s, group=%s",
		business, config.Topic, config.GroupID)

	return nil
}

// GetConsumer 获取指定业务的消费者
func (m *ConsumerManager) GetConsumer(business string) (*Consumer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	consumer, exists := m.consumers[business]
	if !exists {
		return nil, fmt.Errorf("consumer for business '%s' not found", business)
	}

	return consumer, nil
}

// StartAll 启动所有已注册的消费者
func (m *ConsumerManager) StartAll(ctx context.Context, handlers map[string]TopicHandler) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.consumers) == 0 {
		return fmt.Errorf("no consumers registered")
	}

	m.log.Infof("开始启动所有消费者，总数: %d", len(m.consumers))

	var wg sync.WaitGroup
	errChan := make(chan error, len(m.consumers))

	for business, consumer := range m.consumers {
		handler, exists := handlers[business]
		if !exists {
			m.log.Warnf("未找到业务 '%s' 的消息处理器，跳过启动", business)
			continue
		}

		wg.Add(1)
		go func(biz string, cons *Consumer, h TopicHandler) {
			defer wg.Done()

			m.log.Infof("启动业务消费者: business=%s", biz)
			if err := cons.Subscribe(ctx, h); err != nil {
				errChan <- fmt.Errorf("failed to start consumer for business '%s': %w", biz, err)
			}
		}(business, consumer, handler)
	}

	// 等待所有消费者启动
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// 检查是否有错误
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors occurred while starting consumers: %v", errs)
	}

	m.log.Info("所有消费者启动完成")
	return nil
}

// Start 启动指定业务的消费者
func (m *ConsumerManager) Start(ctx context.Context, business string, handler TopicHandler) error {
	consumer, err := m.GetConsumer(business)
	if err != nil {
		return err
	}

	m.log.Infof("启动业务消费者: business=%s", business)
	return consumer.Subscribe(ctx, handler)
}

// CloseAll 关闭所有消费者
func (m *ConsumerManager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.log.Infof("开始关闭所有消费者，总数: %d", len(m.consumers))

	for business, consumer := range m.consumers {
		m.log.Infof("关闭业务消费者: business=%s", business)
		consumer.Close()
	}

	m.consumers = make(map[string]*Consumer)
	m.log.Info("所有消费者已关闭")
}

// Close 关闭指定业务的消费者
func (m *ConsumerManager) Close(business string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	consumer, exists := m.consumers[business]
	if !exists {
		return fmt.Errorf("consumer for business '%s' not found", business)
	}

	consumer.Close()
	delete(m.consumers, business)

	m.log.Infof("已关闭业务消费者: business=%s", business)
	return nil
}

// ListConsumers 列出所有已注册的消费者
func (m *ConsumerManager) ListConsumers() map[string]*Consumer {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*Consumer, len(m.consumers))
	for business, consumer := range m.consumers {
		result[business] = consumer
	}

	return result
}

// GetConsumerCount 获取已注册的消费者数量
func (m *ConsumerManager) GetConsumerCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.consumers)
}

// HealthCheck 健康检查，返回所有消费者的状态
func (m *ConsumerManager) HealthCheck() map[string]ConsumerHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]ConsumerHealth)
	for business, consumer := range m.consumers {
		result[business] = ConsumerHealth{
			Business:  business,
			Topic:     consumer.config.Topic,
			GroupID:   consumer.config.GroupID,
			Status:    "healthy",
			Timestamp: time.Now(),
		}
	}

	return result
}

// validateConfig 验证消费者配置
func (m *ConsumerManager) validateConfig(config *ConsumerConfig) error {
	if len(config.Brokers) == 0 {
		return fmt.Errorf("brokers cannot be empty")
	}

	if config.GroupID == "" {
		return fmt.Errorf("group_id cannot be empty")
	}

	if config.Topic == "" && len(config.Topics) == 0 {
		return fmt.Errorf("topic or topics must be configured")
	}

	return nil
}

// applyDefaults 应用默认配置
func (m *ConsumerManager) applyDefaults(config *ConsumerConfig) {
	defaults := GetConsumerDefaults()

	// 只设置未配置的字段
	if config.MinBytes == 0 {
		config.MinBytes = defaults.MinBytes
	}
	if config.MaxBytes == 0 {
		config.MaxBytes = defaults.MaxBytes
	}
	if config.MaxWait == 0 {
		config.MaxWait = defaults.MaxWait
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = defaults.HeartbeatInterval
	}
	if config.SessionTimeout == 0 {
		config.SessionTimeout = defaults.SessionTimeout
	}
	if config.RebalanceTimeout == 0 {
		config.RebalanceTimeout = defaults.RebalanceTimeout
	}
	if config.ReadBackoffMin == 0 {
		config.ReadBackoffMin = defaults.ReadBackoffMin
	}
	if config.ReadBackoffMax == 0 {
		config.ReadBackoffMax = defaults.ReadBackoffMax
	}
	if config.QueueCapacity == 0 {
		config.QueueCapacity = defaults.QueueCapacity
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = defaults.MaxRetries
	}
	if config.RetryBackoff == 0 {
		config.RetryBackoff = defaults.RetryBackoff
	}
	if config.JoinGroupBackoff == 0 {
		config.JoinGroupBackoff = defaults.JoinGroupBackoff
	}
	if config.RetentionTime == 0 {
		config.RetentionTime = defaults.RetentionTime
	}
	if config.ReadLagInterval == 0 {
		config.ReadLagInterval = defaults.ReadLagInterval
	}
	// AutoCommit 保持原值，不覆盖
}

// createConsumer 创建消费者实例
func (m *ConsumerManager) createConsumer(config *ConsumerConfig) (*Consumer, error) {
	log := logrus.WithField("module", "Kafka Consumer")

	// 构建 ReaderConfig
	readerConfig := kafka.ReaderConfig{
		Brokers:           config.Brokers,
		GroupID:           config.GroupID,
		MinBytes:          config.MinBytes,
		MaxBytes:          config.MaxBytes,
		MaxWait:           config.MaxWait,
		ReadLagInterval:   config.ReadLagInterval,
		HeartbeatInterval: config.HeartbeatInterval,
		SessionTimeout:    config.SessionTimeout,
		RebalanceTimeout:  config.RebalanceTimeout,
		StartOffset:       getStartOffset(config.StartOffset),
		ReadBackoffMin:    config.ReadBackoffMin,
		ReadBackoffMax:    config.ReadBackoffMax,
		CommitInterval:    config.CommitInterval,
	}

	// 根据配置选择单主题或多主题模式
	if len(config.Topics) > 0 {
		readerConfig.GroupTopics = config.Topics
		log.Infof("Kafka消费者初始化成功（多主题模式）: brokers=%v, topics=%v, group=%s",
			config.Brokers, config.Topics, config.GroupID)
	} else {
		readerConfig.Topic = config.Topic
		log.Infof("Kafka消费者初始化成功（单主题模式）: brokers=%v, topic=%s, group=%s",
			config.Brokers, config.Topic, config.GroupID)
	}

	reader := kafka.NewReader(readerConfig)

	return &Consumer{
		reader: reader,
		log:    log,
		config: config,
	}, nil
}

// ConsumerHealth 消费者健康状态
type ConsumerHealth struct {
	Business  string    `json:"business"`
	Topic     string    `json:"topic"`
	GroupID   string    `json:"group_id"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// ==================== 向后兼容的全局函数 ====================

// RegisterConsumer 注册全局默认消费者（向后兼容）
func RegisterConsumer(business string, config *ConsumerConfig) error {
	return GetConsumerManager().RegisterConsumer(business, config)
}

// GetBusinessConsumer 获取指定业务的消费者（向后兼容）
func GetBusinessConsumer(business string) (*Consumer, error) {
	return GetConsumerManager().GetConsumer(business)
}
