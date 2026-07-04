package kafka

import (
	"context"
	"testing"
	"time"
)

// TestConsumerManager_RegisterConsumer 测试注册消费者
func TestConsumerManager_RegisterConsumer(t *testing.T) {
	manager := NewConsumerManager()

	// 测试正常注册
	config := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:  []string{"localhost:9092"},
			ClientID: "test-service",
		},
		Topic:   "test-topic",
		GroupID: "test-group",
	}

	err := manager.RegisterConsumer("test", config)
	if err != nil {
		t.Fatalf("注册消费者失败: %v", err)
	}

	// 验证注册成功
	if manager.GetConsumerCount() != 1 {
		t.Errorf("期望消费者数量为 1，实际为 %d", manager.GetConsumerCount())
	}

	// 测试重复注册
	err = manager.RegisterConsumer("test", config)
	if err == nil {
		t.Error("期望重复注册失败，但成功了")
	}

	// 测试空业务名
	err = manager.RegisterConsumer("", config)
	if err == nil {
		t.Error("期望空业务名注册失败，但成功了")
	}

	// 测试空配置
	err = manager.RegisterConsumer("test2", nil)
	if err == nil {
		t.Error("期望空配置注册失败，但成功了")
	}
}

// TestConsumerManager_GetConsumer 测试获取消费者
func TestConsumerManager_GetConsumer(t *testing.T) {
	manager := NewConsumerManager()

	config := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:   "test-topic",
		GroupID: "test-group",
	}

	manager.RegisterConsumer("test", config)

	// 测试获取存在的消费者
	consumer, err := manager.GetConsumer("test")
	if err != nil {
		t.Fatalf("获取消费者失败: %v", err)
	}
	if consumer == nil {
		t.Error("期望获取到消费者，但为 nil")
	}

	// 测试获取不存在的消费者
	_, err = manager.GetConsumer("nonexistent")
	if err == nil {
		t.Error("期望获取不存在的消费者失败，但成功了")
	}
}

// TestConsumerManager_Close 测试关闭消费者
func TestConsumerManager_Close(t *testing.T) {
	manager := NewConsumerManager()

	config := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:   "test-topic",
		GroupID: "test-group",
	}

	manager.RegisterConsumer("test", config)

	// 测试关闭存在的消费者
	err := manager.Close("test")
	if err != nil {
		t.Fatalf("关闭消费者失败: %v", err)
	}

	// 验证已关闭
	if manager.GetConsumerCount() != 0 {
		t.Errorf("期望消费者数量为 0，实际为 %d", manager.GetConsumerCount())
	}

	// 测试关闭不存在的消费者
	err = manager.Close("nonexistent")
	if err == nil {
		t.Error("期望关闭不存在的消费者失败，但成功了")
	}
}

// TestConsumerManager_CloseAll 测试关闭所有消费者
func TestConsumerManager_CloseAll(t *testing.T) {
	manager := NewConsumerManager()

	// 注册多个消费者
	for i := 0; i < 3; i++ {
		config := &ConsumerConfig{
			CommonConfig: CommonConfig{
				Brokers: []string{"localhost:9092"},
			},
			Topic:   "test-topic",
			GroupID: "test-group",
		}
		manager.RegisterConsumer(string(rune('a'+i)), config)
	}

	if manager.GetConsumerCount() != 3 {
		t.Errorf("期望消费者数量为 3，实际为 %d", manager.GetConsumerCount())
	}

	// 关闭所有
	manager.CloseAll()

	if manager.GetConsumerCount() != 0 {
		t.Errorf("期望消费者数量为 0，实际为 %d", manager.GetConsumerCount())
	}
}

// TestConsumerManager_ListConsumers 测试列出消费者
func TestConsumerManager_ListConsumers(t *testing.T) {
	manager := NewConsumerManager()

	// 注册多个消费者
	businesses := []string{"order", "user", "notification"}
	for _, business := range businesses {
		config := &ConsumerConfig{
			CommonConfig: CommonConfig{
				Brokers: []string{"localhost:9092"},
			},
			Topic:   "test-topic",
			GroupID: "test-group",
		}
		manager.RegisterConsumer(business, config)
	}

	// 列出所有消费者
	consumers := manager.ListConsumers()
	if len(consumers) != 3 {
		t.Errorf("期望消费者数量为 3，实际为 %d", len(consumers))
	}

	// 验证所有业务都存在
	for _, business := range businesses {
		if _, exists := consumers[business]; !exists {
			t.Errorf("期望业务 %s 存在，但不存在", business)
		}
	}
}

// TestConsumerManager_HealthCheck 测试健康检查
func TestConsumerManager_HealthCheck(t *testing.T) {
	manager := NewConsumerManager()

	config := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:   "test-topic",
		GroupID: "test-group",
	}

	manager.RegisterConsumer("test", config)

	// 执行健康检查
	health := manager.HealthCheck()
	if len(health) != 1 {
		t.Errorf("期望健康检查结果为 1，实际为 %d", len(health))
	}

	status, exists := health["test"]
	if !exists {
		t.Error("期望业务 test 的健康状态存在，但不存在")
	}

	if status.Status != "healthy" {
		t.Errorf("期望状态为 healthy，实际为 %s", status.Status)
	}

	if status.Topic != "test-topic" {
		t.Errorf("期望主题为 test-topic，实际为 %s", status.Topic)
	}

	if status.GroupID != "test-group" {
		t.Errorf("期望消费组为 test-group，实际为 %s", status.GroupID)
	}
}

// TestConsumerManager_StartAll_NoConsumers 测试启动时无消费者
func TestConsumerManager_StartAll_NoConsumers(t *testing.T) {
	manager := NewConsumerManager()

	ctx := context.Background()
	handlers := map[string]TopicHandler{}

	err := manager.StartAll(ctx, handlers)
	if err == nil {
		t.Error("期望没有消费者时启动失败，但成功了")
	}
}

// TestConsumerManager_ApplyDefaults 测试应用默认值
func TestConsumerManager_ApplyDefaults(t *testing.T) {
	manager := NewConsumerManager()

	config := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:   "test-topic",
		GroupID: "test-group",
		// 其他字段使用默认值
	}

	manager.applyDefaults(config)

	// 验证默认值是否正确应用
	if config.MinBytes != 1 {
		t.Errorf("期望 MinBytes 为 1，实际为 %d", config.MinBytes)
	}

	if config.MaxBytes != 10000000 {
		t.Errorf("期望 MaxBytes 为 10000000，实际为 %d", config.MaxBytes)
	}

	if config.MaxWait != 1*time.Second {
		t.Errorf("期望 MaxWait 为 1s，实际为 %v", config.MaxWait)
	}

	if config.MaxRetries != 3 {
		t.Errorf("期望 MaxRetries 为 3，实际为 %d", config.MaxRetries)
	}
}

// TestConsumerManager_ValidateConfig 测试配置验证
func TestConsumerManager_ValidateConfig(t *testing.T) {
	manager := NewConsumerManager()

	// 测试有效配置
	validConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:   "test-topic",
		GroupID: "test-group",
	}
	err := manager.validateConfig(validConfig)
	if err != nil {
		t.Errorf("期望有效配置验证通过，但失败: %v", err)
	}

	// 测试空 Brokers
	invalidBrokers := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{},
		},
		Topic:   "test-topic",
		GroupID: "test-group",
	}
	err = manager.validateConfig(invalidBrokers)
	if err == nil {
		t.Error("期望空 Brokers 验证失败，但通过了")
	}

	// 测试空 GroupID
	invalidGroupID := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:   "test-topic",
		GroupID: "",
	}
	err = manager.validateConfig(invalidGroupID)
	if err == nil {
		t.Error("期望空 GroupID 验证失败，但通过了")
	}

	// 测试空 Topic 和 Topics
	invalidTopic := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:   "",
		Topics:  []string{},
		GroupID: "test-group",
	}
	err = manager.validateConfig(invalidTopic)
	if err == nil {
		t.Error("期望空 Topic 验证失败，但通过了")
	}
}

// TestGetConsumerManager_Singleton 测试单例模式
func TestGetConsumerManager_Singleton(t *testing.T) {
	manager1 := GetConsumerManager()
	manager2 := GetConsumerManager()

	if manager1 != manager2 {
		t.Error("期望两次获取的是同一个管理器实例")
	}
}
