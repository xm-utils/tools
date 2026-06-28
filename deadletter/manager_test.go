package deadletter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// TestMessage 测试消息结构
type TestMessage struct {
	ID      string `json:"id"`
	Content string `json:"content"`
}

// TestDeadLetterQueueManager_PushToDeadLetter 测试推入死信队列
func TestDeadLetterQueueManager_PushToDeadLetter(t *testing.T) {
	// 跳过需要真实数据库的测试
	if testing.Short() {
		t.Skip("跳过需要数据库的测试")
	}

	// 创建测试处理器
	handler := func(ctx context.Context, messageData string) error {
		var msg TestMessage
		if err := json.Unmarshal([]byte(messageData), &msg); err != nil {
			return err
		}
		logrus.Infof("处理测试消息: ID=%s, Content=%s", msg.ID, msg.Content)
		return nil
	}

	// 创建配置
	config := DefaultConfig("test_queue")
	config.MaxRetry = 3

	// 创建管理器(不使用持久化)
	manager := NewQueueManager(config, handler, nil)

	// 创建测试消息
	testMsg := TestMessage{
		ID:      "test_001",
		Content: "测试内容",
	}
	messageData, _ := json.Marshal(testMsg)

	// 推入死信队列
	err := manager.PushToDeadLetter(
		"msg_test_001",
		string(messageData),
		"测试错误",
		0,
	)

	assert.NoError(t, err, "推入死信队列不应出错")

	// 验证队列长度
	length, err := manager.GetQueueLength()
	assert.NoError(t, err, "获取队列长度不应出错")
	assert.Greater(t, length, int64(0), "队列长度应大于0")

	// 清理
	manager.Stop()
}

// TestDeadLetterQueueManager_Recovery 测试恢复机制
func TestDeadLetterQueueManager_Recovery(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过需要数据库的测试")
	}

	// 计数器
	processCount := 0

	// 创建会失败的处理器(前2次失败,第3次成功)
	handler := func(ctx context.Context, messageData string) error {
		processCount++
		if processCount <= 2 {
			return errors.New("模拟处理失败")
		}
		return nil
	}

	config := DefaultConfig("test_recovery")
	config.MaxRetry = 3
	config.RecoveryInterval = 1 * time.Second // 快速测试
	config.BatchSize = 10

	manager := NewQueueManager(config, handler, nil)

	// 推入测试消息
	testMsg := TestMessage{ID: "recovery_test", Content: "恢复测试"}
	messageData, _ := json.Marshal(testMsg)
	manager.PushToDeadLetter("msg_recovery", string(messageData), "初始错误", 0)

	// 启动恢复服务
	manager.StartRecovery()

	// 等待恢复执行
	time.Sleep(3 * time.Second)

	// 验证处理器被调用
	assert.Greater(t, processCount, 0, "处理器应被调用")

	// 停止服务
	manager.Stop()
}

// TestDeadLetterQueueManager_Metrics 测试监控指标
func TestDeadLetterQueueManager_Metrics(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过需要数据库的测试")
	}

	handler := func(ctx context.Context, messageData string) error {
		return nil
	}

	config := DefaultConfig("test_metrics")
	manager := NewQueueManager(config, handler, nil)

	// 记录一些指标
	manager.metrics.RecordDeadLetter()
	manager.metrics.RecordDeadLetter()
	manager.metrics.RecordRecovery()
	manager.metrics.RecordAbandoned()

	// 获取统计信息
	stats := manager.metrics.GetStats(context.Background())

	assert.Equal(t, int64(2), stats.TotalDeadLetters, "总死信数应为2")
	assert.Equal(t, int64(1), stats.TotalRecovered, "总恢复数应为1")
	assert.Equal(t, int64(1), stats.TotalAbandoned, "总放弃数应为1")
	assert.Equal(t, "test_metrics", stats.QueueKey, "队列Key应匹配")

	manager.Stop()
}

// TestDefaultDeadLetterConfig 测试默认配置
func TestDefaultDeadLetterConfig(t *testing.T) {
	config := DefaultConfig("my_queue")

	assert.Equal(t, "my_queue", config.QueueKey, "队列Key应匹配")
	assert.Equal(t, "dead_letter:my_queue", config.DeadLetterStream, "Stream Key应匹配")
	assert.Equal(t, 3, config.MaxRetry, "最大重试次数应为3")
	assert.Equal(t, 1*time.Second, config.RetryInterval, "重试间隔应为1秒")
	assert.Equal(t, 5*time.Minute, config.RecoveryInterval, "恢复间隔应为5分钟")
	assert.Equal(t, 10, config.BatchSize, "批量大小应为10")
}

// TestCustomDeadLetterConfig 测试自定义配置
func TestCustomDeadLetterConfig(t *testing.T) {
	config := DefaultConfig("custom_queue")
	config.MaxRetry = 5
	config.RecoveryInterval = 10 * time.Minute
	config.BatchSize = 20

	assert.Equal(t, 5, config.MaxRetry, "最大重试次数应为5")
	assert.Equal(t, 10*time.Minute, config.RecoveryInterval, "恢复间隔应为10分钟")
	assert.Equal(t, 20, config.BatchSize, "批量大小应为20")
}

// BenchmarkPushToDeadLetter 性能测试:推入死信队列
func BenchmarkPushToDeadLetter(b *testing.B) {
	if testing.Short() {
		b.Skip("跳过性能测试")
	}

	handler := func(ctx context.Context, messageData string) error {
		return nil
	}

	config := DefaultConfig("benchmark_queue")
	manager := NewQueueManager(config, handler, nil)
	defer manager.Stop()

	testMsg := TestMessage{ID: "bench", Content: "性能测试"}
	messageData, _ := json.Marshal(testMsg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		messageID := fmt.Sprintf("msg_%d", i)
		manager.PushToDeadLetter(messageID, string(messageData), "测试错误", 0)
	}
}

// ExampleDeadLetterQueueManager_Usage 使用示例
func ExampleDeadLetterQueueManager_Usage() {
	// 定义消息处理器
	handler := func(ctx context.Context, messageData string) error {
		var msg TestMessage
		if err := json.Unmarshal([]byte(messageData), &msg); err != nil {
			return err
		}
		fmt.Printf("处理消息: %s\n", msg.Content)
		return nil
	}

	// 创建配置
	config := DefaultConfig("example_queue")
	config.MaxRetry = 3

	// 创建管理器(不使用持久化)
	manager := NewQueueManager(config, handler, nil)
	defer manager.Stop()

	// 启动恢复服务
	manager.StartRecovery()

	// 推入死信消息
	testMsg := TestMessage{ID: "ex_001", Content: "示例消息"}
	messageData, _ := json.Marshal(testMsg)
	manager.PushToDeadLetter("msg_ex_001", string(messageData), "示例错误", 0)

	// 输出: 处理消息: 示例消息
}
