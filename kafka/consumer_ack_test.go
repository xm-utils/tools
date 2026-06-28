package kafka

import (
	"testing"
	"time"
)

// TestMessageAckConfig 测试消息确认配置
func TestMessageAckConfig(t *testing.T) {
	config := &Config{
		Brokers:      []string{"localhost:9092"},
		Topic:        "test-topic",
		GroupID:      "test-group",
		AutoCommit:   false,
		MaxRetries:   3,
		RetryBackoff: 1 * time.Second,
		StartOffset:  -2, // FirstOffset
	}

	// 验证默认值设置
	setConsumerDefaults(config)

	if config.AutoCommit != false {
		t.Errorf("Expected AutoCommit=false, got %v", config.AutoCommit)
	}

	if config.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries=3, got %d", config.MaxRetries)
	}

	if config.RetryBackoff != 1*time.Second {
		t.Errorf("Expected RetryBackoff=1s, got %v", config.RetryBackoff)
	}

	t.Logf("Config validated: AutoCommit=%v, MaxRetries=%d, RetryBackoff=%v",
		config.AutoCommit, config.MaxRetries, config.RetryBackoff)
}

// TestGetStartOffset 测试起始offset转换
func TestGetStartOffset(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected int64
	}{
		{"FirstOffset constant", -2, -2},
		{"LastOffset constant", -1, -1},
		{"Zero value", 0, -2},      // 默认应该是 FirstOffset
		{"Invalid value", 100, -2}, // 无效值也应该返回 FirstOffset
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getStartOffset(tt.input)
			if result != tt.expected {
				t.Errorf("getStartOffset(%d) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

// TestRetryBackoff 测试重试退避时间计算
func TestRetryBackoff(t *testing.T) {
	config := &Config{
		Brokers:      []string{"localhost:9092"},
		Topic:        "test-topic",
		GroupID:      "test-group",
		RetryBackoff: 2 * time.Second,
	}

	consumer := &Consumer{
		config: config,
	}

	// 测试指数退避
	expectedBackoffs := []time.Duration{
		2 * time.Second,  // 2^0 * 2s
		4 * time.Second,  // 2^1 * 2s
		8 * time.Second,  // 2^2 * 2s
		16 * time.Second, // 2^3 * 2s
		32 * time.Second, // 2^4 * 2s
	}

	for attempt, expected := range expectedBackoffs {
		backoff := consumer.getRetryBackoff(attempt)
		if backoff != expected {
			t.Errorf("Attempt %d: expected backoff %v, got %v", attempt, expected, backoff)
		}
	}

	// 测试最大退避时间限制（不超过5分钟）
	largeAttempt := 20
	backoff := consumer.getRetryBackoff(largeAttempt)
	if backoff > 5*time.Minute {
		t.Errorf("Backoff should not exceed 5 minutes, got %v", backoff)
	}
}

// TestDefaultRetryBackoff 测试默认退避策略
func TestDefaultRetryBackoff(t *testing.T) {
	config := &Config{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
		// RetryBackoff = 0，使用默认策略
	}

	consumer := &Consumer{
		config: config,
	}

	defaultBackoffs := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
		60 * time.Second,
		60 * time.Second, // 之后都应该是60秒
	}

	for attempt, expected := range defaultBackoffs {
		backoff := consumer.getRetryBackoff(attempt)
		if backoff != expected {
			t.Errorf("Attempt %d: expected backoff %v, got %v", attempt, expected, backoff)
		}
	}
}

// TestMessageContext 测试消息上下文
func TestMessageContext(t *testing.T) {
	msgCtx := &MessageContext{
		Retry:     0,
		MaxRetry:  3,
		ShouldAck: true,
	}

	if msgCtx.Retry != 0 {
		t.Errorf("Expected initial Retry=0, got %d", msgCtx.Retry)
	}

	if msgCtx.MaxRetry != 3 {
		t.Errorf("Expected MaxRetry=3, got %d", msgCtx.MaxRetry)
	}

	if msgCtx.ShouldAck != true {
		t.Errorf("Expected ShouldAck=true, got %v", msgCtx.ShouldAck)
	}

	t.Logf("MessageContext created: Retry=%d, MaxRetry=%d, ShouldAck=%v",
		msgCtx.Retry, msgCtx.MaxRetry, msgCtx.ShouldAck)
}

// TestConsumerDefaults 测试消费者默认配置
func TestConsumerDefaults(t *testing.T) {
	config := &Config{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "test-group",
		// 其他字段留空，测试默认值
	}

	setConsumerDefaults(config)

	if config.MinBytes != 1 {
		t.Errorf("Expected MinBytes=1, got %d", config.MinBytes)
	}

	if config.MaxBytes != 1048576 {
		t.Errorf("Expected MaxBytes=1048576, got %d", config.MaxBytes)
	}

	if config.QueueCapacity != 1000 {
		t.Errorf("Expected QueueCapacity=1000, got %d", config.QueueCapacity)
	}

	if config.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries=3, got %d", config.MaxRetries)
	}

	if config.RetryBackoff != 1*time.Second {
		t.Errorf("Expected RetryBackoff=1s, got %v", config.RetryBackoff)
	}

	if config.StartOffset != -2 {
		t.Errorf("Expected StartOffset=-2 (FirstOffset), got %d", config.StartOffset)
	}

	t.Logf("All defaults validated successfully")
}

// TestAutoCommitMode 测试自动提交模式配置
func TestAutoCommitMode(t *testing.T) {
	config := &Config{
		Brokers:        []string{"localhost:9092"},
		Topic:          "test-topic",
		GroupID:        "test-group",
		AutoCommit:     true,
		CommitInterval: 5 * time.Second,
	}

	setConsumerDefaults(config)

	if config.AutoCommit != true {
		t.Errorf("Expected AutoCommit=true, got %v", config.AutoCommit)
	}

	if config.CommitInterval != 5*time.Second {
		t.Errorf("Expected CommitInterval=5s, got %v", config.CommitInterval)
	}

	t.Logf("AutoCommit mode configured: AutoCommit=%v, CommitInterval=%v",
		config.AutoCommit, config.CommitInterval)
}

// TestManualCommitConfig 测试手动提交模式配置
func TestManualCommitConfig(t *testing.T) {
	config := &Config{
		Brokers:    []string{"localhost:9092"},
		Topic:      "orders",
		GroupID:    "order-processor",
		AutoCommit: false, // 手动提交（推荐）
		MaxRetries: 3,
	}

	setConsumerDefaults(config)

	if config.AutoCommit != false {
		t.Errorf("Expected AutoCommit=false, got %v", config.AutoCommit)
	}

	if config.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries=3, got %d", config.MaxRetries)
	}

	t.Logf("Manual commit mode configured: AutoCommit=%v, MaxRetries=%d",
		config.AutoCommit, config.MaxRetries)
}

// TestAutoCommitConfig 测试自动提交模式配置
func TestAutoCommitConfig(t *testing.T) {
	config := &Config{
		Brokers:        []string{"localhost:9092"},
		Topic:          "logs",
		GroupID:        "log-consumer",
		AutoCommit:     true,
		CommitInterval: 5 * time.Second,
	}

	setConsumerDefaults(config)

	if config.AutoCommit != true {
		t.Errorf("Expected AutoCommit=true, got %v", config.AutoCommit)
	}

	if config.CommitInterval != 5*time.Second {
		t.Errorf("Expected CommitInterval=5s, got %v", config.CommitInterval)
	}

	t.Logf("AutoCommit mode configured: AutoCommit=%v, CommitInterval=%v",
		config.AutoCommit, config.CommitInterval)
}
