package kafka

import "time"

// Config Kafka配置
type Config struct {
	Brokers       []string      `yaml:"brokers"`        // Kafka broker地址列表
	Topic         string        `yaml:"topic"`          // 默认主题（单主题模式）
	Topics        []string      `yaml:"topics"`         // 多主题列表（多主题模式）
	GroupID       string        `yaml:"group_id"`       // 消费者组ID
	ClientID      string        `yaml:"client_id"`      // 客户端ID
	MaxAttempts   int           `yaml:"max_attempts"`   // 最大重试次数
	DialTimeout   time.Duration `yaml:"dial_timeout"`   // 连接超时时间
	ReadTimeout   time.Duration `yaml:"read_timeout"`   // 读取超时时间
	WriteTimeout  time.Duration `yaml:"write_timeout"`  // 写入超时时间
	BatchSize     int           `yaml:"batch_size"`     // 批量大小
	BatchBytes    int64         `yaml:"batch_bytes"`    // 批量字节数
	MinBytes      int           `yaml:"min_bytes"`      // 最小字节数
	MaxBytes      int           `yaml:"max_bytes"`      // 最大字节数
	QueueCapacity int           `yaml:"queue_capacity"` // 队列容量

	// 消费者确认相关配置
	CommitInterval time.Duration `yaml:"commit_interval"` // 自动提交间隔（默认0表示手动提交）
	AutoCommit     bool          `yaml:"auto_commit"`     // 是否自动提交offset（默认false，推荐手动提交）
	StartOffset    int64         `yaml:"start_offset"`    // 起始offset（-2: FirstOffset, -1: LastOffset）
	MaxRetries     int           `yaml:"max_retries"`     // 消息处理最大重试次数
	RetryBackoff   time.Duration `yaml:"retry_backoff"`   // 重试退避时间
}
