package redis_stream

import "time"

// ==================== Stream 队列配置 ====================

// RedisConfig Redis 连接配置
type RedisConfig struct {
	Addr         string        // Redis 地址，格式: host:port
	Password     string        // Redis 密码（可选）
	DB           int           // Redis 数据库编号
	PoolSize     int           // 连接池大小
	MinIdleConns int           // 最小空闲连接数
	MaxRetries   int           // 最大重试次数
	DialTimeout  time.Duration // 连接超时时间
	ReadTimeout  time.Duration // 读取超时时间
	WriteTimeout time.Duration // 写入超时时间
}

func DefaultRedisConfig() *RedisConfig {
	return &RedisConfig{
		Addr:         "localhost:6379",
		Password:     "",
		DB:           0,
		PoolSize:     10,
		MinIdleConns: 5,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
}

// QueueConfig Stream 队列配置
type QueueConfig struct {
	StreamKey      string
	ConsumerGroup  string
	ReadBlockMs    time.Duration
	ReadBatchCount int64
}

func DefaultQueueConfig(streamKey, groupName string) *QueueConfig {
	return &QueueConfig{
		StreamKey:      streamKey,
		ConsumerGroup:  groupName,
		ReadBlockMs:    ReadBlockMs,
		ReadBatchCount: ReadBatchCount,
	}
}

// ==================== 监控与告警配置 ====================

// AlertConfig 告警配置
type AlertConfig struct {
	StreamLengthThreshold int64         // Stream 长度告警阈值
	ConsumerLagThreshold  int64         // 消费延迟告警阈值
	DeadLetterGrowthRate  int64         // 死信队列增长率告警阈值(每小时)
	CallbackFailureRate   float64       // 回调失败率告警阈值
	AlertCheckInterval    time.Duration // 告警检查间隔
	AlertCooldown         time.Duration // 告警冷却时间(避免频繁告警)
}

// DefaultAlertConfig 默认告警配置
var DefaultAlertConfig = &AlertConfig{
	StreamLengthThreshold: AlertStreamLengthThreshold,
	ConsumerLagThreshold:  AlertConsumerLagThreshold,
	DeadLetterGrowthRate:  AlertDeadLetterGrowthRate,
	CallbackFailureRate:   AlertCallbackFailureRate,
	AlertCheckInterval:    1 * time.Minute,
	AlertCooldown:         10 * time.Minute,
}
