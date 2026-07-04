package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// CommonConfig Kafka通用配置（生产者和消费者共用）
type CommonConfig struct {
	Brokers  []string `yaml:"brokers"`   // Kafka broker地址列表
	ClientID string   `yaml:"client_id"` // 客户端ID
}

// ProducerConfig 生产者配置
type ProducerConfig struct {
	CommonConfig

	// Topic 默认主题（单主题模式）
	Topic string `yaml:"topic"`

	// MaxAttempts 消息发送的最大重试次数。
	// 当消息发送失败时，Writer会自动重试直到达到此最大次数。
	//
	// 默认值：3
	MaxAttempts int `yaml:"max_attempts"` // 最大重试次数

	// BatchSize 批量发送的消息数量阈值。
	// 当队列中的消息数量达到此值时，会触发批量发送。
	//
	// 默认值：1000
	BatchSize int `yaml:"batch_size"` // 批量大小

	// BatchBytes 批量发送的字节数阈值。
	// 当队列中的消息总字节数达到此值时，会触发批量发送。
	//
	// 默认值：10MB
	BatchBytes int64 `yaml:"batch_bytes"` // 批量字节数限制

	// BatchTimeout 批量发送的超时时间。
	// 即使未达到 BatchSize 或 BatchBytes 阈值，超过此时间也会触发发送。
	//
	// 默认值：10毫秒
	BatchTimeout time.Duration `yaml:"batch_timeout"` // 批量超时时间

	// ReadTimeout 从Kafka代理读取响应的超时时间。
	//
	// 默认值：10秒
	ReadTimeout time.Duration `yaml:"read_timeout"` // 读取超时时间

	// WriteTimeout 向Kafka代理写入请求的超时时间。
	//
	// 默认值：10秒
	WriteTimeout time.Duration `yaml:"write_timeout"` // 写入超时时间

	// DialTimeout 连接Kafka代理的超时时间。
	//
	// 默认值：10秒
	DialTimeout time.Duration `yaml:"dial_timeout"` // 连接超时时间

	// RequiredAcks 指定在认为消息写入成功之前需要多少个副本确认。
	//
	// 可选值：
	//   - kafka.RequireNone (0): 不需要任何确认，性能最高但可靠性最低
	//   - kafka.WaitForLocal (1): 只需要leader副本确认
	//   - kafka.RequireAll (-1): 需要所有同步副本确认，可靠性最高
	//
	// 默认值：kafka.RequireAll
	RequiredAcks kafka.RequiredAcks `yaml:"required_acks"` // 需要的确认类型

	// Async 是否启用异步发送模式。
	// 启用后，WriteMessages 调用会立即返回，不等待消息实际发送完成。
	// 需要通过 Completion 回调函数来处理发送结果。
	//
	// 默认值：true（异步发送）
	Async bool `yaml:"async"` // 是否异步发送

	// AllowAutoTopicCreation 是否允许自动创建主题。
	// 如果设置为 true，当主题不存在时，Kafka会自动创建主题。
	// 建议在生产环境中设置为 false，手动管理主题创建。
	//
	// 默认值：false
	AllowAutoTopicCreation bool `yaml:"allow_auto_topic_creation"` // 是否允许自动创建主题

	// Compression 消息压缩编解码器。
	// 启用压缩可以减少网络带宽使用，但会增加CPU开销。
	//
	// 可选值：
	//   - kafka.CompressionNone: 不压缩
	//   - kafka.CompressionGzip: Gzip压缩
	//   - kafka.CompressionSnappy: Snappy压缩
	//   - kafka.CompressionLz4: LZ4压缩
	//   - kafka.CompressionZstd: Zstd压缩
	//
	// 默认值：kafka.CompressionNone（不压缩）
	Compression kafka.Compression `yaml:"compression"` // 压缩编解码器

	// Balancer 负载均衡策略，决定消息如何分配到不同分区。
	// 需要在代码中设置，支持的实现包括：
	//   - &kafka.LeastBytes{}: 选择当前字节数最少的分区（推荐）
	//   - &kafka.RoundRobin{}: 轮询分配
	//   - &kafka.Hash{}: 基于key的哈希分配
	//   - &kafka.CRC32Balancer{}: CRC32哈希分配
	//   - &kafka.Murmur2Balancer{}: Murmur2哈希分配（与librdkafka兼容）
	//
	// 默认值：需在代码中设置（推荐 &kafka.LeastBytes{}）
	Balancer kafka.Balancer `yaml:"-"` // 负载均衡策略（需代码设置）

	// Completion 消息发送完成的回调函数。
	// 无论消息发送成功还是失败，都会调用此函数。
	// 在异步模式下（Async=true），这是获取发送结果的唯一方式。
	//
	// 参数：
	//   - messages: 已发送的消息列表
	//   - err: 如果发送失败，包含错误信息；成功则为nil
	//
	// 默认值：nil
	Completion func([]kafka.Message, error) `yaml:"-"` // 完成回调函数（需代码设置）

	// Transport 自定义传输层，用于高级网络配置。
	// 可以自定义TLS配置、代理设置等。
	// 通常使用默认的传输层即可，除非有特殊需求。
	//
	// 默认值：nil（使用默认传输层）
	Transport kafka.RoundTripper `yaml:"-"` // 自定义传输层（需代码设置）
}

// ConsumerConfig 消费者配置
type ConsumerConfig struct {
	CommonConfig

	// Reader基本配置

	// Topic 主题（单主题模式）
	Topic string `yaml:"topic"`

	// Topics 多主题列表（多主题模式，使用GroupTopics）
	Topics []string `yaml:"topics"`

	// GroupID 消费者组ID（必需）
	GroupID string `yaml:"group_id"`

	// Partition 指定分区（用于直接读取特定分区，不使用消费者组时）
	Partition int `yaml:"partition"`

	// MinBytes 向代理指示消费者可接受的最小批量大小。
	// 在从低流量主题消费时，如果将最小值设置得较高，当代理没有足够的数据来满足定义的最小值时，可能会导致延迟交付。
	// 默认值：1
	MinBytes int `yaml:"min_bytes"` // 最小字节数

	// MaxBytes 向代理指示消费者可接受的最大批量大小。
	// 代理会截断消息以满足此最大值，因此请选择一个足够大的值，以适应您最大的消息大小。
	// 默认值：1MB
	MaxBytes int `yaml:"max_bytes"` // 最大字节数

	// MaxWait 从Kafka获取批量消息时，等待新数据到来的最长时间。
	MaxWait time.Duration `yaml:"max_wait"` // 最大等待时间

	// ReadLagInterval 设置读取器滞后更新的频率。将此字段设置为负值可禁用滞后报告。
	ReadLagInterval time.Duration `yaml:"read_lag_interval"` // 滞后更新间隔（负值禁用）

	// HeartbeatInterval 用于设置读取器向消费者组发送心跳更新的可选频率。
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"` // 心跳间隔

	// SessionTimeout 可选地设置了一个时间长度，在此时间内如果没有心跳信号，协调器就会认为消费者已死，并启动重新平衡。
	SessionTimeout time.Duration `yaml:"session_timeout"` // 会话超时时间

	// RebalanceTimeout（重新平衡超时时间）用于设置协调器在重新平衡过程中等待成员加入的时间长度。
	// 对于负载较高的Kafka服务器，将此值设置得更高可能会有所帮助。
	RebalanceTimeout time.Duration `yaml:"rebalance_timeout"` // 重新平衡超时时间

	// StartOffset 决定了当消费者组发现一个没有已提交偏移量的分区时，应从何处开始消费。
	// 如果非零，则必须设置为 kafka.FirstOffset 或 kafka.LastOffset 之一。
	//
	// 默认值：kafka.FirstOffset
	//
	// 仅在设置了 GroupID 时使用
	StartOffset int64 `yaml:"start_offset"`

	// ReadBackoffMin 可选地设置读取器在轮询新消息之前将等待的最短时间。
	//
	// 默认值：100毫秒
	ReadBackoffMin time.Duration `yaml:"read_backoff_min"` // 最小回退时间

	// ReadBackoffMax 可选地设置读取器在轮询新消息之前将等待的最长时间。
	//
	// 默认值：1秒
	ReadBackoffMax time.Duration `yaml:"read_backoff_max"` // 最大回退时间
	// CommitInterval 表示将偏移量提交到代理的间隔时间。
	// 如果为0，则提交将同步处理。
	//
	// 默认值：0
	//
	// 仅在设置了GroupID时使用
	CommitInterval time.Duration `yaml:"commit_interval"` // 自动提交间隔（0表示同步提交）

	// 消费者确认相关配置
	AutoCommit   bool          `yaml:"auto_commit"`   // 是否自动提交offset（默认false，推荐手动提交）
	MaxRetries   int           `yaml:"max_retries"`   // 消息处理最大重试次数
	RetryBackoff time.Duration `yaml:"retry_backoff"` // 重试退避时间

	// 队列配置
	QueueCapacity int `yaml:"queue_capacity"` // 队列容量

	// 高级配置
	IsolationLevel   kafka.IsolationLevel `yaml:"-"`                  // 隔离级别（需代码设置）
	JoinGroupBackoff time.Duration        `yaml:"join_group_backoff"` // 加入组退避时间
	RetentionTime    time.Duration        `yaml:"retention_time"`     // 保留时间
	StartOffsets     map[string]int64     `yaml:"-"`                  // 各分区的起始offset（需代码设置）
	Logger           kafka.Logger         `yaml:"-"`                  // 自定义日志器（需代码设置）
	ErrorLogger      kafka.Logger         `yaml:"-"`                  // 错误日志器（需代码设置）
	Dialer           *kafka.Dialer        `yaml:"-"`                  // 自定义拨号器（需代码设置）
}

// Deprecated: Config已废弃，请使用ProducerConfig或ConsumerConfig
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

// GetProducerDefaults 获取生产者默认配置
func GetProducerDefaults() *ProducerConfig {
	return &ProducerConfig{
		MaxAttempts:            3,
		DialTimeout:            10 * time.Second,
		ReadTimeout:            10 * time.Second,
		WriteTimeout:           10 * time.Second,
		BatchSize:              1000,
		BatchBytes:             10e6, // 10MB
		BatchTimeout:           10 * time.Millisecond,
		RequiredAcks:           kafka.RequireAll, // 默认等待所有副本确认
		Async:                  true,             // 同步发送
		AllowAutoTopicCreation: false,            // 不允许自动创建主题
	}
}

// GetConsumerDefaults 获取消费者默认配置
func GetConsumerDefaults() *ConsumerConfig {
	return &ConsumerConfig{
		MinBytes:          1,
		MaxBytes:          10e6, // 10MB
		MaxWait:           1 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
		RebalanceTimeout:  30 * time.Second,
		ReadBackoffMin:    100 * time.Millisecond,
		ReadBackoffMax:    1 * time.Second,
		QueueCapacity:     1000,
		MaxRetries:        3,               // 默认重试3次
		RetryBackoff:      1 * time.Second, // 默认退避1秒
		JoinGroupBackoff:  3 * time.Second,
		RetentionTime:     24 * time.Hour, // 默认保留24小时
		ReadLagInterval:   -1,             // 禁用滞后报告
		AutoCommit:        false,          // 推荐手动提交
		// CommitInterval: 0                      // 同步提交
		// StartOffset 需在代码中转换为 kafka.FirstOffset 或 kafka.LastOffset
		// IsolationLevel: kafka.ReadUncommitted
	}
}
