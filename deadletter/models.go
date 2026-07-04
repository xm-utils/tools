package deadletter

import (
	"context"
	"fmt"
	"time"
)

// QueueStatus 死信队列消息状态
type QueueStatus int8

const (
	DLQStatusPending    QueueStatus = 1 // 待处理
	DLQStatusProcessing QueueStatus = 2 // 处理中
	DLQStatusProcessed  QueueStatus = 3 // 已处理
	DLQStatusAbandoned  QueueStatus = 4 // 已放弃
)

// QueueMsgRecord 死信队列表模型
type QueueMsgRecord struct {
	ID            uint64      `json:"id" comment:"主键ID"`
	QueueKey      string      `json:"queueKey" comment:"队列Key标识"`
	MessageID     string      `json:"messageId" comment:"消息唯一ID"`
	MessageData   string      `json:"messageData" comment:"消息内容(JSON格式)"`
	ErrorMessage  string      `json:"errorMessage" comment:"失败原因"`
	RetryCount    int         `json:"retryCount" comment:"重试次数"`
	MaxRetry      int         `json:"maxRetry" comment:"最大重试次数"`
	Status        QueueStatus `json:"status" comment:"状态: 1-待处理, 2-处理中, 3-已处理, 4-已放弃"`
	Operator      string      `json:"operator" comment:"操作人"`
	OperatorId    uint64      `json:"operatorId" comment:"操作人ID"`
	NextRetryTime *time.Time  `json:"nextRetryTime" comment:"下次重试时间"`
	LastErrorTime *time.Time  `json:"lastErrorTime" comment:"最后错误时间"`
	ProcessedTime *time.Time  `json:"processedTime" comment:"处理完成时间"`
	CreatedTime   time.Time   `json:"createdTime" comment:"创建时间"`
	UpdatedTime   time.Time   `json:"updatedTime" comment:"更新时间"`
}

// DLQMessage 死信队列消息结构
type DLQMessage struct {
	MessageID    string `json:"messageId"`
	MessageData  string `json:"messageData"`
	ErrorMessage string `json:"errorMessage"`
	RetryCount   int    `json:"retryCount"`
	MaxRetry     int    `json:"maxRetry"`
	Timestamp    int64  `json:"timestamp"`
}

// MessageHandler 消息处理器接口
type MessageHandler func(ctx context.Context, messageData string) error

// Config 死信队列配置
type Config struct {
	QueueKey         string        // Redis队列Key(支持自定义)
	DeadLetterStream string        // 死信Stream Key
	MaxRetry         int           // 最大重试次数(默认3次)
	RetryInterval    time.Duration // 重试间隔(默认1秒)
	RecoveryInterval time.Duration // 恢复检查间隔(默认5分钟)
	BatchSize        int           // 批量处理大小(默认10)
}

// DefaultConfig 返回默认配置
func DefaultConfig(queueKey string) *Config {
	return &Config{
		QueueKey:         queueKey,
		DeadLetterStream: fmt.Sprintf("dead_letter:%s", queueKey),
		MaxRetry:         3,
		RetryInterval:    1 * time.Second,
		RecoveryInterval: 5 * time.Minute,
		BatchSize:        10,
	}
}
