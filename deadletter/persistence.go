package deadletter

import (
	"context"
	"time"
)

// PersistenceStore 持久化存储接口
// 外部程序可以实现此接口来自定义持久化逻辑(如MySQL、PostgreSQL、MongoDB等)
type PersistenceStore interface {
	// Save 保存死信消息记录
	Save(ctx context.Context, record *QueueMsgRecord) error

	// UpdateStatus 更新消息状态
	UpdateStatus(ctx context.Context, queueKey, messageID string, status QueueStatus, processedTime *time.Time) error

	// UpdateRetryInfo 更新重试信息
	UpdateRetryInfo(ctx context.Context, queueKey, messageID string, retryCount int, errorMessage string, lastErrorTime, nextRetryTime time.Time) error

	// FindByMessageID 根据消息ID查询记录
	FindByMessageID(ctx context.Context, queueKey, messageID string) (*QueueMsgRecord, error)

	// GetStats 获取队列统计信息(用于监控服务)
	GetStats(ctx context.Context, queueKey string) (*DatabaseStats, error)
}

// DatabaseStats 数据库统计信息(用于监控服务)
type DatabaseStats struct {
	PendingCount    int64   // 待处理数量
	ProcessingCount int64   // 处理中数量
	ProcessedCount  int64   // 已处理数量
	AbandonedCount  int64   // 已放弃数量
	AvgRetryCount   float64 // 平均重试次数
}
