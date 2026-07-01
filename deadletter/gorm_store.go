package deadletter

import (
	"context"
	"time"

	"gorm.io/gorm"
)

// GormPersistenceStore 基于GORM的持久化存储实现
// 这是一个参考实现,外部程序可以根据自己的需求实现PersistenceStore接口
type GormPersistenceStore struct {
	db        *gorm.DB
	tableName string
}

// NewGormPersistenceStore 创建GORM持久化存储
func NewGormPersistenceStore(db *gorm.DB, tableName string) *GormPersistenceStore {
	if tableName == "" {
		tableName = "dead_letter_queue"
	}
	return &GormPersistenceStore{
		db:        db,
		tableName: tableName,
	}
}

// Save 保存死信消息记录
func (s *GormPersistenceStore) Save(ctx context.Context, record *QueueMsgRecord) error {
	record.CreatedTime = time.Now()
	record.UpdatedTime = time.Now()
	return s.db.WithContext(ctx).Table(s.tableName).Create(record).Error
}

// UpdateStatus 更新消息状态
func (s *GormPersistenceStore) UpdateStatus(ctx context.Context, queueKey, messageID string, status QueueStatus, processedTime *time.Time) error {
	updates := map[string]interface{}{
		"status": status,
	}
	if processedTime != nil {
		updates["processed_time"] = processedTime
	}
	updates["updated_time"] = time.Now()
	return s.db.WithContext(ctx).Table(s.tableName).
		Where("queue_key = ? AND message_id = ?", queueKey, messageID).
		Updates(updates).Error
}

// UpdateRetryInfo 更新重试信息
func (s *GormPersistenceStore) UpdateRetryInfo(ctx context.Context, queueKey, messageID string, retryCount int, errorMessage string, lastErrorTime, nextRetryTime time.Time) error {
	return s.db.WithContext(ctx).Table(s.tableName).
		Where("queue_key = ? AND message_id = ?", queueKey, messageID).
		Updates(map[string]interface{}{
			"retry_count":     retryCount,
			"error_message":   errorMessage,
			"last_error_time": lastErrorTime,
			"next_retry_time": nextRetryTime,
			"updated_time":    time.Now(),
		}).Error
}

// FindByMessageID 根据消息ID查询记录
func (s *GormPersistenceStore) FindByMessageID(ctx context.Context, queueKey, messageID string) (*QueueMsgRecord, error) {
	var record QueueMsgRecord
	err := s.db.WithContext(ctx).Table(s.tableName).
		Where("queue_key = ? AND message_id = ?", queueKey, messageID).
		First(&record).Error

	if err != nil {
		return nil, err
	}
	return &record, nil
}

// GetStats 获取队列统计信息(用于监控服务)
func (s *GormPersistenceStore) GetStats(ctx context.Context, queueKey string) (*DatabaseStats, error) {
	stats := &DatabaseStats{}

	// 统计各状态数量和平均重试次数
	type StatsResult struct {
		PendingCount    int64   `gorm:"column:pending_count"`
		ProcessingCount int64   `gorm:"column:processing_count"`
		ProcessedCount  int64   `gorm:"column:processed_count"`
		AbandonedCount  int64   `gorm:"column:abandoned_count"`
		AvgRetryCount   float64 `gorm:"column:avg_retry_count"`
	}

	var result StatsResult
	err := s.db.WithContext(ctx).Table(s.tableName).
		Where("queue_key = ?", queueKey).
		Select("COUNT(CASE WHEN status = 1 THEN 1 END) as pending_count, " +
			"COUNT(CASE WHEN status = 2 THEN 1 END) as processing_count, " +
			"COUNT(CASE WHEN status = 3 THEN 1 END) as processed_count, " +
			"COUNT(CASE WHEN status = 4 THEN 1 END) as abandoned_count, " +
			"AVG(retry_count) as avg_retry_count").
		Scan(&result).Error

	if err != nil {
		return nil, err
	}

	stats.PendingCount = result.PendingCount
	stats.ProcessingCount = result.ProcessingCount
	stats.ProcessedCount = result.ProcessedCount
	stats.AbandonedCount = result.AbandonedCount
	stats.AvgRetryCount = result.AvgRetryCount

	return stats, nil
}
