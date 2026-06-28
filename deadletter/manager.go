package deadletter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/redis"
)

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

// QueueManager 死信队列管理器
type QueueManager struct {
	config  *Config
	handler MessageHandler
	store   PersistenceStore
	log     *logrus.Entry
	metrics *QueueMetrics
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewQueueManager 创建死信队列管理器
func NewQueueManager(config *Config, handler MessageHandler, store PersistenceStore) *QueueManager {
	if config == nil {
		config = DefaultConfig("default")
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &QueueManager{
		config:  config,
		handler: handler,
		store:   store,
		log: logrus.WithFields(logrus.Fields{
			"module":   "QueueManager",
			"queueKey": config.QueueKey,
		}),
		metrics: NewQueueMetrics(config.QueueKey, store),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// PushToDeadLetter 将消息推入死信队列(Redis List + Stream双写)
func (m *QueueManager) PushToDeadLetter(messageID string, messageData string, errorMessage string, retryCount int) error {
	now := time.Now()

	// 1. 写入Redis List(用于快速恢复)
	dlqMessage := &DLQMessage{
		MessageID:    messageID,
		MessageData:  messageData,
		ErrorMessage: errorMessage,
		RetryCount:   retryCount,
		MaxRetry:     m.config.MaxRetry,
		Timestamp:    now.UnixMilli(),
	}

	payload, err := json.Marshal(dlqMessage)
	if err != nil {
		m.log.Errorf("序列化死信消息失败: messageID=%s, err=%v", messageID, err)
		return err
	}

	// 推入Redis List
	if err := redis.LPush(m.ctx, m.config.DeadLetterStream, string(payload)); err != nil {
		m.log.Errorf("推入Redis死信队列失败: messageID=%s, err=%v", messageID, err)
		return err
	}

	// 2. 持久化到存储
	dlqRecord := &QueueMsgRecord{
		QueueKey:      m.config.QueueKey,
		MessageID:     messageID,
		MessageData:   messageData,
		ErrorMessage:  errorMessage,
		RetryCount:    retryCount,
		MaxRetry:      m.config.MaxRetry,
		Status:        DLQStatusPending,
		NextRetryTime: &now,
		LastErrorTime: &now,
	}

	if m.store != nil {
		if err := m.store.Save(m.ctx, dlqRecord); err != nil {
			m.log.Errorf("持久化死信消息失败: messageID=%s, err=%v", messageID, err)
			// 持久化失败不影响Redis操作,仅记录日志
		}
	}

	m.metrics.RecordDeadLetter()
	m.log.Warnf("消息已移入死信队列: messageID=%s, retryCount=%d/%d", messageID, retryCount, m.config.MaxRetry)

	return nil
}

// StartRecovery 启动死信恢复服务(定期从Redis恢复消息并重试)
func (m *QueueManager) StartRecovery() {
	go func() {
		ticker := time.NewTicker(m.config.RecoveryInterval)
		defer ticker.Stop()

		m.log.Infof("死信恢复服务已启动, 检查间隔: %v", m.config.RecoveryInterval)

		for {
			select {
			case <-m.ctx.Done():
				m.log.Info("死信恢复服务已停止")
				return
			case <-ticker.C:
				m.recoverAndRetry()
			}
		}
	}()
}

// recoverAndRetry 从死信队列恢复消息并重试1次
func (m *QueueManager) recoverAndRetry() {
	m.log.Info("开始执行死信消息恢复...")

	// 从Redis List中批量获取消息
	messages, err := redis.LRange[string](m.ctx, m.config.DeadLetterStream, 0, int64(m.config.BatchSize-1))
	if err != nil || len(messages) == 0 {
		if err != nil {
			m.log.Errorf("从Redis读取死信消息失败: err=%v", err)
		}
		return
	}

	recovered := 0
	failed := 0

	for _, msgStr := range messages {
		var dlqMsg DLQMessage
		if err := json.Unmarshal([]byte(msgStr), &dlqMsg); err != nil {
			m.log.Errorf("解析死信消息失败: err=%v", err)
			// 移除无效消息
			redis.LRem(m.ctx, m.config.DeadLetterStream, 1, msgStr)
			continue
		}

		// 检查是否超过最大重试次数
		if dlqMsg.RetryCount >= dlqMsg.MaxRetry {
			m.log.Warnf("消息超过最大重试次数,标记为放弃: messageID=%s", dlqMsg.MessageID)
			m.markAsAbandoned(&dlqMsg)
			redis.LRem(m.ctx, m.config.DeadLetterStream, 1, msgStr)
			failed++
			continue
		}

		// 执行重试(仅重试1次)
		m.log.Infof("重试死信消息: messageID=%s, retryCount=%d", dlqMsg.MessageID, dlqMsg.RetryCount+1)

		err := m.handler(m.ctx, dlqMsg.MessageData)
		if err != nil {
			m.log.Errorf("死信消息重试失败: messageID=%s, err=%v", dlqMsg.MessageID, err)
			// 更新重试计数并重新推入队列尾部
			dlqMsg.RetryCount++
			updatedPayload, _ := json.Marshal(dlqMsg)
			redis.LRem(m.ctx, m.config.DeadLetterStream, 1, msgStr)
			redis.RPush(m.ctx, m.config.DeadLetterStream, string(updatedPayload))

			// 更新存储
			m.updateRetryCount(dlqMsg.MessageID, dlqMsg.RetryCount, err.Error())
			failed++
		} else {
			m.log.Infof("死信消息重试成功: messageID=%s", dlqMsg.MessageID)
			// 从队列中移除
			redis.LRem(m.ctx, m.config.DeadLetterStream, 1, msgStr)
			// 标记为已处理
			m.markAsProcessed(&dlqMsg)
			recovered++
			m.metrics.RecordRecovery()
		}
	}

	m.log.Infof("死信恢复完成: 成功=%d, 失败=%d, 总数=%d", recovered, failed, len(messages))
}

// markAsProcessed 标记消息为已处理
func (m *QueueManager) markAsProcessed(msg *DLQMessage) {
	if m.store == nil {
		return
	}

	now := time.Now()
	err := m.store.UpdateStatus(m.ctx, m.config.QueueKey, msg.MessageID, DLQStatusProcessed, &now)

	if err != nil {
		m.log.Errorf("更新消息状态为已处理失败: messageID=%s, err=%v", msg.MessageID, err)
	}
}

// markAsAbandoned 标记消息为已放弃
func (m *QueueManager) markAsAbandoned(msg *DLQMessage) {
	if m.store == nil {
		return
	}

	now := time.Now()
	err := m.store.UpdateStatus(m.ctx, m.config.QueueKey, msg.MessageID, DLQStatusAbandoned, &now)

	if err != nil {
		m.log.Errorf("更新消息状态为已放弃失败: messageID=%s, err=%v", msg.MessageID, err)
	}
}

// updateRetryCount 更新重试计数
func (m *QueueManager) updateRetryCount(messageID string, retryCount int, errorMessage string) {
	if m.store == nil {
		return
	}

	now := time.Now()
	nextRetry := now.Add(m.config.RetryInterval)

	err := m.store.UpdateRetryInfo(m.ctx, m.config.QueueKey, messageID, retryCount, errorMessage, now, nextRetry)

	if err != nil {
		m.log.Errorf("更新重试计数失败: messageID=%s, err=%v", messageID, err)
	}
}

// GetQueueLength 获取队列长度
func (m *QueueManager) GetQueueLength() (int64, error) {
	return redis.LLen(m.ctx, m.config.DeadLetterStream)
}

// Stop 停止死信队列服务
func (m *QueueManager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.log.Info("死信队列管理器已停止")
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
