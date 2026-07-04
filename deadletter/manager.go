package deadletter

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/redis"
)

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

// PushToDeadLetter 将消息推入死信队列(仅写入Redis List)
func (m *QueueManager) PushToDeadLetter(messageID string, messageData string, errorMessage string, retryCount int) error {
	now := time.Now()

	// 写入Redis List(用于快速恢复)
	dlqMessage := &DLQMessage{
		MessageID:    messageID,
		MessageData:  messageData,
		ErrorMessage: errorMessage,
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

// recoverAndRetry 从死信队列恢复消息并执行(仅执行一次,不重试)
func (m *QueueManager) recoverAndRetry() {
	m.log.Info("开始执行死信消息恢复...")

	// 先获取队列总长度
	queueLen, err := m.GetQueueLength()
	if err != nil {
		m.log.Errorf("获取队列长度失败: err=%v", err)
		return
	}

	if queueLen == 0 {
		m.log.Info("死信队列为空,无需处理")
		return
	}

	m.log.Infof("死信队列中共有 %d 条消息,将按批次处理(每批 %d 条)", queueLen, m.config.BatchSize)

	totalProcessed := 0
	totalFailed := 0
	batchCount := 0

	// 循环处理直到队列为空
	for {
		// 从Redis List中批量获取消息
		messages, err := redis.LRange[string](m.ctx, m.config.DeadLetterStream, 0, int64(m.config.BatchSize-1))
		if err != nil || len(messages) == 0 {
			if err != nil {
				m.log.Errorf("从Redis读取死信消息失败: err=%v", err)
			}
			break
		}

		batchCount++
		m.log.Infof("开始处理第 %d 批次,本批次消息数: %d", batchCount, len(messages))

		var failedRecords []*QueueMsgRecord
		batchProcessed := 0
		batchFailed := 0

		for _, msgStr := range messages {
			var dlqMsg DLQMessage
			if err := json.Unmarshal([]byte(msgStr), &dlqMsg); err != nil {
				m.log.Errorf("解析死信消息失败: err=%v", err)
				// 移除无效消息
				redis.LRem(m.ctx, m.config.DeadLetterStream, 1, msgStr)
				continue
			}

			// 执行消息处理(仅执行一次,不重试)
			m.log.Debugf("处理死信消息: messageID=%s", dlqMsg.MessageID)

			err := m.handler(m.ctx, dlqMsg.MessageData)

			// 无论成功与否,都从Redis List中移除
			redis.LRem(m.ctx, m.config.DeadLetterStream, 1, msgStr)

			if err != nil {
				// 执行失败,收集到批量记录中
				m.log.Warnf("死信消息处理失败,将批量保存: messageID=%s, err=%v", dlqMsg.MessageID, err)
				now := time.Now()
				failedRecords = append(failedRecords, &QueueMsgRecord{
					QueueKey:      m.config.QueueKey,
					MessageID:     dlqMsg.MessageID,
					MessageData:   dlqMsg.MessageData,
					ErrorMessage:  err.Error(),
					Status:        DLQStatusAbandoned,
					Operator:      "system",
					OperatorId:    0,
					LastErrorTime: &now,
					ProcessedTime: &now,
				})
				batchFailed++
			} else {
				// 执行成功
				m.log.Debugf("死信消息处理成功: messageID=%s", dlqMsg.MessageID)
				batchProcessed++
				m.metrics.RecordRecovery()
			}
		}

		// 批量保存本批次失败的消息
		if len(failedRecords) > 0 && m.store != nil {
			if err := m.store.BatchSave(m.ctx, failedRecords); err != nil {
				m.log.Errorf("批量保存失败消息失败: batch=%d, count=%d, err=%v", batchCount, len(failedRecords), err)
			} else {
				m.log.Infof("批量保存失败消息成功: batch=%d, count=%d", batchCount, len(failedRecords))
			}
		} else if len(failedRecords) > 0 && m.store == nil {
			m.log.Warnf("未配置持久化存储,%d条失败消息将被丢弃", len(failedRecords))
		}

		totalProcessed += batchProcessed
		totalFailed += batchFailed

		m.log.Infof("第 %d 批次处理完成: 成功=%d, 失败=%d", batchCount, batchProcessed, batchFailed)

		// 检查是否还有剩余消息
		remainingLen, err := m.GetQueueLength()
		if err != nil {
			m.log.Warnf("获取剩余队列长度失败: err=%v", err)
			break
		}

		if remainingLen == 0 {
			m.log.Infof("所有批次处理完成,队列已清空")
			break
		}

		m.log.Infof("剩余消息数: %d,继续处理下一批次...", remainingLen)
	}

	m.log.Infof("死信恢复全部完成: 总成功=%d, 总失败=%d, 总批次=%d", totalProcessed, totalFailed, batchCount)
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
