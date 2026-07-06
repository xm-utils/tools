package redis_stream

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

const (
	// StreamMaxLen 队列最大长度限制 (每个 Stream 最多保留 100万条消息)
	StreamMaxLen = 1000000

	// DeadLetterKey 死信队列 Key
	DeadLetterKey = "stream:dead_letter"

	// PendingCheckInterval Pending 消息检查间隔
	PendingCheckInterval = 60 * time.Second

	// 消费者配置
	ReadBatchCount = 100             // XREADGROUP COUNT 参数: 每次拉取100条
	ReadBlockMs    = 5 * time.Second // XREADGROUP BLOCK 参数: 阻塞5秒
)

// ==================== 消息体结构 ====================

// StreamMessage 消息结构体
type StreamMessage struct {
	MessageID        string `json:"messageId"`   // 消息唯一ID (Redis Stream Message ID)
	RequestID        string `json:"requestId"`   // 请求唯一ID (用于幂等性)
	Payload          string `json:"payload"`     // 请求载荷
	EnqueueTime      int64  `json:"enqueueTime"` // 入队时间戳 (Unix毫秒)
	RetryCount       int    `json:"retryCount"`  // 重试次数
	Status           string `json:"status"`      // 消息状态 (pending/processing/success/failed/dead_letter)
	DeadLetterReason string `json:"dead_letter_reason"`
	DeadLetterTime   int64  `json:"dead_letter_time"`
}

// ==================== Stream 队列管理器 ====================

// StreamQueue Redis Stream 队列管理器
type StreamQueue struct {
	log    *logrus.Entry
	config *QueueConfig
	client *redis.Client
}

// NewStreamQueue 创建 Stream 队列管理器
func NewStreamQueue(config *QueueConfig, client *redis.Client) (*StreamQueue, error) {

	return &StreamQueue{
		log:    logrus.WithField("module", "stream_queue"),
		config: config,
		client: client,
	}, nil
}

// InitStream 初始化 Stream 和消费者组
func (m *StreamQueue) InitStream(ctx context.Context) error {
	// 创建消费者组 (如果已存在会返回错误,忽略)
	err := m.client.XGroupCreateMkStream(ctx, m.config.StreamKey, m.config.ConsumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		m.log.Errorf("创建消费者组失败: stream=%s, group=%s, err=%v", m.config.StreamKey, m.config.ConsumerGroup, err)
		return err
	}

	m.log.Infof("Stream 初始化成功: stream=%s, group=%s", m.config.StreamKey, m.config.ConsumerGroup)
	return nil
}

// EnqueueMessage 推送消息到 Stream (XADD)
func (m *StreamQueue) EnqueueMessage(ctx context.Context, msg *StreamMessage) (string, error) {
	msg.EnqueueTime = time.Now().UnixMilli()
	msg.Status = "pending"
	msg.RetryCount = 0

	// 序列化消息体
	payload, err := json.Marshal(msg)
	if err != nil {
		m.log.Errorf("消息序列化失败: requestID=%s, err=%v", msg.RequestID, err)
		return "", err
	}

	// XADD 添加消息到 Stream
	messageID, err := m.client.XAdd(ctx, &redis.XAddArgs{
		Stream: m.config.StreamKey,
		MaxLen: StreamMaxLen,
		Values: map[string]interface{}{
			"data": string(payload),
		},
	}).Result()

	if err != nil {
		m.log.Errorf("消息入队失败: requestID=%s, err=%v", msg.RequestID, err)
		return "", err
	}

	msg.MessageID = messageID
	m.log.Infof("消息入队成功: stream=%s, messageId=%s, requestID=%s", m.config.StreamKey, messageID, msg.RequestID)

	return messageID, nil
}

// ReadMessages 从 Stream 读取消息 (XREADGROUP)
func (m *StreamQueue) ReadMessages(ctx context.Context, consumerName string) ([]StreamMessage, error) {
	// XREADGROUP 阻塞拉取消息
	streams, err := m.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    m.config.ConsumerGroup,
		Consumer: consumerName,
		Streams:  []string{m.config.StreamKey, ">"}, // Stream 和 ID 交替排列
		Count:    m.config.ReadBatchCount,
		Block:    m.config.ReadBlockMs,
	}).Result()

	if err != nil {
		if err == redis.Nil {
			// 没有新消息,正常情况
			return nil, nil
		}
		m.log.Errorf("读取消息失败: stream=%s, err=%v", m.config.StreamKey, err)
		return nil, err
	}

	var messages []StreamMessage
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			var streamMsg StreamMessage
			if data, ok := msg.Values["data"].(string); ok {
				if err := json.Unmarshal([]byte(data), &streamMsg); err != nil {
					m.log.Errorf("消息反序列化失败: messageId=%s, err=%v", msg.ID, err)
					continue
				}
				streamMsg.MessageID = msg.ID
				messages = append(messages, streamMsg)
			}
		}
	}

	if len(messages) > 0 {
		m.log.Debugf("读取消息成功: count=%d", len(messages))
	}

	return messages, nil
}

// AckMessage 确认消息处理完成 (XACK)
func (m *StreamQueue) AckMessage(ctx context.Context, messageID string) error {
	m.log.Debugf("确认消息处理: messageId=%s", messageID)
	_, err := m.client.XAck(ctx, m.config.StreamKey, m.config.ConsumerGroup, messageID).Result()
	if err != nil {
		m.log.Errorf("消息确认失败: messageId=%s, err=%v", messageID, err)
		return err
	}
	m.log.Debugf("消息确认成功: messageId=%s", messageID)
	// 消息确认成功后立即删除
	_ = m.DelMessage(ctx, messageID)
	return nil
}

// GetPendingMessages 获取待处理的消息 (XPENDING)
func (m *StreamQueue) GetPendingMessages(ctx context.Context) ([]redis.XPendingExt, error) {
	pending, err := m.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: m.config.StreamKey,
		Group:  m.config.ConsumerGroup,
		Idle:   0,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()

	if err != nil {
		m.log.Errorf("获取 Pending 消息失败: stream=%s, err=%v", m.config.StreamKey, err)
		return nil, err
	}

	return pending, nil
}

// ClaimMessage 认领超时未处理的消息 (XCLAIM)
func (m *StreamQueue) ClaimMessage(ctx context.Context, consumerName string, messageID string) (*StreamMessage, error) {
	msgs, err := m.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   m.config.StreamKey,
		Group:    m.config.ConsumerGroup,
		Consumer: consumerName,
		MinIdle:  5 * time.Minute,
		Messages: []string{messageID},
	}).Result()

	if err != nil {
		m.log.Errorf("认领消息失败: messageId=%s, err=%v", messageID, err)
		return nil, err
	}

	if len(msgs) == 0 {
		return nil, nil
	}

	var streamMsg StreamMessage
	if data, ok := msgs[0].Values["data"].(string); ok {
		if err := json.Unmarshal([]byte(data), &streamMsg); err != nil {
			return nil, err
		}
		streamMsg.MessageID = msgs[0].ID
		return &streamMsg, nil
	}

	return nil, nil
}

// GetStreamLength 获取 Stream 长度
func (m *StreamQueue) GetStreamLength(ctx context.Context) (int64, error) {
	length, err := m.client.XLen(ctx, m.config.StreamKey).Result()
	if err != nil {
		return 0, err
	}
	return length, nil
}

// DelMessage 删除消息
func (m *StreamQueue) DelMessage(ctx context.Context, messageID string) error {
	_, err := m.client.XDel(ctx, m.config.StreamKey, messageID).Result()
	return err
}
