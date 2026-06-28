package deadletter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// ExampleOrderMessage 示例:订单消息结构
type ExampleOrderMessage struct {
	OrderNo  string `json:"orderNo"`
	Amount   uint64 `json:"amount"`
	Merchant int64  `json:"merchant"`
}

// ExampleUsage 死信队列使用示例
func ExampleUsage() {
	// 1. 定义消息处理器
	orderHandler := func(ctx context.Context, messageData string) error {
		var order ExampleOrderMessage
		if err := json.Unmarshal([]byte(messageData), &order); err != nil {
			return fmt.Errorf("解析订单消息失败: %w", err)
		}

		logger := logrus.WithField("module", "ExampleOrderHandler")
		logger.Infof("处理订单消息: orderNo=%s, amount=%d", order.OrderNo, order.Amount)

		// TODO: 执行实际的业务逻辑
		// 例如:调用第三方支付接口、更新订单状态等

		// 模拟处理失败(用于测试)
		// return errors.New("模拟处理失败")

		return nil
	}

	// 2. 创建死信队列配置
	config := DefaultConfig("order_callback")
	config.MaxRetry = 3
	config.RecoveryInterval = 5 * time.Minute
	config.BatchSize = 10

	// 3. 创建持久化存储(可选,传nil则不持久化)
	// 使用GORM实现
	// store := NewGormPersistenceStore(db, "dead_letter_queue")
	// 或者传nil禁用持久化
	var store PersistenceStore = nil

	// 4. 创建死信队列管理器
	manager := NewQueueManager(config, orderHandler, store)

	// 5. 启动死信恢复服务
	manager.StartRecovery()

	// 6. 创建监控服务(可选)
	alertFunc := func(stats *QueueStats) {
		logrus.Warnf("死信队列告警: %+v", stats)
		// TODO: 发送告警通知(邮件、短信、钉钉等)
	}

	monitor := NewMetricsMonitor(manager, 1*time.Minute, alertFunc)
	monitor.Start()

	// 7. 在业务代码中使用 - 当消息处理失败时推入死信队列
	// 示例场景:订单回调处理失败
	failedOrderMsg := ExampleOrderMessage{
		OrderNo:  "ORD20260617001",
		Amount:   10000,
		Merchant: 1001,
	}

	messageData, _ := json.Marshal(failedOrderMsg)
	err := manager.PushToDeadLetter(
		"msg_001",           // 消息ID
		string(messageData), // 消息数据
		"第三方支付接口超时",         // 错误信息
		0,                   // 当前重试次数
	)

	if err != nil {
		logrus.Errorf("推入死信队列失败: %v", err)
	}

	// 8. 获取队列统计信息
	stats := manager.metrics.GetStats(context.Background())
	logrus.Infof("队列统计: %+v", stats)

	// 9. 应用关闭时停止服务
	defer func() {
		manager.Stop()
		monitor.Stop()
	}()
}

// ExampleCustomQueueKey 示例:自定义队列Key
func ExampleCustomQueueKey() {
	// 不同业务使用不同的队列Key
	paymentQueue := DefaultConfig("payment_callback")
	refundQueue := DefaultConfig("refund_callback")
	accountQueue := DefaultConfig("account_event")

	// 每个队列独立管理
	var store PersistenceStore = nil // 实际使用时传入具体的存储实现
	paymentManager := NewQueueManager(paymentQueue, paymentHandler, store)
	refundManager := NewQueueManager(refundQueue, refundHandler, store)
	accountManager := NewQueueManager(accountQueue, accountHandler, store)

	// 分别启动恢复服务
	paymentManager.StartRecovery()
	refundManager.StartRecovery()
	accountManager.StartRecovery()

	// 使用对应的manager推入死信
	// paymentManager.PushToDeadLetter(...)
	// refundManager.PushToDeadLetter(...)
	// accountManager.PushToDeadLetter(...)
}

// paymentHandler 支付回调处理器示例
func paymentHandler(ctx context.Context, messageData string) error {
	// TODO: 实现支付回调处理逻辑
	return nil
}

// refundHandler 退款回调处理器示例
func refundHandler(ctx context.Context, messageData string) error {
	// TODO: 实现退款回调处理逻辑
	return nil
}

// accountHandler 账户事件处理器示例
func accountHandler(ctx context.Context, messageData string) error {
	// TODO: 实现账户事件处理逻辑
	return nil
}

// ExampleIntegrationWithStreamConsumer 示例:与Stream消费者集成
func ExampleIntegrationWithStreamConsumer() {
	// 在Stream消费者中,当消息重试次数耗尽时推入死信队列
	/*
		type StreamConsumer struct {
			dlqManager *deadletter.QueueManager
		}

		func (c *StreamConsumer) processMessage(msg *StreamMessage) {
			err := c.handler(ctx, msg)
			if err != nil {
				msg.RetryCount++

				// 检查是否超过最大重试次数
				if msg.RetryCount >= MaxRetryCount {
					// 推入死信队列
					c.dlqManager.PushToDeadLetter(
						msg.MessageID,
						msg.Payload,
						err.Error(),
						msg.RetryCount,
					)
					return
				}

				// 否则继续重试
				c.retryMessage(msg)
			}
		}
	*/
}
