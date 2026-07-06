package redis_stream

import (
	"context"
	"encoding/json"

	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/database"
	"github.com/xm-utils/tools/deadletter"
)

// ==================== 死信队列装饰器 ====================

// DeadLetterDecorator 死信队列装饰器
type DeadLetterDecorator struct {
	dlqManager *deadletter.QueueManager
	log        *logrus.Entry
	ctx        context.Context
	manager    *StreamQueue
}

// NewDeadLetterDecorator 创建死信队列装饰器
func NewDeadLetterDecorator(ctx context.Context, manager *StreamQueue, deadLetterKey string) *DeadLetterDecorator {
	letterConfig := deadletter.DefaultConfig(deadLetterKey)
	deadletterStore := deadletter.NewGormPersistenceStore(
		database.GetDB(),
		database.TableName("dead_letter_queue"),
	)
	dlqManager := deadletter.NewQueueManager(letterConfig, nil, deadletterStore)

	return &DeadLetterDecorator{
		dlqManager: dlqManager,
		log:        logrus.WithField("module", "dead_letter_decorator"),
		ctx:        ctx,
		manager:    manager,
	}
}

// Decorate 装饰消息处理器
func (d *DeadLetterDecorator) Decorate(handler MessageHandler) MessageHandler {
	return func(ctx context.Context, msg *StreamMessage) (interface{}, error) {
		// 调用下一个处理器
		result, err := handler(ctx, msg)

		// 如果处理失败，移入死信队列
		if err != nil {
			d.log.Warnf("消息处理失败，准备移入死信队列: messageId=%s, reason=%v", msg.MessageID, err)
			_ = d.moveToDeadLetter(ctx, msg, err.Error())
		}

		return result, err
	}
}

// moveToDeadLetter 移动消息到死信队列
func (d *DeadLetterDecorator) moveToDeadLetter(ctx context.Context, msg *StreamMessage, reason string) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		d.log.Errorf("死信消息序列化失败: messageId=%s, err=%v", msg.MessageID, err)
		return err
	}

	err = d.dlqManager.PushToDeadLetter(msg.MessageID, string(payload), reason, msg.RetryCount)
	if err != nil {
		d.log.Errorf("消息移入死信队列失败: messageId=%s, err=%v", msg.MessageID, err)
		return err
	}

	d.log.Warnf("消息移入死信队列: messageId=%s, reason=%s, retryCount=%d", msg.MessageID, reason, msg.RetryCount)

	return nil
}
