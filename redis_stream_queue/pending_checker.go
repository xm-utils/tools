package redis_stream

import (
	"time"

	"github.com/sirupsen/logrus"
)

// ==================== Pending 消息检查器 ====================

// PendingChecker Pending 消息检查器
type PendingChecker struct {
	consumer *Consumer
	log      *logrus.Entry
}

// NewPendingChecker 创建 Pending 检查器
func NewPendingChecker(consumer *Consumer) *PendingChecker {
	return &PendingChecker{
		consumer: consumer,
		log:      logrus.WithField("module", "pending_checker"),
	}
}

// Start 启动检查循环
func (p *PendingChecker) Start() {
	defer p.consumer.wg.Done()

	ticker := time.NewTicker(PendingCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.consumer.ctx.Done():
			p.log.Info("Pending 检查循环退出")
			return
		case <-ticker.C:
			p.checkPendingMessages()
		}
	}
}

// checkPendingMessages 检查并处理超时未确认的消息
func (p *PendingChecker) checkPendingMessages() {
	p.log.Debug("开始检查 Pending 消息...")

	pendingMsgs, err := p.consumer.manager.GetPendingMessages(p.consumer.ctx)
	if err != nil {
		p.log.Errorf("获取 Pending 消息失败: %v", err)
		return
	}

	if len(pendingMsgs) == 0 {
		return
	}

	p.log.Infof("发现 %d 条超时 Pending 消息", len(pendingMsgs))

	for _, pending := range pendingMsgs {
		msg, err := p.consumer.manager.ClaimMessage(p.consumer.ctx, p.consumer.consumerName, pending.ID)
		if err != nil {
			p.log.Errorf("认领 Pending 消息失败: messageId=%s, err=%v", pending.ID, err)
			continue
		}

		if msg == nil {
			continue
		}

		p.log.Warnf("认领超时消息: messageId=%s, idleTime=%v", pending.ID, pending.Idle)

		// 重新处理消息
		p.consumer.processMessage(msg)
	}
}
