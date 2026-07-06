package redis_stream

import (
	"github.com/sirupsen/logrus"
)

// ==================== 多消费者管理 ====================

// MultiStreamConsumer 多流消费者管理器
type MultiStreamConsumer struct {
	consumers []*Consumer
	log       *logrus.Entry
}

// NewMultiStreamConsumer 创建多流消费者管理器
func NewMultiStreamConsumer() *MultiStreamConsumer {
	return &MultiStreamConsumer{
		consumers: make([]*Consumer, 0),
		log:       logrus.WithField("module", "multi_stream_consumer"),
	}
}

// AddConsumer 添加消费者
func (m *MultiStreamConsumer) AddConsumer(consumer *Consumer) {
	m.consumers = append(m.consumers, consumer)
}

// StartAll 启动所有消费者
func (m *MultiStreamConsumer) StartAll() {
	m.log.Infof("启动 %d 个消费者", len(m.consumers))
	for _, consumer := range m.consumers {
		consumer.Start()
	}
}

// StopAll 停止所有消费者
func (m *MultiStreamConsumer) StopAll() {
	m.log.Info("停止所有消费者...")
	for _, consumer := range m.consumers {
		consumer.Stop()
	}
	m.log.Info("所有消费者已停止")
}
