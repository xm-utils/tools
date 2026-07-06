package redis_stream

import (
	"context"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/sirupsen/logrus"
)

// ==================== 消费者接口定义 ====================

// EnhancedHandler 增强后的消息处理器（经过所有装饰器）
//type EnhancedHandler func(ctx context.Context, msg *StreamMessage) (interface{}, error)

// ==================== 基础消费者（负责队列操作）====================

// Consumer 消费者（负责队列操作）
type Consumer struct {
	consumerName   string
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	log            *logrus.Entry
	manager        *StreamQueue
	handler        MessageHandler // 增强后的处理器（经过装饰器）
	callback       func(ctx context.Context, msg *StreamMessage, result *MessageResult)
	pendingChecker *PendingChecker
	pool           *ants.Pool // ants 协程池
	poolSize       int        // 协程池大小
}

// NewConsumer 创建消费者
func NewConsumer(manager *StreamQueue, consumerName string, handler MessageHandler) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		consumerName: consumerName,
		ctx:          ctx,
		cancel:       cancel,
		log:          logrus.WithField("module", "consumer").WithField("consumer", consumerName),
		manager:      manager,
		handler:      handler,
	}

	c.pendingChecker = NewPendingChecker(c)
	return c
}

// WithCallback 配置回调
func (c *Consumer) WithCallback(callback func(ctx context.Context, msg *StreamMessage, result *MessageResult)) *Consumer {
	c.callback = callback

	c.log.Info("已启用消息回调")

	return c
}

func (c *Consumer) WithPool(poolSize int) *Consumer {
	c.poolSize = poolSize
	// 创建 ants 协程池
	pool, err := ants.NewPool(poolSize, ants.WithPreAlloc(false), ants.WithNonblocking(false))
	if err != nil {
		c.log.Errorf("创建协程池失败: %v", err)
		panic(err)
	}
	c.pool = pool
	return c
}

// Start 启动消费者
func (c *Consumer) Start() {
	c.log.Info("启动消费者...")

	// 启动 Pending 消息检查任务
	c.wg.Add(1)
	go c.pendingChecker.Start()

	// 启动消息拉取循环
	c.wg.Add(1)
	go c.consumeLoop()

	c.log.Info("消费者已启动")
}

// Stop 停止消费者
func (c *Consumer) Stop() {
	c.log.Info("停止消费者...")
	c.cancel()
	c.wg.Wait()
	// 释放协程池资源
	if c.pool != nil {
		c.pool.Release()
		c.log.Info("协程池已释放")
	}
	c.log.Info("消费者已停止")
}

// consumeLoop 消费主循环
func (c *Consumer) consumeLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			c.log.Info("消费循环退出")
			return
		default:
			// 从 Stream 读取消息 (阻塞拉取)
			messages, err := c.manager.ReadMessages(c.ctx, c.consumerName)
			if err != nil {
				c.log.Errorf("读取消息失败: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if len(messages) == 0 {
				continue
			}

			c.log.Debugf("收到 %d 条消息,提交处理", len(messages))

			// 处理消息 - 通过协程池异步处理
			for _, msg := range messages {
				msgCopy := msg
				if c.pool == nil {
					c.processMessage(&msgCopy)
				} else {
					c.processMessageWithPool(&msgCopy)
				}
			}
		}
	}
}
func (c *Consumer) processMessageWithPool(msg *StreamMessage) {
	// 提交到协程池处理
	err1 := c.pool.Submit(func() {
		c.processMessage(msg)
	})
	if err1 != nil {
		c.log.Errorf("提交消息到协程池失败: messageId=%s, err=%v", msg.MessageID, err1)
		// 如果提交失败，同步处理作为降级方案
		c.processMessage(msg)
	}
}

// processMessage 处理单条消息
func (c *Consumer) processMessage(msg *StreamMessage) {
	c.log.Infof("开始处理消息: messageId=%s, requestId=%s", msg.MessageID, msg.RequestID)

	// 标记为处理中
	msg.Status = "processing"

	// 调用增强后的处理器（经过所有装饰器）
	data, err := c.handler(c.ctx, msg)

	var result *MessageResult
	if err != nil {
		c.log.Errorf("消息处理失败: messageId=%s, err=%v", msg.MessageID, err)
		// 创建错误结果
		result = NewErrorResult(err)
	} else {
		c.log.Infof("消息处理成功: messageId=%s, result=%v", msg.MessageID, data)
		result = NewSuccessResult(data)
	}

	// 同步调用回调函数（如果启用）
	if c.callback != nil {
		c.log.Debugf("执行消息回调: messageId=%s", msg.MessageID)
		c.callback(c.ctx, msg, result)
	}

	// 确认消息（无论成功与否都要 ACK）
	_ = c.manager.AckMessage(c.ctx, msg.MessageID)
}

// GetContext 获取上下文
func (c *Consumer) GetContext() context.Context {
	return c.ctx
}

// GetManager 获取 StreamQueue 管理器
func (c *Consumer) GetManager() *StreamQueue {
	return c.manager
}

// GetConsumerName 获取消费者名称
func (c *Consumer) GetConsumerName() string {
	return c.consumerName
}

// GetLogger 获取日志记录器
func (c *Consumer) GetLogger() *logrus.Entry {
	return c.log
}

// GetPoolSize 获取协程池大小
func (c *Consumer) GetPoolSize() int {
	return c.poolSize
}

// GetRunningWorkers 获取正在运行的工作协程数量
func (c *Consumer) GetRunningWorkers() int {
	if c.pool == nil {
		return 0
	}
	return c.pool.Running()
}
