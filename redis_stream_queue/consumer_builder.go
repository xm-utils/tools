package redis_stream

import (
	"context"
	"runtime"
	"time"

	"github.com/xm-utils/tools/retry"
)

// ==================== 消费者构建器 ====================

// ConsumerBuilder 消费者构建器
type ConsumerBuilder struct {
	manager          *StreamQueue
	consumerName     string
	processor        MessageProcessor // 消息处理器
	enableIdempotent bool
	idempotentPrefix string
	idempotentExpire time.Duration
	enableRetry      bool
	retryConfig      *retry.Config
	enableDeadLetter bool
	deadLetterKey    string
	poolSize         int
}

// NewConsumerBuilder 创建消费者构建器
func NewConsumerBuilder(manager *StreamQueue, consumerName string, processor MessageProcessor) *ConsumerBuilder {
	return &ConsumerBuilder{
		manager:          manager,
		consumerName:     consumerName,
		processor:        processor,
		enableIdempotent: false,
		enableRetry:      false,
		enableDeadLetter: false,
		poolSize:         runtime.NumCPU() * 2,
	}
}

// WithIdempotent 配置幂等性
func (b *ConsumerBuilder) WithIdempotent(prefix string, expire time.Duration) *ConsumerBuilder {
	b.enableIdempotent = true
	b.idempotentPrefix = prefix
	b.idempotentExpire = expire
	return b
}

// WithoutIdempotent 禁用幂等性
func (b *ConsumerBuilder) WithoutIdempotent() *ConsumerBuilder {
	b.enableIdempotent = false
	return b
}

// WithRetry 配置重试
func (b *ConsumerBuilder) WithRetry(config *retry.Config) *ConsumerBuilder {
	b.enableRetry = true
	b.retryConfig = config
	return b
}

// WithoutRetry 禁用重试
func (b *ConsumerBuilder) WithoutRetry() *ConsumerBuilder {
	b.enableRetry = false
	return b
}

// WithDeadLetter 配置死信队列
func (b *ConsumerBuilder) WithDeadLetter(key string) *ConsumerBuilder {
	b.enableDeadLetter = true
	b.deadLetterKey = key
	return b
}

// WithoutDeadLetter 禁用死信队列
func (b *ConsumerBuilder) WithoutDeadLetter() *ConsumerBuilder {
	b.enableDeadLetter = false
	return b
}

// WithPoolSize 配置协程池大小
func (b *ConsumerBuilder) WithPoolSize(poolSize int) *ConsumerBuilder {

	b.poolSize = poolSize
	return b
}

// Build 构建消费者
func (b *ConsumerBuilder) Build() *Consumer {
	// 1. 验证配置
	if b.processor == nil {
		panic("MessageProcessor cannot be nil")
	}

	// 2. 将 MessageProcessorConfig 转换为 MessageHandler（装饰器内部使用的函数类型）
	baseHandler := b.processor.Handler
	//baseHandler := func(ctx context.Context, msg *StreamMessage) (*MessageResult, error) {
	//	return b.processor.Handle(ctx, msg)
	//}

	// 3. 创建装饰器链
	chain := NewDecoratorChain()
	ctx := context.Background()

	// 4. 按顺序添加装饰器（从外到内）
	// 注意：装饰器应用顺序与添加顺序相反
	// 最终执行顺序：DeadLetter -> Retry -> Idempotent -> BaseHandler

	// 最外层：死信队列装饰器（最后执行，捕获所有失败）
	if b.enableDeadLetter {
		chain.Add(NewDeadLetterDecorator(ctx, b.manager, b.deadLetterKey))
	}

	// 中间层：重试装饰器
	if b.enableRetry {
		chain.Add(NewRetryDecorator(ctx, b.retryConfig))
	}

	// 最内层：幂等性装饰器（最先执行，紧邻业务处理器）
	if b.enableIdempotent {
		chain.Add(NewIdempotentDecorator(ctx, b.idempotentPrefix, b.idempotentExpire))
	}

	// 5. 构建增强后的处理器
	enhancedHandler := chain.Build(baseHandler)

	// 6. 创建消费者并传递 processor
	var consumer *Consumer
	consumer = NewConsumer(b.manager, b.consumerName, enhancedHandler).WithPool(b.poolSize)

	if b.processor.HasCallback() {
		consumer = consumer.WithCallback(b.processor.Callback)
	}

	return consumer
}

// ==================== 便捷函数 ====================

// RunConsumer 运行单个消费者（便捷函数）
func RunConsumer(manager *StreamQueue, consumerName string, processor MessageProcessor) *Consumer {
	consumer := NewConsumerBuilder(manager, consumerName, processor).Build()
	consumer.Start()
	return consumer
}

// RunFullConsumer 运行完整功能消费者（便捷函数）
func RunFullConsumer(manager *StreamQueue, consumerName string, processor MessageProcessor) *Consumer {
	return RunConsumer(manager, consumerName, processor)
}
