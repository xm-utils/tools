package redis_stream

import (
	"context"
)

// ==================== 装饰器接口 ====================

// MessageHandler 消息处理函数类型（装饰器内部使用）
type MessageHandler func(ctx context.Context, msg *StreamMessage) (interface{}, error)

// HandlerDecorator 消息处理器装饰器接口
type HandlerDecorator interface {
	// Decorate 装饰消息处理器
	Decorate(handler MessageHandler) MessageHandler
}

// ==================== 装饰器链构建器 ====================

// DecoratorChain 装饰器链
type DecoratorChain struct {
	decorators []HandlerDecorator
}

// NewDecoratorChain 创建装饰器链
func NewDecoratorChain() *DecoratorChain {
	return &DecoratorChain{
		decorators: make([]HandlerDecorator, 0),
	}
}

// Add 添加装饰器
func (dc *DecoratorChain) Add(decorator HandlerDecorator) *DecoratorChain {
	dc.decorators = append(dc.decorators, decorator)
	return dc
}

// Build 构建增强后的处理器
func (dc *DecoratorChain) Build(baseHandler MessageHandler) MessageHandler {
	// 从后往前应用装饰器
	handler := baseHandler
	for i := len(dc.decorators) - 1; i >= 0; i-- {
		handler = dc.decorators[i].Decorate(handler)
	}
	return handler
	//return EnhancedHandler(handler)
}
