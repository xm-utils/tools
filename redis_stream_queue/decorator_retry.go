package redis_stream

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/retry"
)

// ==================== 重试装饰器 ====================

// RetryDecorator 重试装饰器
type RetryDecorator struct {
	config        *retry.Config
	retryExecutor *retry.Executor
	log           *logrus.Entry
	ctx           context.Context
}

// NewRetryDecorator 创建重试装饰器
func NewRetryDecorator(ctx context.Context, config *retry.Config) *RetryDecorator {
	if config == nil {
		config = retry.DefaultRetryConfig()
	}

	retryExecutor := retry.NewRetryExecutor(config)

	return &RetryDecorator{
		config:        config,
		retryExecutor: retryExecutor,
		log:           logrus.WithField("module", "retry_decorator"),
		ctx:           ctx,
	}
}

// Decorate 装饰消息处理器
func (d *RetryDecorator) Decorate(handler MessageHandler) MessageHandler {
	return func(ctx context.Context, msg *StreamMessage) (interface{}, error) {
		execute := d.retryExecutor.Execute(func(ctx context.Context) (interface{}, error) {
			return handler(ctx, msg)
		}, msg)

		res := <-execute

		return res.Data, res.Error
	}
}
