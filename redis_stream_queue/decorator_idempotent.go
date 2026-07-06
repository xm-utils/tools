package redis_stream

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/redis"
)

// ==================== 幂等性装饰器 ====================

// IdempotentDecorator 幂等性装饰器
type IdempotentDecorator struct {
	prefix string
	expire time.Duration
	log    *logrus.Entry
	ctx    context.Context
}

// NewIdempotentDecorator 创建幂等性装饰器
func NewIdempotentDecorator(ctx context.Context, prefix string, expire time.Duration) *IdempotentDecorator {
	return &IdempotentDecorator{
		prefix: prefix,
		expire: expire,
		log:    logrus.WithField("module", "idempotent_decorator"),
		ctx:    ctx,
	}
}

// Decorate 装饰消息处理器
func (d *IdempotentDecorator) Decorate(handler MessageHandler) MessageHandler {
	return func(ctx context.Context, msg *StreamMessage) (interface{}, error) {
		// 1. 幂等性检查
		if d.isIdempotentProcessed(msg.RequestID) {
			d.log.Warnf("消息已处理(幂等跳过): requestId=%s", msg.RequestID)
			return nil, nil
		}

		// 2. 调用下一个处理器
		result, err := handler(ctx, msg)

		// 3. 如果处理成功，标记为已处理
		if err == nil {
			d.markIdempotentProcessed(msg.RequestID)
		}

		return result, err
	}
}

// isIdempotentProcessed 检查消息是否已处理
func (d *IdempotentDecorator) isIdempotentProcessed(requestID string) bool {
	key := d.buildIdempotentKey(requestID)
	return redis.IsExist(d.ctx, key)
}

// markIdempotentProcessed 标记消息已处理
func (d *IdempotentDecorator) markIdempotentProcessed(requestID string) {
	key := d.buildIdempotentKey(requestID)
	err := redis.SetEX(d.ctx, key, "1", int64(d.expire.Seconds()))
	if err != nil {
		d.log.Errorf("设置幂等键失败: requestId=%s, err=%v", requestID, err)
	}
}

// buildIdempotentKey 构建幂等键
func (d *IdempotentDecorator) buildIdempotentKey(requestID string) string {
	return fmt.Sprintf("%s:%s", d.prefix, requestID)
}
