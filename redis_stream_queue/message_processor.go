package redis_stream

import (
	"context"
)

// ==================== 消息处理器配置结构体 ====================

// MessageProcessor 消息处理器接口
type MessageProcessor interface {
	// HasCallback 判断是否配置了回调函数
	HasCallback() bool

	// Handler 消息处理函数（必需）
	// 定义消息处理的核心业务逻辑
	Handler(ctx context.Context, msg *StreamMessage) (interface{}, error)

	// Callback 消息回调函数（可选）
	// 在消息处理完成后同步调用，用于通知应用程序处理结果
	// 如果不需要回调，可以设置为 nil
	Callback(ctx context.Context, msg *StreamMessage, result *MessageResult)
}
