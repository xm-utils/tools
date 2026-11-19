// Package mq 提供消息队列统一门面抽象。
//
// 业务代码只依赖本包定义的 Message / Handler / Producer / Consumer 接口，
// Kafka（mq/kafka）与 Redis 系后端（mq/redis/stream 消费者组队列、
// mq/redis/list 安全队列、mq/redis/zset 延迟队列、mq/redis/pubsub 发布订阅）
// 由子包以适配器形式实现接口，切换 MQ 时业务代码零改动。
//
// 语义约定：
//   - at-least-once 投递：确认（Commit/Reject）由消费侧驱动，处理成功前不确认；
//   - Handler 返回结果驱动自动确认：nil 表示确认；返回 ErrManualAck 表示延迟到
//     终态由业务显式 Commit/Reject；其他错误表示处理失败，由适配器按各自策略处置；
//   - 所有对外 API 不向调用方抛 panic，Handler 内 panic 统一 recover 转为 error。
package mq

import (
	"context"
	"fmt"
)

// Message 统一消息模型。业务只依赖本结构，底层原始消息保留在 Metadata 中
// （如 Kafka 侧为 kafka.Message，Redis Stream 侧为 redis_stream.StreamMessage），
// 供高级场景取用分区、offset、idle 时间等专有字段。
type Message struct {
	// ID 适配器签发的**不透明投递令牌**：含 group 作用域前缀（kafka / redis-stream 为
	// "group|…"，redis-list / redis-zset 为领取者+进程内序号，redis-pubsub 为订阅者+时间戳）。
	// 仅用于日志追踪与原样回传 Commit/Reject，业务不应解析其结构或据此幂等去重
	// （幂等请用 Key / Headers）。
	ID string
	// Key 消息业务键（Kafka 分区键 / Redis Stream 幂等请求 ID），可为空。
	Key string
	// Topic 消息所属主题（Kafka: topic；Redis Stream: stream key）。
	Topic string
	// Body 消息体字节流，门面不感知其编码格式。
	Body []byte
	// Headers 消息头（重试计数、追踪上下文等元信息）。
	Headers map[string]string
	// Metadata 底层原始消息，类型由各适配器定义，可为 nil。
	Metadata any
}

// Header 读取消息头，不存在时返回空串。
func (m *Message) Header(name string) string {
	if m == nil || m.Headers == nil {
		return ""
	}
	return m.Headers[name]
}

// SetHeader 设置消息头（惰性初始化 Headers，调用方无需判空）。
func (m *Message) SetHeader(name, value string) {
	if m.Headers == nil {
		m.Headers = make(map[string]string)
	}
	m.Headers[name] = value
}

// Handler 消息处理函数。返回值语义直接决定适配器的确认行为：
//   - nil：处理成功，适配器自动 Commit；
//   - ErrManualAck：不在 Handler 返回点确认，延迟到业务终态显式 Commit/Reject
//     （pipeline 桥接即基于该模式把确认时机挂到引擎终态钩子）；
//   - 其他 error：处理失败，适配器按各自失败策略处置（见子包文档）。
//
// Handler 内 panic 会被 InvokeHandler 统一 recover 转为 error，不会击穿消费循环。
type Handler func(ctx context.Context, msg *Message) error

// Producer 消息生产者抽象。实现必须线程安全。
type Producer interface {
	// Produce 向指定主题发送一条消息。msg.Topic 字段被忽略，以 topic 参数为准。
	Produce(ctx context.Context, topic string, msg *Message) error
	// ProduceBatch 向指定主题批量发送消息。
	ProduceBatch(ctx context.Context, topic string, msgs []*Message) error
	// Close 释放生产者资源，重复调用安全。
	Close() error
}

// Consumer 消息消费者抽象。确认粒度以 Message 为单位，
// 具体落地由适配器映射到底层 MQ 的确认机制（Kafka offset 提交 / Redis XACK）。
type Consumer interface {
	// Subscribe 订阅 group-topic 并阻塞式消费，直到 ctx 结束或 Close：退出返回值为
	// ctx.Err()（Close 触发取消时为 context.Canceled），其余非 nil error 仅发生在
	// 不可恢复的初始化失败。每条消息经 InvokeHandler 调用 handler，按返回结果自动
	// 确认（契约见 Handler）。重复订阅同一 group-topic 返回 error；退出后同一
	// group-topic 可重新订阅。常驻消费请将本方法放入独立 goroutine。
	Subscribe(ctx context.Context, group, topic string, handler Handler) error
	// Commit 显式确认一条 ErrManualAck 模式下的在途消息（终态成功处置）。
	// 对已确认/未知消息的重复 Commit 应返回 nil 或明确 error，不得 panic。
	// 无持久化与确认原语的后端（如 mq/redis/pubsub）下本方法为 no-op 返回 nil，
	// 投递保证降为 at-most-once，适用边界见子包文档。
	Commit(ctx context.Context, msg *Message) error
	// Reject 显式否定一条在途消息：requeue=true 重回队列等待再次投递；
	// requeue=false 终态丢弃（死信留存由业务在调用前自行完成）。
	// 无持久化后端（如 mq/redis/pubsub）下重投无从实现，同样为 no-op 返回 nil。
	Reject(ctx context.Context, msg *Message, requeue bool) error
	// Close 停止全部订阅并释放消费者资源，重复调用安全。
	Close() error
}

// InvokeHandler 以 panic 安全方式执行 handler：handler 内 panic 被 recover
// 并包装为 error 返回，与仓库「panic 转 error」规范一致。适配器消费循环
// 必须经本函数调用 Handler，不得直接调用。
func InvokeHandler(ctx context.Context, handler Handler, msg *Message) (err error) {
	if handler == nil {
		return fmt.Errorf("mq: handler 不能为 nil")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("mq: handler panic: %v", r)
		}
	}()
	return handler(ctx, msg)
}
