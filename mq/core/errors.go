package mq

import "errors"

// ErrManualAck 延迟手动确认哨兵：Handler 返回该错误时，适配器在本次返回点
// 既不 Commit 也不 Reject，消息保持「在途」状态，业务须在终态（成功落库/
// 耗尽转死信等）显式调用 Consumer.Commit / Consumer.Reject 完成确认。
// pipeline 桥接（mq/pipequeue）即基于该模式把确认时机挂到引擎终态钩子。
//
// 注意：Kafka 的 offset 提交粒度为分区级整体前移，手动确认模式下乱序 Commit
// 会连同此前 offset 一并视为已提交（至少一次语义下多投可接受）；Redis Stream
// 的 PEL 逐条确认无此问题。
var ErrManualAck = errors.New("mq: 延迟手动确认")

// ErrNotSubscribed 表示对未订阅（或已取消订阅）的 group-topic 消息执行 Commit/Reject。
var ErrNotSubscribed = errors.New("mq: 消息所属 group-topic 未订阅")

// ErrConsumerClosed 表示消费者已关闭，操作被拒绝。
var ErrConsumerClosed = errors.New("mq: 消费者已关闭")
