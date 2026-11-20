package trace

import (
	"context"

	oteltrace "go.opentelemetry.io/otel/trace"
)

// 消息队列侧的 span 便捷封装：把「注入/提取 traceparent + 开收发消息 Span」合并为
// 一对高阶函数，调用方（mq 适配器）只需传入消息头 map 与命名，无需 import otel。
// headers 约定为 mq.Message.Headers（map[string]string），载体键为 W3C "traceparent"。
// 未 Init 时传播器无操作、provider 为 noop，两个函数退化为「原样返回 ctx + 空 end」。

// StartProducerSpan 开启一个 PRODUCER span 并把其 traceparent 写入 headers
// （headers 必须已初始化，nil 时无法写回）。返回携带 span 的 ctx 与结束回调，
// 调用方 defer end()。发送端应在把 headers 序列化进消息体/消息头之前调用。
func StartProducerSpan(ctx context.Context, tracerName, spanName string, headers map[string]string) (context.Context, func()) {
	ctx, span := Tracer(tracerName).Start(ctx, spanName, oteltrace.WithSpanKind(oteltrace.SpanKindProducer))
	InjectMap(ctx, headers)
	return ctx, func() { span.End() }
}

// StartConsumerSpan 从 headers 还原上游链路并开启 CONSUMER span，
// 返回携带 span 的 ctx（供 handler 使用，TraceID 与生产端贯通）与结束回调。
func StartConsumerSpan(ctx context.Context, tracerName, spanName string, headers map[string]string) (context.Context, func()) {
	ctx = ExtractMap(ctx, headers)
	ctx, span := Tracer(tracerName).Start(ctx, spanName, oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	return ctx, func() { span.End() }
}
