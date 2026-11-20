// Package trace 基于 OpenTelemetry 提供链路追踪与指标计量的初始化、
// W3C traceparent 上下文跨进程传播的统一封装。
//
// 组件定位（见 ARCHITECTURE_ROADMAP.md 3.1.1/3.1.2）：
//   - Tracer / Meter 全局注册：Init 一次装配，业务经 Tracer()/Meter() 取用；
//   - 上下文贯通辅助：Inject*/Extract* 覆盖 grpc metadata、HTTP Header、
//     mq.Message.Headers 三类载体，传播格式统一为 W3C traceparent；
//   - 采样策略：TraceIDRatio 头部采样 + 父级优先（同一条链采样决策一致），
//     远端动态采样预留 Config.Sampler 扩展点。
//
// 设计约束：
//   - 对 otel SDK 的适配全部写在本组件内，不 fork 三方库；
//   - 对外 API 不向调用方抛 panic，一律以 error 返回；
//   - 未 Init 或 Init 失败时全部函数保持安全零行为（取到 noop Tracer/Meter、
//     Extract 原样返回 ctx），业务无需判空。
package trace

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Tracer 返回全局 Tracer。name 建议传组件名（如 "grpcx"、业务服务名）。
// 未 Init 时返回 noop 实现，调用安全。
func Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	return otel.GetTracerProvider().Tracer(name, opts...)
}

// Meter 返回全局 Meter。name 建议传组件名。
// 指标命名遵循 xmt_<component>_<name> 约定（见 MetricName）。
// 未 Init 时返回 noop 实现，调用安全。
func Meter(name string, opts ...metric.MeterOption) metric.Meter {
	return otel.GetMeterProvider().Meter(name, opts...)
}

// MetricName 按仓库命名规范拼装指标名：xmt_<component>_<name>。
// 例：MetricName("breaker", "open_total") -> "xmt_breaker_open_total"
func MetricName(component, name string) string {
	return fmt.Sprintf("xmt_%s_%s", component, name)
}

// Start 便捷方法：以 op 为名在 ctx 下开启一个 Span，返回新上下文与 Span。
// 调用方负责 defer span.End()。
func Start(ctx context.Context, tracerName, op string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return Tracer(tracerName).Start(ctx, op, opts...)
}

// TraceIDFromCtx 提取当前上下文的 TraceID 十六进制串；无有效上下文返回空串。
// 供日志、统一响应体（common 层）复用，全链路 grep 排障的锚点。
func TraceIDFromCtx(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}

// SpanIDFromCtx 提取当前上下文的 SpanID 十六进制串；无有效上下文返回空串。
func SpanIDFromCtx(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if sc := trace.SpanContextFromContext(ctx); sc.HasSpanID() {
		return sc.SpanID().String()
	}
	return ""
}

// HasSpanContext 判断上下文是否已携带有效链路上下文。
func HasSpanContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	return trace.SpanContextFromContext(ctx).IsValid()
}
