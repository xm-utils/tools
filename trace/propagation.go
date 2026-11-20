package trace

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/metadata"
)

// 本文件覆盖三类跨进程载体的注入/提取，传播格式统一走 Init 装配的
// W3C 组合传播器（traceparent 为主）。未 Init 时 otel 全局为 noop 传播器：
// Inject 不写入任何键、Extract 原样返回 ctx，调用均安全。

// ---- gRPC metadata（grpcx Interceptor 用，载体约定 traceparent）----

// InjectGRPC 将 ctx 的链路上下文写入出站 grpc metadata，返回携带新 metadata 的 ctx，
// 供客户端 interceptor 在发起调用前调用；既有 metadata 键保留不动。
func InjectGRPC(ctx context.Context) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	} else {
		md = md.Copy()
	}
	otel.GetTextMapPropagator().Inject(ctx, mdCarrier(md))
	return metadata.NewOutgoingContext(ctx, md)
}

// mdCarrier 把 grpc metadata 适配为 TextMapCarrier（otel 官方未提供，
// Set 为整键覆盖，与 metadata.Set 语义一致）。
type mdCarrier metadata.MD

func (c mdCarrier) Get(key string) string {
	if v := metadata.MD(c).Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}

func (c mdCarrier) Set(key, value string) {
	metadata.MD(c).Set(key, value)
}

func (c mdCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// ExtractGRPC 从入站 grpc metadata 还原链路上下文并挂回 ctx，供服务端
// interceptor 在 handler 执行前调用；metadata 缺失或无携带时返回原 ctx。
func ExtractGRPC(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, mdCarrier(md))
}

// ---- HTTP Header（httpclient RoundTripper / 服务端 Handler 包装用）----

// InjectHTTP 将 ctx 的链路上下文写入 req.Header（标准 traceparent 头）。
func InjectHTTP(ctx context.Context, req *http.Request) {
	if req == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
}

// ExtractHTTP 从 req.Header 还原链路上下文并挂回 ctx；无携带时返回原 ctx。
func ExtractHTTP(ctx context.Context, req *http.Request) context.Context {
	if req == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(req.Header))
}

// ---- 消息头（mq.Message.Headers / kafka header 等 map[string]string 载体）----
// 不 import mq/core：Headers 的载体类型就是 map[string]string，避免跨模块依赖；
// 调用方直接传 mq 门面 Message.Headers（由调用方负责惰性初始化）。

// InjectMap 将 ctx 的链路上下文写入 carrier（如 mq.Message.Headers）。
// carrier 为 nil 时无法写回，静默无效——调用方需保证已初始化。
func InjectMap(ctx context.Context, carrier map[string]string) {
	if carrier == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(carrier))
}

// ExtractMap 从 carrier（如 mq.Message.Headers）还原链路上下文；无携带时返回原 ctx。
func ExtractMap(ctx context.Context, carrier map[string]string) context.Context {
	if len(carrier) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
}

// TraceParentFromCtx 返回当前上下文的 W3C traceparent 单值串（如
// "00-<traceId>-<spanId>-01"）；无有效上下文返回空串。供不便传整张
// 载体表的场景使用（如写入 DB 审计字段、日志 JSON 附加字段）。
func TraceParentFromCtx(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier["traceparent"]
}
