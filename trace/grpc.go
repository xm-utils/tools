package trace

import (
	"context"
	"io"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// gRPC 拦截器：把 W3C traceparent 沿 metadata 贯通，并为每次调用开/收一个 Span。
// 全部基于 Init 装配的全局 TracerProvider 与传播器；未 Init 时 provider 为 noop、
// 传播器无操作，拦截器退化为「直接透传」，不产生任何 Span 也不写 metadata。
//
// tracerName 建议传服务名/组件名，与 Tracer() 取用口径一致。

// grpcCodeAttr 记录 gRPC 状态码属性名（对齐 OTel semconv 的 rpc.grpc.status_code）。
const keyRPCSystem = "rpc.system"
const keyRPCService = "rpc.service"
const keyRPCMethod = "rpc.method"
const keyRPCStatus = "rpc.grpc.status_code"

// UnaryServerInterceptor 服务端一元拦截器：从入站 metadata 还原上游上下文，
// 以「父级优先」开启 SERVER span，处理结束按 error 落状态并 End。
func UnaryServerInterceptor(tracerName string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		attrs := grpcServerAttrs(ctx, info)
		ctx = ExtractGRPC(ctx)
		ctx, span := startRemote(ctx, tracerName, info.FullMethod, oteltrace.SpanKindServer, attrs)
		resp, err := handler(ctx, req)
		endGRPCSpan(span, err)
		return resp, err
	}
}

// StreamServerInterceptor 服务端流拦截器：还原上游上下文并开启 SERVER span，
// 通过包装 grpc.ServerStream 使 handler 内 s.Context() 携带该 span，
// 直到流正常结束（io.EOF）或出错时收口。
func StreamServerInterceptor(tracerName string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		attrs := grpcServerAttrs(ss.Context(), &grpc.UnaryServerInfo{FullMethod: info.FullMethod})
		ctx := ExtractGRPC(ss.Context())
		ctx, span := startRemote(ctx, tracerName, info.FullMethod, oteltrace.SpanKindServer, attrs)
		err := handler(srv, &serverStream{ServerStream: ss, ctx: ctx})
		endGRPCSpan(span, err)
		return err
	}
}

// UnaryClientInterceptor 客户端一元拦截器：开启 CLIENT span，把 traceparent
// 注入出站 metadata，随调用传播到对端；按返回 error 落状态。
func UnaryClientInterceptor(tracerName string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		attrs := []attribute.KeyValue{
			attribute.String(keyRPCSystem, "grpc"),
			attribute.String("rpc.target", cc.Target()),
		}
		attrs = append(attrs, grpcMethodAttrs(method)...)
		ctx, span := Tracer(tracerName).Start(ctx, method,
			oteltrace.WithSpanKind(oteltrace.SpanKindClient),
			oteltrace.WithAttributes(attrs...))
		defer span.End()

		err := invoker(InjectGRPC(ctx), method, req, reply, cc, opts...)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(attribute.Int(keyRPCStatus, int(status.Code(err))))
		}
		return err
	}
}

// StreamClientInterceptor 客户端流拦截器：开启 CLIENT span 并注入出站 metadata，
// 流建立成功即把 span 上下文挂到返回的 ClientStream；span 在流终结
// （RecvMsg 返回 io.EOF 或 error）时收口。
func StreamClientInterceptor(tracerName string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
		streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		attrs := []attribute.KeyValue{attribute.String(keyRPCSystem, "grpc")}
		attrs = append(attrs, grpcMethodAttrs(method)...)
		ctx, span := Tracer(tracerName).Start(ctx, method,
			oteltrace.WithSpanKind(oteltrace.SpanKindClient),
			oteltrace.WithAttributes(attrs...))

		cs, err := streamer(InjectGRPC(ctx), desc, cc, method, opts...)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return cs, err
		}
		return &clientStream{ClientStream: cs, span: span}, nil
	}
}

// serverStream 覆写 Context 使服务端 handler 拿到携带 span 的上下文。
type serverStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *serverStream) Context() context.Context { return s.ctx }

// clientStream 在流终结时 End span。
type clientStream struct {
	grpc.ClientStream
	span oteltrace.Span
}

func (c *clientStream) RecvMsg(m any) error {
	err := c.ClientStream.RecvMsg(m)
	switch {
	case err == nil:
		return nil
	case err == io.EOF:
		c.span.End()
		return err
	default:
		c.span.RecordError(err)
		c.span.SetStatus(codes.Error, err.Error())
		c.span.End()
		return err
	}
}

// startRemote 在已从 carrier 提取的 ctx 基础上开启 span；若上游 context 无效
// （未 Init 或无携带）则退化为普通 Start。ctx 已由 ExtractGRPC 挂好远端 span
// 上下文，ParentBased 采样据此保持链路一致。
func startRemote(ctx context.Context, tracerName, name string, kind oteltrace.SpanKind, attrs []attribute.KeyValue) (context.Context, oteltrace.Span) {
	return Tracer(tracerName).Start(ctx, name,
		oteltrace.WithSpanKind(kind),
		oteltrace.WithAttributes(attrs...))
}

// grpcServerAttrs 构造服务端 span 属性：rpc 系统/服务/方法 + 对端地址。
// 对端信息取自 gRPC 入站 ctx 中的 peer；缺失时自然跳过。
func grpcServerAttrs(ctx context.Context, info *grpc.UnaryServerInfo) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String(keyRPCSystem, "grpc")}
	attrs = append(attrs, grpcMethodAttrs(info.FullMethod)...)
	if p, ok := peer.FromContext(ctx); ok && p != nil && p.Addr != nil {
		attrs = append(attrs, attribute.String("net.peer.name", p.Addr.String()))
	}
	return attrs
}

// grpcMethodAttrs 拆分 /pkg.Service/Method 为 service + method 属性。
func grpcMethodAttrs(fullMethod string) []attribute.KeyValue {
	service, method := splitGRPCMethod(fullMethod)
	attrs := make([]attribute.KeyValue, 0, 2)
	if service != "" {
		attrs = append(attrs, attribute.String(keyRPCService, service))
	}
	if method != "" {
		attrs = append(attrs, attribute.String(keyRPCMethod, method))
	}
	return attrs
}

// endGRPCSpan 按处理结果收口服务端 span：error 记录并置 Error 状态，最后 End。
func endGRPCSpan(span oteltrace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.Int(keyRPCStatus, int(status.Code(err))))
	}
	span.End()
}

// splitGRPCMethod 解析 gRPC 全方法名 "/pkg.Service/Method"。
func splitGRPCMethod(full string) (service, method string) {
	// 去掉前导 '/'
	if len(full) > 0 && full[0] == '/' {
		full = full[1:]
	}
	for i := 0; i < len(full); i++ {
		if full[i] == '/' {
			return full[:i], full[i+1:]
		}
	}
	return "", full
}
