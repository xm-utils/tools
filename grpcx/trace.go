package grpcx

import (
	"github.com/xm-utils/tools/trace"
	"google.golang.org/grpc"
)

// 链路追踪装配：把 trace 组件的 gRPC 拦截器封装成 grpcx 友好的 Option，
// 使 grpcx 成为治理能力的装配现场。全部依赖 trace.Init 预先装配的全局
// TracerProvider / 传播器；未 Init 时拦截器为透传零行为，接入无副作用。

// TraceServerOptions 返回服务端链路追踪 grpc.ServerOption（一元 + 流拦截器）。
// 展开传入 NewGrpcServer / WithGrpcServer 的 opts 即可：
//
//	grpcx.NewGrpcServer(name, addr, register, grpcx.TraceServerOptions(name)...)
//
// 与业务既有拦截器共存时，建议置于链首以最先提取上游 traceparent。
func TraceServerOptions(tracerName string) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(trace.UnaryServerInterceptor(tracerName)),
		grpc.ChainStreamInterceptor(trace.StreamServerInterceptor(tracerName)),
	}
}

// WithClientTrace 为 GRPCClient 追加客户端链路追踪拦截器（一元 + 流），
// 发起调用时自动开启 CLIENT span 并把 traceparent 注入出站 metadata。
//
//	cli := grpcx.NewGRPCClient(creator, grpcx.WidthHost(host), grpcx.WithClientTrace("order-svc"))
func WithClientTrace(tracerName string) ClientOption {
	return func(opts *clientOptions) {
		opts.grpcOptions = append(opts.grpcOptions,
			grpc.WithChainUnaryInterceptor(trace.UnaryClientInterceptor(tracerName)),
			grpc.WithChainStreamInterceptor(trace.StreamClientInterceptor(tracerName)),
		)
	}
}

// TraceClientDialOptions 直接返回客户端 grpc.DialOption，供不走 GRPCClient、
// 自行 grpc.NewClient 的场景复用。
func TraceClientDialOptions(tracerName string) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(trace.UnaryClientInterceptor(tracerName)),
		grpc.WithChainStreamInterceptor(trace.StreamClientInterceptor(tracerName)),
	}
}
