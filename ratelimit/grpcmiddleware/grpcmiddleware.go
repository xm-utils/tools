// Package grpcmiddleware 把 ratelimit 挂载为 gRPC 服务端拦截器(ROADMAP 3.2):
// grpcx.NewServer(grpc.StatsHandler...) 同点位一行装配,grpcx 本体零改动。
//
// 两种限流维度装配(互斥,二选一):
//   - Limiter:服务级——全部方法共享一份额额;
//   - Local + KeyFunc:接口级/租户级——按 Key 从 Local 后端取限流器,
//     KeyFunc 返回空串的请求直通不限流(公开端点场景)。
//
// 拒绝语义:超限返回 codes.ResourceExhausted,trailer 携带 retry-after-ms;
// 限流后端自身故障默认 fail-closed 返回 codes.Internal,FailOpen=true 时
// 放行(限流是防护而非鉴权,后端抖动不应拖垮业务)。
package grpcmiddleware

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/xm-utils/tools/ratelimit"
)

// TrailerRetryAfterMs trailer 中建议重试等待毫秒数的键名
const TrailerRetryAfterMs = "retry-after-ms"

// Config 拦截器装配配置
type Config struct {
	// Limiter 服务级固定限流器;与 Local 互斥
	Limiter ratelimit.Limiter
	// Local 本地内存后端,配合 KeyFunc 做多维限流;与 Limiter 互斥
	Local *ratelimit.Local
	// KeyFunc 从请求提取限流 Key(nil 时:Local 模式默认用 fullMethod;
	// 租户级可经 md,ok := metadata.FromIncomingContext(ctx) 取租户头)
	KeyFunc func(ctx context.Context, fullMethod string) (string, error)
	// FailOpen 限流后端故障时放行(默认 false:故障返回 codes.Internal)
	FailOpen bool
}

// resolve 按 (ctx, fullMethod) 定位限流器;返回 nil 表示直通不限流
type resolve func(ctx context.Context, fullMethod string) (ratelimit.Limiter, error)

func newResolve(cfg Config) (resolve, error) {
	if (cfg.Limiter == nil) == (cfg.Local == nil) {
		return nil, fmt.Errorf("%w: Limiter 与 Local 必须二选一", ratelimit.ErrInvalidConfig)
	}
	if cfg.Limiter != nil {
		return func(context.Context, string) (ratelimit.Limiter, error) {
			return cfg.Limiter, nil
		}, nil
	}
	keyFn := cfg.KeyFunc
	if keyFn == nil {
		keyFn = func(_ context.Context, fullMethod string) (string, error) {
			return fullMethod, nil
		}
	}
	return func(ctx context.Context, fullMethod string) (ratelimit.Limiter, error) {
		key, err := keyFn(ctx, fullMethod)
		if err != nil {
			return nil, err
		}
		if key == "" {
			return nil, nil
		}
		return cfg.Local.Limiter(key)
	}, nil
}

// NewUnaryServerInterceptor 构造一元服务端拦截器;Config 非法(两者皆 nil 或
// 同时设置)返回 error,不 panic
func NewUnaryServerInterceptor(cfg Config) (grpc.UnaryServerInterceptor, error) {
	r, err := newResolve(cfg)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		l, err := r(ctx, info.FullMethod)
		if err != nil {
			return onBackendError(cfg, err, func() (interface{}, error) { return handler(ctx, req) })
		}
		if l == nil {
			return handler(ctx, req)
		}
		aerr := l.Allow(ctx)
		switch {
		case aerr == nil:
			return handler(ctx, req)
		case errors.Is(aerr, ratelimit.ErrRateLimited):
			reject(ctx, l)
			return nil, status.Error(codes.ResourceExhausted, aerr.Error())
		default:
			return onBackendError(cfg, aerr, func() (interface{}, error) { return handler(ctx, req) })
		}
	}, nil
}

// NewStreamServerInterceptor 构造流服务端拦截器;限流判定在流建立时执行一次
// (长流不逐条消息计数,消息级配额如需可在业务链路内嵌 Local.AllowN)
func NewStreamServerInterceptor(cfg Config) (grpc.StreamServerInterceptor, error) {
	r, err := newResolve(cfg)
	if err != nil {
		return nil, err
	}
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		l, err := r(ctx, info.FullMethod)
		if err != nil {
			return onStreamBackendError(cfg, err, func() error { return handler(srv, ss) })
		}
		if l == nil {
			return handler(srv, ss)
		}
		aerr := l.Allow(ctx)
		switch {
		case aerr == nil:
			return handler(srv, ss)
		case errors.Is(aerr, ratelimit.ErrRateLimited):
			reject(ctx, l)
			return status.Error(codes.ResourceExhausted, aerr.Error())
		default:
			return onStreamBackendError(cfg, aerr, func() error { return handler(srv, ss) })
		}
	}, nil
}

// reject 尽力写入 trailer retry-after-ms:仅真实 server transport 上下文生效,
// 单测/无 transport 场景 SetTrailer 返回 error,忽略不影响拒绝语义
func reject(ctx context.Context, l ratelimit.Limiter) {
	if wait := l.Peek().RetryAfter; wait > 0 {
		_ = grpc.SetTrailer(ctx, metadata.Pairs(TrailerRetryAfterMs, strconv.FormatInt(wait.Milliseconds(), 10)))
	}
}

// onBackendError 后端故障处置:FailOpen 放行,否则 codes.Internal(cause 原文入消息)
func onBackendError(cfg Config, cause error, proceed func() (interface{}, error)) (interface{}, error) {
	if cfg.FailOpen {
		return proceed()
	}
	return nil, status.Errorf(codes.Internal, "ratelimit backend error: %v", cause)
}

func onStreamBackendError(cfg Config, cause error, proceed func() error) error {
	if cfg.FailOpen {
		return proceed()
	}
	return status.Errorf(codes.Internal, "ratelimit backend error: %v", cause)
}
