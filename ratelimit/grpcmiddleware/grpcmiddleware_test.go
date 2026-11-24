package grpcmiddleware

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/xm-utils/tools/ratelimit"
)

// errBoom 后端故障哨兵(非 ErrRateLimited / ErrClosed 的普通错误)
var errBoom = errors.New("backend boom")

// stubLimiter 可编程限流器:固定返回 allowErr,并统计 Allow/Peek 调用
type stubLimiter struct {
	allowErr   error
	peekResult ratelimit.Result
	allowCalls atomic.Int64
}

func (s *stubLimiter) Allow(ctx context.Context) error {
	s.allowCalls.Add(1)
	return s.allowErr
}

func (s *stubLimiter) AllowN(ctx context.Context, n int) (bool, error) {
	return false, s.allowErr
}

func (s *stubLimiter) Peek() ratelimit.Result {
	if s.peekResult != (ratelimit.Result{}) {
		return s.peekResult
	}
	return ratelimit.Result{RetryAfter: 200 * time.Millisecond}
}

func (s *stubLimiter) Close() error { return nil }

// fakeStream 最小 grpc.ServerStream 实现
type fakeStream struct {
	ctx   context.Context
	md    metadata.MD
	recv  interface{}
	sentN atomic.Int64
}

func (f *fakeStream) SetHeader(md metadata.MD) error  { f.md = md; return nil }
func (f *fakeStream) SendHeader(md metadata.MD) error { f.md = md; return nil }
func (f *fakeStream) SetTrailer(md metadata.MD)       { f.md = md }
func (f *fakeStream) Context() context.Context        { return f.ctx }
func (f *fakeStream) SendMsg(m interface{}) error     { f.sentN.Add(1); return nil }
func (f *fakeStream) RecvMsg(m interface{}) error     { return nil }

func unaryInfo(method string) *grpc.UnaryServerInfo {
	return &grpc.UnaryServerInfo{FullMethod: method}
}

var handlerCalled = errors.New("handler executed")

func okHandler(ctx context.Context, req interface{}) (interface{}, error) {
	return "ok", nil
}

func TestNewUnaryServerInterceptor_InvalidConfig(t *testing.T) {
	l := &stubLimiter{}
	local, _ := ratelimit.NewLocal(ratelimit.LocalConfig{Rate: 1, Burst: 1})

	if _, err := NewUnaryServerInterceptor(Config{}); !errors.Is(err, ratelimit.ErrInvalidConfig) {
		t.Fatalf("两者皆 nil 应报 ErrInvalidConfig: %v", err)
	}
	if _, err := NewUnaryServerInterceptor(Config{Limiter: l, Local: local}); !errors.Is(err, ratelimit.ErrInvalidConfig) {
		t.Fatalf("两者同时设置应报 ErrInvalidConfig: %v", err)
	}
	if _, err := NewStreamServerInterceptor(Config{}); !errors.Is(err, ratelimit.ErrInvalidConfig) {
		t.Fatalf("流拦截器同契约: %v", err)
	}
}

func TestUnary_ServiceLevelAllowAndReject(t *testing.T) {
	local, _ := ratelimit.NewLocal(ratelimit.LocalConfig{
		Rate: 10, Burst: 2, Clock: frozenClock(),
	})
	itc, err := NewUnaryServerInterceptor(Config{Local: local})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if resp, err := itc(ctx, nil, unaryInfo("/pkg.Svc/Method"), okHandler); err != nil || resp != "ok" {
			t.Fatalf("前 2 次应到 handler: resp=%v err=%v", resp, err)
		}
	}
	_, err = itc(ctx, nil, unaryInfo("/pkg.Svc/Method"), okHandler)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("第 3 次应 ResourceExhausted, 得到 %v", err)
	}
	// 不同方法默认按 fullMethod 分 Key,互不影响
	if resp, err := itc(ctx, nil, unaryInfo("/pkg.Svc/Other"), okHandler); err != nil || resp != "ok" {
		t.Fatalf("其他方法应有独立份额: %v", err)
	}
}

func TestUnary_KeyFuncEmptyKeyPassthrough(t *testing.T) {
	local, _ := ratelimit.NewLocal(ratelimit.LocalConfig{Rate: 1, Burst: 1})
	itc, err := NewUnaryServerInterceptor(Config{
		Local:   local,
		KeyFunc: func(context.Context, string) (string, error) { return "", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	// 空 Key 直通:不应触达任何限流器(stub 根本不会被引用,以 Local 未被消耗佐证)
	before := local.Stats("any")
	if _, err := itc(context.Background(), nil, unaryInfo("/a/b"), okHandler); err != nil {
		t.Fatalf("空 Key 应直通: %v", err)
	}
	if after := local.Stats("any"); after != before {
		t.Fatalf("直通不应创建/消耗限流器: %+v vs %+v", after, before)
	}
}

func TestUnary_BackendError(t *testing.T) {
	t.Run("默认 fail-closed", func(t *testing.T) {
		stub := &stubLimiter{allowErr: errBoom}
		itc, _ := NewUnaryServerInterceptor(Config{Limiter: stub})
		_, err := itc(context.Background(), nil, unaryInfo("/a/b"), okHandler)
		if status.Code(err) != codes.Internal {
			t.Fatalf("期望 codes.Internal, 得到 %v", err)
		}
	})
	t.Run("FailOpen 放行", func(t *testing.T) {
		stub := &stubLimiter{allowErr: errBoom}
		itc, _ := NewUnaryServerInterceptor(Config{Limiter: stub, FailOpen: true})
		resp, err := itc(context.Background(), nil, unaryInfo("/a/b"), okHandler)
		if err != nil || resp != "ok" {
			t.Fatalf("FailOpen 应到达 handler: resp=%v err=%v", resp, err)
		}
	})
	t.Run("拒绝时 trailer 写入不 panic", func(t *testing.T) {
		// 无真实 transport 的 ctx:SetTrailer 返回 error 被忽略,拒绝语义不受影响
		stub := &stubLimiter{allowErr: ratelimit.ErrRateLimited}
		itc, _ := NewUnaryServerInterceptor(Config{Limiter: stub})
		_, err := itc(context.Background(), nil, unaryInfo("/a/b"), okHandler)
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("期望 ResourceExhausted, 得到 %v", err)
		}
	})
}

func TestStream_BasicAllowReject(t *testing.T) {
	local, _ := ratelimit.NewLocal(ratelimit.LocalConfig{
		Rate: 10, Burst: 1, Clock: frozenClock(),
	})
	itc, err := NewStreamServerInterceptor(Config{Local: local})
	if err != nil {
		t.Fatal(err)
	}
	ss := &fakeStream{ctx: context.Background()}
	info := &grpc.StreamServerInfo{FullMethod: "/pkg.Svc/Stream"}
	called := false
	handler := func(srv interface{}, stream grpc.ServerStream) error {
		called = true
		return nil
	}

	if err := itc(nil, ss, info, handler); err != nil || !called {
		t.Fatalf("首条流应放行: err=%v called=%v", err, called)
	}
	called = false
	err = itc(nil, ss, info, handler)
	if status.Code(err) != codes.ResourceExhausted || called {
		t.Fatalf("第二条流应被拒且不进 handler: err=%v called=%v", err, called)
	}
}

func TestStream_BackendError(t *testing.T) {
	stub := &stubLimiter{allowErr: errBoom}
	itc, _ := NewStreamServerInterceptor(Config{Limiter: stub})
	ss := &fakeStream{ctx: context.Background()}
	handler := func(srv interface{}, stream grpc.ServerStream) error { return nil }

	if err := itc(nil, ss, &grpc.StreamServerInfo{FullMethod: "/a/b"}, handler); status.Code(err) != codes.Internal {
		t.Fatalf("期望 codes.Internal, 得到 %v", err)
	}
	itcOpen, _ := NewStreamServerInterceptor(Config{Limiter: stub, FailOpen: true})
	if err := itcOpen(nil, ss, &grpc.StreamServerInfo{FullMethod: "/a/b"}, handler); err != nil {
		t.Fatalf("FailOpen 应放行: %v", err)
	}
}

// frozenClock 冻结假时钟:回填恒为 0,判定确定
func frozenClock() ratelimit.Clock {
	var t atomic.Int64
	t.Store(time.Unix(1700000000, 0).UnixNano())
	return ratelimit.ClockFunc(func() time.Time {
		return time.Unix(0, t.Load())
	})
}
