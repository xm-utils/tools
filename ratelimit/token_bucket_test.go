package ratelimit

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

// newFakeClock 返回可推进的假时钟:base 为起点,advance 按间隔前移
func newFakeClock() (Clock, func(time.Duration)) {
	var now atomic.Int64
	now.Store(time.Unix(1700000000, 0).UnixNano())
	clock := ClockFunc(func() time.Time { return time.Unix(0, now.Load()) })
	advance := func(d time.Duration) { now.Add(int64(d)) }
	return clock, advance
}

func TestNewTokenBucket_InvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  TokenBucketConfig
	}{
		{"Rate为零", TokenBucketConfig{Rate: 0, Burst: 10}},
		{"Rate为负", TokenBucketConfig{Rate: -1, Burst: 10}},
		{"Rate为NaN", TokenBucketConfig{Rate: math.NaN(), Burst: 10}},
		{"Rate为Inf", TokenBucketConfig{Rate: math.Inf(1), Burst: 10}},
		{"Burst为零", TokenBucketConfig{Rate: 1, Burst: 0}},
		{"Burst为负", TokenBucketConfig{Rate: 1, Burst: -2}},
		{"Burst为NaN", TokenBucketConfig{Rate: 1, Burst: math.NaN()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tb, err := NewTokenBucket(tc.cfg)
			if tb != nil || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("期望 ErrInvalidConfig, 得到 tb=%v err=%v", tb, err)
			}
		})
	}
}

func TestTokenBucket_BurstThenReject(t *testing.T) {
	clock, _ := newFakeClock()
	tb, err := NewTokenBucket(TokenBucketConfig{Rate: 10, Burst: 5, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// 初始满桶:前 5 次全放行
	for i := 0; i < 5; i++ {
		if err := tb.Allow(ctx); err != nil {
			t.Fatalf("第 %d 次应放行, 实际被拒: %v", i+1, err)
		}
	}
	// 第 6 次:桶空,拒绝且携带 RetryAfter
	err = tb.Allow(ctx)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("期望 ErrRateLimited, 得到 %v", err)
	}
	if err != nil && err.Error() == "" {
		t.Fatal("错误信息不应为空")
	}
}

func TestTokenBucket_RefillByElapsed(t *testing.T) {
	clock, advance := newFakeClock()
	tb, _ := NewTokenBucket(TokenBucketConfig{Rate: 10, Burst: 5, Clock: clock})
	ctx := context.Background()

	// 耗尽 5 个令牌
	for i := 0; i < 5; i++ {
		if err := tb.Allow(ctx); err != nil {
			t.Fatalf("预置耗尽失败: %v", err)
		}
	}
	// 推进 300ms → 回填 3 个
	advance(300 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if err := tb.Allow(ctx); err != nil {
			t.Fatalf("回填后前 3 次应放行: %v", err)
		}
	}
	if err := tb.Allow(ctx); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("第 4 次应被拒, 得到 %v", err)
	}
	// 推进 1s → 满桶 5 个(封顶不超 Burst)
	advance(time.Second)
	for i := 0; i < 5; i++ {
		if err := tb.Allow(ctx); err != nil {
			t.Fatalf("满桶应放行 5 个: %v", err)
		}
	}
	if err := tb.Allow(ctx); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("封顶后应被拒, 得到 %v", err)
	}
}

func TestTokenBucket_ClockRewindDefensive(t *testing.T) {
	clock, advance := newFakeClock()
	tb, _ := NewTokenBucket(TokenBucketConfig{Rate: 10, Burst: 5, Clock: clock})
	ctx := context.Background()

	// 消耗 2 个后时钟回拨 1s:不扣令牌、不 panic,剩余 3 个仍可用
	if ok, err := tb.AllowN(ctx, 2); err != nil || !ok {
		t.Fatalf("预置消耗失败: ok=%v err=%v", ok, err)
	}
	advance(-time.Second)
	res := tb.Peek()
	if res.Remaining < 2.99 || res.Remaining > 3.01 {
		t.Fatalf("回拨后剩余令牌应维持约 3, 得到 %v", res.Remaining)
	}
}

func TestTokenBucket_AllowN(t *testing.T) {
	clock, advance := newFakeClock()
	tb, _ := NewTokenBucket(TokenBucketConfig{Rate: 10, Burst: 10, Clock: clock})
	ctx := context.Background()

	if _, err := tb.AllowN(ctx, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("n=0 应返回 ErrInvalidConfig, 得到 %v", err)
	}
	if _, err := tb.AllowN(ctx, -1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("n<0 应返回 ErrInvalidConfig, 得到 %v", err)
	}
	if ok, err := tb.AllowN(ctx, 10); err != nil || !ok {
		t.Fatalf("一次消耗满桶应放行, ok=%v err=%v", ok, err)
	}
	if ok, _ := tb.AllowN(ctx, 5); ok {
		t.Fatal("空桶消耗应被拒")
	}
	advance(500 * time.Millisecond) // 回填 5 个
	if ok, err := tb.AllowN(ctx, 5); err != nil || !ok {
		t.Fatalf("回填 5 后应放行, ok=%v err=%v", ok, err)
	}
}

func TestTokenBucket_CtxCancelled(t *testing.T) {
	tb, _ := NewTokenBucket(TokenBucketConfig{Rate: 10, Burst: 10})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := tb.Allow(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx 取消应优先返回 ctx.Err(), 得到 %v", err)
	}
	if ok, err := tb.AllowN(ctx, 1); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("AllowN 同契约, ok=%v err=%v", ok, err)
	}
}

func TestTokenBucket_Peek(t *testing.T) {
	clock, advance := newFakeClock()
	tb, _ := NewTokenBucket(TokenBucketConfig{Rate: 2, Burst: 4, Clock: clock})
	ctx := context.Background()

	res := tb.Peek()
	if !res.OK || res.Remaining != 4 || res.Limit != 2 {
		t.Fatalf("初始 Peek 应为满桶可用: %+v", res)
	}
	for i := 0; i < 4; i++ {
		if err := tb.Allow(ctx); err != nil {
			t.Fatalf("消耗失败: %v", err)
		}
	}
	res = tb.Peek()
	if res.OK || res.Remaining != 0 || res.RetryAfter != 500*time.Millisecond {
		t.Fatalf("空桶 Peek 期望 RetryAfter=500ms: %+v", res)
	}
	// Peek 不消耗:再查一次应保持一致
	if res2 := tb.Peek(); res2.Remaining != res.Remaining {
		t.Fatalf("Peek 不应改变桶状态: %+v vs %+v", res2, res)
	}
	advance(time.Second)
	if res := tb.Peek(); !res.OK || res.Remaining != 2 {
		t.Fatalf("1s 后应回填 2 个: %+v", res)
	}
}

func TestTokenBucket_SetParams(t *testing.T) {
	tb, _ := NewTokenBucket(TokenBucketConfig{Rate: 10, Burst: 10})

	if err := tb.SetParams(0, 5); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("非法 rate 应报错, 得到 %v", err)
	}
	if err := tb.SetParams(5, -1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("非法 burst 应报错, 得到 %v", err)
	}
	// 缩小 Burst:当前令牌同步截断
	if err := tb.SetParams(1, 3); err != nil {
		t.Fatal(err)
	}
	res := tb.Peek()
	if res.Remaining != 3 || res.Limit != 1 {
		t.Fatalf("截断期望 Remaining=3 Limit=1: %+v", res)
	}
}

func TestTokenBucket_Close(t *testing.T) {
	tb, _ := NewTokenBucket(TokenBucketConfig{Rate: 10, Burst: 10})
	ctx := context.Background()

	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tb.Close(); err != nil {
		t.Fatalf("Close 应幂等: %v", err)
	}
	if err := tb.Allow(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Allow 应返回 ErrClosed, 得到 %v", err)
	}
	if ok, err := tb.AllowN(ctx, 1); ok || !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 AllowN 应返回 ErrClosed, ok=%v err=%v", ok, err)
	}
	if err := tb.SetParams(1, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 SetParams 应返回 ErrClosed, 得到 %v", err)
	}
	if res := tb.Peek(); res.OK || res.Remaining != 0 {
		t.Fatalf("关闭后 Peek 应为零值: %+v", res)
	}
}

func TestTokenBucket_ConcurrentAllow(t *testing.T) {
	// 冻结时钟:回填恒为 0,放行总数必须严格等于 Burst——
	// 验证互斥下无超额放行(无竞争时 tokens 检查与扣减可能被穿插双放)
	clock, _ := newFakeClock()
	tb, _ := NewTokenBucket(TokenBucketConfig{Rate: 100, Burst: 50, Clock: clock})
	ctx := context.Background()

	const goroutines = 32
	var allowed atomic.Int64
	var rejected atomic.Int64
	done := make(chan struct{})
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			<-start
			for j := 0; j < 100; j++ {
				if err := tb.Allow(ctx); err == nil {
					allowed.Add(1)
				} else if errors.Is(err, ErrRateLimited) {
					rejected.Add(1)
				} else {
					t.Errorf("意外错误: %v", err)
				}
			}
			done <- struct{}{}
		}()
	}
	close(start)
	for i := 0; i < goroutines; i++ {
		<-done
	}

	if got := allowed.Load(); got != 50 {
		t.Fatalf("冻结时钟下放行数应严格等于 Burst=50, 实际 %d", got)
	}
	if got := rejected.Load(); got != goroutines*100-50 {
		t.Fatalf("拒绝数应为 %d, 实际 %d", goroutines*100-50, got)
	}
}

func BenchmarkTokenBucketAllow(b *testing.B) {
	tb, _ := NewTokenBucket(TokenBucketConfig{Rate: 1e9, Burst: 1e9})
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = tb.Allow(ctx)
		}
	})
}
