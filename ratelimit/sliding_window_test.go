package ratelimit

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewSlidingWindow_InvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  SlidingWindowConfig
	}{
		{"Limit为零", SlidingWindowConfig{Limit: 0, Window: time.Second}},
		{"Limit为负", SlidingWindowConfig{Limit: -1, Window: time.Second}},
		{"Window为零", SlidingWindowConfig{Limit: 10, Window: 0}},
		{"Window为负", SlidingWindowConfig{Limit: 10, Window: -time.Second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sw, err := NewSlidingWindow(tc.cfg)
			if sw != nil || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("期望 ErrInvalidConfig, 得到 sw=%v err=%v", sw, err)
			}
		})
	}
}

// 窗口内严格计数:满额即拒,最早记录滑出窗口后逐个释放
func TestSlidingWindow_WindowSlideReleases(t *testing.T) {
	clock, advance := newFakeClock()
	sw, err := NewSlidingWindow(SlidingWindowConfig{Limit: 3, Window: time.Second, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := sw.Allow(ctx); err != nil {
			t.Fatalf("前 3 次应放行, 第 %d 次被拒: %v", i+1, err)
		}
	}
	if err := sw.Allow(ctx); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("第 4 次应被拒, 得到 %v", err)
	}

	// 差 1ms 就滑出:仍在窗口内,不放行
	advance(time.Second - time.Millisecond)
	if err := sw.Allow(ctx); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("未满窗口不应放行, 得到 %v", err)
	}

	// 满 1s:3 条记录同时滑出,窗口重新空出 3 个额度
	advance(time.Millisecond)
	for i := 0; i < 3; i++ {
		if err := sw.Allow(ctx); err != nil {
			t.Fatalf("记录滑出后应放行 3 个: %v", err)
		}
	}
	if err := sw.Allow(ctx); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("再次满额应被拒, 得到 %v", err)
	}
}

// 滑动窗口消除固定窗口临界双倍:跨窗口边界的 1s 内总数仍不超过 Limit
func TestSlidingWindow_NoFixedWindowDoubleBurst(t *testing.T) {
	clock, advance := newFakeClock()
	sw, _ := NewSlidingWindow(SlidingWindowConfig{Limit: 2, Window: time.Second, Clock: clock})
	ctx := context.Background()

	if err := sw.Allow(ctx); err != nil {
		t.Fatal(err)
	}
	// 窗口尾端再放行 1 个
	advance(900 * time.Millisecond)
	if err := sw.Allow(ctx); err != nil {
		t.Fatal(err)
	}
	// 下一个"固定窗口"开头:若按固定窗口此刻应重置为 2 额度,滑动窗口只放出 1 个
	advance(200 * time.Millisecond)
	if err := sw.Allow(ctx); err != nil {
		t.Fatalf("新窗口起点应有 1 个可用额度: %v", err)
	}
	if err := sw.Allow(ctx); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("900ms~1100ms 区间已放行 2 个, 不应再放行: %v", err)
	}
}

func TestSlidingWindow_AllowN(t *testing.T) {
	clock, advance := newFakeClock()
	sw, _ := NewSlidingWindow(SlidingWindowConfig{Limit: 5, Window: time.Second, Clock: clock})
	ctx := context.Background()

	if _, err := sw.AllowN(ctx, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("n=0 应返回 ErrInvalidConfig, 得到 %v", err)
	}
	if _, err := sw.AllowN(ctx, -3); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("n<0 应返回 ErrInvalidConfig, 得到 %v", err)
	}
	if ok, err := sw.AllowN(ctx, 3); err != nil || !ok {
		t.Fatalf("消耗 3/5 应放行, ok=%v err=%v", ok, err)
	}
	if ok, err := sw.AllowN(ctx, 3); err != nil || ok {
		t.Fatalf("超出窗口额度应被拒且无 error, ok=%v err=%v", ok, err)
	}
	// 剩 2 个额度正好够
	if ok, err := sw.AllowN(ctx, 2); err != nil || !ok {
		t.Fatalf("消耗余下 2 个应放行, ok=%v err=%v", ok, err)
	}
	// 窗口整体滑出后重新可用
	advance(time.Second)
	if ok, err := sw.AllowN(ctx, 5); err != nil || !ok {
		t.Fatalf("窗口重置后应放行整批, ok=%v err=%v", ok, err)
	}
}

func TestSlidingWindow_RetryAfterFromOldestEntry(t *testing.T) {
	clock, advance := newFakeClock()
	sw, _ := NewSlidingWindow(SlidingWindowConfig{Limit: 1, Window: time.Second, Clock: clock})
	ctx := context.Background()

	if err := sw.Allow(ctx); err != nil {
		t.Fatal(err)
	}
	advance(300 * time.Millisecond)
	err := sw.Allow(ctx)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("应被拒, 得到 %v", err)
	}
	// 最早记录在 t0,滑出窗口还需 1s,已过去 300ms → 退避 700ms
	if !strings.Contains(err.Error(), "700ms") {
		t.Fatalf("退避时长应为 700ms, 得到 %q", err.Error())
	}
}

func TestSlidingWindow_ClockRewindDefensive(t *testing.T) {
	clock, advance := newFakeClock()
	sw, _ := NewSlidingWindow(SlidingWindowConfig{Limit: 2, Window: time.Second, Clock: clock})
	ctx := context.Background()

	if ok, err := sw.AllowN(ctx, 2); err != nil || !ok {
		t.Fatalf("预置消耗失败: ok=%v err=%v", ok, err)
	}
	// 回拨 1s:在窗记录不会被误判过期,额度仍为 0
	advance(-time.Second)
	if ok, err := sw.AllowN(ctx, 1); err != nil || ok {
		t.Fatalf("回拨后不应误放行, ok=%v err=%v", ok, err)
	}
	if res := sw.Peek(); res.Remaining != 0 {
		t.Fatalf("回拨后 Remaining 应仍为 0, 得到 %v", res.Remaining)
	}
}

func TestSlidingWindow_Peek(t *testing.T) {
	clock, advance := newFakeClock()
	sw, _ := NewSlidingWindow(SlidingWindowConfig{Limit: 4, Window: 2 * time.Second, Clock: clock})
	ctx := context.Background()

	if res := sw.Peek(); !res.OK || res.Remaining != 4 || res.Limit != 2 {
		t.Fatalf("初始应为空窗口可用(等效速率 2/秒): %+v", res)
	}
	if ok, err := sw.AllowN(ctx, 3); err != nil || !ok {
		t.Fatalf("消耗 3 应放行, ok=%v err=%v", ok, err)
	}
	res := sw.Peek()
	if !res.OK || res.Remaining != 1 {
		t.Fatalf("剩 1 个额度时仍应可放行单次: %+v", res)
	}
	// Peek 不消耗
	if res2 := sw.Peek(); res2.Remaining != res.Remaining {
		t.Fatalf("Peek 不应改变窗口状态: %+v vs %+v", res2, res)
	}
	// 整批 2 个放不下:被拒且不写入记录(不占额度)
	if ok, err := sw.AllowN(ctx, 2); err != nil || ok {
		t.Fatalf("整批放不下应被拒, ok=%v err=%v", ok, err)
	}
	// 单次仍放得下:消耗掉最后 1 个额度
	if err := sw.Allow(ctx); err != nil {
		t.Fatalf("剩 1 额度时 Allow 应放行, 得到 %v", err)
	}
	res = sw.Peek()
	if res.OK || res.Remaining != 0 || res.RetryAfter != 2*time.Second {
		t.Fatalf("额度耗尽应 OK=false 且退避为窗口宽度 2s: %+v", res)
	}
	advance(2 * time.Second)
	if res := sw.Peek(); !res.OK || res.Remaining != 4 {
		t.Fatalf("窗口重置后应恢复满额度: %+v", res)
	}
}

func TestSlidingWindow_SetParams(t *testing.T) {
	clock, _ := newFakeClock()
	sw, _ := NewSlidingWindow(SlidingWindowConfig{Limit: 10, Window: time.Second, Clock: clock})
	ctx := context.Background()

	if err := sw.SetParams(0, time.Second); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("非法 limit 应报错, 得到 %v", err)
	}
	if err := sw.SetParams(5, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("非法 window 应报错, 得到 %v", err)
	}
	if ok, err := sw.AllowN(ctx, 8); err != nil || !ok {
		t.Fatalf("原额度 10 应放行 8, ok=%v err=%v", ok, err)
	}
	// 收紧额度不追溯撤销已放行记录,仅影响后续判定
	if err := sw.SetParams(5, time.Second); err != nil {
		t.Fatal(err)
	}
	if ok, err := sw.AllowN(ctx, 1); err != nil || ok {
		t.Fatalf("已用 8 > 新 Limit 5, 后续应被拒, ok=%v err=%v", ok, err)
	}
	res := sw.Peek()
	if res.OK || res.Limit != 5 || res.Remaining != 0 {
		// 已在窗 8 条记录不追溯撤销,但余量不报负数
		t.Fatalf("收紧后等效速率应为 5、余量截为 0: %+v", res)
	}
}

func TestSlidingWindow_Close(t *testing.T) {
	sw, _ := NewSlidingWindow(SlidingWindowConfig{Limit: 5, Window: time.Second})
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sw.Close(); err != nil {
		t.Fatalf("Close 应幂等, 得到 %v", err)
	}
	if err := sw.Allow(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Allow 应返回 ErrClosed, 得到 %v", err)
	}
	if ok, err := sw.AllowN(context.Background(), 1); ok || !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 AllowN 应返回 ErrClosed, ok=%v err=%v", ok, err)
	}
	if err := sw.SetParams(5, time.Second); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后热更新应返回 ErrClosed, 得到 %v", err)
	}
	if res := sw.Peek(); res != (Result{}) {
		t.Fatalf("关闭后 Peek 应返回零值, 得到 %+v", res)
	}
}

func TestSlidingWindow_CtxCancelled(t *testing.T) {
	sw, _ := NewSlidingWindow(SlidingWindowConfig{Limit: 5, Window: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := sw.Allow(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx 取消应优先返回 ctx.Err(), 得到 %v", err)
	}
	if ok, err := sw.AllowN(ctx, 1); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("AllowN 同契约, ok=%v err=%v", ok, err)
	}
}

// 并发下冻结时钟:放行总数严格等于 Limit,不超放也不漏放
func TestSlidingWindow_ConcurrentExactCount(t *testing.T) {
	clock, _ := newFakeClock()
	sw, _ := NewSlidingWindow(SlidingWindowConfig{Limit: 50, Window: time.Second, Clock: clock})
	ctx := context.Background()

	var passed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				if err := sw.Allow(ctx); err == nil {
					passed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if got := passed.Load(); got != 50 {
		t.Fatalf("冻结时钟下放行数应严格等于 Limit=50, 得到 %d", got)
	}
}
