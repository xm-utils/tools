package ratelimit

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewLeakyBucket_InvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  LeakyBucketConfig
	}{
		{"Rate为零", LeakyBucketConfig{Rate: 0, Queue: 5}},
		{"Rate为负", LeakyBucketConfig{Rate: -1, Queue: 5}},
		{"Rate为NaN", LeakyBucketConfig{Rate: math.NaN(), Queue: 5}},
		{"Rate为Inf", LeakyBucketConfig{Rate: math.Inf(1), Queue: 5}},
		{"Queue为零", LeakyBucketConfig{Rate: 10, Queue: 0}},
		{"Queue为负", LeakyBucketConfig{Rate: 10, Queue: -2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lb, err := NewLeakyBucket(tc.cfg)
			if lb != nil || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("期望 ErrInvalidConfig, 得到 lb=%v err=%v", lb, err)
			}
		})
	}
}

// Queue=1 即纯匀速限速:任何背靠背请求都会被拒,必须等出一个出水间隔
func TestLeakyBucket_StrictPacingNoBurst(t *testing.T) {
	clock, advance := newFakeClock()
	lb, err := NewLeakyBucket(LeakyBucketConfig{Rate: 10, Queue: 1, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := lb.Allow(ctx); err != nil {
		t.Fatalf("空桶首次应放行, 得到 %v", err)
	}
	// 与令牌桶的关键差别:无突发容量,紧接着的请求一律被拒
	for i := 0; i < 5; i++ {
		if err := lb.Allow(ctx); !errors.Is(err, ErrRateLimited) {
			t.Fatalf("匀速间隔内应被拒, 得到 %v", err)
		}
	}
	// 差 1ms 未满一个间隔:仍拒
	advance(99 * time.Millisecond)
	if err := lb.Allow(ctx); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("未满出水间隔不应放行, 得到 %v", err)
	}
	advance(time.Millisecond)
	if err := lb.Allow(ctx); err != nil {
		t.Fatalf("满一个间隔后应放行, 得到 %v", err)
	}
}

// Queue>0 吸收突发涌入:接纳数量受桶容量约束,超出的按出水速率拒绝
func TestLeakyBucket_QueueAbsorbsBurst(t *testing.T) {
	clock, advance := newFakeClock()
	lb, _ := NewLeakyBucket(LeakyBucketConfig{Rate: 10, Queue: 5, Clock: clock})
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := lb.Allow(ctx); err != nil {
			t.Fatalf("前 5 个应进桶, 第 %d 个被拒: %v", i+1, err)
		}
	}
	err := lb.Allow(ctx)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("桶满应被拒, 得到 %v", err)
	}
	// 溢出 1 个水位 → 需漏完 1 个间隔才容得下本次
	if !strings.Contains(err.Error(), "100ms") {
		t.Fatalf("退避时长应为 100ms, 得到 %q", err.Error())
	}

	// 漏掉 1 个水位后腾出 1 个位置
	advance(100 * time.Millisecond)
	if err := lb.Allow(ctx); err != nil {
		t.Fatalf("漏出 1 位后应放行, 得到 %v", err)
	}
	if err := lb.Allow(ctx); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("桶重新填满后应被拒, 得到 %v", err)
	}
}

// Reserve 交出"本次请求应当排多久",排队时长随桶内水位线性累加
func TestLeakyBucket_ReserveReturnsShapingDelay(t *testing.T) {
	clock, _ := newFakeClock()
	lb, _ := NewLeakyBucket(LeakyBucketConfig{Rate: 10, Queue: 100, Clock: clock})
	ctx := context.Background()

	if delay, err := lb.Reserve(ctx, 1); err != nil || delay != 0 {
		t.Fatalf("空桶应可立即出水, delay=%v err=%v", delay, err)
	}
	if delay, err := lb.Reserve(ctx, 1); err != nil || delay != 100*time.Millisecond {
		t.Fatalf("第 2 个应排 100ms, delay=%v err=%v", delay, err)
	}
	// 整批 n 个占住 n 个间隔,后续请求顺延
	if delay, err := lb.Reserve(ctx, 3); err != nil || delay != 200*time.Millisecond {
		t.Fatalf("第 3 批首令牌应排 200ms, delay=%v err=%v", delay, err)
	}
	if delay, err := lb.Reserve(ctx, 1); err != nil || delay != 500*time.Millisecond {
		t.Fatalf("桶内已有 5 个水位, 下一个应排 500ms, delay=%v err=%v", delay, err)
	}
	if _, err := lb.Reserve(ctx, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("n=0 应报 ErrInvalidConfig, 得到 %v", err)
	}
}

func TestLeakyBucket_WaitBlocksUntilEmit(t *testing.T) {
	// 真实时钟:Rate=100 → 出水间隔 10ms
	lb, _ := NewLeakyBucket(LeakyBucketConfig{Rate: 100, Queue: 10})
	ctx := context.Background()

	start := time.Now()
	if err := lb.Wait(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := lb.Wait(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 8*time.Millisecond {
		t.Fatalf("第二次 Wait 应被整形至约 10ms 后, 实际 %v", elapsed)
	}
}

func TestLeakyBucket_WaitCtxDeadline(t *testing.T) {
	lb, _ := NewLeakyBucket(LeakyBucketConfig{Rate: 1, Queue: 100})
	if _, err := lb.Reserve(context.Background(), 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	start := time.Now()
	// 本次需排 1s,而 ctx 只给 10ms:应立刻因 ctx 结束返回,不硬等
	err := lb.Wait(ctx, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("期望 context.DeadlineExceeded, 得到 %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("ctx 到期后不应继续阻塞, 实际等待 %v", elapsed)
	}
}

func TestLeakyBucket_AllowN(t *testing.T) {
	clock, advance := newFakeClock()
	lb, _ := NewLeakyBucket(LeakyBucketConfig{Rate: 10, Queue: 5, Clock: clock})
	ctx := context.Background()

	if _, err := lb.AllowN(ctx, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("n=0 应返回 ErrInvalidConfig, 得到 %v", err)
	}
	if _, err := lb.AllowN(ctx, -1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("n<0 应返回 ErrInvalidConfig, 得到 %v", err)
	}
	if ok, err := lb.AllowN(ctx, 3); err != nil || !ok {
		t.Fatalf("整批 3/5 应放行, ok=%v err=%v", ok, err)
	}
	if ok, err := lb.AllowN(ctx, 3); err != nil || ok {
		t.Fatalf("水位 3+3 超出桶容量应被拒且无 error, ok=%v err=%v", ok, err)
	}
	if ok, err := lb.AllowN(ctx, 2); err != nil || !ok {
		t.Fatalf("补满 5 应放行, ok=%v err=%v", ok, err)
	}
	// 漏完全部水位后重新可用
	advance(time.Second)
	if ok, err := lb.AllowN(ctx, 5); err != nil || !ok {
		t.Fatalf("空桶后应放行整批, ok=%v err=%v", ok, err)
	}
}

func TestLeakyBucket_ClockRewindDefensive(t *testing.T) {
	clock, advance := newFakeClock()
	lb, _ := NewLeakyBucket(LeakyBucketConfig{Rate: 10, Queue: 3, Clock: clock})
	ctx := context.Background()

	if ok, err := lb.AllowN(ctx, 3); err != nil || !ok {
		t.Fatalf("预置灌满失败: ok=%v err=%v", ok, err)
	}
	// 回拨 1s:水位不得被"倒灌"成负数而误放行
	advance(-time.Second)
	if ok, err := lb.AllowN(ctx, 1); err != nil || ok {
		t.Fatalf("回拨后桶仍满, 不应放行, ok=%v err=%v", ok, err)
	}
	// 恢复并继续推进:水位按真实时长回落
	advance(2 * time.Second)
	if ok, err := lb.AllowN(ctx, 3); err != nil || !ok {
		t.Fatalf("水漏尽后应重新放行, ok=%v err=%v", ok, err)
	}
}

func TestLeakyBucket_Peek(t *testing.T) {
	clock, advance := newFakeClock()
	lb, _ := NewLeakyBucket(LeakyBucketConfig{Rate: 10, Queue: 5, Clock: clock})
	ctx := context.Background()

	if res := lb.Peek(); !res.OK || res.Remaining != 5 || res.Limit != 10 {
		t.Fatalf("初始应为空桶: %+v", res)
	}
	if ok, err := lb.AllowN(ctx, 2); err != nil || !ok {
		t.Fatalf("消耗 2 应放行, ok=%v err=%v", ok, err)
	}
	res := lb.Peek()
	if !res.OK || res.Remaining != 3 || res.RetryAfter != 0 {
		t.Fatalf("桶内 2 水位时应余 3 且无需退避: %+v", res)
	}
	// Peek 不消耗
	if res2 := lb.Peek(); res2.Remaining != res.Remaining {
		t.Fatalf("Peek 不应改变水位: %+v vs %+v", res2, res)
	}
	if ok, err := lb.AllowN(ctx, 4); err != nil || ok {
		t.Fatalf("余 3 放不下 4, 应被拒, ok=%v err=%v", ok, err)
	}
	res = lb.Peek()
	if !res.OK || res.Remaining != 3 || res.RetryAfter != 0 {
		t.Fatalf("被拒的整批不占水位, 应仍余 3 且无需退避: %+v", res)
	}
	// 把桶填满:单次也放不下,退避为漏出 1 个水位的间隔
	if ok, err := lb.AllowN(ctx, 3); err != nil || !ok {
		t.Fatalf("补满至 5 应放行, ok=%v err=%v", ok, err)
	}
	res = lb.Peek()
	if res.OK || res.Remaining != 0 || res.RetryAfter != 100*time.Millisecond {
		t.Fatalf("桶满应 OK=false 并给出 100ms 退避预估: %+v", res)
	}
	advance(time.Second)
	if res := lb.Peek(); !res.OK || res.Remaining != 5 {
		t.Fatalf("水漏尽后应恢复满容量: %+v", res)
	}
}

func TestLeakyBucket_SetParams(t *testing.T) {
	clock, advance := newFakeClock()
	lb, _ := NewLeakyBucket(LeakyBucketConfig{Rate: 10, Queue: 5, Clock: clock})
	ctx := context.Background()

	if err := lb.SetParams(0, 5); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("非法 rate 应报错, 得到 %v", err)
	}
	if err := lb.SetParams(10, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("非法 queue 应报错, 得到 %v", err)
	}
	if ok, err := lb.AllowN(ctx, 3); err != nil || !ok {
		t.Fatalf("预置水位 3 应放行, ok=%v err=%v", ok, err)
	}
	// 收紧容量不追溯撤销桶内水位,但判定即刻受限、余量不报负数
	if err := lb.SetParams(10, 2); err != nil {
		t.Fatal(err)
	}
	if ok, err := lb.AllowN(ctx, 1); err != nil || ok {
		t.Fatalf("水位 3 已超新容量 2, 应被拒, ok=%v err=%v", ok, err)
	}
	res := lb.Peek()
	if res.OK || res.Remaining != 0 || res.Limit != 10 {
		t.Fatalf("收紧后余量应截为 0: %+v", res)
	}
	// 提速后出水间隔变短
	if err := lb.SetParams(100, 2); err != nil {
		t.Fatal(err)
	}
	advance(100 * time.Millisecond)
	if ok, err := lb.AllowN(ctx, 1); err != nil || !ok {
		t.Fatalf("提速后 100ms 应漏尽水位, ok=%v err=%v", ok, err)
	}
}

func TestLeakyBucket_Close(t *testing.T) {
	lb, _ := NewLeakyBucket(LeakyBucketConfig{Rate: 5, Queue: 5})
	if err := lb.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lb.Close(); err != nil {
		t.Fatalf("Close 应幂等, 得到 %v", err)
	}
	if err := lb.Allow(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Allow 应返回 ErrClosed, 得到 %v", err)
	}
	if ok, err := lb.AllowN(context.Background(), 1); ok || !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 AllowN 应返回 ErrClosed, ok=%v err=%v", ok, err)
	}
	if _, err := lb.Reserve(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Reserve 应返回 ErrClosed, 得到 %v", err)
	}
	if err := lb.Wait(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Wait 应返回 ErrClosed, 得到 %v", err)
	}
	if err := lb.SetParams(5, 5); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后热更新应返回 ErrClosed, 得到 %v", err)
	}
	if res := lb.Peek(); res != (Result{}) {
		t.Fatalf("关闭后 Peek 应返回零值, 得到 %+v", res)
	}
}

func TestLeakyBucket_CtxCancelled(t *testing.T) {
	lb, _ := NewLeakyBucket(LeakyBucketConfig{Rate: 10, Queue: 10})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := lb.Allow(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx 取消应优先返回 ctx.Err(), 得到 %v", err)
	}
	if ok, err := lb.AllowN(ctx, 1); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("AllowN 同契约, ok=%v err=%v", ok, err)
	}
	if _, err := lb.Reserve(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Reserve 同契约, 得到 %v", err)
	}
}

// 并发冻结时钟:进桶总数严格等于桶容量,不超放也不漏放
func TestLeakyBucket_ConcurrentQueueBound(t *testing.T) {
	clock, _ := newFakeClock()
	lb, _ := NewLeakyBucket(LeakyBucketConfig{Rate: 10, Queue: 5, Clock: clock})
	ctx := context.Background()

	var passed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if err := lb.Allow(ctx); err == nil {
					passed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if got := passed.Load(); got != 5 {
		t.Fatalf("冻结时钟下进桶数应严格等于 Queue=5, 得到 %d", got)
	}
}
