package redisstore

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"

	"github.com/xm-utils/tools/ratelimit"
)

// setup 启动 miniredis 并返回与其直连的 client 工厂(同 server 可建多个 client,
// 模拟多实例共享同一 Redis 集群后端)
func setup(t *testing.T) (*miniredis.Miniredis, func() *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("启动 miniredis 失败: %v", err)
	}
	t.Cleanup(mr.Close)
	return mr, func() *redis.Client {
		return redis.NewClient(&redis.Options{Addr: mr.Addr()})
	}
}

// newTestLimiter 假时钟 + 指定参数的限流器
func newTestLimiter(t *testing.T, client *redis.Client, key string, rate, burst float64, clock ratelimit.Clock, ttl time.Duration) *Limiter {
	t.Helper()
	l, err := New(Config{
		Client: client,
		Key:    key,
		Rate:   rate,
		Burst:  burst,
		TTL:    ttl,
		Clock:  clock,
	})
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	return l
}

func newFakeClock() (ratelimit.Clock, func(time.Duration)) {
	base := time.Unix(1700000000, 0)
	var cur atomic.Value
	cur.Store(base)
	clock := ratelimit.ClockFunc(func() time.Time {
		return cur.Load().(time.Time)
	})
	advance := func(d time.Duration) {
		cur.Store(cur.Load().(time.Time).Add(d))
	}
	return clock, advance
}

func TestNew_InvalidConfig(t *testing.T) {
	_, newClient := setup(t)
	c := newClient()

	cases := []struct {
		name string
		cfg  Config
	}{
		{"nil client", Config{Key: "k", Rate: 1, Burst: 1}},
		{"空 Key", Config{Client: c, Rate: 1, Burst: 1}},
		{"Rate 非正", Config{Client: c, Key: "k", Rate: 0, Burst: 1}},
		{"Burst 非正", Config{Client: c, Key: "k", Rate: 1, Burst: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.cfg); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("期望 ErrInvalidConfig, 得到 %v", err)
			}
		})
	}
}

func TestLimiter_BurstThenReject(t *testing.T) {
	_, newClient := setup(t)
	clock, _ := newFakeClock() // 冻结时钟:回填恒为 0
	l := newTestLimiter(t, newClient(), "api:test", 10, 5, clock, 0)
	defer l.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := l.Allow(ctx); err != nil {
			t.Fatalf("前 5 次应放行(初始满桶): %v", err)
		}
	}
	err := l.Allow(ctx)
	if !errors.Is(err, ratelimit.ErrRateLimited) {
		t.Fatalf("第 6 次应被拒, 得到 %v", err)
	}
	// 被拒错误的等待时长与脚本同批返回:缺 1 令牌 / 10 每秒 = 100ms
	if !strings.Contains(err.Error(), "100ms") {
		t.Fatalf("错误应含 retry after 100ms, 实际: %v", err)
	}
}

func TestLimiter_RefillByElapsed(t *testing.T) {
	_, newClient := setup(t)
	clock, advance := newFakeClock()
	l := newTestLimiter(t, newClient(), "api:refill", 10, 5, clock, 0)
	defer l.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := l.Allow(ctx); err != nil {
			t.Fatalf("预置耗尽失败: %v", err)
		}
	}
	advance(300 * time.Millisecond) // 回填 3 个
	for i := 0; i < 3; i++ {
		if err := l.Allow(ctx); err != nil {
			t.Fatalf("回填 3 个应放行: %v", err)
		}
	}
	if err := l.Allow(ctx); !errors.Is(err, ratelimit.ErrRateLimited) {
		t.Fatalf("第 4 次应被拒, 得到 %v", err)
	}
}

func TestLimiter_SharedAcrossClients(t *testing.T) {
	// 两个 client 模拟两个服务实例:份额必须全局共享而非各自计数
	_, newClient := setup(t)
	clock, _ := newFakeClock()
	a := newTestLimiter(t, newClient(), "shared", 1, 4, clock, 0)
	b := newTestLimiter(t, newClient(), "shared", 1, 4, clock, 0)
	defer a.Close()
	defer b.Close()
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if err := a.Allow(ctx); err != nil {
			t.Fatalf("实例 A 预置消耗失败: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := b.Allow(ctx); err != nil {
			t.Fatalf("实例 B 应能用剩余份额: %v", err)
		}
	}
	if err := a.Allow(ctx); !errors.Is(err, ratelimit.ErrRateLimited) {
		t.Fatalf("全局份额已耗尽, A 应被拒, 得到 %v", err)
	}
}

func TestLimiter_ClockRewindDefensive(t *testing.T) {
	_, newClient := setup(t)
	clock, advance := newFakeClock()
	l := newTestLimiter(t, newClient(), "rewind", 10, 5, clock, 0)
	defer l.Close()
	ctx := context.Background()

	if err := l.Allow(ctx); err != nil {
		t.Fatal(err)
	}
	advance(-time.Hour) // 应用实例时钟大幅回拨
	// 脚本按 elapsed=0 处理:已消耗不回补,剩余 4 个仍可用
	for i := 0; i < 4; i++ {
		if err := l.Allow(ctx); err != nil {
			t.Fatalf("回拨后剩余 %d 个应继续放行: %v", 4-i, err)
		}
	}
	if err := l.Allow(ctx); !errors.Is(err, ratelimit.ErrRateLimited) {
		t.Fatalf("份额用尽应被拒, 得到 %v", err)
	}
}

func TestLimiter_Peek(t *testing.T) {
	_, newClient := setup(t)
	clock, advance := newFakeClock()
	l := newTestLimiter(t, newClient(), "peek", 2, 4, clock, 0)
	defer l.Close()
	ctx := context.Background()

	// 未初始化:视为满桶
	res := l.Peek()
	if !res.OK || res.Remaining != 4 {
		t.Fatalf("初始 Peek 应满桶可用: %+v", res)
	}
	for i := 0; i < 4; i++ {
		if err := l.Allow(ctx); err != nil {
			t.Fatalf("消耗失败: %v", err)
		}
	}
	res = l.Peek()
	if res.OK || res.Remaining != 0 || res.RetryAfter != 500*time.Millisecond {
		t.Fatalf("空桶 Peek 期望 RetryAfter=500ms: %+v", res)
	}
	// 假时钟推进后只读重算(不消耗)
	advance(time.Second)
	res = l.Peek()
	if !res.OK || res.Remaining != 2 {
		t.Fatalf("1s 后应回填 2 个: %+v", res)
	}
}

func TestLimiter_AllowN(t *testing.T) {
	_, newClient := setup(t)
	clock, advance := newFakeClock()
	l := newTestLimiter(t, newClient(), "batch", 10, 10, clock, 0)
	defer l.Close()
	ctx := context.Background()

	if _, err := l.AllowN(ctx, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("n=0 应报 ErrInvalidConfig, 得到 %v", err)
	}
	// 定点溢出自检:n×1e9 超出 int64 前拦截,不得静默回绕成负数进脚本
	if _, err := l.AllowN(ctx, maxJudgementN+1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("超出定点上限应报 ErrInvalidConfig, 得到 %v", err)
	}
	if ok, err := l.AllowN(ctx, maxJudgementN); err != nil || ok {
		t.Fatalf("上限内大 n 应正常进判定并被拒(份额不足), ok=%v err=%v", ok, err)
	}
	if ok, err := l.AllowN(ctx, 10); err != nil || !ok {
		t.Fatalf("一次消耗满桶应放行, ok=%v err=%v", ok, err)
	}
	if ok, err := l.AllowN(ctx, 5); err != nil || ok {
		t.Fatalf("空桶消耗应被拒且无 error, ok=%v err=%v", ok, err)
	}
	advance(500 * time.Millisecond)
	if ok, err := l.AllowN(ctx, 5); err != nil || !ok {
		t.Fatalf("回填 5 后应放行, ok=%v err=%v", ok, err)
	}
}

func TestLimiter_ScriptStateLayout(t *testing.T) {
	mr, newClient := setup(t)
	clock, _ := newFakeClock()
	l := newTestLimiter(t, newClient(), "layout", 10, 5, clock, 0)
	defer l.Close()

	if err := l.Allow(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 状态收敛在 {tag} 单 Key Hash 内(Cluster 单 slot),字段 last/tokens 可读
	stored := mr.HGet("xm_ratelimit:{layout}", "tokens")
	if stored == "" {
		t.Fatal("判定后 Hash 内应存在 tokens 字段")
	}
}

func TestLimiter_NoscriptFallback(t *testing.T) {
	// 第二个 client 从未 EVAL 过该脚本,EVALSHA 必回 NOSCRIPT → Run 自动回落 EVAL
	_, newClient := setup(t)
	clock, _ := newFakeClock()
	l1 := newTestLimiter(t, newClient(), "noscript", 10, 3, clock, 0)
	l2 := newTestLimiter(t, newClient(), "noscript", 10, 3, clock, 0)
	defer l1.Close()
	defer l2.Close()
	ctx := context.Background()

	if err := l1.Allow(ctx); err != nil {
		t.Fatal(err)
	}
	// l2 首次判定即触发 NOSCRIPT 路径,仍应正常放行剩余份额
	if err := l2.Allow(ctx); err != nil {
		t.Fatalf("NOSCRIPT 回落 EVAL 应透明, 得到 %v", err)
	}
}

func TestLimiter_TTLExpiry(t *testing.T) {
	mr, newClient := setup(t)
	clock, advance := newFakeClock()
	l := newTestLimiter(t, newClient(), "ttl", 10, 5, clock, time.Minute)
	defer l.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := l.Allow(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// miniredis 按 TTL 过期清理状态(服务端时钟)
	mr.FastForward(2 * time.Minute)
	if mr.Exists("xm_ratelimit:{ttl}") {
		t.Fatal("超过 TTL 后桶状态应已过期清理")
	}
	advance(2 * time.Minute)
	if err := l.Allow(ctx); err != nil {
		t.Fatalf("状态清理后应重新满桶初始化并放行: %v", err)
	}
}

func TestLimiter_ConcurrentAtomicity(t *testing.T) {
	// 冻结时钟:全局放行总数必须严格等于 Burst——脚本原子执行,无跨实例超卖
	_, newClient := setup(t)
	clock, _ := newFakeClock()
	l := newTestLimiter(t, newClient(), "atomic", 100, 40, clock, 0)
	defer l.Close()
	ctx := context.Background()

	const goroutines = 16
	var allowed atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 20; j++ {
				if err := l.Allow(ctx); err == nil {
					allowed.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := allowed.Load(); got != 40 {
		t.Fatalf("冻结时钟下全局放行应严格等于 Burst=40, 实际 %d", got)
	}
}

func TestLimiter_Errors(t *testing.T) {
	_, newClient := setup(t)
	clock, _ := newFakeClock()

	t.Run("Close 后判定", func(t *testing.T) {
		l := newTestLimiter(t, newClient(), "closed", 10, 10, clock, 0)
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		if err := l.Close(); err != nil {
			t.Fatalf("Close 应幂等: %v", err)
		}
		if err := l.Allow(context.Background()); !errors.Is(err, ratelimit.ErrClosed) {
			t.Fatalf("期望 ErrClosed, 得到 %v", err)
		}
		if ok, err := l.AllowN(context.Background(), 1); ok || !errors.Is(err, ratelimit.ErrClosed) {
			t.Fatalf("AllowN 同契约, ok=%v err=%v", ok, err)
		}
	})

	t.Run("ctx 取消优先", func(t *testing.T) {
		l := newTestLimiter(t, newClient(), "ctx", 10, 10, clock, 0)
		defer l.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := l.Allow(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("期望 context.Canceled, 得到 %v", err)
		}
	})

	t.Run("服务端故障传播为 error", func(t *testing.T) {
		mr, err := miniredis.Run()
		if err != nil {
			t.Fatal(err)
		}
		bad := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		mr.Close() // 服务端消失:判定应返回包装 error 而非 panic
		l := newTestLimiter(t, bad, "down", 10, 10, clock, 0)
		defer l.Close()
		err = l.Allow(context.Background())
		if err == nil || errors.Is(err, ratelimit.ErrRateLimited) {
			t.Fatalf("期望后端故障 error, 得到 %v", err)
		}
	})
}
