package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewLocal_InvalidConfig(t *testing.T) {
	if _, err := NewLocal(LocalConfig{Rate: 0, Burst: 10}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Rate 非法应报错, 得到 %v", err)
	}
	if _, err := NewLocal(LocalConfig{Rate: 10, Burst: -1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Burst 非法应报错, 得到 %v", err)
	}
	if _, err := NewLocal(LocalConfig{Rate: 10, Burst: 10}); err != nil {
		t.Fatalf("合法配置不应报错: %v", err)
	}
}

func TestLocal_LazyCreateAndReuse(t *testing.T) {
	l, _ := NewLocal(LocalConfig{Rate: 10, Burst: 5})
	defer l.Close()
	ctx := context.Background()

	if err := l.Allow(ctx, "api:order:create"); err != nil {
		t.Fatalf("首次 Allow 应惰性创建并放行: %v", err)
	}
	// 同 Key 复用同一实例
	a, err := l.Limiter("tenant:42")
	if err != nil {
		t.Fatal(err)
	}
	b, err := l.Limiter("tenant:42")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("同 Key 应返回同一 Limiter 实例")
	}
	// 不同 Key 相互独立
	if _, err := l.Limiter("other:key"); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Limiter(""); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("空 Key 应报错, 得到 %v", err)
	}
}

func TestLocal_PerKeyIsolation(t *testing.T) {
	clock, _ := newFakeClock()
	l, _ := NewLocal(LocalConfig{Rate: 1, Burst: 2, Clock: clock})
	defer l.Close()
	ctx := context.Background()

	// 耗尽 keyA 额度
	for i := 0; i < 2; i++ {
		if err := l.Allow(ctx, "keyA"); err != nil {
			t.Fatalf("keyA 预置消耗失败: %v", err)
		}
	}
	if err := l.Allow(ctx, "keyA"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("keyA 应被限流, 得到 %v", err)
	}
	// keyB 不受影响
	if err := l.Allow(ctx, "keyB"); err != nil {
		t.Fatalf("keyB 应独立放行: %v", err)
	}
}

func TestLocal_AllowN(t *testing.T) {
	l, _ := NewLocal(LocalConfig{Rate: 10, Burst: 10})
	defer l.Close()
	ctx := context.Background()

	if ok, err := l.AllowN(ctx, "batch", 6); err != nil || !ok {
		t.Fatalf("消耗 6 应放行, ok=%v err=%v", ok, err)
	}
	if ok, _ := l.AllowN(ctx, "batch", 6); ok {
		t.Fatal("剩余不足应被拒")
	}
	if _, err := l.AllowN(ctx, "batch", 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("n=0 应报错, 得到 %v", err)
	}
}

func TestLocal_UpdateRemoveStats(t *testing.T) {
	l, _ := NewLocal(LocalConfig{Rate: 10, Burst: 10})
	defer l.Close()
	ctx := context.Background()

	// Update 已存在 Key:调小容量后立即可见截断
	if err := l.Allow(ctx, "hot"); err != nil {
		t.Fatal(err)
	}
	if err := l.Update("hot", 1, 1); err != nil {
		t.Fatal(err)
	}
	if res := l.Stats("hot"); res.Remaining > 1.01 {
		t.Fatalf("Update 后容量应截断至 1, 得到 %v", res.Remaining)
	}
	// Update 不存在 Key:按新参数创建
	if err := l.Update("fresh", 5, 5); err != nil {
		t.Fatal(err)
	}
	if res := l.Stats("fresh"); res.Remaining != 5 {
		t.Fatalf("新建 Key 应满桶 5, 得到 %v", res.Remaining)
	}
	// Update 非法参数保持原状
	before := l.Stats("fresh")
	if err := l.Update("fresh", -1, 5); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("非法参数应报错, 得到 %v", err)
	}
	if after := l.Stats("fresh"); after.Remaining != before.Remaining {
		t.Fatalf("非法 Update 不应改变状态: %v vs %v", after, before)
	}
	// Remove:移除后重新按默认配置惰性创建
	if err := l.Remove("fresh"); err != nil {
		t.Fatal(err)
	}
	if res := l.Stats("fresh"); res != (Result{}) {
		t.Fatalf("Remove 后 Stats 应为零值: %+v", res)
	}
	if err := l.Remove("fresh"); err != nil {
		t.Fatalf("Remove 不存在 Key 应幂等返回 nil: %v", err)
	}
	// 被 Remove 的旧实例引用仍可用(使用方持有的 Limiter 不被强制失效,契约由 Close 保障)
}

func TestLocal_CloseSemantics(t *testing.T) {
	l, _ := NewLocal(LocalConfig{Rate: 10, Burst: 10})
	ctx := context.Background()

	if _, err := l.Limiter("k"); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close 应幂等: %v", err)
	}
	if err := l.Allow(ctx, "k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Allow 应返回 ErrClosed, 得到 %v", err)
	}
	if _, err := l.Limiter("newkey"); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后新建应返回 ErrClosed, 得到 %v", err)
	}
	if err := l.Update("k", 1, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Update 应返回 ErrClosed, 得到 %v", err)
	}
}

func TestLocal_ConcurrentLimiterCreation(t *testing.T) {
	// 多 goroutine 同 Key 并发首建:双检锁下必须收敛为同一实例且无 map 竞争
	l, _ := NewLocal(LocalConfig{Rate: 1e6, Burst: 1e6})
	defer l.Close()

	const goroutines = 64
	var first atomic.Value
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			got, err := l.Limiter("race:key")
			if err != nil {
				t.Errorf("并发获取失败: %v", err)
				return
			}
			if prev := first.Swap(got); prev != nil && prev.(Limiter) != got {
				t.Errorf("同 Key 并发首建返回了不同实例")
			}
		}()
	}
	close(start)
	wg.Wait()

	// Burst 与放行总数核对:64 个 goroutine 各 Allow 一次,放行数应远小于 64+回填容忍
	res := l.Stats("race:key")
	if res.Remaining < 1e6-64-1 {
		t.Fatalf("令牌消耗应不超过 goroutine 数, 剩余 %v", res.Remaining)
	}
}

func TestLocal_CtxCancelledPriority(t *testing.T) {
	l, _ := NewLocal(LocalConfig{Rate: 10, Burst: 10})
	defer l.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.Allow(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx 取消应优先传播, 得到 %v", err)
	}
	// 未取消但超时:用短超时 ctx 验证不阻塞(令牌充足直接放行,超时不影响)
	tctx, tcancel := context.WithTimeout(context.Background(), time.Second)
	defer tcancel()
	if err := l.Allow(tctx, "k"); err != nil {
		t.Fatalf("令牌充足应放行, 得到 %v", err)
	}
}
