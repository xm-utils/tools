package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// SlidingWindowConfig 滑动窗口配置
type SlidingWindowConfig struct {
	// Limit 窗口内允许的最大请求数,必须 > 0
	Limit int64
	// Window 窗口宽度,必须 > 0
	Window time.Duration
	// Clock 时间源,nil 时使用 SystemClock;测试可注入假时钟
	Clock Clock
}

// windowEntry 一条放行记录:到达时间与权重(AllowN 一次记一条,避免逐令牌展开)
type windowEntry struct {
	at time.Time
	n  int64
}

// SlidingWindow 滑动窗口限流器:精确计数场景——任意长度为 Window 的区间内
// 放行总数严格不超过 Limit(区别于固定窗口临界双倍问题)。
// 线程安全;每条放行以记录形式驻留至过期,内存上界 O(Limit),
// 高频大额场景配合合理 Limit 使用。
type SlidingWindow struct {
	mu      sync.Mutex
	cfg     SlidingWindowConfig
	entries []windowEntry
	total   int64 // entries 权重和,随入队/修剪同步维护
	closed  bool
}

var _ Limiter = (*SlidingWindow)(nil)

// NewSlidingWindow 创建滑动窗口限流器;配置非法返回包装 ErrInvalidConfig 的 error,不 panic
func NewSlidingWindow(cfg SlidingWindowConfig) (*SlidingWindow, error) {
	if cfg.Limit <= 0 {
		return nil, fmt.Errorf("%w: Limit 必须为正数, 当前 %d", ErrInvalidConfig, cfg.Limit)
	}
	if cfg.Window <= 0 {
		return nil, fmt.Errorf("%w: Window 必须为正数, 当前 %v", ErrInvalidConfig, cfg.Window)
	}
	if cfg.Clock == nil {
		cfg.Clock = SystemClock{}
	}
	return &SlidingWindow{cfg: cfg}, nil
}

// Allow 判定单次请求是否放行;被拒时返回包装 ErrRateLimited 的 error,
// 退避时长为最早记录滑出窗口所需时间(与 Allow 同一次加锁内算出)
func (sw *SlidingWindow) Allow(ctx context.Context) error {
	return sw.judge(ctx, 1)
}

// AllowN 判定一次消耗 n 个额度;n <= 0 返回包装 ErrInvalidConfig 的 error;
// 被拒只返回 (false, nil),与 TokenBucket 一致
func (sw *SlidingWindow) AllowN(ctx context.Context, n int) (bool, error) {
	if n <= 0 {
		return false, fmt.Errorf("%w: n 必须为正数, 当前 %d", ErrInvalidConfig, n)
	}
	if err := sw.judge(ctx, int64(n)); err != nil {
		if errors.Is(err, ErrRateLimited) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Peek 返回当前窗口余量快照(不消耗)
func (sw *SlidingWindow) Peek() Result {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.closed {
		return Result{}
	}
	now := sw.cfg.Clock.Now()
	sw.pruneLocked(now)
	return sw.resultLocked(now, 1)
}

// SetParams 热更新窗口额度与宽度(规则热更新的应用端点,监听装配由使用方实现)。
// 收紧 Limit 后当前计数不追溯撤销,仅影响后续判定;非法参数保持原配置并返回 error。
func (sw *SlidingWindow) SetParams(limit int64, window time.Duration) error {
	if limit <= 0 {
		return fmt.Errorf("%w: limit 必须为正数, 当前 %d", ErrInvalidConfig, limit)
	}
	if window <= 0 {
		return fmt.Errorf("%w: window 必须为正数, 当前 %v", ErrInvalidConfig, window)
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.closed {
		return ErrClosed
	}
	sw.pruneLocked(sw.cfg.Clock.Now())
	sw.cfg.Limit, sw.cfg.Window = limit, window
	return nil
}

// Close 关闭限流器,幂等;关闭后判定一律返回 ErrClosed
func (sw *SlidingWindow) Close() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.closed = true
	sw.entries = nil
	sw.total = 0
	return nil
}

// judge 核心判定:一次加锁内完成修剪、判定、记账与退避预估;
// 被拒返回包装 ErrRateLimited 的 error(消息携带 retry after,供 Allow 直接透出)
func (sw *SlidingWindow) judge(ctx context.Context, n int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.closed {
		return ErrClosed
	}
	now := sw.cfg.Clock.Now()
	sw.pruneLocked(now)

	if sw.total+n <= sw.cfg.Limit {
		sw.entries = append(sw.entries, windowEntry{at: now, n: n})
		sw.total += n
		return nil
	}
	res := sw.resultLocked(now, n)
	return fmt.Errorf("%w: retry after %s", ErrRateLimited, res.RetryAfter)
}

// pruneLocked 滑出过期记录;必须在持有 sw.mu 时调用。
// 时钟回拨时 now-window 更早,不会误删在窗记录(过期判定天然免疫回拨)。
func (sw *SlidingWindow) pruneLocked(now time.Time) {
	expireBefore := now.Add(-sw.cfg.Window)
	i := 0
	for i < len(sw.entries) && !sw.entries[i].at.After(expireBefore) {
		i++
	}
	if i > 0 {
		for _, e := range sw.entries[:i] {
			sw.total -= e.n
		}
		sw.entries = append(sw.entries[:0], sw.entries[i:]...)
	}
}

// resultLocked 组装当前快照;n 为本次意图消耗(用于预估等待)
func (sw *SlidingWindow) resultLocked(now time.Time, n int64) Result {
	remaining := sw.cfg.Limit - sw.total
	if remaining < 0 {
		// 热更新收紧 Limit 后已在窗记录可能超额,余量不报负数
		remaining = 0
	}
	res := Result{
		OK:        sw.total+n <= sw.cfg.Limit,
		Limit:     sw.rateLocked(),
		Remaining: float64(remaining),
	}
	if !res.OK && len(sw.entries) > 0 {
		// 最早记录滑出窗口后即可释放其权重
		res.RetryAfter = sw.entries[0].at.Add(sw.cfg.Window).Sub(now)
		if res.RetryAfter < 0 {
			res.RetryAfter = 0
		}
	}
	return res
}

// rateLocked 窗口配额折算成的等效速率(个/秒);必须在持有 sw.mu 时调用
func (sw *SlidingWindow) rateLocked() float64 {
	seconds := sw.cfg.Window.Seconds()
	if seconds <= 0 {
		return 0
	}
	return float64(sw.cfg.Limit) / seconds
}
