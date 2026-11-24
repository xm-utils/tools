package ratelimit

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// TokenBucketConfig 令牌桶配置
type TokenBucketConfig struct {
	// Rate 令牌回填速率(个/秒),必须 > 0
	Rate float64
	// Burst 桶容量(最大令牌数),必须 > 0;允许突发流量上限
	Burst float64
	// Clock 时间源,nil 时使用 SystemClock;测试可注入假时钟
	Clock Clock
}

// TokenBucket 令牌桶限流器:默认算法,支持突发(Burst)与平滑回填。
// 线程安全;判定采用懒回填——每次请求时按经过的时间补充令牌,无后台协程。
type TokenBucket struct {
	mu     sync.Mutex
	cfg    TokenBucketConfig
	tokens float64   // 当前可用令牌数
	last   time.Time // 上次回填时间点
	closed bool
}

// NewTokenBucket 创建令牌桶,初始为满桶(允许开局突发)。
// 配置非法返回包装 ErrInvalidConfig 的 error,不 panic。
func NewTokenBucket(cfg TokenBucketConfig) (*TokenBucket, error) {
	if cfg.Rate <= 0 || math.IsNaN(cfg.Rate) || math.IsInf(cfg.Rate, 0) {
		return nil, fmt.Errorf("%w: Rate 必须为正数, 当前 %v", ErrInvalidConfig, cfg.Rate)
	}
	if cfg.Burst <= 0 || math.IsNaN(cfg.Burst) || math.IsInf(cfg.Burst, 0) {
		return nil, fmt.Errorf("%w: Burst 必须为正数, 当前 %v", ErrInvalidConfig, cfg.Burst)
	}
	if cfg.Clock == nil {
		cfg.Clock = SystemClock{}
	}
	return &TokenBucket{
		cfg:    cfg,
		tokens: cfg.Burst,
		last:   cfg.Clock.Now(),
	}, nil
}

// Allow 判定单次请求是否放行;被拒时返回包装 ErrRateLimited 的 error,
// 退避时长在同一次加锁判定内算出,避免锁间隙回填导致误判为可立即重试
func (tb *TokenBucket) Allow(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.closed {
		return ErrClosed
	}
	tb.refill()
	if tb.tokens >= 1 {
		tb.tokens--
		return nil
	}
	wait := tb.retryAfterLocked(1)
	return fmt.Errorf("%w: retry after %s", ErrRateLimited, wait)
}

// AllowN 判定一次消耗 n 个令牌;n <= 0 视为非法参数直接拒绝(error 返回)
func (tb *TokenBucket) AllowN(ctx context.Context, n int) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if n <= 0 {
		return false, fmt.Errorf("%w: n 必须为正数, 当前 %d", ErrInvalidConfig, n)
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.closed {
		return false, ErrClosed
	}

	tb.refill()
	if tb.tokens >= float64(n) {
		tb.tokens -= float64(n)
		return true, nil
	}
	return false, nil
}

// Peek 返回当前可用令牌快照(不消耗)
func (tb *TokenBucket) Peek() Result {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.closed {
		return Result{}
	}
	tb.refill()
	return Result{
		OK:         tb.tokens >= 1,
		RetryAfter: tb.retryAfterLocked(1),
		Limit:      tb.cfg.Rate,
		Remaining:  tb.tokens,
	}
}

// SetParams 热更新速率与容量(规则热更新的应用端点,监听装配由使用方实现)。
// 新 Burst 缩小时当前令牌数同步截断;非法参数保持原配置并返回 error。
func (tb *TokenBucket) SetParams(rate, burst float64) error {
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return fmt.Errorf("%w: rate 必须为正数, 当前 %v", ErrInvalidConfig, rate)
	}
	if burst <= 0 || math.IsNaN(burst) || math.IsInf(burst, 0) {
		return fmt.Errorf("%w: burst 必须为正数, 当前 %v", ErrInvalidConfig, burst)
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.closed {
		return ErrClosed
	}
	tb.refill()
	tb.cfg.Rate, tb.cfg.Burst = rate, burst
	if tb.tokens > burst {
		tb.tokens = burst
	}
	return nil
}

// Close 关闭限流器,幂等;关闭后判定一律返回 ErrClosed
func (tb *TokenBucket) Close() error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.closed = true
	return nil
}

// refill 按经过的时间补充令牌;必须在持有 tb.mu 时调用。
// 时钟回拨(now 早于 last)时不扣减令牌,仅重置基准点,防御负 elapsed。
func (tb *TokenBucket) refill() {
	now := tb.cfg.Clock.Now()
	elapsed := now.Sub(tb.last)
	if elapsed <= 0 {
		tb.last = now
		return
	}
	tb.tokens = math.Min(tb.cfg.Burst, tb.tokens+elapsed.Seconds()*tb.cfg.Rate)
	tb.last = now
}

// retryAfterLocked 预估 n 个令牌可用所需等待时长;必须在持有 tb.mu 时调用
func (tb *TokenBucket) retryAfterLocked(n int) time.Duration {
	deficit := float64(n) - tb.tokens
	if deficit <= 0 {
		return 0
	}
	return time.Duration(deficit / tb.cfg.Rate * float64(time.Second))
}

var _ Limiter = (*TokenBucket)(nil)
