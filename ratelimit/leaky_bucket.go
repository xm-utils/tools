package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// LeakyBucketConfig 漏桶配置
type LeakyBucketConfig struct {
	// Rate 出水速率(个/秒),输出严格不超过该速率,必须 > 0
	Rate float64
	// Queue 桶容量(可同时容纳的水位数,即允许的排队上限),必须 >= 1。
	// Queue=1 即纯匀速限速(任何背靠背请求都会被拒);越大越能吸收突发涌入,
	// 但只影响"是否接纳",不改变出水速率。
	Queue int64
	// Clock 时间源,nil 时使用 SystemClock;测试可注入假时钟
	Clock Clock
}

// LeakyBucket 漏桶限流器:流量整形场景——无论入口如何突发,出水的平均速率
// 被严格压在 Rate 以内(区别于令牌桶:后者允许 Burst 规模的瞬时突发输出)。
//
// 判定按"水位"记账:请求被接纳即入桶,桶以 Rate 匀速漏水;水位超过 Queue 则拒绝。
// 线程安全;漏水同样懒执行(判定时刻按经过时间折算水位),无后台协程。
//
// Allow/AllowN 是非阻塞判定,不代替调用方等待;需要真正匀速放行时用
// Reserve/Wait(见下)。
type LeakyBucket struct {
	mu       sync.Mutex
	cfg      LeakyBucketConfig
	spill    time.Time // 桶内现有水位全部漏完的虚拟时刻
	nowFloor time.Time // 已观测到的最大时间,用于时钟回拨防御
	closed   bool
}

var _ Limiter = (*LeakyBucket)(nil)

// NewLeakyBucket 创建漏桶,初始为空桶。
// 配置非法返回包装 ErrInvalidConfig 的 error,不 panic。
func NewLeakyBucket(cfg LeakyBucketConfig) (*LeakyBucket, error) {
	if cfg.Rate <= 0 || math.IsNaN(cfg.Rate) || math.IsInf(cfg.Rate, 0) {
		return nil, fmt.Errorf("%w: Rate 必须为正数, 当前 %v", ErrInvalidConfig, cfg.Rate)
	}
	if cfg.Queue < 1 {
		return nil, fmt.Errorf("%w: Queue 必须不小于 1, 当前 %d", ErrInvalidConfig, cfg.Queue)
	}
	if cfg.Clock == nil {
		cfg.Clock = SystemClock{}
	}
	return &LeakyBucket{cfg: cfg}, nil
}

// Allow 判定单次请求是否放行;被拒时返回包装 ErrRateLimited 的 error,
// 消息内的退避时长为本次额度需等待水位回落的时间
func (lb *LeakyBucket) Allow(ctx context.Context) error {
	_, err := lb.reserve(ctx, 1)
	return err
}

// AllowN 判定一次消耗 n 个额度;n <= 0 返回包装 ErrInvalidConfig 的 error,
// 被拒时仅返回 (false, nil)
func (lb *LeakyBucket) AllowN(ctx context.Context, n int) (bool, error) {
	if n <= 0 {
		return false, fmt.Errorf("%w: n 必须为正数, 当前 %d", ErrInvalidConfig, n)
	}
	if _, err := lb.reserve(ctx, int64(n)); err != nil {
		if errors.Is(err, ErrRateLimited) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Reserve 预定 n 个额度,返回本次请求按匀速出水应当等待的时长
// (0 表示桶空可立即放行)。被拒时返回包装 ErrRateLimited 的 error,
// 退避时长为该错误消息中水位回落所需时间。
//
// 与 Allow 的差别仅在语义:Reserve 明确把"应当排多久"交给调用方,
// 用于出口调用自我限速(拿到时长后 time.Sleep 或按 Wait 阻塞)。
func (lb *LeakyBucket) Reserve(ctx context.Context, n int) (time.Duration, error) {
	if n <= 0 {
		return 0, fmt.Errorf("%w: n 必须为正数, 当前 %d", ErrInvalidConfig, n)
	}
	return lb.reserve(ctx, int64(n))
}

// Wait 阻塞至本请求按匀速出水应放行的时刻;n <= 0 返回包装 ErrInvalidConfig
// 的 error,被拒返回包装 ErrRateLimited 的 error(不阻塞)。
// ctx 取消时立即返回 ctx.Err();额度已占用不做回滚(与令牌桶预约不同,
// 漏桶的水位只能等它自然漏完)。
func (lb *LeakyBucket) Wait(ctx context.Context, n int) error {
	delay, err := lb.Reserve(ctx, n)
	if err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Peek 返回当前水位快照(不消耗)
func (lb *LeakyBucket) Peek() Result {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed {
		return Result{}
	}
	now := lb.monotonicNow()
	return lb.resultLocked(now, 1)
}

// SetParams 热更新出水速率与桶容量(规则热更新的应用端点,监听装配由使用方实现)。
// 桶内水位个数保持不变,但按新速率重排出水时刻;收紧 Queue 后已有水位
// 不追溯撤销,仅影响后续判定。非法参数保持原配置并返回 error。
func (lb *LeakyBucket) SetParams(rate float64, queue int64) error {
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return fmt.Errorf("%w: rate 必须为正数, 当前 %v", ErrInvalidConfig, rate)
	}
	if queue < 1 {
		return fmt.Errorf("%w: queue 必须不小于 1, 当前 %d", ErrInvalidConfig, queue)
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed {
		return ErrClosed
	}
	now := lb.monotonicNow()
	pending := lb.pendingAtLocked(now)
	lb.cfg.Rate, lb.cfg.Queue = rate, queue
	// 以"水位个数"为不变量重排 spill:否则提速后旧 spill 绝对时刻会被新速率
	// 折算成成倍虚增的水位
	lb.spill = now.Add(lb.interval() * time.Duration(pending))
	return nil
}

// Close 关闭限流器,幂等;关闭后判定一律返回 ErrClosed
func (lb *LeakyBucket) Close() error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.closed = true
	return nil
}

func (lb *LeakyBucket) reserve(ctx context.Context, n int64) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed {
		return 0, ErrClosed
	}
	now := lb.monotonicNow()

	// 桶已漏空时把基准点拉回当下,避免陈年 spill 叠加后续等待
	if lb.spill.Before(now) {
		lb.spill = now
	}
	if lb.pendingAtLocked(now)+n > lb.cfg.Queue {
		res := lb.resultLocked(now, n)
		return 0, fmt.Errorf("%w: retry after %s", ErrRateLimited, res.RetryAfter)
	}

	// 本次批量首令牌的实际出水时刻 = max(now, 原 spill),等待时长即二者之差
	start := lb.spill
	if start.Before(now) {
		start = now
	}
	delay := start.Sub(now)
	lb.spill = start.Add(lb.interval() * time.Duration(n))
	return delay, nil
}

// resultLocked 组装指定时刻的快照;n 为本次意图消耗
func (lb *LeakyBucket) resultLocked(now time.Time, n int64) Result {
	pending := lb.pendingAtLocked(now)
	remaining := lb.cfg.Queue - pending
	if remaining < 0 {
		// 热更新收紧 Queue 后桶内水位可能超额,余量不报负数
		remaining = 0
	}
	res := Result{
		OK:        pending+n <= lb.cfg.Queue,
		Limit:     lb.cfg.Rate,
		Remaining: float64(remaining),
	}
	if !res.OK {
		// 需漏掉 (pending+n-Queue) 个水位才容得下本次消耗
		res.RetryAfter = lb.interval() * time.Duration(pending+n-lb.cfg.Queue)
		if res.RetryAfter < 0 {
			res.RetryAfter = 0
		}
	}
	return res
}

// pendingAtLocked 按指定时刻折算当前水位(未漏完的令牌数,向上取整
// 保证非零水位不被浮点截断吞掉);必须在持有 lb.mu 时调用
func (lb *LeakyBucket) pendingAtLocked(now time.Time) int64 {
	remain := lb.spill.Sub(now)
	if remain <= 0 {
		return 0
	}
	return int64(math.Ceil(remain.Seconds() * lb.cfg.Rate))
}

// interval 单令牌出水间隔
func (lb *LeakyBucket) interval() time.Duration {
	return time.Duration(float64(time.Second) / lb.cfg.Rate)
}

// monotonicNow 取单调不减的当前时间:时钟回拨时钳制在上次观测点,
// 水位只按真实推进的时间回落,不因回拨而"倒灌水";必须在持有 lb.mu 时调用
func (lb *LeakyBucket) monotonicNow() time.Time {
	now := lb.cfg.Clock.Now()
	if now.Before(lb.nowFloor) {
		return lb.nowFloor
	}
	lb.nowFloor = now
	return now
}
