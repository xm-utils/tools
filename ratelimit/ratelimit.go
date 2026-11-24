// Package ratelimit 提供限流与降级能力(ROADMAP 3.2):主动流量防护,
// 入口流量超标时保护自身、出口调用时约束自我频率。
//
// 三种算法共用 Limiter 契约,按场景选取:
//   - TokenBucket:允许突发(Burst)、回填平滑,默认选型;
//   - SlidingWindow:任意窗口内总数严格不超 Limit,精确计数场景;
//   - LeakyBucket:出水速率恒定,流量整形/出口自我限速场景。
//
// 后端与装配:Local(单实例按 Key 多维限流)、redisstore(Redis Lua 原子共享份额)、
// grpcmiddleware/httpmiddleware(入口中间件)。
// 待演进:breaker 告警联动、规则热更新的配置源监听装配。
package ratelimit

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrRateLimited 请求被限流拒绝(无剩余等待信息场景)
	ErrRateLimited = errors.New("ratelimit: rate limited")
	// ErrInvalidConfig 限流配置非法(非正速率/容量、空 Key 等),构造与热更新时返回
	ErrInvalidConfig = errors.New("ratelimit: invalid config")
	// ErrClosed 限流器或后端已关闭
	ErrClosed = errors.New("ratelimit: closed")
)

// Clock 时间源抽象:生产用 SystemClock,测试注入假时钟确定性推进
type Clock interface {
	Now() time.Time
}

// SystemClock 基于 time.Now 的默认时钟
type SystemClock struct{}

// Now 返回当前时间
func (SystemClock) Now() time.Time { return time.Now() }

// ClockFunc 函数适配器,让 time.Now 直接满足 Clock 接口
type ClockFunc func() time.Time

// Now 调用底层函数
func (f ClockFunc) Now() time.Time { return f() }

// Result 一次限流判定的结果,字段语义对齐 golang.org/x/time/rate.Reservation
type Result struct {
	OK         bool          // 是否放行
	RetryAfter time.Duration // 被拒时距下一可用令牌的理论等待(0 表示无法预估)
	Limit      float64       // 当前速率(个/秒,调试用)
	Remaining  float64       // 判定后剩余令牌数
}

// Limiter 限流器统一接口(接口级/服务级/租户 Key 级共用同一契约)
type Limiter interface {
	// Allow 判定单次请求是否放行;被拒时返回包装 ErrRateLimited 的 error,
	// ctx 取消/超时优先返回 ctx.Err()
	Allow(ctx context.Context) error

	// AllowN 判定一次消耗 n 个令牌(n 必须为正数)
	AllowN(ctx context.Context, n int) (bool, error)

	// Peek 返回当前可用令牌快照(不消耗,用于观测与预估)
	Peek() Result

	// Close 关闭限流器,关闭后判定一律返回 ErrClosed
	Close() error
}
