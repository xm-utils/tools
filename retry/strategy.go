package retry

import (
	"math"
	"time"
)

// Strategy 重试策略接口
type Strategy interface {
	// GetDelay 获取下次重试的延迟时间
	// attempt: 当前重试次数(从0开始)
	GetDelay(attempt int) time.Duration
}

// FixedRetryStrategy 固定间隔重试策略
type FixedRetryStrategy struct {
	Interval time.Duration // 固定间隔时间
}

func (s *FixedRetryStrategy) GetDelay(attempt int) time.Duration {
	return s.Interval
}

// ExponentialBackoffStrategy 指数退避重试策略
type ExponentialBackoffStrategy struct {
	InitialDelay time.Duration // 初始延迟
	MaxDelay     time.Duration // 最大延迟
	Multiplier   float64       // 退避倍数(默认2.0)
}

func (s *ExponentialBackoffStrategy) GetDelay(attempt int) time.Duration {
	delay := float64(s.InitialDelay) * math.Pow(s.Multiplier, float64(attempt))

	// 限制最大延迟
	if delay > float64(s.MaxDelay) {
		return s.MaxDelay
	}

	return time.Duration(delay)
}

// LinearBackoffStrategy 线性退避重试策略
type LinearBackoffStrategy struct {
	InitialDelay time.Duration // 初始延迟
	Increment    time.Duration // 每次递增的时间
	MaxDelay     time.Duration // 最大延迟
}

func (s *LinearBackoffStrategy) GetDelay(attempt int) time.Duration {
	delay := s.InitialDelay + s.Increment*time.Duration(attempt)

	// 限制最大延迟
	if delay > s.MaxDelay {
		return s.MaxDelay
	}

	return delay
}
