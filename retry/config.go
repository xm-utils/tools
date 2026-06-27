package retry

import (
	"context"
	"time"
)

// Config 重试配置
type Config struct {
	MaxRetries      int             // 最大重试次数
	Strategy        Strategy        // 重试策略
	Timeout         time.Duration   // 单次执行超时时间
	RetryableErrors []error         // 可重试的错误列表(为空则全部重试)
	Context         context.Context // 上下文(用于取消)
}

// DefaultRetryConfig 返回默认重试配置
func DefaultRetryConfig() *Config {
	return &Config{
		MaxRetries: 5,
		Strategy: &ExponentialBackoffStrategy{
			InitialDelay: 1 * time.Second,
			MaxDelay:     60 * time.Second,
			Multiplier:   2.0,
		},
		Timeout: 10 * time.Second,
	}
}
