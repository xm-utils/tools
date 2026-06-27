package breaker

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/retry"
)

// CircuitBreakerWithRetry 熔断器与重试组合执行器
type CircuitBreakerWithRetry struct {
	circuitBreaker *CircuitBreaker
	retryExecutor  *retry.Executor
	config         *CombinedConfig
	log            *logrus.Entry
}

// CombinedConfig 组合配置
type CombinedConfig struct {
	CircuitBreakerConfig *CircuitBreakerConfig
	RetryConfig          *retry.Config
}

// DefaultCombinedConfig 返回默认组合配置
func DefaultCombinedConfig() *CombinedConfig {
	return &CombinedConfig{
		CircuitBreakerConfig: DefaultCircuitBreakerConfig(),
		RetryConfig:          retry.DefaultRetryConfig(),
	}
}

// NewCircuitBreakerWithRetry 创建熔断器+重试+死信队列组合执行器
func NewCircuitBreakerWithRetry(name string, config *CombinedConfig) *CircuitBreakerWithRetry {
	if config == nil {
		config = DefaultCombinedConfig()
	}

	cb := NewCircuitBreaker(name, config.CircuitBreakerConfig)
	retryExec := retry.NewRetryExecutor(config.RetryConfig)

	return &CircuitBreakerWithRetry{
		circuitBreaker: cb,
		retryExecutor:  retryExec,
		config:         config,
		log: logrus.WithFields(logrus.Fields{
			"module": "CircuitBreakerWithRetry",
			"name":   name,
		}),
	}
}

// Execute 执行受保护的调用(先熔断判断,再重试)
func (c *CircuitBreakerWithRetry) Execute(task func(ctx context.Context) (interface{}, error), args ...interface{}) <-chan *ExecutionResult {
	resultChan := make(chan *ExecutionResult, 1)

	go func() {
		defer close(resultChan)

		startTime := time.Now()
		execResult := &ExecutionResult{
			Name:      c.circuitBreaker.GetName(),
			Timestamp: startTime,
		}

		// 1. 先检查熔断器状态
		cbState := c.circuitBreaker.GetState()
		execResult.CircuitBreakerState = cbState

		if cbState == StateOpen {
			c.log.Warnf("熔断器处于OPEN状态, 跳过重试直接降级")
			execResult.RejectedByCircuitBreaker = true
			execResult.Duration = time.Since(startTime)

			// 执行降级函数
			if c.circuitBreaker.config.FallbackFunc != nil {
				data, err := c.circuitBreaker.config.FallbackFunc(args...)
				execResult.Data = data
				execResult.Error = err
				execResult.FallbackExecuted = true
			} else {
				execResult.Error = ErrCircuitOpen
			}

			resultChan <- execResult
			return
		}

		// 2. 熔断器未开启,执行重试逻辑
		c.log.Debugf("熔断器状态: %s, 开始执行重试逻辑", cbState)

		// 包装任务,在熔断器保护下执行
		wrappedTask := func(ctx context.Context) (interface{}, error) {
			return c.circuitBreaker.ExecuteWithContext(ctx, task, args...)
		}

		// 3. 执行重试
		retryResultChan := c.retryExecutor.Execute(wrappedTask, args)
		retryResult := <-retryResultChan

		execResult.RetryResult = retryResult
		execResult.Duration = time.Since(startTime)
		execResult.Data = retryResult.Data
		execResult.Error = retryResult.Error

		resultChan <- execResult
	}()

	return resultChan
}

// GetCircuitBreaker 获取熔断器实例
func (c *CircuitBreakerWithRetry) GetCircuitBreaker() *CircuitBreaker {
	return c.circuitBreaker
}

// ExecutionResult 执行结果
type ExecutionResult struct {
	Name                     string
	Timestamp                time.Time
	Duration                 time.Duration
	CircuitBreakerState      State
	RejectedByCircuitBreaker bool
	SentToDeadLetter         bool
	FallbackExecuted         bool
	Data                     interface{}
	Error                    error
	RetryResult              *retry.Result
}

// IsSuccess 是否成功
func (r *ExecutionResult) IsSuccess() bool {
	return r.Error == nil && !r.RejectedByCircuitBreaker
}
