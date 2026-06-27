package retry

import (
	"context"
	"fmt"
	"time"
)

// Result 重试结果
type Result struct {
	Success       bool          // 是否成功
	Data          any           // 返回数据
	Error         error         // 错误信息
	Attempts      int           // 总尝试次数
	TotalDuration time.Duration // 总耗时
	Retries       []Attempt     // 每次重试的详细信息
}

// Attempt 单次重试尝试
type Attempt struct {
	Attempt  int           // 尝试次数(从0开始)
	Duration time.Duration // 本次耗时
	Error    error         // 错误信息
	Delay    time.Duration // 重试前的延迟时间
}

// Task 重试任务接口
type Task func(ctx context.Context) (any, error)

// Callback 重试完成回调
type Callback func(result *Result, arg ...any)

// Executor 重试执行器
type Executor struct {
	config   *Config
	callback Callback
}

// NewRetryExecutor 创建重试执行器
func NewRetryExecutor(config *Config) *Executor {
	if config == nil {
		config = DefaultRetryConfig()
	}
	if config.Strategy == nil {
		config.Strategy = &ExponentialBackoffStrategy{
			InitialDelay: 1 * time.Second,
			MaxDelay:     60 * time.Second,
			Multiplier:   2.0,
		}
	}

	return &Executor{
		config: config,
	}
}

// SetCallback 设置重试完成回调
func (e *Executor) SetCallback(callback Callback) {
	e.callback = callback
}

// Execute 非阻塞执行重试任务
func (e *Executor) Execute(task Task, arg ...any) <-chan *Result {
	resultChan := make(chan *Result, 1)

	go func() {
		defer close(resultChan)

		result := e.executeWithRetry(task)
		resultChan <- result

		// 触发回调
		if e.callback != nil {
			e.callback(result, arg...)
		}
	}()

	return resultChan
}

// executeWithRetry 执行重试逻辑
func (e *Executor) executeWithRetry(task Task) *Result {
	startTime := time.Now()
	result := &Result{
		Attempts: 0,
		Retries:  make([]Attempt, 0),
	}

	// 使用配置的上下文或创建新的上下文
	ctx := e.config.Context
	if ctx == nil {
		ctx = context.Background()
	}

	for attempt := 0; attempt <= e.config.MaxRetries; attempt++ {
		result.Attempts = attempt + 1

		// 检查上下文是否已取消
		select {
		case <-ctx.Done():
			result.Error = ctx.Err()
			result.TotalDuration = time.Since(startTime)
			return result
		default:
		}

		// 执行任务(带超时控制)
		attemptStart := time.Now()
		taskCtx, cancel := context.WithTimeout(ctx, e.config.Timeout)
		data, err := task(taskCtx)
		cancel()
		attemptDuration := time.Since(attemptStart)

		// 记录重试信息
		retryAttempt := Attempt{
			Attempt:  attempt,
			Duration: attemptDuration,
			Error:    err,
		}

		// 计算下次重试延迟
		if attempt < e.config.MaxRetries && err != nil {
			retryAttempt.Delay = e.config.Strategy.GetDelay(attempt)
		}

		result.Retries = append(result.Retries, retryAttempt)

		// 成功则返回
		if err == nil {
			result.Success = true
			result.Data = data
			result.Error = nil
			result.TotalDuration = time.Since(startTime)
			return result
		}

		// 检查是否是可重试的错误
		if !e.isRetryableError(err) {
			result.Error = err
			result.TotalDuration = time.Since(startTime)
			return result
		}

		// 如果还有重试机会,等待后继续
		if attempt < e.config.MaxRetries {
			delay := e.config.Strategy.GetDelay(attempt)
			select {
			case <-ctx.Done():
				result.Error = ctx.Err()
				result.TotalDuration = time.Since(startTime)
				return result
			case <-time.After(delay):
				// 继续重试
			}
		}
	}

	// 所有重试都失败
	result.Error = fmt.Errorf("重试%d次后仍然失败: %w", e.config.MaxRetries+1, result.Retries[len(result.Retries)-1].Error)
	result.TotalDuration = time.Since(startTime)
	return result
}

// isRetryableError 检查错误是否可重试
func (e *Executor) isRetryableError(err error) bool {
	// 如果没有指定可重试错误列表,则全部重试
	if len(e.config.RetryableErrors) == 0 {
		return true
	}

	// 检查错误是否在可重试列表中
	for _, retryableErr := range e.config.RetryableErrors {
		if err == retryableErr {
			return true
		}
	}

	return false
}

// ExecuteSync 同步执行重试任务(阻塞)
func (e *Executor) ExecuteSync(task Task, arg ...any) *Result {
	resultChan := e.Execute(task, arg)
	return <-resultChan
}
