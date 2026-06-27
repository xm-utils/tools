package retry

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestFixedRetryStrategy 测试固定间隔策略
func TestFixedRetryStrategy(t *testing.T) {
	strategy := &FixedRetryStrategy{
		Interval: 2 * time.Second,
	}

	assert.Equal(t, 2*time.Second, strategy.GetDelay(0))
	assert.Equal(t, 2*time.Second, strategy.GetDelay(1))
	assert.Equal(t, 2*time.Second, strategy.GetDelay(100))
}

// TestExponentialBackoffStrategy 测试指数退避策略
func TestExponentialBackoffStrategy(t *testing.T) {
	strategy := &ExponentialBackoffStrategy{
		InitialDelay: 1 * time.Second,
		MaxDelay:     60 * time.Second,
		Multiplier:   2.0,
	}

	// 延迟序列应该是: 1s, 2s, 4s, 8s, 16s, 32s, 60s(达到上限)
	assert.Equal(t, 1*time.Second, strategy.GetDelay(0))
	assert.Equal(t, 2*time.Second, strategy.GetDelay(1))
	assert.Equal(t, 4*time.Second, strategy.GetDelay(2))
	assert.Equal(t, 8*time.Second, strategy.GetDelay(3))
	assert.Equal(t, 16*time.Second, strategy.GetDelay(4))
	assert.Equal(t, 32*time.Second, strategy.GetDelay(5))
	assert.Equal(t, 60*time.Second, strategy.GetDelay(6)) // 达到最大值
	assert.Equal(t, 60*time.Second, strategy.GetDelay(10))
}

// TestLinearBackoffStrategy 测试线性退避策略
func TestLinearBackoffStrategy(t *testing.T) {
	strategy := &LinearBackoffStrategy{
		InitialDelay: 1 * time.Second,
		Increment:    2 * time.Second,
		MaxDelay:     30 * time.Second,
	}

	// 延迟序列应该是: 1s, 3s, 5s, 7s, 9s...
	assert.Equal(t, 1*time.Second, strategy.GetDelay(0))
	assert.Equal(t, 3*time.Second, strategy.GetDelay(1))
	assert.Equal(t, 5*time.Second, strategy.GetDelay(2))
	assert.Equal(t, 7*time.Second, strategy.GetDelay(3))
	assert.Equal(t, 30*time.Second, strategy.GetDelay(20)) // 达到最大值
}

// TestRetryExecutor_SuccessOnFirstTry 第一次就成功
func TestRetryExecutor_SuccessOnFirstTry(t *testing.T) {
	task := func(ctx context.Context) (interface{}, error) {
		return "success", nil
	}

	executor := NewRetryExecutor(DefaultRetryConfig())
	resultChan := executor.Execute(task, nil)

	result := <-resultChan

	assert.True(t, result.Success)
	assert.Equal(t, "success", result.Data)
	assert.Nil(t, result.Error)
	assert.Equal(t, 1, result.Attempts)
	assert.Greater(t, result.TotalDuration, time.Duration(0))
}

// TestRetryExecutor_SuccessAfterRetries 重试后成功
func TestRetryExecutor_SuccessAfterRetries(t *testing.T) {
	var attemptCount int32

	task := func(ctx context.Context) (interface{}, error) {
		count := atomic.AddInt32(&attemptCount, 1)
		if count <= 2 {
			return nil, errors.New("temporary error")
		}
		return "success", nil
	}

	config := DefaultRetryConfig()
	config.Strategy = &FixedRetryStrategy{Interval: 100 * time.Millisecond} // 快速测试

	executor := NewRetryExecutor(config)
	resultChan := executor.Execute(task, nil)

	result := <-resultChan

	assert.True(t, result.Success)
	assert.Equal(t, "success", result.Data)
	assert.Equal(t, 3, result.Attempts) // 第3次成功
	assert.Len(t, result.Retries, 3)
}

// TestRetryExecutor_AllRetriesFailed 所有重试都失败
func TestRetryExecutor_AllRetriesFailed(t *testing.T) {
	task := func(ctx context.Context) (interface{}, error) {
		return nil, errors.New("persistent error")
	}

	config := DefaultRetryConfig()
	config.MaxRetries = 2
	config.Strategy = &FixedRetryStrategy{Interval: 100 * time.Millisecond}

	executor := NewRetryExecutor(config)
	resultChan := executor.Execute(task)

	result := <-resultChan

	assert.False(t, result.Success)
	assert.Nil(t, result.Data)
	assert.NotNil(t, result.Error)
	assert.Equal(t, 3, result.Attempts) // 初始1次 + 重试2次
}

// TestRetryExecutor_WithTimeout 超时控制
func TestRetryExecutor_WithTimeout(t *testing.T) {
	task := func(ctx context.Context) (interface{}, error) {
		time.Sleep(200 * time.Millisecond) // 超过超时时间
		return "success", nil
	}

	config := DefaultRetryConfig()
	config.Timeout = 100 * time.Millisecond
	config.MaxRetries = 1
	config.Strategy = &FixedRetryStrategy{Interval: 100 * time.Millisecond}

	executor := NewRetryExecutor(config)
	resultChan := executor.Execute(task)

	result := <-resultChan

	assert.False(t, result.Success)
	assert.NotNil(t, result.Error)
	assert.Contains(t, result.Error.Error(), "context deadline exceeded")
}

// TestRetryExecutor_WithContextCancellation 上下文取消
func TestRetryExecutor_WithContextCancellation(t *testing.T) {
	task := func(ctx context.Context) (interface{}, error) {
		time.Sleep(1 * time.Second)
		return "success", nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	config := DefaultRetryConfig()
	config.Context = ctx
	config.MaxRetries = 5
	config.Strategy = &FixedRetryStrategy{Interval: 200 * time.Millisecond}

	executor := NewRetryExecutor(config)
	resultChan := executor.Execute(task)

	// 100ms后取消
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	result := <-resultChan

	assert.False(t, result.Success)
	assert.Equal(t, context.Canceled, result.Error)
}

// TestRetryExecutor_WithErrorFiltering 错误过滤
func TestRetryExecutor_WithErrorFiltering(t *testing.T) {
	var ErrRetryable = errors.New("retryable error")
	var ErrNonRetryable = errors.New("non-retryable error")

	// 可重试错误
	task1 := func(ctx context.Context) (interface{}, error) {
		return nil, ErrRetryable
	}

	config := DefaultRetryConfig()
	config.MaxRetries = 2
	config.RetryableErrors = []error{ErrRetryable}
	config.Strategy = &FixedRetryStrategy{Interval: 100 * time.Millisecond}

	executor := NewRetryExecutor(config)
	result := executor.ExecuteSync(task1)

	assert.False(t, result.Success)
	assert.Equal(t, 3, result.Attempts) // 会重试

	// 不可重试错误
	task2 := func(ctx context.Context) (interface{}, error) {
		return nil, ErrNonRetryable
	}

	result = executor.ExecuteSync(task2)

	assert.False(t, result.Success)
	assert.Equal(t, 1, result.Attempts) // 不会重试
}

// TestRetryExecutor_Callback 回调测试
func TestRetryExecutor_Callback(t *testing.T) {
	callbackCalled := false

	var attemptCount int32

	task := func(ctx context.Context) (interface{}, error) {
		count := atomic.AddInt32(&attemptCount, 1)
		time.Sleep(100 * time.Millisecond)
		if count <= 4 {
			return nil, errors.New("temporary error")
		}
		return "success", nil
	}

	//&{
	//Success:true
	//Data:success
	//Error:<nil>
	//Attempts:5
	//TotalDuration:909.468542ms
	//Retries:[
	//{Attempt:0 Duration:101.043416ms Error:temporary error Delay:100ms}
	//{Attempt:1 Duration:101.060667ms Error:temporary error Delay:100ms}
	//{Attempt:2 Duration:101.066291ms Error:temporary error Delay:100ms}
	//{Attempt:3 Duration:100.864959ms Error:temporary error Delay:100ms}
	//{Attempt:4 Duration:101.125792ms Error:<nil> Delay:0s}]}

	//task := func(ctx context.Context) (interface{}, error) {
	//	time.Sleep(100 * time.Millisecond)
	//	return "ok", nil
	//}

	config := DefaultRetryConfig()
	config.Strategy = &FixedRetryStrategy{Interval: 100 * time.Millisecond} // 快速测试

	executor := NewRetryExecutor(config)
	executor.SetCallback(func(result *Result, args ...interface{}) {
		callbackCalled = true
		t.Logf("回调结果: %+v", result)
	})

	resultChan := executor.Execute(task)
	// 6. 异步接收结果(不阻塞主线程)
	go func() {
		result := <-resultChan
		t.Logf("收到结果: success=%v, data=%v", result.Success, result.Data)
	}()

	time.Sleep(10000 * time.Millisecond)
	assert.True(t, callbackCalled)
}

// TestBatchRetryExecutor 批量重试测试
func TestBatchRetryExecutor(t *testing.T) {
	// 创建10个任务
	tasks := make([]BatchTask, 10)
	for i := 0; i < 10; i++ {
		taskID := i
		tasks[i] = BatchTask{
			ID: fmt.Sprintf("task_%d", i),
			Task: func(ctx context.Context) (interface{}, error) {
				return fmt.Sprintf("result_%d", taskID), nil
			},
		}
	}

	config := DefaultBatchRetryConfig()
	config.PoolSize = 3
	config.BatchSize = 5

	executor := NewBatchRetryExecutor(config)
	resultChan := executor.ExecuteBatch(tasks)

	result := <-resultChan

	assert.Equal(t, 10, result.TotalTasks)
	assert.Equal(t, 10, result.SuccessCount)
	assert.Equal(t, 0, result.FailedCount)
	assert.Greater(t, result.Duration, time.Duration(0))
	assert.Len(t, result.Results, 10)
}

// TestBatchRetryExecutor_ProgressCallback 进度回调测试
func TestBatchRetryExecutor_ProgressCallback(t *testing.T) {
	progressCalls := 0

	tasks := make([]BatchTask, 5)
	for i := 0; i < 5; i++ {
		tasks[i] = BatchTask{
			ID: fmt.Sprintf("task_%d", i),
			Task: func(ctx context.Context) (interface{}, error) {
				return "ok", nil
			},
		}
	}

	config := DefaultBatchRetryConfig()
	config.PoolSize = 2
	config.ProgressCallback = func(completed, total int) {
		progressCalls++
	}

	executor := NewBatchRetryExecutor(config)
	resultChan := executor.ExecuteBatch(tasks)
	<-resultChan

	assert.Greater(t, progressCalls, 0)
}

// BenchmarkRetryExecutor 性能测试
func BenchmarkRetryExecutor(b *testing.B) {
	task := func(ctx context.Context) (interface{}, error) {
		return "ok", nil
	}

	config := DefaultRetryConfig()
	config.MaxRetries = 0 // 不重试,测试基本开销
	executor := NewRetryExecutor(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resultChan := executor.Execute(task)
		<-resultChan
	}
}

// BenchmarkBatchRetryExecutor 批量性能测试
func BenchmarkBatchRetryExecutor(b *testing.B) {
	tasks := make([]BatchTask, 100)
	for i := 0; i < 100; i++ {
		tasks[i] = BatchTask{
			ID: fmt.Sprintf("task_%d", i),
			Task: func(ctx context.Context) (interface{}, error) {
				return "ok", nil
			},
		}
	}

	config := DefaultBatchRetryConfig()
	config.PoolSize = 10
	executor := NewBatchRetryExecutor(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resultChan := executor.ExecuteBatch(tasks)
		<-resultChan
	}
}
