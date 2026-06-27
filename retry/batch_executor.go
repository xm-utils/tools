package retry

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// BatchRetryConfig 批量重试配置
type BatchRetryConfig struct {
	PoolSize         int                        // 协程池大小
	MaxRetries       int                        // 最大重试次数
	Strategy         Strategy                   // 重试策略
	Timeout          time.Duration              // 单次超时
	BatchSize        int                        // 批处理大小
	ProgressCallback func(completed, total int) // 进度回调
}

// DefaultBatchRetryConfig 返回默认批量重试配置
func DefaultBatchRetryConfig() *BatchRetryConfig {
	return &BatchRetryConfig{
		PoolSize:   10,
		MaxRetries: 3,
		Strategy: &ExponentialBackoffStrategy{
			InitialDelay: 1 * time.Second,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
		},
		Timeout:   30 * time.Second,
		BatchSize: 100,
	}
}

// BatchTask 批量任务
type BatchTask struct {
	ID   string // 任务ID
	Task Task   // 任务函数
}

// BatchResult 批量重试结果
type BatchResult struct {
	TotalTasks   int                // 总任务数
	SuccessCount int                // 成功数
	FailedCount  int                // 失败数
	Results      map[string]*Result // 每个任务的结果
	Duration     time.Duration      // 总耗时
}

// BatchRetryExecutor 批量重试执行器
type BatchRetryExecutor struct {
	config   *BatchRetryConfig
	log      *logrus.Entry
	poolChan chan struct{} // 信号量控制并发
}

// NewBatchRetryExecutor 创建批量重试执行器
func NewBatchRetryExecutor(config *BatchRetryConfig) *BatchRetryExecutor {
	if config == nil {
		config = DefaultBatchRetryConfig()
	}

	return &BatchRetryExecutor{
		config:   config,
		log:      logrus.WithField("module", "BatchRetryExecutor"),
		poolChan: make(chan struct{}, config.PoolSize),
	}
}

// ExecuteBatch 非阻塞执行批量重试任务
func (e *BatchRetryExecutor) ExecuteBatch(tasks []BatchTask) <-chan *BatchResult {
	resultChan := make(chan *BatchResult, 1)

	go func() {
		defer close(resultChan)

		startTime := time.Now()
		result := &BatchResult{
			TotalTasks: len(tasks),
			Results:    make(map[string]*Result),
		}

		var wg sync.WaitGroup
		var mu sync.Mutex // 保护Results的并发访问

		// 分批处理
		for i := 0; i < len(tasks); i += e.config.BatchSize {
			end := i + e.config.BatchSize
			if end > len(tasks) {
				end = len(tasks)
			}

			batch := tasks[i:end]
			e.log.Infof("处理批次 [%d-%d/%d]", i+1, end, len(tasks))

			// 并发执行当前批次
			for _, task := range batch {
				wg.Add(1)

				// 获取信号量(控制并发)
				e.poolChan <- struct{}{}

				go func(t BatchTask) {
					defer wg.Done()
					defer func() { <-e.poolChan }() // 释放信号量

					// 创建重试执行器
					retryConfig := &Config{
						MaxRetries: e.config.MaxRetries,
						Strategy:   e.config.Strategy,
						Timeout:    e.config.Timeout,
					}
					executor := NewRetryExecutor(retryConfig)

					// 执行重试
					retryResult := executor.ExecuteSync(t.Task, nil)

					// 保存结果
					mu.Lock()
					result.Results[t.ID] = retryResult
					if retryResult.Success {
						result.SuccessCount++
					} else {
						result.FailedCount++
					}
					mu.Unlock()

					// 触发进度回调
					if e.config.ProgressCallback != nil {
						completed := result.SuccessCount + result.FailedCount
						e.config.ProgressCallback(completed, result.TotalTasks)
					}
				}(task)
			}

			// 等待当前批次完成
			wg.Wait()
		}

		result.Duration = time.Since(startTime)
		e.log.Infof("批量重试完成: 总数=%d, 成功=%d, 失败=%d, 耗时=%v",
			result.TotalTasks, result.SuccessCount, result.FailedCount, result.Duration)

		resultChan <- result
	}()

	return resultChan
}

// ExecuteBatchSync 同步执行批量重试(阻塞)
func (e *BatchRetryExecutor) ExecuteBatchSync(tasks []BatchTask) *BatchResult {
	resultChan := e.ExecuteBatch(tasks)
	return <-resultChan
}

// GetStats 获取执行器统计信息
func (e *BatchRetryExecutor) GetStats() map[string]any {
	return map[string]interface{}{
		"pool_size":         e.config.PoolSize,
		"max_retries":       e.config.MaxRetries,
		"batch_size":        e.config.BatchSize,
		"timeout":           e.config.Timeout,
		"active_goroutines": len(e.poolChan),
	}
}
