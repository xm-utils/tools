package retry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// ExampleBasicUsage 基本使用示例
func ExampleBasicUsage() {
	// 1. 定义任务
	task := func(ctx context.Context) (interface{}, error) {
		// 模拟网络请求
		logrus.Info("执行任务...")

		// 模拟失败(前2次失败,第3次成功)
		if time.Now().Unix()%3 != 0 {
			return nil, errors.New("临时错误")
		}

		return "成功结果", nil
	}

	// 2. 创建配置
	config := DefaultRetryConfig()
	config.MaxRetries = 3

	// 3. 创建执行器
	executor := NewRetryExecutor(config)

	// 4. 设置回调
	executor.SetCallback(func(result *Result, arg ...interface{}) {
		if result.Success {
			logrus.Infof("任务成功: 尝试%d次, 耗时%v", result.Attempts, result.TotalDuration)
		} else {
			logrus.Errorf("任务失败: 尝试%d次, 错误=%v", result.Attempts, result.Error)
		}
	})

	// 5. 非阻塞执行
	resultChan := executor.Execute(task, nil)

	// 6. 异步接收结果(不阻塞主线程)
	go func() {
		result := <-resultChan
		logrus.Infof("收到结果: success=%v, data=%v", result.Success, result.Data)
	}()

	// 主线程继续执行其他任务...
	logrus.Info("主线程继续执行...")
}

// ExampleDifferentStrategies 不同重试策略示例
func ExampleDifferentStrategies() {
	task := func(ctx context.Context) (interface{}, error) {
		return "ok", nil
	}

	// 策略1: 固定间隔重试
	fixedConfig := DefaultRetryConfig()
	fixedConfig.Strategy = &FixedRetryStrategy{
		Interval: 2 * time.Second, // 每次等待2秒
	}
	executor1 := NewRetryExecutor(fixedConfig)
	executor1.Execute(task, nil)

	// 策略2: 指数退避重试(推荐)
	expConfig := DefaultRetryConfig()
	expConfig.Strategy = &ExponentialBackoffStrategy{
		InitialDelay: 1 * time.Second,  // 初始1秒
		MaxDelay:     60 * time.Second, // 最大60秒
		Multiplier:   2.0,              // 每次翻倍
	}
	// 延迟序列: 1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s...
	executor2 := NewRetryExecutor(expConfig)
	executor2.Execute(task, nil)

	// 策略3: 线性退避重试
	linearConfig := DefaultRetryConfig()
	linearConfig.Strategy = &LinearBackoffStrategy{
		InitialDelay: 1 * time.Second,  // 初始1秒
		Increment:    2 * time.Second,  // 每次增加2秒
		MaxDelay:     30 * time.Second, // 最大30秒
	}
	// 延迟序列: 1s, 3s, 5s, 7s, 9s, 11s...
	executor3 := NewRetryExecutor(linearConfig)
	executor3.Execute(task, nil)
}

// ExampleWithErrorFiltering 带错误过滤的重试示例
func ExampleWithErrorFiltering() {
	// 定义可重试和不可重试的错误
	var ErrTimeout = errors.New("timeout")
	var ErrNetwork = errors.New("network error")
	// 不可重试

	task := func(ctx context.Context) (interface{}, error) {
		// 模拟业务逻辑
		return nil, ErrTimeout
	}

	config := DefaultRetryConfig()
	config.RetryableErrors = []error{ErrTimeout, ErrNetwork} // 只重试这些错误

	executor := NewRetryExecutor(config)
	resultChan := executor.Execute(task, nil)

	go func() {
		result := <-resultChan
		if !result.Success {
			// 如果是不可重试的错误,会立即返回,不会重试
			logrus.Errorf("失败原因: %v", result.Error)
		}
	}()
}

// ExampleWithTimeout 带超时控制的示例
func ExampleWithTimeout() {
	task := func(ctx context.Context) (interface{}, error) {
		// 模拟长时间运行的任务
		select {
		case <-time.After(5 * time.Second):
			return "完成", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	config := DefaultRetryConfig()
	config.Timeout = 3 * time.Second // 单次执行超时3秒

	executor := NewRetryExecutor(config)
	resultChan := executor.Execute(task, nil)

	go func() {
		result := <-resultChan
		if !result.Success {
			logrus.Warnf("任务超时: %v", result.Error)
		}
	}()
}

// ExampleWithContextCancellation 上下文取消示例
func ExampleWithContextCancellation() {
	task := func(ctx context.Context) (interface{}, error) {
		// 长时间运行的任务
		time.Sleep(10 * time.Second)
		return "完成", nil
	}

	// 创建可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())

	config := DefaultRetryConfig()
	config.Context = ctx

	executor := NewRetryExecutor(config)
	resultChan := executor.Execute(task, nil)

	// 5秒后取消
	go func() {
		time.Sleep(5 * time.Second)
		cancel() // 取消所有正在执行的任务
		logrus.Info("已取消任务")
	}()

	go func() {
		result := <-resultChan
		if result.Error == context.Canceled {
			logrus.Info("任务已被取消")
		}
	}()
}

// ExampleBatchRetry 批量重试示例
func ExampleBatchRetry() {
	// 创建100个任务
	tasks := make([]BatchTask, 100)
	for i := 0; i < 100; i++ {
		taskID := fmt.Sprintf("task_%d", i)
		tasks[i] = BatchTask{
			ID: taskID,
			Task: func(ctx context.Context) (interface{}, error) {
				// 模拟API调用
				logrus.Infof("执行任务: %s", taskID)
				return fmt.Sprintf("result_%s", taskID), nil
			},
		}
	}

	// 创建批量执行器
	config := DefaultBatchRetryConfig()
	config.PoolSize = 10 // 最多10个并发
	config.MaxRetries = 3
	config.BatchSize = 20 // 每批20个任务

	executor := NewBatchRetryExecutor(config)

	// 设置进度回调
	config.ProgressCallback = func(completed, total int) {
		progress := float64(completed) / float64(total) * 100
		logrus.Infof("进度: %.2f%% (%d/%d)", progress, completed, total)
	}

	// 非阻塞执行
	resultChan := executor.ExecuteBatch(tasks)

	// 异步接收结果
	go func() {
		result := <-resultChan
		logrus.Infof("批量重试完成:")
		logrus.Infof("  总数: %d", result.TotalTasks)
		logrus.Infof("  成功: %d", result.SuccessCount)
		logrus.Infof("  失败: %d", result.FailedCount)
		logrus.Infof("  耗时: %v", result.Duration)

		// 查看失败的任务
		for id, retryResult := range result.Results {
			if !retryResult.Success {
				logrus.Errorf("任务失败: %s, 错误: %v", id, retryResult.Error)
			}
		}
	}()

	// 主线程继续执行...
	logrus.Info("批量任务已在后台执行...")
}

// ExampleIntegrationWithDeadLetter 与死信队列集成示例
func ExampleIntegrationWithDeadLetter() {
	/*
		import "gitlab.novgate.com/xm/pay/internal/common/deadletter"

		type CallbackService struct {
			dlqManager *deadletter.DeadLetterQueueManager
		}

		func (s *CallbackService) HandleCallback(callback *OrderCallback) {
			messageData, _ := json.Marshal(callback)

			// 创建重试任务
			task := func(ctx context.Context) (interface{}, error) {
				// 尝试处理回调
				return nil, processCallback(callback)
			}

			// 创建重试执行器
			config := retry.DefaultRetryConfig()
			config.MaxRetries = 3
			config.Strategy = &retry.ExponentialBackoffStrategy{
				InitialDelay: 1 * time.Second,
				MaxDelay:     30 * time.Second,
				Multiplier:   2.0,
			}

			executor := retry.NewRetryExecutor(config)

			// 设置回调: 重试失败后推入死信队列
			executor.SetCallback(func(result *retry.Result) {
				if !result.Success {
					// 所有重试都失败,推入死信队列
					s.dlqManager.PushToDeadLetter(
						callback.OrderNo,
						string(messageData),
						result.Error.Error(),
						result.Attempts - 1,
					)
					logrus.Warnf("回调处理失败,已移入死信队列: orderNo=%s", callback.OrderNo)
				} else {
					logrus.Infof("回调处理成功: orderNo=%s", callback.OrderNo)
				}
			})

			// 非阻塞执行
			executor.Execute(task)

			// 函数立即返回,不阻塞调用方
		}
	*/
}

// ExampleIntegrationWithStreamConsumer 与Stream消费者集成示例
func ExampleIntegrationWithStreamConsumer() {
	/*
		type StreamConsumer struct {
			retryExecutor *retry.Executor
		}

		func (c *StreamConsumer) processMessage(msg *StreamMessage) {
			// 创建重试任务
			task := func(ctx context.Context) (interface{}, error) {
				return nil, c.handler(ctx, msg)
			}

			// 非阻塞重试
			c.retryExecutor.Execute(task)

			// 立即确认消息,不阻塞消费者
			redis.XAck(ctx, streamKey, group, msg.MessageID)
		}

		func NewStreamConsumer() *StreamConsumer {
			config := retry.DefaultRetryConfig()
			config.MaxRetries = 5

			return &StreamConsumer{
				retryExecutor: retry.NewRetryExecutor(config),
			}
		}
	*/
}

// ExampleMonitoring 监控示例
func ExampleMonitoring() {
	task := func(ctx context.Context) (interface{}, error) {
		return "ok", nil
	}

	config := DefaultRetryConfig()
	executor := NewRetryExecutor(config)

	// 设置详细回调用于监控
	executor.SetCallback(func(result *Result, arg ...interface{}) {
		// 记录指标
		logrus.Infof("重试统计:")
		logrus.Infof("  成功: %v", result.Success)
		logrus.Infof("  尝试次数: %d", result.Attempts)
		logrus.Infof("  总耗时: %v", result.TotalDuration)

		// 记录每次重试的详细信息
		for i, attempt := range result.Retries {
			logrus.Infof("  重试[%d]: 耗时=%v, 延迟=%v, 错误=%v",
				i, attempt.Duration, attempt.Delay, attempt.Error)
		}

		// TODO: 上报到监控系统
		// metrics.RecordRetryDuration(result.TotalDuration)
		// metrics.RecordRetryAttempts(result.Attempts)
		// metrics.RecordRetrySuccess(result.Success)
	})

	executor.Execute(task, nil)
}

// ExampleSyncExecution 同步执行示例(阻塞)
func ExampleSyncExecution() {
	task := func(ctx context.Context) (interface{}, error) {
		return "同步结果", nil
	}

	config := DefaultRetryConfig()
	executor := NewRetryExecutor(config)

	// 同步执行(阻塞等待结果)
	result := executor.ExecuteSync(task, nil)

	if result.Success {
		logrus.Infof("成功: %v", result.Data)
	} else {
		logrus.Errorf("失败: %v", result.Error)
	}
}
