# Retry - 快速开始指南

## 🚀 3分钟快速上手

### 1. 基本使用(异步非阻塞)

```go
package main

import (
    "context"
    "github.com/xm-utils/tools/retry"
)

func main() {
    // 定义任务
    task := func(ctx context.Context) (interface{}, error) {
        // 你的业务逻辑
        return fetchData()
    }
    
    // 创建执行器并执行(非阻塞)
    executor := retry.NewRetryExecutor(retry.DefaultRetryConfig())
    resultChan := executor.Execute(task)
    
    // 异步接收结果
    go func() {
        result := <-resultChan
        if result.Success {
            println("成功:", result.Data)
        } else {
            println("失败:", result.Error)
        }
    }()
    
    // 主线程继续执行,不会被阻塞
}
```

### 2. 同步执行(阻塞等待)

```go
// 如果需要同步等待结果
executor := retry.NewRetryExecutor(retry.DefaultRetryConfig())
result := executor.ExecuteSync(task)

if result.Success {
    println("成功:", result.Data)
} else {
    println("失败:", result.Error)
}
```

### 3. 自定义重试策略

```go
// 指数退避(推荐用于网络请求)
config := retry.DefaultRetryConfig()
config.Strategy = &retry.ExponentialBackoffStrategy{
    InitialDelay: 1 * time.Second,
    MaxDelay:     60 * time.Second,
    Multiplier:   2.0,
}

executor := retry.NewRetryExecutor(config)
executor.Execute(task)
// 延迟序列: 1s, 2s, 4s, 8s, 16s, 32s, 60s...
```

### 4. 设置回调

```go
executor.SetCallback(func(result *retry.Result) {
    if result.Success {
        log.Printf("成功: 尝试%d次, 耗时%v", 
            result.Attempts, result.TotalDuration)
    } else {
        log.Printf("失败: %v, 尝试%d次", 
            result.Error, result.Attempts)
    }
})

executor.Execute(task)
// 回调会自动触发
```

### 5. 批量处理

```go
// 创建任务列表
tasks := []retry.BatchTask{
    {ID: "task1", Task: task1},
    {ID: "task2", Task: task2},
    // ...
}

// 批量执行(非阻塞)
config := retry.DefaultBatchRetryConfig()
config.PoolSize = 10  // 最多10个并发

executor := retry.NewBatchRetryExecutor(config)
resultChan := executor.ExecuteBatch(tasks)

go func() {
    result := <-resultChan
    log.Printf("完成: 成功=%d, 失败=%d", 
        result.SuccessCount, result.FailedCount)
}()
```

## 💼 常见场景

### 场景1: API 调用重试

```go
task := func(ctx context.Context) (interface{}, error) {
    resp, err := http.Get("https://api.example.com/data")
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()
    
    var data interface{}
    json.NewDecoder(resp.Body).Decode(&data)
    return data, nil
}

config := retry.DefaultRetryConfig()
config.MaxRetries = 3
config.Timeout = 10 * time.Second

executor := retry.NewRetryExecutor(config)
resultChan := executor.Execute(task)

go func() {
    result := <-resultChan
    if result.Success {
        processData(result.Data)
    }
}()
```

### 场景2: 数据库操作重试

```go
task := func(ctx context.Context) (interface{}, error) {
    _, err := db.ExecContext(ctx, 
        "UPDATE orders SET status=? WHERE id=?", 
        status, orderID)
    return nil, err
}

var ErrDeadlock = errors.New("deadlock detected")

config := retry.DefaultRetryConfig()
config.RetryableErrors = []error{ErrDeadlock}

executor := retry.NewRetryExecutor(config)
executor.Execute(task)
```

### 场景3: 与死信队列集成

```go
func HandleOrderCallback(callback *OrderCallback) {
    messageData, _ := json.Marshal(callback)
    
    task := func(ctx context.Context) (interface{}, error) {
        return nil, processCallback(callback)
    }
    
    config := retry.DefaultRetryConfig()
    config.MaxRetries = 3
    
    executor := retry.NewRetryExecutor(config)
    
    // 重试失败后推入死信队列
    executor.SetCallback(func(result *retry.Result) {
        if !result.Success {
            dlqManager.PushToDeadLetter(
                callback.OrderNo,
                string(messageData),
                result.Error.Error(),
                result.Attempts - 1,
            )
        }
    })
    
    // 非阻塞执行
    executor.Execute(task)
}
```

### 场景4: 批量发送通知

```go
// 准备1000个通知任务
tasks := make([]retry.BatchTask, 1000)
for i, notification := range notifications {
    notif := notification // 捕获变量
    tasks[i] = retry.BatchTask{
        ID: fmt.Sprintf("notif_%d", i),
        Task: func(ctx context.Context) (interface{}, error) {
            return nil, sendNotification(notif)
        },
    }
}

// 批量执行
config := retry.DefaultBatchRetryConfig()
config.PoolSize = 20      // 20个并发
config.BatchSize = 50     // 每批50个

executor := retry.NewBatchRetryExecutor(config)

// 进度监控
config.ProgressCallback = func(completed, total int) {
    progress := float64(completed) / float64(total) * 100
    log.Printf("发送进度: %.2f%%", progress)
}

resultChan := executor.ExecuteBatch(tasks)

go func() {
    result := <-resultChan
    log.Printf("发送完成: 成功=%d, 失败=%d", 
        result.SuccessCount, result.FailedCount)
    
    // 处理失败的通知
    for id, retryResult := range result.Results {
        if !retryResult.Success {
            log.Printf("通知失败: %s, 错误: %v", id, retryResult.Error)
        }
    }
}()
```

### 场景5: 超时控制

```go
task := func(ctx context.Context) (interface{}, error) {
    // 模拟长时间运行的任务
    select {
    case <-time.After(5 * time.Second):
        return "完成", nil
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}

config := retry.DefaultRetryConfig()
config.Timeout = 3 * time.Second  // 单次执行超时3秒

executor := retry.NewRetryExecutor(config)
resultChan := executor.Execute(task)

go func() {
    result := <-resultChan
    if !result.Success {
        log.Printf("任务超时: %v", result.Error)
    }
}()
```

### 场景6: 上下文取消

```go
// 创建可取消的上下文
ctx, cancel := context.WithCancel(context.Background())

config := retry.DefaultRetryConfig()
config.Context = ctx

executor := retry.NewRetryExecutor(config)
resultChan := executor.Execute(task)

// 5秒后取消
go func() {
    time.Sleep(5 * time.Second)
    cancel()  // 取消所有正在执行的任务
}()

go func() {
    result := <-resultChan
    if result.Error == context.Canceled {
        log.Println("任务已被取消")
    }
}()
```

## 📊 重试策略选择

| 场景 | 推荐策略 | 配置示例 |
|------|---------|---------|
| **API调用** | 指数退避 | InitialDelay=1s, MaxDelay=60s, Multiplier=2.0 |
| **数据库操作** | 固定间隔 | Interval=2s |
| **消息队列** | 线性退避 | InitialDelay=1s, Increment=2s, MaxDelay=30s |

## ⚙️ 配置建议

### 网络请求

```go
config := retry.DefaultRetryConfig()
config.MaxRetries = 3
config.Timeout = 10 * time.Second
config.Strategy = &retry.ExponentialBackoffStrategy{
    InitialDelay: 1 * time.Second,
    MaxDelay:     60 * time.Second,
    Multiplier:   2.0,
}
```

### 数据库操作

```go
config := retry.DefaultRetryConfig()
config.MaxRetries = 2
config.Timeout = 5 * time.Second
config.Strategy = &retry.FixedRetryStrategy{
    Interval: 2 * time.Second,
}
```

### 批量任务

```go
config := retry.DefaultBatchRetryConfig()
config.PoolSize = 10       // 根据系统负载调整
config.MaxRetries = 3
config.BatchSize = 100     // 避免一次性创建过多goroutine
config.Timeout = 30 * time.Second
```

## ❓ 常见问题

### Q1: 如何确保不阻塞主线程?

A: `Execute()` 方法立即返回 channel，在后台 goroutine 中执行:

```go
resultChan := executor.Execute(task)  // 立即返回,不阻塞

// 可以选择不等待结果(完全异步)
// 或者在另一个goroutine中等待
go func() {
    result := <-resultChan
    // 处理结果
}()
```

### Q2: 如何取消正在执行的重试?

A: 使用上下文:

```go
ctx, cancel := context.WithCancel(context.Background())
config.Context = ctx

executor := retry.NewRetryExecutor(config)
executor.Execute(task)

// 需要时取消
cancel()
```

### Q3: 如何只重试特定错误?

A: 配置 RetryableErrors:

```go
var ErrTimeout = errors.New("timeout")
var ErrNetwork = errors.New("network error")

config.RetryableErrors = []error{ErrTimeout, ErrNetwork}
// 其他错误不会重试
```

### Q4: 批量任务如何控制并发?

A: 通过 PoolSize 控制:

```go
config.PoolSize = 10  // 最多10个任务同时执行
```

### Q5: 如何监控重试情况?

A: 使用回调:

```go
executor.SetCallback(func(result *retry.Result) {
    // 记录指标
    metrics.RecordRetryAttempts(result.Attempts)
    metrics.RecordRetryDuration(result.TotalDuration)
    metrics.RecordRetrySuccess(result.Success)
    
    // 详细日志
    for i, attempt := range result.Retries {
        log.Printf("重试[%d]: 耗时=%v, 延迟=%v, 错误=%v",
            i, attempt.Duration, attempt.Delay, attempt.Error)
    }
})
```

### Q6: Execute 和 ExecuteSync 有什么区别?

A: 
- **Execute**: 异步非阻塞，立即返回 channel
- **ExecuteSync**: 同步阻塞，等待完成后返回结果

```go
// 异步
resultChan := executor.Execute(task)
// 主线程继续执行...

// 同步
result := executor.ExecuteSync(task)
// 等待完成后才继续执行
```

### Q7: 如何查看每次重试的详细信息?

A: 通过 Result.Retries:

```go
executor.SetCallback(func(result *retry.Result) {
    for i, attempt := range result.Retries {
        log.Printf("第%d次尝试:", i+1)
        log.Printf("  耗时: %v", attempt.Duration)
        log.Printf("  延迟: %v", attempt.Delay)
        log.Printf("  错误: %v", attempt.Error)
    }
})
```

### Q8: 批量任务如何处理失败的任务?

A: 检查 BatchResult.Results:

```go
result := <-resultChan
for id, retryResult := range result.Results {
    if !retryResult.Success {
        log.Printf("任务失败: %s", id)
        log.Printf("错误: %v", retryResult.Error)
        log.Printf("尝试次数: %d", retryResult.Attempts)
        
        // 处理失败的任务
        handleFailedTask(id, retryResult)
    }
}
```

## 🎯 最佳实践清单

### ✅ 必须做

- [ ] 确保任务幂等性
- [ ] 设置合理的超时时间
- [ ] 区分可重试和不可重试错误
- [ ] 添加监控和告警
- [ ] 使用指数退避策略(网络请求)

### ✅ 推荐做

- [ ] 设置重试完成回调
- [ ] 批量任务控制并发数
- [ ] 记录每次重试详情
- [ ] 使用上下文支持取消
- [ ] 定期清理失败任务

### ❌ 不要做

- [ ] 不要设置过大的重试次数(建议3-5次)
- [ ] 不要在重试中执行不可逆操作
- [ ] 不要忽略错误分类
- [ ] 不要忘记关闭资源
- [ ] 不要在生产环境使用过短的延迟

## 📚 下一步

- 📖 阅读 [README.md](README.md) 了解完整功能
- 💡 查看 [example.go](example.go) 获取更多示例
- 🧪 运行测试: `go test ./retry/...`
- 🔧 根据业务需求调整配置

## 🆘 技术支持

如有问题，请联系开发团队或提交 Issue。
