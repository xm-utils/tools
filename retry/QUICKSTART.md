# 非阻塞重试执行器 - 快速开始

## 3分钟快速上手

### 1. 基本使用

```go
package main

import (
    "context"
    "gitlab.novgate.com/xm/pay/internal/common/retry"
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
        }
    }()
    
    // 主线程继续执行,不会被阻塞
}
```

### 2. 自定义重试策略

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
```

### 3. 设置回调

```go
executor.SetCallback(func(result *retry.RetryResult) {
    if result.Success {
        log.Printf("成功: 尝试%d次", result.Attempts)
    } else {
        log.Printf("失败: %v", result.Error)
    }
})

executor.Execute(task)
// 回调会自动触发
```

### 4. 批量处理

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

## 常见场景

### 场景1: API调用重试

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
    executor.SetCallback(func(result *retry.RetryResult) {
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

## 重试策略选择

| 场景 | 推荐策略 | 配置示例 |
|------|---------|---------|
| **API调用** | 指数退避 | InitialDelay=1s, MaxDelay=60s, Multiplier=2.0 |
| **数据库操作** | 固定间隔 | Interval=2s |
| **消息队列** | 线性退避 | InitialDelay=1s, Increment=2s, MaxDelay=30s |

## 配置建议

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

## 常见问题

### Q1: 如何确保不阻塞主线程?

A: `Execute()` 方法立即返回channel,在后台goroutine中执行:

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

A: 配置RetryableErrors:

```go
var ErrTimeout = errors.New("timeout")
var ErrNetwork = errors.New("network error")

config.RetryableErrors = []error{ErrTimeout, ErrNetwork}
// 其他错误不会重试
```

### Q4: 批量任务如何控制并发?

A: 通过PoolSize控制:

```go
config.PoolSize = 10  // 最多10个任务同时执行
```

### Q5: 如何监控重试情况?

A: 使用回调:

```go
executor.SetCallback(func(result *retry.RetryResult) {
    // 记录指标
    metrics.RecordRetryAttempts(result.Attempts)
    metrics.RecordRetryDuration(result.TotalDuration)
    metrics.RecordRetrySuccess(result.Success)
})
```

## 下一步

- 📖 阅读 [README.md](README.md) 了解完整功能
- 💡 查看 [example.go](example.go) 获取更多示例
- 🧪 运行测试: `go test ./internal/common/retry/...`

---

**开始使用吧!** 🚀
