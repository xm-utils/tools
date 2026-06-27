# 非阻塞重试策略执行器

基于Go协程的非阻塞重试执行器,支持多种重试策略、批量并发处理、完善的监控和回调机制。

## ✨ 核心特性

✅ **非阻塞执行** - 基于goroutine异步执行,不阻塞主线程  
✅ **多种重试策略** - 固定间隔、指数退避、线性退避  
✅ **批量并发处理** - 支持协程池控制并发,分批处理大量任务  
✅ **灵活配置** - 超时控制、错误过滤、上下文取消  
✅ **完善回调** - 支持重试完成回调、进度回调  
✅ **详细监控** - 记录每次重试的耗时、延迟、错误信息  

## 📦 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                    RetryExecutor                             │
│                                                             │
│  Execute(task) -> chan Result  (非阻塞,返回channel)         │
│  └─> goroutine                                              │
│       ├─> 执行任务(带超时)                                   │
│       ├─> 失败? 计算延迟                                     │
│       ├─> 等待延迟时间                                       │
│       └─> 重试或返回结果                                     │
│                                                             │
│  重试策略:                                                   │
│  ├─ FixedRetryStrategy        (固定间隔)                     │
│  ├─ ExponentialBackoffStrategy (指数退避,推荐)               │
│  └─ LinearBackoffStrategy     (线性退避)                     │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                  BatchRetryExecutor                          │
│                                                             │
│  ExecuteBatch(tasks) -> chan BatchResult  (非阻塞)          │
│  └─> goroutine                                              │
│       ├─> 分批处理(每批100个)                                │
│       └─> 并发执行(协程池控制)                               │
│            ├─> PoolSize=10 (最多10个并发)                    │
│            ├─> 信号量控制并发                                │
│            └─> 进度回调                                      │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 快速开始

### 基本使用

```go
package main

import (
    "context"
    "gitlab.novgate.com/xm/pay/internal/common/retry"
)

func main() {
    // 1. 定义任务
    task := func(ctx context.Context) (interface{}, error) {
        // 执行你的业务逻辑
        return fetchDataFromAPI()
    }
    
    // 2. 创建执行器(使用默认配置)
    executor := retry.NewRetryExecutor(retry.DefaultRetryConfig())
    
    // 3. 非阻塞执行
    resultChan := executor.Execute(task)
    
    // 4. 异步接收结果(不阻塞)
    go func() {
        result := <-resultChan
        if result.Success {
            println("成功:", result.Data)
        } else {
            println("失败:", result.Error)
        }
    }()
    
    // 主线程继续执行其他任务...
}
```

### 自定义重试策略

```go
// 指数退避策略(推荐)
config := retry.DefaultRetryConfig()
config.Strategy = &retry.ExponentialBackoffStrategy{
    InitialDelay: 1 * time.Second,  // 初始延迟1秒
    MaxDelay:     60 * time.Second,  // 最大延迟60秒
    Multiplier:   2.0,               // 每次翻倍
}
// 延迟序列: 1s, 2s, 4s, 8s, 16s, 32s, 60s...

executor := retry.NewRetryExecutor(config)
executor.Execute(task)
```

### 设置回调

```go
executor.SetCallback(func(result *retry.RetryResult) {
    if result.Success {
        log.Printf("任务成功: 尝试%d次, 耗时%v", 
            result.Attempts, result.TotalDuration)
    } else {
        log.Printf("任务失败: 尝试%d次, 错误=%v", 
            result.Attempts, result.Error)
    }
})

executor.Execute(task)
// 回调会在重试完成后自动触发
```

## 📊 重试策略对比

| 策略 | 适用场景 | 延迟序列示例 |
|------|---------|-------------|
| **FixedRetryStrategy** | 简单场景,固定间隔 | 2s, 2s, 2s, 2s... |
| **ExponentialBackoffStrategy** ⭐ | 网络请求,API调用(推荐) | 1s, 2s, 4s, 8s, 16s... |
| **LinearBackoffStrategy** | 需要线性增长的场景 | 1s, 3s, 5s, 7s, 9s... |

## 💡 使用场景

### 场景1: API调用重试

```go
task := func(ctx context.Context) (interface{}, error) {
    resp, err := http.Get("https://api.example.com/data")
    if err != nil {
        return nil, err
    }
    return resp.Body, nil
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
    return nil, db.ExecContext(ctx, "UPDATE orders SET status=? WHERE id=?", 
        status, orderID)
}

// 只重试特定错误
var ErrDeadlock = errors.New("deadlock")
config := retry.DefaultRetryConfig()
config.RetryableErrors = []error{ErrDeadlock}

executor := retry.NewRetryExecutor(config)
executor.Execute(task)
```

### 场景3: 与死信队列集成

```go
func HandleCallback(callback *OrderCallback) {
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

### 场景4: 批量处理任务

```go
// 创建1000个任务
tasks := make([]retry.BatchTask, 1000)
for i := 0; i < 1000; i++ {
    tasks[i] = retry.BatchTask{
        ID: fmt.Sprintf("task_%d", i),
        Task: func(ctx context.Context) (interface{}, error) {
            return processItem(i)
        },
    }
}

// 创建批量执行器
config := retry.DefaultBatchRetryConfig()
config.PoolSize = 20      // 最多20个并发
config.BatchSize = 50     // 每批50个任务

executor := retry.NewBatchRetryExecutor(config)

// 进度回调
config.ProgressCallback = func(completed, total int) {
    progress := float64(completed) / float64(total) * 100
    log.Printf("进度: %.2f%%", progress)
}

// 非阻塞执行
resultChan := executor.ExecuteBatch(tasks)

go func() {
    result := <-resultChan
    log.Printf("完成: 成功=%d, 失败=%d, 耗时=%v",
        result.SuccessCount, result.FailedCount, result.Duration)
}()
```

## 🔧 配置说明

### RetryConfig (单个任务重试配置)

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| MaxRetries | int | 3 | 最大重试次数 |
| Strategy | RetryStrategy | ExponentialBackoff | 重试策略 |
| Timeout | time.Duration | 30s | 单次执行超时 |
| RetryableErrors | []error | nil(全部重试) | 可重试的错误列表 |
| Context | context.Context | background | 上下文(用于取消) |

### BatchRetryConfig (批量重试配置)

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| PoolSize | int | 10 | 协程池大小(并发数) |
| MaxRetries | int | 3 | 最大重试次数 |
| Strategy | RetryStrategy | ExponentialBackoff | 重试策略 |
| Timeout | time.Duration | 30s | 单次超时 |
| BatchSize | int | 100 | 批处理大小 |
| ProgressCallback | func(int,int) | nil | 进度回调 |

## 📈 监控指标

### RetryResult (重试结果)

```go
type RetryResult struct {
    Success       bool           // 是否成功
    Data          interface{}    // 返回数据
    Error         error          // 错误信息
    Attempts      int            // 总尝试次数
    TotalDuration time.Duration  // 总耗时
    Retries       []RetryAttempt // 每次重试详情
}

type RetryAttempt struct {
    Attempt  int           // 尝试次数
    Duration time.Duration // 本次耗时
    Error    error         // 错误信息
    Delay    time.Duration // 重试前延迟
}
```

### 使用示例

```go
executor.SetCallback(func(result *retry.RetryResult) {
    // 记录指标
    metrics.RecordRetryDuration(result.TotalDuration)
    metrics.RecordRetryAttempts(result.Attempts)
    metrics.RecordRetrySuccess(result.Success)
    
    // 详细日志
    for i, attempt := range result.Retries {
        log.Printf("重试[%d]: 耗时=%v, 延迟=%v, 错误=%v",
            i, attempt.Duration, attempt.Delay, attempt.Error)
    }
})
```

## 🎯 最佳实践

### 1. 选择合适的重试策略

- **网络请求/API调用**: 使用指数退避(ExponentialBackoff)
- **数据库操作**: 使用固定间隔(Fixed)或线性退避(Linear)
- **临时性故障**: 使用指数退避,避免雪崩

### 2. 设置合理的超时时间

```go
config.Timeout = 10 * time.Second  // 根据业务调整
```

### 3. 过滤不可重试的错误

```go
var ErrInvalidData = errors.New("invalid data")

config.RetryableErrors = []error{
    ErrTimeout,
    ErrNetwork,
    // 不包含 ErrInvalidData,因为数据错误重试无意义
}
```

### 4. 使用上下文控制取消

```go
ctx, cancel := context.WithCancel(context.Background())
config.Context = ctx

// 在需要时取消所有重试
cancel()
```

### 5. 批量任务控制并发

```go
config.PoolSize = 10      // 根据系统负载调整
config.BatchSize = 100    // 避免一次性创建过多goroutine
```

### 6. 监控和告警

```go
executor.SetCallback(func(result *retry.RetryResult) {
    if !result.Success {
        // 发送告警
        alertService.SendAlert("重试失败", result.Error)
    }
    
    // 记录指标
    metrics.RecordRetry(result)
})
```

## 🔍 性能考虑

- **单个任务**: 几乎无额外开销,仅增加一个goroutine
- **批量任务**: 通过协程池控制并发,避免资源耗尽
- **内存占用**: 每个任务约1-5KB(取决于返回值大小)
- **建议并发数**: CPU核数 × 2 ~ 4

## ⚠️ 注意事项

1. **幂等性**: 确保任务是幂等的,避免重复执行导致数据不一致
2. **资源泄漏**: 注意关闭连接、释放资源
3. **超时设置**: 合理设置超时,避免长时间阻塞
4. **错误分类**: 区分可重试和不可重试错误
5. **监控告警**: 及时发现问题,避免静默失败

## 🧪 测试

```bash
# 运行测试
go test ./internal/common/retry/...

# 运行示例
go run internal/common/retry/example.go
```

## 📝 完整示例

查看 [example.go](example.go) 获取更多使用示例:

- ✅ 基本使用
- ✅ 不同重试策略
- ✅ 错误过滤
- ✅ 超时控制
- ✅ 上下文取消
- ✅ 批量重试
- ✅ 与死信队列集成
- ✅ 监控回调

## 🚀 下一步

- 📖 阅读 [example.go](example.go) 了解详细用法
- 💡 在实际项目中集成使用
- 📊 添加监控和告警
- 🔧 根据业务需求调整配置

---

**祝你使用愉快!** 🎉
