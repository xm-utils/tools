# Circuit Breaker - 熔断器组件

## 概述

熔断器是一个保护系统免受下游服务故障影响的容错机制。通过监控请求成功率、响应时间等指标，在检测到异常时自动切断对下游服务的调用，防止资源耗尽和级联故障。

## 核心特性

### 1. 状态机机制

- **关闭状态 (Closed)**: 正常流量通过，实时监控请求指标
- **打开状态 (Open)**: 拒绝所有请求，直接返回降级结果
- **半开状态 (Half-Open)**: 允许少量探测请求，验证下游服务是否恢复

### 2. 滑动窗口统计

- 支持基于时间的滑动窗口（如最近10秒）
- 支持基于计数的滑动窗口（如最近100次请求）
- 实时统计错误率、慢调用比例等指标

### 3. 监控告警

- 实时监控熔断器状态变更
- 自动检测Open状态持续时间和拒绝率
- 支持自定义告警回调函数

## 架构设计

```
┌─────────────┐
│  Client     │
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│  Circuit Breaker    │ ◄── 状态检查 (Closed/Open/Half-Open)
└──────┬──────────────┘
       │
       ├─ OPEN ──► ┌──────────────┐
       │           │  Fallback    │ ◄── 降级处理
       │           └──────────────┘
       │
       └─ CLOSED/HALF-OPEN ──► ┌──────────────┐
                               │ Downstream   │
                               │   Service    │
                               └──────────────┘
```

## 快速开始

### 基础用法

```go
import "gitlab.novgate.com/xm/pay/internal/common/circuitbreaker"

// 1. 创建熔断器
config := circuitbreaker.DefaultCircuitBreakerConfig()
cb := circuitbreaker.NewCircuitBreaker("payment-service", config)

// 2. 执行受保护的调用
result, err := cb.Execute(func() (interface{}, error) {
    return callPaymentService()
})

if err != nil {
    log.Printf("调用失败: %v\n", err)
}
```

### 带降级策略

```go
config := circuitbreaker.DefaultCircuitBreakerConfig()

// 设置降级函数
config.FallbackFunc = func(args ...interface{}) (interface{}, error) {
    // 返回缓存数据或默认值
    return cachedData, nil
}

cb := circuitbreaker.NewCircuitBreaker("order-service", config)
result, err := cb.Execute(func() (interface{}, error) {
    return callOrderService()
})
```

### 使用管理器统一管理

```go
// 1. 获取管理器单例
manager := circuitbreaker.GetManager()

// 2. 创建多个熔断器
paymentCB := manager.GetOrCreateBreaker("payment-service", nil)
orderCB := manager.GetOrCreateBreaker("order-service", nil)

// 3. 使用熔断器
paymentCB.Execute(func() (interface{}, error) {
    return callPaymentService()
})

// 4. 查看所有熔断器指标
allMetrics := manager.GetAllMetrics()
for name, metrics := range allMetrics {
    fmt.Printf("[%s] 总请求: %d, 成功率: %.2f%%\n", 
        name, 
        metrics.TotalRequests,
        metrics.SuccessRate*100,
    )
}
```

## 配置参数详解

### CircuitBreakerConfig

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| WindowType | WindowType | WindowTypeTime | 窗口类型（时间/计数） |
| WindowSize | time.Duration | 10s | 时间窗口大小 |
| WindowCount | int | 100 | 计数窗口大小 |
| ErrorThreshold | float64 | 0.5 | 错误率阈值（0-1） |
| SlowCallThreshold | float64 | 0.5 | 慢调用比例阈值（0-1） |
| SlowCallDuration | time.Duration | 3s | 慢调用判定阈值 |
| MinRequests | int | 10 | 最小请求数才进行评估 |
| WaitDurationInOpenState | time.Duration | 30s | Open状态持续时间 |
| HalfOpenMaxRequests | int | 5 | Half-Open状态最大探测请求数 |
| FallbackFunc | FallbackFunc | nil | 降级函数 |
| OnStateChange | func | nil | 状态变更回调 |

## 状态转换流程

```
                    错误率/慢调用超过阈值
CLOSED ──────────────────────────────► OPEN
  ▲                                    │
  │                                    │ 等待WaitDuration
  │         探测成功                   │
  └──────── HALF_OPEN ◄───────────────┘
               │
               │ 探测失败
               └──────────► OPEN
```

## 监控指标

### CircuitBreakerMetrics

- **TotalRequests**: 总请求数
- **SuccessRequests**: 成功请求数
- **FailedRequests**: 失败请求数
- **RejectedRequests**: 被熔断器拒绝的请求数
- **SuccessRate**: 成功率
- **FailureRate**: 失败率
- **RejectionRate**: 拒绝率
- **MinDuration**: 最小响应时间
- **MaxDuration**: 最大响应时间
- **AvgDuration**: 平均响应时间
- **StateChanges**: 状态变更统计

### 获取指标

```go
metrics := cb.GetMetrics().GetSnapshot()
fmt.Printf("总请求: %d, 成功率: %.2f%%\n", 
    metrics.TotalRequests, 
    metrics.SuccessRate*100,
)
```

## 告警机制

### AlertConfig

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| Enabled | bool | true | 是否启用告警 |
| OpenStateAlertDelay | time.Duration | 1min | Open状态持续多久后告警 |
| RejectionRateThreshold | float64 | 0.8 | 拒绝率告警阈值 |
| AlertCallback | func | nil | 自定义告警回调 |

### 自定义告警回调

```go
alertConfig := circuitbreaker.DefaultAlertConfig()
alertConfig.AlertCallback = func(alert *circuitbreaker.AlertEvent) {
    // 发送钉钉/企业微信告警
    sendNotification(alert.Message)
    
    // 记录到监控系统
    prometheus.GaugeSet("circuit_breaker_alert", 1)
}

alertManager := circuitbreaker.NewAlertManager(alertConfig)
alertManager.RegisterBreaker(cb)
alertManager.StartMonitoring()
```

## 最佳实践

### 1. 配置调优

根据业务场景调整参数：

- **核心业务**: 降低ErrorThreshold（如0.3），快速熔断保护
- **非核心业务**: 提高ErrorThreshold（如0.7），容忍短暂故障
- **高QPS场景**: 增大MinRequests（如50），避免误判
- **低QPS场景**: 减小MinRequests（如5），快速响应

### 2. 降级策略

```go
config.FallbackFunc = func(args ...interface{}) (interface{}, error) {
    // 优先级1: 返回本地缓存
    if cached := getFromCache(); cached != nil {
        return cached, nil
    }
    
    // 优先级2: 返回默认值
    return defaultValue, nil
    
    // 优先级3: 返回友好提示
    return nil, errors.New("服务暂时不可用，请稍后重试")
}
```

### 3. 监控告警

- 实时监控熔断器状态变化
- Open状态立即触发告警
- 定期查看指标面板，提前发现潜在问题

## 应用场景

### 1. 微服务调用保护

```go
// 保护对订单服务的调用
orderCB := manager.GetOrCreateBreaker("order-service", config)
result, err := orderCB.Execute(func() (interface{}, error) {
    return grpcClient.CreateOrder(ctx, request)
})
```

### 2. 数据库访问保护

```go
// 保护对数据库的查询
dbCB := manager.GetOrCreateBreaker("database-query", config)
result, err := dbCB.Execute(func() (interface{}, error) {
    return db.Query("SELECT * FROM users WHERE id = ?", userId)
})
```

### 3. 第三方API调用保护

```go
// 保护对支付网关的调用
paymentCB := manager.GetOrCreateBreaker("payment-gateway", config)
result, err := paymentCB.Execute(func() (interface{}, error) {
    return paymentGateway.Charge(amount)
})
```

## 注意事项

1. **线程安全**: 熔断器内部已实现线程安全，可在多个goroutine中共享
2. **资源管理**: 使用管理器统一管理熔断器生命周期
3. **配置持久化**: 建议将配置存储在Nacos等配置中心，支持动态调整
4. **测试验证**: 在生产环境使用前，充分测试各种故障场景

## 故障排查

### 熔断器频繁打开

- 检查下游服务健康状况
- 调整ErrorThreshold和MinRequests
- 查看慢调用日志，优化性能瓶颈

### 请求被大量拒绝

- 检查熔断器状态和指标
- 确认是否在Open状态等待期
- 查看告警日志，定位根本原因

## 参考资料

- [Martin Fowler - Circuit Breaker](https://martinfowler.com/articles/circuitBreaker.html)
- [Resilience4j Circuit Breaker](https://resilience4j.readme.io/docs/circuitbreaker)
- [Hystrix Documentation](https://github.com/Netflix/Hystrix/wiki)
