# Circuit Breaker - 熔断器组件

## 📋 概述

熔断器是一个保护系统免受下游服务故障影响的容错机制。通过监控请求成功率、响应时间等指标，在检测到异常时自动切断对下游服务的调用，防止资源耗尽和级联故障。

### 核心特性

- ✅ **状态机机制** - Closed/Open/Half-Open 三种状态自动转换
- ✅ **滑动窗口统计** - 支持基于时间或计数的滑动窗口
- ✅ **降级策略** - 支持自定义降级函数 (Fallback)
- ✅ **慢调用保护** - 识别并熔断响应过慢的调用
- ✅ **实时监控告警** - 自动检测 Open 状态持续时间和拒绝率
- ✅ **统一管理** - 管理器模式统一管理多个熔断器
- ✅ **线程安全** - 内部实现读写锁，支持高并发场景

## 🏗️ 架构设计

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

状态转换流程:
                    错误率/慢调用超过阈值
CLOSED ──────────────────────────────► OPEN
  ▲                                    │
  │                                    │ 等待 WaitDuration
  │         探测成功                   │
  └──────── HALF_OPEN ◄───────────────┘
               │
               │ 探测失败
               └──────────► OPEN
```

## 🚀 快速开始

### 基础用法

```go
import "github.com/xm-utils/tools/breaker"

// 1. 创建熔断器
config := breaker.DefaultCircuitBreakerConfig()
cb := breaker.NewCircuitBreaker("payment-service", config)

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
config := breaker.DefaultCircuitBreakerConfig()

// 设置降级函数
config.FallbackFunc = func(args ...interface{}) (interface{}, error) {
    // 返回缓存数据或默认值
    return cachedData, nil
}

cb := breaker.NewCircuitBreaker("order-service", config)
result, err := cb.Execute(func() (interface{}, error) {
    return callOrderService()
})
```

### 使用管理器统一管理

```go
// 1. 获取管理器单例
manager := breaker.GetManager()

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

## ⚙️ 配置参数详解

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
| WaitDurationInOpenState | time.Duration | 30s | Open 状态持续时间 |
| HalfOpenMaxRequests | int | 5 | Half-Open 状态最大探测请求数 |
| FallbackFunc | FallbackFunc | nil | 降级函数 |
| OnStateChange | func | nil | 状态变更回调 |

### 状态说明

- **StateClosed (关闭状态)**: 正常流量通过，实时监控请求指标
- **StateOpen (打开状态)**: 拒绝所有请求，直接返回降级结果
- **StateHalfOpen (半开状态)**: 允许少量探测请求，验证下游服务是否恢复

## 📊 监控指标

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

## 🔔 告警机制

### AlertConfig

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| Enabled | bool | true | 是否启用告警 |
| OpenStateAlertDelay | time.Duration | 1min | Open 状态持续多久后告警 |
| RejectionRateThreshold | float64 | 0.8 | 拒绝率告警阈值 |
| AlertCallback | func | nil | 自定义告警回调 |

### 自定义告警回调

```go
alertConfig := breaker.DefaultAlertConfig()
alertConfig.AlertCallback = func(alert *breaker.AlertEvent) {
    // 发送钉钉/企业微信告警
    sendNotification(alert.Message)
    
    // 记录到监控系统
    prometheus.GaugeSet("circuit_breaker_alert", 1)
}

alertManager := breaker.NewAlertManager(alertConfig)
alertManager.RegisterBreaker(cb)
alertManager.StartMonitoring()
```

## 💡 最佳实践

### 1. 配置调优

根据业务场景调整参数：

- **核心业务** (如支付、订单): 降低 ErrorThreshold（如 0.3），快速熔断保护
- **非核心业务** (如通知、日志): 提高 ErrorThreshold（如 0.7），容忍短暂故障
- **高 QPS 场景** (如商品查询): 增大 MinRequests（如 50），避免误判
- **低 QPS 场景** (如报表生成): 减小 MinRequests（如 5），快速响应

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

### 3. 状态变更回调

```go
config.OnStateChange = func(oldState, newState breaker.State) {
    fmt.Printf("熔断器状态变更: %s -> %s\n", oldState, newState)
    
    if newState == breaker.StateOpen {
        // 发送告警通知
        sendAlert("熔断器打开告警")
    }
}
```

### 4. 监控告警

- 实时监控熔断器状态变化
- Open 状态立即触发告警
- 定期查看指标面板，提前发现潜在问题

## 🎯 应用场景

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

### 3. 第三方 API 调用保护

```go
// 保护对支付网关的调用
paymentCB := manager.GetOrCreateBreaker("payment-gateway", config)
result, err := paymentCB.Execute(func() (interface{}, error) {
    return paymentGateway.Charge(amount)
})
```

### 4. HTTP 客户端保护

```go
// 保护 HTTP 请求
httpCB := manager.GetOrCreateBreaker("http-client", config)
result, err := httpCB.Execute(func() (interface{}, error) {
    resp, err := httpClient.Get("https://api.example.com/data")
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()
    return io.ReadAll(resp.Body)
})
```

## ⚠️ 注意事项

1. **线程安全**: 熔断器内部已实现线程安全，可在多个 goroutine 中共享
2. **资源管理**: 使用管理器统一管理熔断器生命周期
3. **配置持久化**: 建议将配置存储在配置中心（如 Nacos），支持动态调整
4. **测试验证**: 在生产环境使用前，充分测试各种故障场景
5. **降级函数**: 务必实现合理的降级策略，避免熔断后业务完全不可用
6. **告警配置**: 启用告警功能，及时发现和处理问题

## 🔍 故障排查

### 熔断器频繁打开

**可能原因:**
- 下游服务确实存在故障
- ErrorThreshold 设置过低
- MinRequests 设置过小导致误判

**解决方案:**
```go
// 1. 检查下游服务健康状态
// 2. 调整配置
config.ErrorThreshold = 0.6  // 提高阈值
config.MinRequests = 50      // 增大样本量
```

### 请求被大量拒绝

**可能原因:**
- 熔断器处于 Open 状态
- 下游服务长时间未恢复

**解决方案:**
```go
// 1. 查看熔断器状态
cb := manager.GetBreaker("service-name")
state := cb.GetState()

// 2. 手动重置（谨慎使用）
cb.Reset()

// 3. 检查告警日志定位根本原因
```

### 降级函数未生效

**可能原因:**
- FallbackFunc 未正确配置
- 降级函数本身抛出异常

**解决方案:**
```go
config.FallbackFunc = func(args ...interface{}) (interface{}, error) {
    // 确保降级函数不会 panic
    defer func() {
        if r := recover(); r != nil {
            log.Printf("降级函数异常: %v", r)
        }
    }()
    
    // 返回安全的默认值
    return defaultValue, nil
}
```

## 🧪 测试示例

```go
package breaker_test

import (
    "testing"
    "time"
    "github.com/xm-utils/tools/breaker"
)

func TestCircuitBreaker(t *testing.T) {
    config := breaker.DefaultCircuitBreakerConfig()
    config.ErrorThreshold = 0.5
    config.MinRequests = 5
    
    cb := breaker.NewCircuitBreaker("test-service", config)
    
    // 模拟失败请求
    for i := 0; i < 10; i++ {
        _, err := cb.Execute(func() (interface{}, error) {
            return nil, fmt.Errorf("模拟失败")
        })
        if err != nil {
            t.Logf("请求失败: %v", err)
        }
    }
    
    // 检查状态是否为 Open
    if cb.GetState() != breaker.StateOpen {
        t.Errorf("期望状态为 Open, 实际为 %v", cb.GetState())
    }
}
```

## 📚 参考资料

- [Martin Fowler - Circuit Breaker](https://martinfowler.com/articles/circuitBreaker.html)
- [Resilience4j Circuit Breaker](https://resilience4j.readme.io/docs/circuitbreaker)
- [Hystrix Documentation](https://github.com/Netflix/Hystrix/wiki)

## 🆘 技术支持

如有问题，请联系开发团队或提交 Issue。
