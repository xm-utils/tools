# Circuit Breaker 快速开始指南

## 1. 安装与导入

熔断器组件位于 `internal/common/circuitbreaker` 目录，无需额外安装，直接导入即可使用：

```go
import "gitlab.novgate.com/xm/pay/internal/common/circuitbreaker"
```

## 2. 最简示例

### 基础用法（3步完成）

```go
package main

import (
    "fmt"
    "gitlab.novgate.com/xm/pay/internal/common/circuitbreaker"
)

func main() {
    // 第1步: 创建熔断器
    cb := circuitbreaker.NewCircuitBreaker("my-service", nil)
    
    // 第2步: 执行受保护的调用
    result, err := cb.Execute(func() (interface{}, error) {
        // 你的业务逻辑
        return callYourService()
    })
    
    // 第3步: 处理结果
    if err != nil {
        fmt.Printf("调用失败: %v\n", err)
    } else {
        fmt.Printf("调用成功: %v\n", result)
    }
}
```

## 3. 常见场景

### 场景1: 保护gRPC调用

```go
// 在order服务中调用payment服务
manager := circuitbreaker.GetManager()
cb := manager.GetOrCreateBreaker("payment-grpc", nil)

result, err := cb.Execute(func() (interface{}, error) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    return paymentClient.CreatePayment(ctx, &PaymentRequest{
        Amount: 100.00,
        OrderId: "ORDER_12345",
    })
})

if err != nil {
    log.Errorf("支付调用失败: %v", err)
    // 返回友好提示给用户
    return nil, errors.New("支付服务暂时不可用")
}
```

### 场景2: 带降级策略

```go
config := circuitbreaker.DefaultCircuitBreakerConfig()
config.FallbackFunc = func(args ...interface{}) (interface{}, error) {
    // 降级: 返回缓存数据
    if cachedData, ok := cache.Get("product_list"); ok {
        return cachedData, nil
    }
    
    // 降级: 返回默认值
    return []Product{}, nil
}

cb := circuitbreaker.NewCircuitBreaker("product-service", config)
products, _ := cb.Execute(func() (interface{}, error) {
    return productService.GetAllProducts()
})
```

### 场景3: 监控所有熔断器状态

```go
// 在管理后台或监控接口中使用
manager := circuitbreaker.GetManager()

// 获取所有熔断器指标
allMetrics := manager.GetAllMetrics()

for name, metrics := range allMetrics {
    fmt.Printf("[%s]\n", name)
    fmt.Printf("  状态: %s\n", /* 需要额外记录状态 */)
    fmt.Printf("  总请求: %d\n", metrics.TotalRequests)
    fmt.Printf("  成功率: %.2f%%\n", metrics.SuccessRate*100)
    fmt.Printf("  拒绝率: %.2f%%\n", metrics.RejectionRate*100)
    fmt.Printf("  平均响应时间: %v\n", metrics.AvgDuration)
}
```

## 4. 配置调优建议

### 核心业务（如支付、订单）

```go
config := circuitbreaker.DefaultCircuitBreakerConfig()
config.ErrorThreshold = 0.3           // 更敏感，30%错误率就熔断
config.MinRequests = 20               // 样本量充足
config.WaitDurationInOpenState = 60 * time.Second // 等待时间长一些
config.SlowCallDuration = 2 * time.Second
```

### 非核心业务（如通知、日志）

```go
config := circuitbreaker.DefaultCircuitBreakerConfig()
config.ErrorThreshold = 0.7           // 容忍度高
config.MinRequests = 10
config.WaitDurationInOpenState = 10 * time.Second // 快速恢复
```

### 高QPS场景（如商品查询）

```go
config := circuitbreaker.DefaultCircuitBreakerConfig()
config.WindowType = circuitbreaker.WindowTypeCount
config.WindowCount = 1000             // 基于计数窗口
config.MinRequests = 100              // 最小样本量大
config.ErrorThreshold = 0.5
```

### 低QPS场景（如报表生成）

```go
config := circuitbreaker.DefaultCircuitBreakerConfig()
config.MinRequests = 5                // 最小样本量小
config.ErrorThreshold = 0.5
```

## 5. 最佳实践清单

✅ **必须做**
- [ ] 为每个下游服务创建独立的熔断器
- [ ] 设置合理的ErrorThreshold和MinRequests
- [ ] 实现降级函数FallbackFunc
- [ ] 启用告警监控

✅ **推荐做**
- [ ] 定期查看熔断器指标
- [ ] 将配置存储在Nacos支持动态调整

❌ **不要做**
- [ ] 不要将MinRequests设置过小（容易误判）
- [ ] 不要忽略告警信息

## 6. 故障排查

### 问题1: 熔断器频繁打开

**可能原因:**
- 下游服务确实存在故障
- ErrorThreshold设置过低
- MinRequests设置过小导致误判

**解决方案:**
```go
// 1. 检查下游服务健康状态
// 2. 调整配置
config.ErrorThreshold = 0.6  // 提高阈值
config.MinRequests = 50      // 增大样本量
```

### 问题2: 请求被大量拒绝

**可能原因:**
- 熔断器处于Open状态
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

## 7. 完整实战示例

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"
    
    "gitlab.novgate.com/xm/pay/internal/common/circuitbreaker"
)

func main() {
    // 1. 初始化熔断器管理器
    manager := circuitbreaker.GetManager()
    
    // 2. 配置支付服务熔断器
    paymentConfig := circuitbreaker.DefaultCircuitBreakerConfig()
    paymentConfig.ErrorThreshold = 0.4
    paymentConfig.MinRequests = 20
    paymentConfig.FallbackFunc = func(args ...interface{}) (interface{}, error) {
        log.Warn("支付服务降级: 返回排队状态")
        return map[string]string{"status": "queued"}, nil
    }
    
    paymentCB := manager.GetOrCreateBreaker("payment-service", paymentConfig)
    
    // 3. 模拟业务调用
    for i := 0; i < 100; i++ {
        // 调用支付服务
        go func(id int) {
            result, err := paymentCB.Execute(func() (interface{}, error) {
                return processPayment(id)
            })
            
            if err != nil {
                log.Printf("支付失败: %v", err)
            } else {
                log.Printf("支付成功: %v", result)
            }
        }(i)
        
        time.Sleep(100 * time.Millisecond)
    }
    
    // 4. 定期打印指标
    ticker := time.NewTicker(10 * time.Second)
    for range ticker.C {
        metrics := manager.GetAllMetrics()
        for name, m := range metrics {
            fmt.Printf("[%s] 请求:%d 成功率:%.2f%% 拒绝率:%.2f%%\n", 
                name, 
                m.TotalRequests,
                m.SuccessRate*100,
                m.RejectionRate*100,
            )
        }
    }
}

func processPayment(id int) (interface{}, error) {
    // 模拟支付处理
    time.Sleep(50 * time.Millisecond)
    return map[string]interface{}{
        "orderId": id,
        "status":  "success",
    }, nil
}
```

## 8. 下一步

- 📖 阅读 [README.md](README.md) 了解详细API文档
- 🔍 查看 [example.go](example.go) 查看更多使用示例
- 🧪 运行测试: `go test -v ./internal/common/circuitbreaker/...`
- 📊 集成监控系统（Prometheus、Grafana等）

## 9. 常见问题

**Q: 熔断器会影响性能吗？**  
A: 影响极小。熔断器只做简单的状态检查和统计，开销在微秒级别。

**Q: 如何动态调整配置？**  
A: 建议使用Nacos配置中心，监听配置变化后重建熔断器。

**Q: 熔断器是线程安全的吗？**  
A: 是的，内部已实现读写锁，可在多个goroutine中安全使用。

**Q: 如何在微服务间共享熔断器状态？**  
A: 当前版本是每个服务实例独立维护状态。如需分布式熔断，可结合Redis实现。
