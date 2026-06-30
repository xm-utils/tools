# Circuit Breaker - 快速开始指南

## 🚀 5分钟快速上手

### 第一步:导入熔断器组件

```go
import "github.com/xm-utils/tools/breaker"
```

### 第二步:最简示例(3步完成)

```go
package main

import (
    "fmt"
    "github.com/xm-utils/tools/breaker"
)

func main() {
    // 第1步: 创建熔断器
    cb := breaker.NewCircuitBreaker("my-service", nil)
    
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

## 💼 常见场景

### 场景1: 保护 gRPC 调用

```go
// 在 order 服务中调用 payment 服务
manager := breaker.GetManager()
cb := manager.GetOrCreateBreaker("payment-grpc", nil)

result, err := cb.Execute(func() (interface{}, error) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    return paymentClient.CreatePayment(ctx, &PaymentRequest{
        Amount:  100.00,
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
config := breaker.DefaultCircuitBreakerConfig()
config.FallbackFunc = func(args ...interface{}) (interface{}, error) {
    // 降级: 返回缓存数据
    if cachedData, ok := cache.Get("product_list"); ok {
        return cachedData, nil
    }
    
    // 降级: 返回默认值
    return []Product{}, nil
}

cb := breaker.NewCircuitBreaker("product-service", config)
products, _ := cb.Execute(func() (interface{}, error) {
    return productService.GetAllProducts()
})
```

### 场景3: 状态变更回调

```go
config := breaker.DefaultCircuitBreakerConfig()
config.OnStateChange = func(oldState, newState breaker.State) {
    fmt.Printf("熔断器状态变更: %s -> %s\n", oldState, newState)
    
    if newState == breaker.StateOpen {
        // 发送告警通知
        sendAlert("熔断器打开，下游服务可能故障")
    }
}

cb := breaker.NewCircuitBreaker("notification-service", config)
```

### 场景4: 监控所有熔断器状态

```go
// 在管理后台或监控接口中使用
manager := breaker.GetManager()

// 获取所有熔断器指标
allMetrics := manager.GetAllMetrics()

for name, metrics := range allMetrics {
    fmt.Printf("[%s]\n", name)
    fmt.Printf("  总请求: %d\n", metrics.TotalRequests)
    fmt.Printf("  成功率: %.2f%%\n", metrics.SuccessRate*100)
    fmt.Printf("  拒绝率: %.2f%%\n", metrics.RejectionRate*100)
    fmt.Printf("  平均响应时间: %v\n", metrics.AvgDuration)
}
```

## ⚙️ 配置调优建议

### 核心业务（如支付、订单）

```go
config := breaker.DefaultCircuitBreakerConfig()
config.ErrorThreshold = 0.3                    // 更敏感，30%错误率就熔断
config.MinRequests = 20                        // 样本量充足
config.WaitDurationInOpenState = 60 * time.Second // 等待时间长一些
config.SlowCallDuration = 2 * time.Second
```

### 非核心业务（如通知、日志）

```go
config := breaker.DefaultCircuitBreakerConfig()
config.ErrorThreshold = 0.7                    // 容忍度高
config.MinRequests = 10
config.WaitDurationInOpenState = 10 * time.Second // 快速恢复
```

### 高 QPS 场景（如商品查询）

```go
config := breaker.DefaultCircuitBreakerConfig()
config.WindowType = breaker.WindowTypeCount
config.WindowCount = 1000     // 基于计数窗口
config.MinRequests = 100      // 最小样本量大
config.ErrorThreshold = 0.5
```

### 低 QPS 场景（如报表生成）

```go
config := breaker.DefaultCircuitBreakerConfig()
config.MinRequests = 5        // 最小样本量小
config.ErrorThreshold = 0.5
```

### 慢调用保护场景

```go
config := breaker.DefaultCircuitBreakerConfig()
config.SlowCallDuration = 2 * time.Second  // 超过2秒视为慢调用
config.SlowCallThreshold = 0.5             // 慢调用比例50%触发熔断
```

## ✅ 最佳实践清单

### 必须做

- [ ] 为每个下游服务创建独立的熔断器
- [ ] 设置合理的 ErrorThreshold 和 MinRequests
- [ ] 实现降级函数 FallbackFunc
- [ ] 启用告警监控

### 推荐做

- [ ] 定期查看熔断器指标
- [ ] 将配置存储在配置中心支持动态调整
- [ ] 设置状态变更回调
- [ ] 配置慢调用保护

### 不要做

- [ ] 不要将 MinRequests 设置过小（容易误判）
- [ ] 不要忽略告警信息
- [ ] 不要在降级函数中执行耗时操作
- [ ] 不要忘记重置测试环境的熔断器

## 🔍 故障排查

### 问题1: 熔断器频繁打开

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

### 问题2: 请求被大量拒绝

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

### 问题3: 降级函数未生效

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

## 🎯 完整实战示例

### 示例1: 订单服务中的熔断器应用

```go
package main

import (
	"fmt"
	"time"

	"github.com/xm-utils/tools/breaker"
)

func main() {
	// 1. 初始化熔断器管理器
	manager := breaker.GetManager()

	// 2. 配置支付服务熔断器
	paymentConfig := breaker.DefaultCircuitBreakerConfig()
	paymentConfig.ErrorThreshold = 0.4
	paymentConfig.MinRequests = 20
	paymentConfig.FallbackFunc = func(args ...interface{}) (interface{}, error) {
		fmt.Println("支付服务降级: 返回排队状态")
		return map[string]string{"status": "queued"}, nil
	}

	paymentCB := manager.GetOrCreateBreaker("payment-service", paymentConfig)

	// 3. 模拟业务调用
	for i := 0; i < 100; i++ {
		go func(id int) {
			result, err := paymentCB.Execute(func() (interface{}, error) {
				return processPayment(id)
			})

			if err != nil {
				fmt.Printf("支付失败: %v", err)
			} else {
				fmt.Printf("支付成功: %v", result)
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

### 示例2: HTTP 客户端保护

```go
package httpclient

import (
	"fmt"
    "io"
    "net/http"
    "time"
    
    "github.com/xm-utils/tools/breaker"
)

type ProtectedHTTPClient struct {
    cb         *breaker.CircuitBreaker
    httpClient *http.Client
}

func NewProtectedHTTPClient(serviceName string) *ProtectedHTTPClient {
    config := breaker.DefaultCircuitBreakerConfig()
    config.ErrorThreshold = 0.5
    config.MinRequests = 10
    config.SlowCallDuration = 5 * time.Second
    
    return &ProtectedHTTPClient{
        cb: breaker.NewCircuitBreaker(serviceName, config),
        httpClient: &http.Client{
            Timeout: 10 * time.Second,
        },
    }
}

func (c *ProtectedHTTPClient) Get(url string) ([]byte, error) {
    result, err := c.cb.Execute(func() (interface{}, error) {
        resp, err := c.httpClient.Get(url)
        if err != nil {
            return nil, err
        }
        defer resp.Body.Close()
        
        return io.ReadAll(resp.Body)
    })
    
    if err != nil {
        return nil, err
    }
    
    return result.([]byte), nil
}

// 使用示例
func Example() {
    http_client := NewProtectedHTTPClient("api-service")
    
    data, err := http_client.Get("https://api.example.com/data")
    if err != nil {
        fmt.Printf("请求失败: %v", err)
        return
    }
    
    fmt.Printf("获取数据: %s", string(data))
}
```

### 示例3: 数据库访问保护

```go
package database

import (
    "database/sql"
    "time"
    
    "github.com/xm-utils/tools/breaker"
)

type ProtectedDB struct {
    db *sql.DB
    cb *breaker.CircuitBreaker
}

func NewProtectedDB(db *sql.DB, name string) *ProtectedDB {
    config := breaker.DefaultCircuitBreakerConfig()
    config.ErrorThreshold = 0.3
    config.MinRequests = 5
    config.SlowCallDuration = 3 * time.Second
    
    return &ProtectedDB{
        db: db,
        cb: breaker.NewCircuitBreaker(name, config),
    }
}

func (p *ProtectedDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
    result, err := p.cb.Execute(func() (interface{}, error) {
        return p.db.Query(query, args...)
    })
    
    if err != nil {
        return nil, err
    }
    
    return result.(*sql.Rows), nil
}

// 使用示例
func Example() {
    db, _ := sql.Open("mysql", "user:password@tcp(localhost)/dbname")
    protectedDB := NewProtectedDB(db, "mysql-primary")
    
    rows, err := protectedDB.Query("SELECT * FROM users WHERE id = ?", 123)
    if err != nil {
        log.Printf("查询失败: %v", err)
        return
    }
    defer rows.Close()
    
    // 处理结果...
}
```

## ❓ 常见问题

**Q: 熔断器会影响性能吗？**  
A: 影响极小。熔断器只做简单的状态检查和统计，开销在微秒级别。

**Q: 如何动态调整配置？**  
A: 建议使用配置中心（如 Nacos），监听配置变化后重建熔断器。

**Q: 熔断器是线程安全的吗？**  
A: 是的，内部已实现读写锁，可在多个 goroutine 中安全使用。

**Q: 如何在微服务间共享熔断器状态？**  
A: 当前版本是每个服务实例独立维护状态。如需分布式熔断，可结合 Redis 实现。

**Q: 熔断器打开后多久会自动恢复？**  
A: 由 `WaitDurationInOpenState` 配置决定，默认 30 秒。之后会进入 Half-Open 状态进行探测。

**Q: 降级函数返回的错误会被记录吗？**  
A: 降级函数返回的结果不会被记录为失败，因为这是预期的降级行为。

## 📚 下一步

- 📖 阅读 [README.md](README.md) 了解详细 API 文档
- 🔍 查看 [example.go](example.go) 查看更多使用示例
- 🧪 运行测试: `go test -v ./breaker/...`
- 📊 集成监控系统（Prometheus、Grafana 等）

## 🆘 技术支持

如有问题，请联系开发团队或提交 Issue。
