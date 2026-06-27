package circuitbreaker

import (
	"context"
	"fmt"
	"time"
)

// Example_BasicCircuitBreaker 基础熔断器使用示例
func Example_BasicCircuitBreaker() {
	// 1. 创建熔断器配置
	config := DefaultCircuitBreakerConfig()
	config.ErrorThreshold = 0.5                       // 错误率50%触发熔断
	config.WaitDurationInOpenState = 30 * time.Second // Open状态持续30秒

	// 2. 创建熔断器
	cb := NewCircuitBreaker("payment-service", config)

	// 3. 执行受保护的调用
	result, err := cb.Execute(func() (interface{}, error) {
		// 模拟调用下游服务
		return callPaymentService()
	})

	if err != nil {
		fmt.Printf("调用失败: %v\n", err)
	} else {
		fmt.Printf("调用成功: %v\n", result)
	}

	// 4. 查看当前状态
	fmt.Printf("熔断器状态: %s\n", cb.GetState())

	// 5. 查看监控指标
	metrics := cb.GetMetrics().GetSnapshot()
	fmt.Printf("总请求数: %d, 成功率: %.2f%%\n",
		metrics.TotalRequests,
		metrics.SuccessRate*100,
	)
}

// Example_CircuitBreakerWithFallback 带降级策略的熔断器
func Example_CircuitBreakerWithFallback() {
	config := DefaultCircuitBreakerConfig()

	// 设置降级函数
	config.FallbackFunc = func(args ...interface{}) (interface{}, error) {
		// 返回缓存数据或默认值
		fmt.Println("执行降级逻辑, 返回默认值")
		return map[string]interface{}{
			"status": "fallback",
			"data":   "default_value",
		}, nil
	}

	cb := NewCircuitBreaker("order-service", config)

	// 执行调用, 熔断时会自动降级
	result, err := cb.Execute(func() (interface{}, error) {
		return callOrderService()
	})

	fmt.Printf("结果: %v, 错误: %v\n", result, err)
}

// Example_CircuitBreakerManager 使用管理器统一管理
func Example_CircuitBreakerManager() {
	// 1. 获取管理器单例
	manager := GetManager()

	// 2. 创建多个熔断器
	paymentCB := manager.GetOrCreateBreaker("payment-service", nil)
	orderCB := manager.GetOrCreateBreaker("order-service", nil)

	// 3. 使用熔断器
	paymentCB.Execute(func() (interface{}, error) {
		return callPaymentService()
	})

	orderCB.Execute(func() (interface{}, error) {
		return callOrderService()
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

	// 5. 重置所有熔断器
	// manager.ResetAll()
}

// Example_StateChangeCallback 状态变更回调
func Example_StateChangeCallback() {
	config := DefaultCircuitBreakerConfig()

	// 设置状态变更回调
	config.OnStateChange = func(oldState, newState State) {
		fmt.Printf("熔断器状态变更: %s -> %s\n", oldState, newState)

		// 可以在这里发送通知、记录日志等
		if newState == StateOpen {
			fmt.Println("警告: 熔断器已打开, 下游服务可能故障!")
			// sendAlert("熔断器打开告警")
		}
	}

	cb := NewCircuitBreaker("notification-service", config)
	cb.Execute(func() (interface{}, error) {
		return callNotificationService()
	})
}

// Example_CustomWindowType 自定义窗口类型
func Example_CustomWindowType() {
	config := DefaultCircuitBreakerConfig()

	// 使用基于计数的滑动窗口
	config.WindowType = WindowTypeCount
	config.WindowCount = 100 // 最近100次请求
	config.MinRequests = 20  // 至少20次请求才评估

	cb := NewCircuitBreaker("inventory-service", config)
	cb.Execute(func() (interface{}, error) {
		return callInventoryService()
	})
}

// Example_SlowCallProtection 慢调用保护
func Example_SlowCallProtection() {
	config := DefaultCircuitBreakerConfig()

	// 设置慢调用阈值
	config.SlowCallDuration = 2 * time.Second // 超过2秒视为慢调用
	config.SlowCallThreshold = 0.5            // 慢调用比例50%触发熔断

	cb := NewCircuitBreaker("search-service", config)
	cb.Execute(func() (interface{}, error) {
		return callSearchService()
	})
}

// ===== 模拟的服务调用方法 =====

func callPaymentService() (interface{}, error) {
	// 模拟支付服务调用
	time.Sleep(100 * time.Millisecond)
	return map[string]interface{}{"status": "success"}, nil
}

func callOrderService() (interface{}, error) {
	// 模拟订单服务调用
	time.Sleep(100 * time.Millisecond)
	return map[string]interface{}{"orderId": "12345"}, nil
}

func callUserService(ctx context.Context) (interface{}, error) {
	// 模拟用户服务调用
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(100 * time.Millisecond):
		return map[string]interface{}{"userId": "12345"}, nil
	}
}

func callNotificationService() (interface{}, error) {
	// 模拟通知服务调用
	time.Sleep(100 * time.Millisecond)
	return map[string]interface{}{"sent": true}, nil
}

func callInventoryService() (interface{}, error) {
	// 模拟库存服务调用
	time.Sleep(100 * time.Millisecond)
	return map[string]interface{}{"stock": 100}, nil
}

func callSearchService() (interface{}, error) {
	// 模拟搜索服务调用
	time.Sleep(100 * time.Millisecond)
	return map[string]interface{}{"results": []string{}}, nil
}
