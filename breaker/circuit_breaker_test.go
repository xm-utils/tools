package breaker

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestCircuitBreaker_BasicFlow 测试基础状态流转
func TestCircuitBreaker_BasicFlow(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.MinRequests = 5
	config.ErrorThreshold = 0.5
	config.WaitDurationInOpenState = 1 * time.Second

	cb := NewCircuitBreaker("test-service", config)

	// 初始状态应该是Closed
	if cb.GetState() != StateClosed {
		t.Errorf("期望初始状态为Closed, 实际为: %s", cb.GetState())
	}

	// 模拟5次失败请求(达到MinRequests且错误率超过阈值)
	for i := 0; i < 5; i++ {
		cb.Execute(func() (interface{}, error) {
			return nil, errors.New("service error")
		})
	}

	// 应该触发熔断,进入Open状态
	if cb.GetState() != StateOpen {
		t.Errorf("期望状态为Open, 实际为: %s", cb.GetState())
	}

	// Open状态下应该拒绝请求
	_, err := cb.Execute(func() (interface{}, error) {
		return "should not execute", nil
	})
	if err != ErrCircuitOpen {
		t.Errorf("期望返回ErrCircuitOpen, 实际为: %v", err)
	}

	// 等待WaitDuration,应该转为Half-Open
	time.Sleep(1200 * time.Millisecond)
	if cb.GetState() != StateHalfOpen {
		t.Errorf("期望状态为Half-Open, 实际为: %s", cb.GetState())
	}

	// Half-Open状态下执行成功,应该恢复为Closed
	cb.Execute(func() (interface{}, error) {
		return "success", nil
	})
	if cb.GetState() != StateClosed {
		t.Errorf("期望状态为Closed, 实际为: %s", cb.GetState())
	}
}

// TestCircuitBreaker_HalfOpenFailure 测试Half-Open状态探测失败
func TestCircuitBreaker_HalfOpenFailure(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.MinRequests = 3
	config.ErrorThreshold = 0.5
	config.WaitDurationInOpenState = 500 * time.Millisecond

	cb := NewCircuitBreaker("test-service", config)

	// 触发熔断
	for i := 0; i < 3; i++ {
		cb.Execute(func() (interface{}, error) {
			return nil, errors.New("error")
		})
	}

	if cb.GetState() != StateOpen {
		t.Errorf("期望状态为Open, 实际为: %s", cb.GetState())
	}

	// 等待进入Half-Open
	time.Sleep(600 * time.Millisecond)

	// Half-Open状态下再次失败,应该回到Open
	cb.Execute(func() (interface{}, error) {
		return nil, errors.New("error again")
	})

	if cb.GetState() != StateOpen {
		t.Errorf("期望状态回到Open, 实际为: %s", cb.GetState())
	}
}

// TestCircuitBreaker_Fallback 测试降级函数
func TestCircuitBreaker_Fallback(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.FallbackFunc = func(args ...interface{}) (interface{}, error) {
		return "fallback_value", nil
	}

	cb := NewCircuitBreaker("test-service", config)

	// 手动设置为Open状态
	cb.mu.Lock()
	cb.state = StateOpen
	cb.openedAt = time.Now()
	cb.mu.Unlock()

	// 执行调用,应该触发降级
	result, err := cb.Execute(func() (interface{}, error) {
		return "real_value", nil
	})

	if err != nil {
		t.Errorf("降级函数不应返回错误: %v", err)
	}

	if result != "fallback_value" {
		t.Errorf("期望返回fallback_value, 实际为: %v", result)
	}
}

// TestCircuitBreaker_SlowCall 测试慢调用检测
func TestCircuitBreaker_SlowCall(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.MinRequests = 5
	config.SlowCallThreshold = 0.5
	config.SlowCallDuration = 100 * time.Millisecond

	cb := NewCircuitBreaker("test-service", config)

	// 模拟5次慢调用
	for i := 0; i < 5; i++ {
		cb.Execute(func() (interface{}, error) {
			time.Sleep(150 * time.Millisecond) // 超过SlowCallDuration
			return "slow_result", nil
		})
	}

	// 应该因为慢调用比例过高而触发熔断
	if cb.GetState() != StateOpen {
		t.Errorf("期望因慢调用触发熔断, 实际状态为: %s", cb.GetState())
	}
}

// TestCircuitBreaker_Metrics 测试监控指标
func TestCircuitBreaker_Metrics(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	cb := NewCircuitBreaker("test-service", config)

	// 执行一些请求
	for i := 0; i < 10; i++ {
		cb.Execute(func() (interface{}, error) {
			if i%3 == 0 {
				return nil, errors.New("error")
			}
			return "success", nil
		})
	}

	metrics := cb.GetMetrics().GetSnapshot()

	if metrics.TotalRequests != 10 {
		t.Errorf("期望总请求数为10, 实际为: %d", metrics.TotalRequests)
	}

	if metrics.SuccessRequests != 7 {
		t.Errorf("期望成功请求数为7, 实际为: %d", metrics.SuccessRequests)
	}

	if metrics.FailedRequests != 3 {
		t.Errorf("期望失败请求数为3, 实际为: %d", metrics.FailedRequests)
	}

	fmt.Printf("指标快照: %+v\n", metrics)
}

// TestCircuitBreakerManager_Singleton 测试管理器单例
func TestCircuitBreakerManager_Singleton(t *testing.T) {
	manager1 := GetManager()
	manager2 := GetManager()

	if manager1 != manager2 {
		t.Error("管理器应该是单例")
	}
}

// TestCircuitBreakerManager_MultipleBreakers 测试管理多个熔断器
func TestCircuitBreakerManager_MultipleBreakers(t *testing.T) {
	manager := GetManager()

	cb1 := manager.GetOrCreateBreaker("service-1", nil)
	cb2 := manager.GetOrCreateBreaker("service-2", nil)
	cb3 := manager.GetOrCreateBreaker("service-1", nil) // 应该返回已存在的

	if cb1 != cb3 {
		t.Error("相同名称的熔断器应该返回同一个实例")
	}

	if cb1 == cb2 {
		t.Error("不同名称的熔断器应该是不同实例")
	}

	// 获取所有指标
	metrics := manager.GetAllMetrics()
	if len(metrics) < 2 {
		t.Errorf("期望至少2个熔断器指标, 实际为: %d", len(metrics))
	}
}

// TestSlidingWindow_TimeWindow 测试时间窗口
func TestSlidingWindow_TimeWindow(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.WindowType = WindowTypeTime
	config.WindowSize = 1 * time.Second

	window := NewSlidingWindow(config)

	// 记录一些请求
	for i := 0; i < 5; i++ {
		window.RecordRequest(10*time.Millisecond, i%2 == 0)
	}

	total, errors, _ := window.GetStats()
	if total != 5 {
		t.Errorf("期望总数为5, 实际为: %d", total)
	}

	if errors != 2 { // 第1、3次失败
		t.Errorf("期望错误数为2, 实际为: %d", errors)
	}

	// 等待窗口过期
	time.Sleep(1100 * time.Millisecond)

	// 重新统计,应该为0
	total2, _, _ := window.GetStats()
	if total2 != 0 {
		t.Errorf("窗口过期后期望总数为0, 实际为: %d", total2)
	}
}

// TestSlidingWindow_CountWindow 测试计数窗口
func TestSlidingWindow_CountWindow(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.WindowType = WindowTypeCount
	config.WindowCount = 5

	window := NewSlidingWindow(config)

	// 记录10次请求,窗口应该只保留最后5次
	for i := 0; i < 10; i++ {
		window.RecordRequest(10*time.Millisecond, i >= 5) // 前5次失败,后5次成功
	}

	total, _, _ := window.GetStats()
	if total > 5 {
		t.Errorf("计数窗口期望最多5条记录, 实际为: %d", total)
	}
}

// BenchmarkCircuitBreaker_Execute 性能测试
func BenchmarkCircuitBreaker_Execute(b *testing.B) {
	config := DefaultCircuitBreakerConfig()
	cb := NewCircuitBreaker("benchmark-service", config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb.Execute(func() (interface{}, error) {
			return "result", nil
		})
	}
}
