package breaker

import (
	"sync"
)

// CircuitBreakerManager 熔断器管理器(单例模式)
type CircuitBreakerManager struct {
	breakers      map[string]*CircuitBreaker
	retryBreakers map[string]*CircuitBreakerWithRetry
	alertManager  *AlertManager
	mu            sync.RWMutex
}

var (
	instance *CircuitBreakerManager
	once     sync.Once
)

// GetManager 获取熔断器管理器单例
func GetManager() *CircuitBreakerManager {
	once.Do(func() {
		instance = &CircuitBreakerManager{
			breakers:      make(map[string]*CircuitBreaker),
			retryBreakers: make(map[string]*CircuitBreakerWithRetry),
			alertManager:  NewAlertManager(DefaultAlertConfig()),
		}
		// 启动告警监控
		instance.alertManager.StartMonitoring()
	})
	return instance
}

// GetOrCreateBreaker 获取或创建熔断器
func (m *CircuitBreakerManager) GetOrCreateBreaker(name string, config *CircuitBreakerConfig) *CircuitBreaker {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cb, exists := m.breakers[name]; exists {
		return cb
	}

	cb := NewCircuitBreaker(name, config)
	m.breakers[name] = cb

	// 注册到告警管理器
	m.alertManager.RegisterBreaker(cb)

	return cb
}

// GetOrCreateBreakerWithRetry 获取或创建带重试的熔断器
func (m *CircuitBreakerManager) GetOrCreateBreakerWithRetry(name string, config *CombinedConfig) *CircuitBreakerWithRetry {
	m.mu.Lock()
	defer m.mu.Unlock()

	if rb, exists := m.retryBreakers[name]; exists {
		return rb
	}

	rb := NewCircuitBreakerWithRetry(name, config)
	m.retryBreakers[name] = rb

	// 注册到告警管理器
	m.alertManager.RegisterBreaker(rb.GetCircuitBreaker())

	return rb
}

// GetBreaker 获取熔断器
func (m *CircuitBreakerManager) GetBreaker(name string) (*CircuitBreaker, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cb, exists := m.breakers[name]
	return cb, exists
}

// GetBreakerWithRetry 获取带重试的熔断器
func (m *CircuitBreakerManager) GetBreakerWithRetry(name string) (*CircuitBreakerWithRetry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rb, exists := m.retryBreakers[name]
	return rb, exists
}

// GetAllMetrics 获取所有熔断器的指标
func (m *CircuitBreakerManager) GetAllMetrics() map[string]MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metrics := make(map[string]MetricsSnapshot)

	for name, cb := range m.breakers {
		metrics[name] = cb.GetMetrics().GetSnapshot()
	}
	for name, rb := range m.retryBreakers {
		metrics[name] = rb.GetCircuitBreaker().GetMetrics().GetSnapshot()
	}
	return metrics
}

// ResetAll 重置所有熔断器
func (m *CircuitBreakerManager) ResetAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, cb := range m.breakers {
		cb.Reset()
	}

	for _, rb := range m.retryBreakers {
		rb.GetCircuitBreaker().Reset()
	}
}
