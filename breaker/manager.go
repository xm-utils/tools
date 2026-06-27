package breaker

import (
	"sync"
)

// CircuitBreakerManager 熔断器管理器(单例模式)
type CircuitBreakerManager struct {
	breakers     map[string]*CircuitBreaker
	alertManager *AlertManager
	mu           sync.RWMutex
}

var (
	instance *CircuitBreakerManager
	once     sync.Once
)

// GetManager 获取熔断器管理器单例
func GetManager() *CircuitBreakerManager {
	once.Do(func() {
		instance = &CircuitBreakerManager{
			breakers:     make(map[string]*CircuitBreaker),
			alertManager: NewAlertManager(DefaultAlertConfig()),
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

// GetBreaker 获取熔断器
func (m *CircuitBreakerManager) GetBreaker(name string) (*CircuitBreaker, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cb, exists := m.breakers[name]
	return cb, exists
}

// GetAllMetrics 获取所有熔断器的指标
func (m *CircuitBreakerManager) GetAllMetrics() map[string]MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metrics := make(map[string]MetricsSnapshot)

	for name, cb := range m.breakers {
		metrics[name] = cb.GetMetrics().GetSnapshot()
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
}
