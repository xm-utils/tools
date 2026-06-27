package breaker

import (
	"sync"
	"time"
)

// CircuitBreakerMetrics 熔断器监控指标
type CircuitBreakerMetrics struct {
	name string

	// 请求统计
	totalRequests    int64
	successRequests  int64
	failedRequests   int64
	rejectedRequests int64 // 被熔断器拒绝的请求数

	// 响应时间统计
	totalDuration time.Duration
	minDuration   time.Duration
	maxDuration   time.Duration
	avgDuration   time.Duration

	// 状态变更统计
	stateChanges map[string]int64 // key: "FROM->TO", value: count

	// 最后更新时间
	lastUpdated time.Time

	mu sync.RWMutex
}

// NewCircuitBreakerMetrics 创建熔断器指标
func NewCircuitBreakerMetrics(name string) *CircuitBreakerMetrics {
	return &CircuitBreakerMetrics{
		name:         name,
		stateChanges: make(map[string]int64),
		lastUpdated:  time.Now(),
		minDuration:  time.Hour, // 初始化为较大值
	}
}

// RecordSuccess 记录成功请求
func (m *CircuitBreakerMetrics) RecordSuccess(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalRequests++
	m.successRequests++
	m.updateDuration(duration)
	m.lastUpdated = time.Now()
}

// RecordFailure 记录失败请求
func (m *CircuitBreakerMetrics) RecordFailure() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalRequests++
	m.failedRequests++
	m.lastUpdated = time.Now()
}

// RecordRejected 记录被拒绝的请求
func (m *CircuitBreakerMetrics) RecordRejected() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.rejectedRequests++
	m.lastUpdated = time.Now()
}

// RecordStateChange 记录状态变更
func (m *CircuitBreakerMetrics) RecordStateChange(from, to State) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := from.String() + "->" + to.String()
	m.stateChanges[key]++
	m.lastUpdated = time.Now()
}

// updateDuration 更新响应时间统计
func (m *CircuitBreakerMetrics) updateDuration(duration time.Duration) {
	m.totalDuration += duration

	if duration < m.minDuration {
		m.minDuration = duration
	}
	if duration > m.maxDuration {
		m.maxDuration = duration
	}

	if m.successRequests > 0 {
		m.avgDuration = m.totalDuration / time.Duration(m.successRequests)
	}
}

// GetSnapshot 获取指标快照
func (m *CircuitBreakerMetrics) GetSnapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot := MetricsSnapshot{
		Name:             m.name,
		TotalRequests:    m.totalRequests,
		SuccessRequests:  m.successRequests,
		FailedRequests:   m.failedRequests,
		RejectedRequests: m.rejectedRequests,
		MinDuration:      m.minDuration,
		MaxDuration:      m.maxDuration,
		AvgDuration:      m.avgDuration,
		StateChanges:     make(map[string]int64),
		LastUpdated:      m.lastUpdated,
	}

	// 复制状态变更统计
	for k, v := range m.stateChanges {
		snapshot.StateChanges[k] = v
	}

	// 计算成功率
	if m.totalRequests > 0 {
		snapshot.SuccessRate = float64(m.successRequests) / float64(m.totalRequests)
		snapshot.FailureRate = float64(m.failedRequests) / float64(m.totalRequests)
		snapshot.RejectionRate = float64(m.rejectedRequests) / float64(m.totalRequests)
	}

	return snapshot
}

// MetricsSnapshot 指标快照
type MetricsSnapshot struct {
	Name             string
	TotalRequests    int64
	SuccessRequests  int64
	FailedRequests   int64
	RejectedRequests int64
	SuccessRate      float64
	FailureRate      float64
	RejectionRate    float64
	MinDuration      time.Duration
	MaxDuration      time.Duration
	AvgDuration      time.Duration
	StateChanges     map[string]int64
	LastUpdated      time.Time
}

// String 格式化输出指标
func (s *MetricsSnapshot) String() string {
	return formatMetrics(s)
}

// formatMetrics 格式化指标字符串
func formatMetrics(snapshot *MetricsSnapshot) string {
	return ""
}
