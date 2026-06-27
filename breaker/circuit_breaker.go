package breaker

import (
	"sync"
	"time"
)

// State 熔断器状态
type State int

const (
	// StateClosed 关闭状态 - 正常流量通过
	StateClosed State = iota
	// StateOpen 打开状态 - 拒绝所有请求
	StateOpen
	// StateHalfOpen 半开状态 - 允许探测请求
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// CircuitBreakerConfig 熔断器配置
type CircuitBreakerConfig struct {
	// 滑动窗口配置
	WindowType  WindowType    // 窗口类型(时间或计数)
	WindowSize  time.Duration // 时间窗口大小(如10s)
	WindowCount int           // 计数窗口大小(如100次)

	// 错误阈值配置
	ErrorThreshold    float64       // 错误率阈值(0-1, 如0.5表示50%)
	SlowCallThreshold float64       // 慢调用比例阈值(0-1)
	SlowCallDuration  time.Duration // 慢调用判定阈值(如3s)

	// 最小请求数
	MinRequests int // 窗口期内最小请求数才进行统计

	// 等待时长
	WaitDurationInOpenState time.Duration // Open状态持续时间(如30s)

	// 半开状态配置
	HalfOpenMaxRequests int // Half-Open状态允许的最大探测请求数

	// 降级策略
	FallbackFunc FallbackFunc // 降级函数(可选)

	// 状态变更回调
	OnStateChange func(oldState, newState State) // 状态变更回调(可选)
}

// DefaultCircuitBreakerConfig 返回默认配置
func DefaultCircuitBreakerConfig() *CircuitBreakerConfig {
	return &CircuitBreakerConfig{
		WindowType:              WindowTypeTime,
		WindowSize:              10 * time.Second,
		WindowCount:             100,
		ErrorThreshold:          0.5,
		SlowCallThreshold:       0.5,
		SlowCallDuration:        3 * time.Second,
		MinRequests:             10,
		WaitDurationInOpenState: 30 * time.Second,
		HalfOpenMaxRequests:     5,
	}
}

// WindowType 窗口类型
type WindowType int

const (
	// WindowTypeTime 基于时间的滑动窗口
	WindowTypeTime WindowType = iota
	// WindowTypeCount 基于计数的滑动窗口
	WindowTypeCount
)

// FallbackFunc 降级函数类型
type FallbackFunc func(args ...interface{}) (interface{}, error)

// RequestRecord 请求记录
type RequestRecord struct {
	Timestamp time.Time
	Duration  time.Duration
	Success   bool
	SlowCall  bool
}

// SlidingWindow 滑动窗口统计器
type SlidingWindow struct {
	config    *CircuitBreakerConfig
	records   []RequestRecord
	mu        sync.RWMutex
	startTime time.Time // 时间窗口的起始时间
	count     int       // 当前窗口的请求计数(用于计数窗口)
}

// NewSlidingWindow 创建滑动窗口
func NewSlidingWindow(config *CircuitBreakerConfig) *SlidingWindow {
	return &SlidingWindow{
		config:    config,
		records:   make([]RequestRecord, 0),
		startTime: time.Now(),
		count:     0,
	}
}

// RecordRequest 记录请求
func (w *SlidingWindow) RecordRequest(duration time.Duration, success bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()
	slowCall := duration >= w.config.SlowCallDuration

	record := RequestRecord{
		Timestamp: now,
		Duration:  duration,
		Success:   success,
		SlowCall:  slowCall,
	}

	w.records = append(w.records, record)

	// 根据窗口类型清理过期数据
	if w.config.WindowType == WindowTypeTime {
		w.cleanExpiredRecords(now)
	} else {
		w.cleanExcessRecords()
	}
}

// GetStats 获取窗口统计数据
func (w *SlidingWindow) GetStats() (total int, errors int, slowCalls int) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	total = len(w.records)
	for _, record := range w.records {
		if !record.Success {
			errors++
		}
		if record.SlowCall {
			slowCalls++
		}
	}

	return total, errors, slowCalls
}

// CanEvaluate 是否可以评估熔断条件
func (w *SlidingWindow) CanEvaluate() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.config.WindowType == WindowTypeTime {
		return len(w.records) >= w.config.MinRequests
	} else {
		return w.count >= w.config.MinRequests
	}
}

// cleanExpiredRecords 清理过期的时间窗口记录
func (w *SlidingWindow) cleanExpiredRecords(now time.Time) {
	cutoff := now.Add(-w.config.WindowSize)
	validRecords := make([]RequestRecord, 0)

	for _, record := range w.records {
		if record.Timestamp.After(cutoff) {
			validRecords = append(validRecords, record)
		}
	}

	w.records = validRecords
}

// cleanExcessRecords 清理超出计数窗口的记录
func (w *SlidingWindow) cleanExcessRecords() {
	w.count++
	if len(w.records) > w.config.WindowCount {
		// 保留最新的WindowCount条记录
		w.records = w.records[len(w.records)-w.config.WindowCount:]
	}
}

// Reset 重置窗口
func (w *SlidingWindow) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.records = make([]RequestRecord, 0)
	w.startTime = time.Now()
	w.count = 0
}
