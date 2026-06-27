package circuitbreaker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	// ErrCircuitOpen 熔断器打开错误
	ErrCircuitOpen = errors.New("circuit breaker is open")
	// ErrTooManyRequests Half-Open状态下请求过多
	ErrTooManyRequests = errors.New("too many requests in half-open state")
)

// CircuitBreaker 熔断器
type CircuitBreaker struct {
	name   string
	config *CircuitBreakerConfig
	state  State
	window *SlidingWindow
	mu     sync.RWMutex

	// Open状态相关
	openedAt time.Time // 进入Open状态的时间

	// Half-Open状态相关
	halfOpenRequests int // Half-Open状态下的请求数

	// 监控指标
	metrics *CircuitBreakerMetrics

	log *logrus.Entry
}

// NewCircuitBreaker 创建熔断器
func NewCircuitBreaker(name string, config *CircuitBreakerConfig) *CircuitBreaker {
	if config == nil {
		config = DefaultCircuitBreakerConfig()
	}

	cb := &CircuitBreaker{
		name:    name,
		config:  config,
		state:   StateClosed,
		window:  NewSlidingWindow(config),
		metrics: NewCircuitBreakerMetrics(name),
		log: logrus.WithFields(logrus.Fields{
			"module":         "CircuitBreaker",
			"circuitBreaker": name,
		}),
	}

	// 启动状态监控协程
	go cb.monitorState()

	return cb
}

// Execute 执行受保护的调用
func (cb *CircuitBreaker) Execute(fn func() (interface{}, error), args ...interface{}) (interface{}, error) {
	// 1. 检查熔断器状态
	if !cb.allowRequest() {
		cb.metrics.RecordRejected()
		cb.log.Warnf("熔断器处于OPEN状态, 拒绝请求: %s", cb.name)

		// 执行降级函数
		if cb.config.FallbackFunc != nil {
			cb.log.Infof("执行降级函数: %s", cb.name)
			return cb.config.FallbackFunc(args...)
		}

		return nil, ErrCircuitOpen
	}

	// 2. 执行实际调用
	startTime := time.Now()
	result, err := fn()
	duration := time.Since(startTime)

	// 3. 记录结果
	cb.handleResult(duration, err)

	return result, err
}

// ExecuteWithContext 带上下文的执行方法
func (cb *CircuitBreaker) ExecuteWithContext(ctx context.Context, fn func(ctx context.Context) (interface{}, error), args ...interface{}) (interface{}, error) {
	// 1. 检查熔断器状态
	if !cb.allowRequest() {
		cb.metrics.RecordRejected()
		cb.log.Warnf("熔断器处于OPEN状态, 拒绝请求: %s", cb.name)

		// 执行降级函数
		if cb.config.FallbackFunc != nil {
			cb.log.Infof("执行降级函数: %s", cb.name)
			return cb.config.FallbackFunc(args...)
		}

		return nil, ErrCircuitOpen
	}

	// 2. 执行实际调用
	startTime := time.Now()
	result, err := fn(ctx)
	duration := time.Since(startTime)

	// 3. 记录结果
	cb.handleResult(duration, err)

	return result, err
}

// allowRequest 判断是否允许请求通过
func (cb *CircuitBreaker) allowRequest() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	switch cb.state {
	case StateClosed:
		return true

	case StateOpen:
		// 检查是否超过等待时长
		if time.Since(cb.openedAt) > cb.config.WaitDurationInOpenState {
			return true // 允许进入Half-Open状态
		}
		return false

	case StateHalfOpen:
		// 限制Half-Open状态下的请求数
		return cb.halfOpenRequests < cb.config.HalfOpenMaxRequests

	default:
		return false
	}
}

// handleResult 处理调用结果
func (cb *CircuitBreaker) handleResult(duration time.Duration, err error) {
	success := err == nil
	cb.window.RecordRequest(duration, success)

	// 更新指标
	if success {
		cb.metrics.RecordSuccess(duration)
	} else {
		cb.metrics.RecordFailure()
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		cb.checkClosedState()

	case StateHalfOpen:
		cb.halfOpenRequests++
		cb.checkHalfOpenState(success)
	}
}

// checkClosedState 检查Closed状态是否需要转为Open
func (cb *CircuitBreaker) checkClosedState() {
	// 检查是否达到最小请求数
	if !cb.window.CanEvaluate() {
		return
	}

	total, errors, slowCalls := cb.window.GetStats()
	if total == 0 {
		return
	}

	// 计算错误率
	errorRate := float64(errors) / float64(total)
	slowCallRate := float64(slowCalls) / float64(total)

	// 判断是否触发熔断
	if errorRate >= cb.config.ErrorThreshold || slowCallRate >= cb.config.SlowCallThreshold {
		cb.log.Warnf("触发熔断: errorRate=%.2f, slowCallRate=%.2f, total=%d", errorRate, slowCallRate, total)
		cb.transitionTo(StateOpen)
	}
}

// checkHalfOpenState 检查Half-Open状态
func (cb *CircuitBreaker) checkHalfOpenState(success bool) {
	if success {
		// 探测成功,恢复到Closed状态
		cb.log.Infof("Half-Open状态探测成功, 恢复为CLOSED状态: %s", cb.name)
		cb.transitionTo(StateClosed)
		cb.window.Reset()
	} else {
		// 探测失败,重新进入Open状态
		cb.log.Warnf("Half-Open状态探测失败, 重新进入OPEN状态: %s", cb.name)
		cb.transitionTo(StateOpen)
	}
}

// transitionTo 状态转换
func (cb *CircuitBreaker) transitionTo(newState State) {
	oldState := cb.state
	cb.state = newState

	cb.log.Infof("熔断器状态变更: %s -> %s", oldState, newState)

	// 记录指标
	cb.metrics.RecordStateChange(oldState, newState)

	// 触发回调
	if cb.config.OnStateChange != nil {
		cb.config.OnStateChange(oldState, newState)
	}

	// 状态特定处理
	switch newState {
	case StateOpen:
		cb.openedAt = time.Now()
		cb.halfOpenRequests = 0

	case StateHalfOpen:
		cb.halfOpenRequests = 0

	case StateClosed:
		cb.halfOpenRequests = 0
	}
}

// monitorState 监控状态变化(Closed -> Half-Open的自动转换)
func (cb *CircuitBreaker) monitorState() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		cb.mu.Lock()
		if cb.state == StateOpen && time.Since(cb.openedAt) > cb.config.WaitDurationInOpenState {
			cb.log.Infof("等待期结束, 从OPEN转为HALF_OPEN: %s", cb.name)
			cb.state = StateHalfOpen
			cb.halfOpenRequests = 0
			cb.metrics.RecordStateChange(StateOpen, StateHalfOpen)

			if cb.config.OnStateChange != nil {
				cb.config.OnStateChange(StateOpen, StateHalfOpen)
			}
		}
		cb.mu.Unlock()
	}
}

// GetState 获取当前状态
func (cb *CircuitBreaker) GetState() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetMetrics 获取监控指标
func (cb *CircuitBreaker) GetMetrics() *CircuitBreakerMetrics {
	return cb.metrics
}

// Reset 重置熔断器
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	oldState := cb.state
	cb.state = StateClosed
	cb.window.Reset()
	cb.halfOpenRequests = 0

	cb.log.Info("熔断器已重置")
	cb.metrics.RecordStateChange(oldState, StateClosed)

	if cb.config.OnStateChange != nil {
		cb.config.OnStateChange(oldState, StateClosed)
	}
}

// GetName 获取熔断器名称
func (cb *CircuitBreaker) GetName() string {
	return cb.name
}
