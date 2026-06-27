package circuitbreaker

import (
	"time"

	"github.com/sirupsen/logrus"
)

// AlertLevel 告警级别
type AlertLevel int

const (
	AlertLevelInfo AlertLevel = iota
	AlertLevelWarn
	AlertLevelError
	AlertLevelCritical
)

func (l AlertLevel) String() string {
	switch l {
	case AlertLevelInfo:
		return "INFO"
	case AlertLevelWarn:
		return "WARN"
	case AlertLevelError:
		return "ERROR"
	case AlertLevelCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// AlertConfig 告警配置
type AlertConfig struct {
	Enabled                bool                    // 是否启用告警
	OpenStateAlertDelay    time.Duration           // Open状态持续多久后告警(默认1分钟)
	RejectionRateThreshold float64                 // 拒绝率阈值(如0.8表示80%)
	AlertCallback          func(alert *AlertEvent) // 告警回调函数
}

// DefaultAlertConfig 返回默认告警配置
func DefaultAlertConfig() *AlertConfig {
	return &AlertConfig{
		Enabled:                true,
		OpenStateAlertDelay:    1 * time.Minute,
		RejectionRateThreshold: 0.8,
	}
}

// AlertEvent 告警事件
type AlertEvent struct {
	Timestamp   time.Time
	Level       AlertLevel
	CircuitName string
	Message     string
	State       State
	Metrics     MetricsSnapshot
}

// AlertManager 告警管理器
type AlertManager struct {
	config   *AlertConfig
	breakers map[string]*CircuitBreaker
	alerted  map[string]time.Time // 记录已告警的熔断器和时间
	log      *logrus.Entry
}

// NewAlertManager 创建告警管理器
func NewAlertManager(config *AlertConfig) *AlertManager {
	if config == nil {
		config = DefaultAlertConfig()
	}

	return &AlertManager{
		config:   config,
		breakers: make(map[string]*CircuitBreaker),
		alerted:  make(map[string]time.Time),
		log: logrus.WithFields(logrus.Fields{
			"module": "CircuitBreakerAlert",
		}),
	}
}

// RegisterBreaker 注册熔断器进行监控
func (am *AlertManager) RegisterBreaker(cb *CircuitBreaker) {
	am.breakers[cb.GetName()] = cb
}

// StartMonitoring 启动监控
func (am *AlertManager) StartMonitoring() {
	if !am.config.Enabled {
		am.log.Info("告警监控未启用")
		return
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		am.log.Info("熔断器告警监控已启动")

		for range ticker.C {
			am.checkAndAlert()
		}
	}()
}

// checkAndAlert 检查并发送告警
func (am *AlertManager) checkAndAlert() {
	for name, cb := range am.breakers {
		state := cb.GetState()
		metrics := cb.GetMetrics().GetSnapshot()

		// 1. 检查Open状态持续时间
		if state == StateOpen {
			am.checkOpenStateAlert(name, cb, metrics)
		}

		// 2. 检查拒绝率
		am.checkRejectionRateAlert(name, cb, metrics)
	}
}

// checkOpenStateAlert 检查Open状态告警
func (am *AlertManager) checkOpenStateAlert(name string, cb *CircuitBreaker, metrics MetricsSnapshot) {
	lastAlertTime, hasAlerted := am.alerted[name+"_open"]

	// 如果已经告警过,检查是否需要再次告警(避免频繁告警)
	if hasAlerted && time.Since(lastAlertTime) < am.config.OpenStateAlertDelay {
		return
	}

	// 发送告警
	alert := &AlertEvent{
		Timestamp:   time.Now(),
		Level:       AlertLevelWarn,
		CircuitName: name,
		Message:     "熔断器处于OPEN状态, 下游服务可能故障",
		State:       StateOpen,
		Metrics:     metrics,
	}

	am.sendAlert(alert)
	am.alerted[name+"_open"] = alert.Timestamp
}

// checkRejectionRateAlert 检查拒绝率告警
func (am *AlertManager) checkRejectionRateAlert(name string, cb *CircuitBreaker, metrics MetricsSnapshot) {
	if metrics.RejectionRate >= am.config.RejectionRateThreshold && metrics.TotalRequests > 10 {
		lastAlertTime, hasAlerted := am.alerted[name+"_rejection"]

		// 如果已经告警过,5分钟内不再重复告警
		if hasAlerted && time.Since(lastAlertTime) < 5*time.Minute {
			return
		}

		alert := &AlertEvent{
			Timestamp:   time.Now(),
			Level:       AlertLevelError,
			CircuitName: name,
			Message:     "熔断器拒绝率过高, 可能存在严重问题",
			State:       cb.GetState(),
			Metrics:     metrics,
		}

		am.sendAlert(alert)
		am.alerted[name+"_rejection"] = alert.Timestamp
	}
}

// sendAlert 发送告警
func (am *AlertManager) sendAlert(alert *AlertEvent) {
	// 日志告警
	switch alert.Level {
	case AlertLevelWarn:
		am.log.Warnf("[告警] %s - %s", alert.CircuitName, alert.Message)
	case AlertLevelError:
		am.log.Errorf("[告警] %s - %s", alert.CircuitName, alert.Message)
	case AlertLevelCritical:
		am.log.Errorf("[严重告警] %s - %s", alert.CircuitName, alert.Message)
	default:
		am.log.Infof("[告警] %s - %s", alert.CircuitName, alert.Message)
	}

	// 调用自定义回调
	if am.config.AlertCallback != nil {
		am.config.AlertCallback(alert)
	}

	// 打印详细指标
	am.log.Infof("[告警详情] 总请求=%d, 成功=%d, 失败=%d, 拒绝=%d, 拒绝率=%.2f%%",
		alert.Metrics.TotalRequests,
		alert.Metrics.SuccessRequests,
		alert.Metrics.FailedRequests,
		alert.Metrics.RejectedRequests,
		alert.Metrics.RejectionRate*100,
	)
}
