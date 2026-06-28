package deadletter

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/redis"
)

// QueueMetrics 队列监控指标
type QueueMetrics struct {
	queueKey          string
	store             PersistenceStore
	totalDeadLetters  int64 // 总死信数
	totalRecovered    int64 // 总恢复数
	totalAbandoned    int64 // 总放弃数
	lastCheckTime     time.Time
	currentQueueLen   int64
	avgProcessingTime float64
	log               *logrus.Entry
}

// NewQueueMetrics 创建监控指标
func NewQueueMetrics(queueKey string, store PersistenceStore) *QueueMetrics {
	return &QueueMetrics{
		queueKey: queueKey,
		store:    store,
		log: logrus.WithFields(logrus.Fields{
			"module":   "QueueMetrics",
			"queueKey": queueKey,
		}),
	}
}

// RecordDeadLetter 记录死信消息
func (m *QueueMetrics) RecordDeadLetter() {
	atomic.AddInt64(&m.totalDeadLetters, 1)
}

// RecordRecovery 记录恢复消息
func (m *QueueMetrics) RecordRecovery() {
	atomic.AddInt64(&m.totalRecovered, 1)
}

// RecordAbandoned 记录放弃消息
func (m *QueueMetrics) RecordAbandoned() {
	atomic.AddInt64(&m.totalAbandoned, 1)
}

// GetStats 获取队列统计信息
func (m *QueueMetrics) GetStats(ctx context.Context) *QueueStats {
	queueLen, _ := redis.LLen(ctx, fmt.Sprintf("dead_letter:%s", m.queueKey))

	// 从数据库获取详细统计(如果store存在)
	dbStats := m.getDatabaseStats()

	return &QueueStats{
		QueueKey:         m.queueKey,
		CurrentLength:    queueLen,
		TotalDeadLetters: atomic.LoadInt64(&m.totalDeadLetters),
		TotalRecovered:   atomic.LoadInt64(&m.totalRecovered),
		TotalAbandoned:   atomic.LoadInt64(&m.totalAbandoned),
		PendingCount:     dbStats.PendingCount,
		ProcessingCount:  dbStats.ProcessingCount,
		ProcessedCount:   dbStats.ProcessedCount,
		AbandonedCount:   dbStats.AbandonedCount,
		AvgRetryCount:    dbStats.AvgRetryCount,
		LastCheckTime:    m.lastCheckTime,
		CheckTime:        time.Now(),
	}
}

// getDatabaseStats 从持久化存储获取统计信息
func (m *QueueMetrics) getDatabaseStats() *DatabaseStats {
	stats, err := m.store.GetStats(context.Background(), m.queueKey)
	if err != nil {
		m.log.Errorf("获取数据库统计信息失败: %v", err)
		stats = &DatabaseStats{}
	}
	return stats
}

// QueueStats 队列统计信息
type QueueStats struct {
	QueueKey         string    `json:"queueKey"`
	CurrentLength    int64     `json:"currentLength"`    // 当前队列长度(Redis)
	TotalDeadLetters int64     `json:"totalDeadLetters"` // 总死信数
	TotalRecovered   int64     `json:"totalRecovered"`   // 总恢复数
	TotalAbandoned   int64     `json:"totalAbandoned"`   // 总放弃数
	PendingCount     int64     `json:"pendingCount"`     // 待处理数(数据库)
	ProcessingCount  int64     `json:"processingCount"`  // 处理中数(数据库)
	ProcessedCount   int64     `json:"processedCount"`   // 已处理数(数据库)
	AbandonedCount   int64     `json:"abandonedCount"`   // 已放弃数(数据库)
	AvgRetryCount    float64   `json:"avgRetryCount"`    // 平均重试次数
	LastCheckTime    time.Time `json:"lastCheckTime"`    // 上次检查时间
	CheckTime        time.Time `json:"checkTime"`        // 本次检查时间
}

// MetricsMonitor 监控服务
type MetricsMonitor struct {
	manager   *QueueManager
	interval  time.Duration
	ctx       context.Context
	cancel    context.CancelFunc
	log       *logrus.Entry
	alertFunc func(stats *QueueStats) // 告警回调函数
}

// NewMetricsMonitor 创建监控服务
func NewMetricsMonitor(manager *QueueManager, interval time.Duration, alertFunc func(stats *QueueStats)) *MetricsMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	return &MetricsMonitor{
		manager:  manager,
		interval: interval,
		ctx:      ctx,
		cancel:   cancel,
		log: logrus.WithFields(logrus.Fields{
			"module": "MetricsMonitor",
		}),
		alertFunc: alertFunc,
	}
}

// Start 启动监控服务
func (mm *MetricsMonitor) Start() {
	go func() {
		ticker := time.NewTicker(mm.interval)
		defer ticker.Stop()

		mm.log.Infof("队列监控服务已启动, 检查间隔: %v", mm.interval)

		for {
			select {
			case <-mm.ctx.Done():
				mm.log.Info("队列监控服务已停止")
				return
			case <-ticker.C:
				mm.checkAndReport()
			}
		}
	}()
}

// checkAndReport 检查并报告队列状态
func (mm *MetricsMonitor) checkAndReport() {
	stats := mm.manager.metrics.GetStats(mm.ctx)
	mm.manager.metrics.lastCheckTime = stats.CheckTime

	// 输出监控日志
	mm.log.Infof("队列监控统计 - 队列长度: %d, 总死信: %d, 总恢复: %d, 总放弃: %d, 待处理: %d, 平均重试: %.2f",
		stats.CurrentLength,
		stats.TotalDeadLetters,
		stats.TotalRecovered,
		stats.TotalAbandoned,
		stats.PendingCount,
		stats.AvgRetryCount,
	)

	// 触发告警检查
	if mm.alertFunc != nil {
		mm.checkAlerts(stats)
	}
}

// checkAlerts 检查告警条件
func (mm *MetricsMonitor) checkAlerts(stats *QueueStats) {
	// 告警条件1: 队列长度超过阈值
	if stats.CurrentLength > 1000 {
		mm.log.Warnf("[告警] 死信队列长度过大: %d", stats.CurrentLength)
		mm.alertFunc(stats)
	}

	// 告警条件2: 待处理消息过多
	if stats.PendingCount > 500 {
		mm.log.Warnf("[告警] 待处理死信消息过多: %d", stats.PendingCount)
		mm.alertFunc(stats)
	}

	// 告警条件3: 平均重试次数过高
	if stats.AvgRetryCount > 2.5 {
		mm.log.Warnf("[告警] 平均重试次数过高: %.2f", stats.AvgRetryCount)
		mm.alertFunc(stats)
	}

	// 告警条件4: 恢复率过低
	if stats.TotalDeadLetters > 100 {
		recoveryRate := float64(stats.TotalRecovered) / float64(stats.TotalDeadLetters)
		if recoveryRate < 0.5 {
			mm.log.Warnf("[告警] 死信恢复率过低: %.2f%%", recoveryRate*100)
			mm.alertFunc(stats)
		}
	}
}

// Stop 停止监控服务
func (mm *MetricsMonitor) Stop() {
	if mm.cancel != nil {
		mm.cancel()
	}
	mm.log.Info("监控服务已停止")
}
