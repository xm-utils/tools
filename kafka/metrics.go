package kafka

import (
	"fmt"
	"sync"
	"time"
)

// ConsumerMetrics 消费者性能指标
type ConsumerMetrics struct {
	mu sync.RWMutex

	// 时间窗口配置
	windowDuration time.Duration // 统计窗口时长

	// 当前窗口统计数据
	currentWindow *MetricsWindow

	// 历史窗口数据（用于计算趋势）
	historyWindows []*MetricsWindow
	maxHistory     int // 最大保留的历史窗口数

	// 累计统计（从启动开始）
	totalMessages    int64
	totalFetches     int64
	totalErrors      int64
	totalProcessTime time.Duration
	startTime        time.Time

	// 实时速率（最近1分钟）
	recentMessageRate float64 // 消息/秒
	recentFetchRate   float64 // 拉取/秒
	lastUpdate        time.Time
}

// MetricsWindow 时间窗口内的指标数据
type MetricsWindow struct {
	StartTime        time.Time
	EndTime          time.Time
	MessageCount     int64
	FetchCount       int64
	ErrorCount       int64
	TotalProcessTime time.Duration
	AvgProcessTime   time.Duration
	MessageRate      float64 // 消息/秒
	FetchRate        float64 // 拉取/秒
}

// NewConsumerMetrics 创建消费者指标收集器
func NewConsumerMetrics(windowDuration time.Duration) *ConsumerMetrics {
	return &ConsumerMetrics{
		windowDuration: windowDuration,
		currentWindow: &MetricsWindow{
			StartTime: time.Now(),
		},
		historyWindows: make([]*MetricsWindow, 0),
		maxHistory:     60, // 保留60个历史窗口
		startTime:      time.Now(),
		lastUpdate:     time.Now(),
	}
}

// RecordFetch 记录一次消息拉取
func (cm *ConsumerMetrics) RecordFetch(messageCount int, duration time.Duration) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 检查是否需要切换窗口
	cm.checkAndRotateWindow()

	// 更新当前窗口
	cm.currentWindow.FetchCount++
	cm.currentWindow.MessageCount += int64(messageCount)

	// 更新累计统计
	cm.totalFetches++
	cm.totalMessages += int64(messageCount)

	// 更新实时速率
	cm.updateRecentRates()
}

// RecordProcessTime 记录消息处理耗时
func (cm *ConsumerMetrics) RecordProcessTime(duration time.Duration) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 检查是否需要切换窗口
	cm.checkAndRotateWindow()

	// 更新当前窗口
	cm.currentWindow.TotalProcessTime += duration

	// 计算平均处理时间
	if cm.currentWindow.MessageCount > 0 {
		cm.currentWindow.AvgProcessTime = cm.currentWindow.TotalProcessTime / time.Duration(cm.currentWindow.MessageCount)
	}

	// 更新累计统计
	cm.totalProcessTime += duration
}

// RecordError 记录错误
func (cm *ConsumerMetrics) RecordError() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 检查是否需要切换窗口
	cm.checkAndRotateWindow()

	// 更新当前窗口
	cm.currentWindow.ErrorCount++

	// 更新累计统计
	cm.totalErrors++
}

// GetCurrentWindow 获取当前窗口的统计数据
func (cm *ConsumerMetrics) GetCurrentWindow() *MetricsWindow {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// 返回副本
	window := *cm.currentWindow
	return &window
}

// GetHistoryWindows 获取历史窗口数据
func (cm *ConsumerMetrics) GetHistoryWindows() []*MetricsWindow {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	result := make([]*MetricsWindow, len(cm.historyWindows))
	copy(result, cm.historyWindows)
	return result
}

// GetOverallStats 获取总体统计信息
func (cm *ConsumerMetrics) GetOverallStats() *OverallStats {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	uptime := time.Since(cm.startTime)

	var avgProcessTime time.Duration
	if cm.totalMessages > 0 {
		avgProcessTime = cm.totalProcessTime / time.Duration(cm.totalMessages)
	}

	return &OverallStats{
		StartTime:         cm.startTime,
		Uptime:            uptime,
		TotalMessages:     cm.totalMessages,
		TotalFetches:      cm.totalFetches,
		TotalErrors:       cm.totalErrors,
		AvgProcessTime:    avgProcessTime,
		MessageRate:       float64(cm.totalMessages) / uptime.Seconds(),
		FetchRate:         float64(cm.totalFetches) / uptime.Seconds(),
		RecentMessageRate: cm.recentMessageRate,
		RecentFetchRate:   cm.recentFetchRate,
	}
}

// OverallStats 总体统计信息
type OverallStats struct {
	StartTime         time.Time
	Uptime            time.Duration
	TotalMessages     int64
	TotalFetches      int64
	TotalErrors       int64
	AvgProcessTime    time.Duration
	MessageRate       float64 // 平均消息速率（消息/秒）
	FetchRate         float64 // 平均拉取速率（拉取/秒）
	RecentMessageRate float64 // 最近消息速率（消息/秒）
	RecentFetchRate   float64 // 最近拉取速率（拉取/秒）
}

// checkAndRotateWindow 检查并切换时间窗口
func (cm *ConsumerMetrics) checkAndRotateWindow() {
	now := time.Now()
	windowElapsed := now.Sub(cm.currentWindow.StartTime)

	if windowElapsed >= cm.windowDuration {
		// 完成当前窗口
		cm.currentWindow.EndTime = now

		// 计算窗口内的速率
		durationSeconds := windowElapsed.Seconds()
		if durationSeconds > 0 {
			cm.currentWindow.MessageRate = float64(cm.currentWindow.MessageCount) / durationSeconds
			cm.currentWindow.FetchRate = float64(cm.currentWindow.FetchCount) / durationSeconds

			if cm.currentWindow.MessageCount > 0 {
				cm.currentWindow.AvgProcessTime = cm.currentWindow.TotalProcessTime / time.Duration(cm.currentWindow.MessageCount)
			}
		}

		// 保存到历史记录
		cm.historyWindows = append(cm.historyWindows, cm.currentWindow)

		// 限制历史记录数量
		if len(cm.historyWindows) > cm.maxHistory {
			cm.historyWindows = cm.historyWindows[1:]
		}

		// 创建新窗口
		cm.currentWindow = &MetricsWindow{
			StartTime: now,
		}
	}
}

// updateRecentRates 更新实时速率（基于最近1分钟）
func (cm *ConsumerMetrics) updateRecentRates() {
	now := time.Now()
	elapsed := now.Sub(cm.lastUpdate).Seconds()

	// 每5秒更新一次速率
	if elapsed < 5 {
		return
	}

	// 计算最近窗口的消息数
	var recentMessages int64
	var recentFetches int64

	// 查看当前窗口
	recentMessages = cm.currentWindow.MessageCount
	recentFetches = cm.currentWindow.FetchCount

	// 加上最近的历史窗口
	for i := len(cm.historyWindows) - 1; i >= 0 && i >= len(cm.historyWindows)-12; i-- {
		window := cm.historyWindows[i]
		if now.Sub(window.EndTime) <= time.Minute {
			recentMessages += window.MessageCount
			recentFetches += window.FetchCount
		}
	}

	// 计算速率（消息/秒，拉取/秒）
	if elapsed > 0 {
		cm.recentMessageRate = float64(recentMessages) / 60.0 // 最近1分钟的平均速率
		cm.recentFetchRate = float64(recentFetches) / 60.0
	}

	cm.lastUpdate = now
}

// Reset 重置所有统计数据
func (cm *ConsumerMetrics) Reset() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.currentWindow = &MetricsWindow{
		StartTime: time.Now(),
	}
	cm.historyWindows = make([]*MetricsWindow, 0)
	cm.totalMessages = 0
	cm.totalFetches = 0
	cm.totalErrors = 0
	cm.totalProcessTime = 0
	cm.startTime = time.Now()
	cm.recentMessageRate = 0
	cm.recentFetchRate = 0
	cm.lastUpdate = time.Now()
}

// FormatStats 格式化输出统计信息
func (cm *ConsumerMetrics) FormatStats() string {
	stats := cm.GetOverallStats()
	currentWindow := cm.GetCurrentWindow()

	return formatStatsString(stats, currentWindow)
}

func formatStatsString(overall *OverallStats, current *MetricsWindow) string {
	return fmt.Sprintf(`
=== 消费者性能统计 ===
启动时间: %s
运行时长: %s

--- 累计统计 ---
总消息数: %d
总拉取次数: %d
总错误数: %d
平均处理耗时: %v
平均消息速率: %.2f 条/秒
平均拉取速率: %.2f 次/秒

--- 最近1分钟 ---
消息速率: %.2f 条/秒
拉取速率: %.2f 次/秒

--- 当前窗口 (%v) ---
消息数量: %d
拉取次数: %d
错误数量: %d
平均耗时: %v
消息速率: %.2f 条/秒
拉取速率: %.2f 次/秒
========================
`,
		overall.StartTime.Format("2006-01-02 15:04:05"),
		formatDuration(overall.Uptime),
		overall.TotalMessages,
		overall.TotalFetches,
		overall.TotalErrors,
		overall.AvgProcessTime,
		overall.MessageRate,
		overall.FetchRate,
		overall.RecentMessageRate,
		overall.RecentFetchRate,
		formatDuration(time.Since(current.StartTime)),
		current.MessageCount,
		current.FetchCount,
		current.ErrorCount,
		current.AvgProcessTime,
		current.MessageRate,
		current.FetchRate)
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return d.Round(time.Second).String()
	} else if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%d分%d秒", minutes, seconds)
	} else {
		hours := int(d.Hours())
		minutes := int(d.Minutes()) % 60
		return fmt.Sprintf("%d小时%d分", hours, minutes)
	}
}
