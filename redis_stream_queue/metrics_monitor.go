package redis_stream

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

const (
	// MetricStreamLength 监控指标 Key
	MetricStreamLength     = "metric:stream:length:%s"
	MetricConsumerLag      = "metric:consumer:lag:%s"
	MetricDeadLetterGrowth = "metric:dead_letter:growth:%s"
	MetricCallbackSuccess  = "metric:callback:success:%s"
	MetricCallbackFailure  = "metric:callback:failure:%s"

	// AlertStreamLengthThreshold 告警阈值
	AlertStreamLengthThreshold = 10000 // Stream 长度超过1万告警
	AlertConsumerLagThreshold  = 1000  // 消费延迟超过1000告警
	AlertDeadLetterGrowthRate  = 100   // 死信队列每小时增长超过100告警
	AlertCallbackFailureRate   = 0.1   // 回调失败率超过10%告警
)

// ==================== 监控与告警 ====================

// MetricsMonitor 监控指标管理器
type MetricsMonitor struct {
	log         *logrus.Entry
	alertConfig *AlertConfig
	client      *redis.Client // Redis Client
}

// NewMetricsMonitor 创建监控指标管理器
func NewMetricsMonitor(alertConfig *AlertConfig, client *redis.Client) *MetricsMonitor {
	if alertConfig == nil {
		alertConfig = DefaultAlertConfig
	}

	return &MetricsMonitor{
		log:         logrus.WithField("module", "metrics_monitor"),
		alertConfig: alertConfig,
		client:      client,
	}
}

// StartMonitoring 启动监控
func (m *MetricsMonitor) StartMonitoring(ctx context.Context, streamKeys []string) {
	m.log.Infof("启动监控服务,监控 %d 个 Stream", len(streamKeys))

	go func() {
		ticker := time.NewTicker(m.alertConfig.AlertCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				m.log.Info("监控服务已停止")
				return
			case <-ticker.C:
				for _, streamKey := range streamKeys {
					m.checkStreamMetrics(ctx, streamKey)
				}
			}
		}
	}()
}

// checkStreamMetrics 检查 Stream 监控指标
func (m *MetricsMonitor) checkStreamMetrics(ctx context.Context, streamKey string) {
	// 1. 检查 Stream 长度
	m.checkStreamLength(ctx, streamKey)

	// 2. 检查消费延迟
	m.checkConsumerLag(ctx, streamKey)

	// 3. 检查死信队列增长
	m.checkDeadLetterGrowth(ctx, streamKey)

	// 4. 检查回调成功率
	m.checkCallbackSuccessRate(ctx, streamKey)
}

// checkStreamLength 检查 Stream 长度
func (m *MetricsMonitor) checkStreamLength(ctx context.Context, streamKey string) {
	length, err := m.client.XLen(ctx, streamKey).Result()
	if err != nil {
		m.log.Errorf("获取 Stream 长度失败: stream=%s, err=%v", streamKey, err)
		return
	}

	// 记录指标
	metricKey := fmt.Sprintf(MetricStreamLength, streamKey)
	m.client.Set(ctx, metricKey, length, 5*time.Minute)

	// 检查是否超过阈值
	if length > m.alertConfig.StreamLengthThreshold {
		m.sendAlert(fmt.Sprintf("【严重】Stream 长度超限\nStream: %s\n当前长度: %d\n阈值: %d",
			streamKey, length, m.alertConfig.StreamLengthThreshold))
	}
}

// checkConsumerLag 检查消费延迟
func (m *MetricsMonitor) checkConsumerLag(ctx context.Context, streamKey string) {
	// 获取消费者组信息
	groups, err := m.client.XInfoGroups(ctx, streamKey).Result()
	if err != nil {
		m.log.Errorf("获取消费者组信息失败: stream=%s, err=%v", streamKey, err)
		return
	}

	for _, group := range groups {
		lag := group.Pending // Pending 消息数量
		metricKey := fmt.Sprintf(MetricConsumerLag, fmt.Sprintf("%s:%s", streamKey, group.Name))
		m.client.Set(ctx, metricKey, lag, 5*time.Minute)

		if lag > m.alertConfig.ConsumerLagThreshold {
			m.sendAlert(fmt.Sprintf("【警告】消费延迟过高\nStream: %s\n消费者组: %s\nPending 数量: %d\n阈值: %d",
				streamKey, group.Name, lag, m.alertConfig.ConsumerLagThreshold))
		}
	}
}

// checkDeadLetterGrowth 检查死信队列增长
func (m *MetricsMonitor) checkDeadLetterGrowth(ctx context.Context, streamKey string) {
	length, err := m.client.LLen(ctx, DeadLetterKey).Result()
	if err != nil {
		m.log.Errorf("获取死信队列长度失败: key=%s, err=%v", DeadLetterKey, err)
		return
	}

	// 记录当前值
	now := time.Now().Unix()
	metricKey := fmt.Sprintf(MetricDeadLetterGrowth, streamKey)

	// 获取1小时前的值
	oneHourAgo := now - 3600
	oldValue, err := m.client.HGet(ctx, metricKey, fmt.Sprintf("%d", oneHourAgo)).Int64()
	if err != nil {
		oldValue = 0
	}

	growth := length - oldValue
	m.client.HSet(ctx, metricKey, fmt.Sprintf("%d", now), length)
	m.client.Expire(ctx, metricKey, 24*time.Hour)

	// 检查增长率
	if growth > m.alertConfig.DeadLetterGrowthRate {
		m.sendAlert(fmt.Sprintf("【严重】死信队列增长过快\nStream: %s\n1小时增长: %d\n阈值: %d\n当前总数: %d",
			streamKey, growth, m.alertConfig.DeadLetterGrowthRate, length))
	}
}

// checkCallbackSuccessRate 检查回调成功率
func (m *MetricsMonitor) checkCallbackSuccessRate(ctx context.Context, streamKey string) {
	successKey := fmt.Sprintf(MetricCallbackSuccess, streamKey)
	failureKey := fmt.Sprintf(MetricCallbackFailure, streamKey)

	successCount, _ := m.client.Get(ctx, successKey).Float64()
	failureCount, _ := m.client.Get(ctx, failureKey).Float64()

	total := successCount + failureCount
	if total == 0 {
		return // 没有数据
	}

	failureRate := failureCount / total
	if failureRate > m.alertConfig.CallbackFailureRate {
		m.sendAlert(fmt.Sprintf("【严重】回调失败率过高\nStream: %s\n失败率: %.2f%%\n阈值: %.2f%%\n成功: %.0f, 失败: %.0f",
			streamKey, failureRate*100, m.alertConfig.CallbackFailureRate*100, successCount, failureCount))
	}
}

// sendAlert 发送告警
func (m *MetricsMonitor) sendAlert(message string) {
	// 检查告警冷却时间
	alertKey := "passage:alert:cooldown"
	exists := m.client.Exists(context.Background(), alertKey).Val() > 0
	if exists {
		m.log.Warnf("告警冷却中,跳过发送: %s", message)
		return
	}

	// 设置冷却时间
	m.client.SetEX(context.Background(), alertKey, "1", m.alertConfig.AlertCooldown)

	// 记录告警日志
	m.log.Errorf("【告警】%s", message)

	// TODO: 集成实际的告警通知渠道
	// 选项1: 发送邮件
	// 选项2: 发送钉钉/企业微信 webhook
	// 选项3: 调用告警平台 API
	// 示例: sendEmailAlert(message)
	// 示例: sendWebhookAlert(message)
}

// RecordCallbackSuccess 记录回调成功
func (m *MetricsMonitor) RecordCallbackSuccess(ctx context.Context, streamKey string) {
	key := fmt.Sprintf(MetricCallbackSuccess, streamKey)
	m.client.Incr(ctx, key)
	m.client.Expire(ctx, key, 24*time.Hour)
}

// RecordCallbackFailure 记录回调失败
func (m *MetricsMonitor) RecordCallbackFailure(ctx context.Context, streamKey string) {
	key := fmt.Sprintf(MetricCallbackFailure, streamKey)
	m.client.Incr(ctx, key)
	m.client.Expire(ctx, key, 24*time.Hour)
}

// GetMetrics 获取监控指标 (用于 Dashboard)
func (m *MetricsMonitor) GetMetrics(ctx context.Context, streamKey string) map[string]interface{} {
	metrics := make(map[string]interface{})

	// Stream 长度
	length, _ := m.client.XLen(ctx, streamKey).Result()
	metrics["stream_length"] = length

	// 死信队列长度
	deadLetterLength, _ := m.client.LLen(ctx, DeadLetterKey).Result()
	metrics["dead_letter_length"] = deadLetterLength

	// 回调成功率
	successKey := fmt.Sprintf(MetricCallbackSuccess, streamKey)
	failureKey := fmt.Sprintf(MetricCallbackFailure, streamKey)
	successCount, _ := m.client.Get(ctx, successKey).Float64()
	failureCount, _ := m.client.Get(ctx, failureKey).Float64()
	total := successCount + failureCount

	if total > 0 {
		metrics["callback_success_rate"] = successCount / total * 100
	} else {
		metrics["callback_success_rate"] = 0
	}

	metrics["callback_success_count"] = successCount
	metrics["callback_failure_count"] = failureCount

	return metrics
}
