// Package deadletter 把 deadletter 组件的私有指标（QueueMetrics.GetStats）
// 适配为统一注册表可收录的 prometheus.Collector。
//
// 拉取式设计：每次抓取时实时调用 GetStats（含 Redis 队列长度与存储层统计），
// 不改动管理器本体；deadletter 组件零依赖本包（适配器模式，见路线图 3.1.2）。
// panic 安全：GetStats 依赖外部 Redis 与存储层，任一未初始化都会 panic
// （go-redis nil client / nil store 接口），适配器以 recover 兜底并经
// InvalidMetric 上报为抓取错误，不击穿抓取方。
package deadletter

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	xmdeadletter "github.com/xm-utils/tools/deadletter"
	"github.com/xm-utils/tools/metrics"
)

const component = "deadletter"

// Collector 实现 prometheus.Collector，输出 xmt_deadletter_* 指标，
// queue_key 维度取 stats.QueueKey，同进程多队列注册多个 Collector 不冲突。
type Collector struct {
	m *xmdeadletter.QueueMetrics

	messagesDesc *prometheus.Desc // xmt_deadletter_messages_total{queue_key,kind}
	lengthDesc   *prometheus.Desc // xmt_deadletter_queue_length{queue_key}
	dbDesc       *prometheus.Desc // xmt_deadletter_db_messages{queue_key,status}
	retryDesc    *prometheus.Desc // xmt_deadletter_db_avg_retry_count{queue_key}
	checkDesc    *prometheus.Desc // xmt_deadletter_last_check_timestamp_seconds{queue_key}
	errDesc      *prometheus.Desc // 抓取失败占位（InvalidMetric 用）
}

var _ prometheus.Collector = (*Collector)(nil)

// New 为死信队列指标建立采集器。m 由 deadletter.NewQueueMetrics 创建，
// QueueManager 内部实例需业务侧自行持有引用或经装配层注入。
func New(m *xmdeadletter.QueueMetrics) *Collector {
	return &Collector{
		m:            m,
		messagesDesc: desc("messages_total", "死信累计计数：dead/recovered/abandoned", "queue_key", "kind"),
		lengthDesc:   desc("queue_length", "死信队列当前长度（Redis List）", "queue_key"),
		dbDesc:       desc("db_messages", "存储层死信记录数按状态：pending/processing/processed/abandoned", "queue_key", "status"),
		retryDesc:    desc("db_avg_retry_count", "存储层死信平均重试次数", "queue_key"),
		checkDesc:    desc("last_check_timestamp_seconds", "上次巡检时间（Unix 秒）", "queue_key"),
		errDesc:      descNoLabels("collect_error", "抓取失败占位指标，失败时经 InvalidMetric 上报"),
	}
}

func desc(name, help string, labels ...string) *prometheus.Desc {
	fq := mustQualifiedName(name)
	return prometheus.NewDesc(fq, help, labels, nil)
}

func descNoLabels(name, help string) *prometheus.Desc {
	return prometheus.NewDesc(mustQualifiedName(name), help, nil, nil)
}

func mustQualifiedName(name string) string {
	fq, err := metrics.QualifiedName(component, name)
	if err != nil {
		fq = metrics.DefaultNamespace + "_" + component + "_invalid"
	}
	return fq
}

// MetricsCollectors 满足 metrics.Exporter 约定，便于 RegisterExporter 收录。
func (c *Collector) MetricsCollectors() []prometheus.Collector {
	return []prometheus.Collector{c}
}

// Describe 导出全部指标描述符。
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.messagesDesc
	ch <- c.lengthDesc
	ch <- c.dbDesc
	ch <- c.retryDesc
	ch <- c.checkDesc
}

// Collect 实时拉取队列统计并产出样本；panic/nil 转为 InvalidMetric。
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	stats, err := safeStats(c.m)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.errDesc, err)
		return
	}

	queueKey := stats.QueueKey
	ch <- prometheus.MustNewConstMetric(c.messagesDesc, prometheus.CounterValue, float64(stats.TotalDeadLetters), queueKey, "dead")
	ch <- prometheus.MustNewConstMetric(c.messagesDesc, prometheus.CounterValue, float64(stats.TotalRecovered), queueKey, "recovered")
	ch <- prometheus.MustNewConstMetric(c.messagesDesc, prometheus.CounterValue, float64(stats.TotalAbandoned), queueKey, "abandoned")
	ch <- prometheus.MustNewConstMetric(c.lengthDesc, prometheus.GaugeValue, float64(stats.CurrentLength), queueKey)
	ch <- prometheus.MustNewConstMetric(c.dbDesc, prometheus.GaugeValue, float64(stats.PendingCount), queueKey, "pending")
	ch <- prometheus.MustNewConstMetric(c.dbDesc, prometheus.GaugeValue, float64(stats.ProcessingCount), queueKey, "processing")
	ch <- prometheus.MustNewConstMetric(c.dbDesc, prometheus.GaugeValue, float64(stats.ProcessedCount), queueKey, "processed")
	ch <- prometheus.MustNewConstMetric(c.dbDesc, prometheus.GaugeValue, float64(stats.AbandonedCount), queueKey, "abandoned")
	ch <- prometheus.MustNewConstMetric(c.retryDesc, prometheus.GaugeValue, stats.AvgRetryCount, queueKey)
	if !stats.LastCheckTime.IsZero() {
		ch <- prometheus.MustNewConstMetric(c.checkDesc, prometheus.GaugeValue, float64(stats.LastCheckTime.Unix()), queueKey)
	}
}

// safeStats 调用 GetStats 并把外部依赖缺失引发的 panic 转为 error。
func safeStats(m *xmdeadletter.QueueMetrics) (stats *xmdeadletter.QueueStats, err error) {
	if m == nil {
		return nil, fmt.Errorf("metrics/adapter/deadletter: QueueMetrics 为 nil")
	}
	defer func() {
		if r := recover(); r != nil {
			stats, err = nil, fmt.Errorf("metrics/adapter/deadletter: GetStats panic: %v", r)
		}
	}()
	return m.GetStats(context.Background()), nil
}
