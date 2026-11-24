// Package breaker 把 breaker 组件的私有指标（CircuitBreakerMetrics 快照 /
// CircuitBreakerManager 聚合快照）适配为统一注册表可收录的 prometheus.Collector。
//
// 拉取式设计：每次抓取时实时读快照，不改动、不包装熔断器本体；
// breaker 组件零依赖本包（适配器模式，见架构路线图 3.1.2 兼容策略）。
package breaker

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	xmbreaker "github.com/xm-utils/tools/breaker"
	"github.com/xm-utils/tools/metrics"
)

const component = "breaker"

// Collector 实现 prometheus.Collector，将熔断器指标快照翻译为 xmt_breaker_* 指标。
// 通过 New / NewFromManager 构造，注册方式：metrics.Register(collector)。
type Collector struct {
	provider func() map[string]xmbreaker.MetricsSnapshot

	requestsDesc *prometheus.Desc // xmt_breaker_requests_total{name,result}
	durationDesc *prometheus.Desc // xmt_breaker_request_duration_seconds{name,stat}
	rateDesc     *prometheus.Desc // xmt_breaker_request_rate{name,kind}
	changeDesc   *prometheus.Desc // xmt_breaker_state_change_total{name,transition}
	updatedDesc  *prometheus.Desc // xmt_breaker_last_update_timestamp_seconds{name}
}

var _ prometheus.Collector = (*Collector)(nil)

// New 为单个熔断器建立采集器，快照实时拉取。
func New(cb *xmbreaker.CircuitBreaker) *Collector {
	return newCollector(func() map[string]xmbreaker.MetricsSnapshot {
		if cb == nil {
			return nil
		}
		s := cb.GetMetrics().GetSnapshot()
		return map[string]xmbreaker.MetricsSnapshot{cb.GetName(): s}
	})
}

// NewFromManager 为熔断器管理器建立采集器，每次抓取聚合其下全部熔断器快照。
func NewFromManager(m *xmbreaker.CircuitBreakerManager) *Collector {
	return newCollector(func() map[string]xmbreaker.MetricsSnapshot {
		if m == nil {
			return nil
		}
		return m.GetAllMetrics()
	})
}

func newCollector(provider func() map[string]xmbreaker.MetricsSnapshot) *Collector {
	return &Collector{
		provider:     provider,
		requestsDesc: desc(component, "requests_total", "熔断器请求计数，按结果维度 success/failure/rejected 区分", "name", "result"),
		durationDesc: desc(component, "request_duration_seconds", "成功请求耗时统计（秒）：min/avg/max", "name", "stat"),
		rateDesc:     desc(component, "request_rate", "请求占比：success/failure/rejection", "name", "kind"),
		changeDesc:   desc(component, "state_change_total", "状态变更次数，transition 形如 closed_to_open", "name", "transition"),
		updatedDesc:  desc(component, "last_update_timestamp_seconds", "指标最近更新时间（Unix 秒）", "name"),
	}
}

// desc 按统一命名规范构造描述符；名字非法时（如 Init 传入了坏前缀）退化为
// 显式可识别的占位名，抓取阶段会在 README 说明的自检中暴露，不影响运行。
func desc(component, name, help string, labels ...string) *prometheus.Desc {
	fq, err := metrics.QualifiedName(component, name)
	if err != nil {
		fq = metrics.DefaultNamespace + "_" + component + "_invalid"
	}
	return prometheus.NewDesc(fq, help, labels, nil)
}

// MetricsCollectors 满足 metrics.Exporter 约定，便于 RegisterExporter 收录。
func (c *Collector) MetricsCollectors() []prometheus.Collector {
	return []prometheus.Collector{c}
}

// Describe 导出全部指标描述符。
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.requestsDesc
	ch <- c.durationDesc
	ch <- c.rateDesc
	ch <- c.changeDesc
	ch <- c.updatedDesc
}

// Collect 实时拉取快照并产出指标样本。
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	for name, s := range c.provider() {
		ch <- prometheus.MustNewConstMetric(c.requestsDesc, prometheus.CounterValue, float64(s.SuccessRequests), name, "success")
		ch <- prometheus.MustNewConstMetric(c.requestsDesc, prometheus.CounterValue, float64(s.FailedRequests), name, "failure")
		ch <- prometheus.MustNewConstMetric(c.requestsDesc, prometheus.CounterValue, float64(s.RejectedRequests), name, "rejected")

		// 无任何成功请求时 min 仍是初始化哨兵值（1h），产出会误导，跳过耗时统计。
		if s.SuccessRequests > 0 {
			ch <- prometheus.MustNewConstMetric(c.durationDesc, prometheus.GaugeValue, s.MinDuration.Seconds(), name, "min")
			ch <- prometheus.MustNewConstMetric(c.durationDesc, prometheus.GaugeValue, s.AvgDuration.Seconds(), name, "avg")
			ch <- prometheus.MustNewConstMetric(c.durationDesc, prometheus.GaugeValue, s.MaxDuration.Seconds(), name, "max")
		}

		if s.TotalRequests > 0 {
			ch <- prometheus.MustNewConstMetric(c.rateDesc, prometheus.GaugeValue, s.SuccessRate, name, "success")
			ch <- prometheus.MustNewConstMetric(c.rateDesc, prometheus.GaugeValue, s.FailureRate, name, "failure")
			ch <- prometheus.MustNewConstMetric(c.rateDesc, prometheus.GaugeValue, s.RejectionRate, name, "rejection")
		}

		for transition, count := range s.StateChanges {
			ch <- prometheus.MustNewConstMetric(c.changeDesc, prometheus.CounterValue, float64(count), name, normalizeTransition(transition))
		}

		if !s.LastUpdated.IsZero() {
			ch <- prometheus.MustNewConstMetric(c.updatedDesc, prometheus.GaugeValue,
				float64(s.LastUpdated.Unix()), name)
		}
	}
}

// normalizeTransition 把快照 map 键转成小写规范形式，如 "CLOSED->OPEN" → "closed_to_open"。
func normalizeTransition(t string) string {
	from, to, ok := strings.Cut(t, "->")
	if !ok {
		return strings.ToLower(strings.ReplaceAll(t, " ", "_"))
	}
	return strings.ToLower(strings.TrimSpace(from)) + "_to_" + strings.ToLower(strings.TrimSpace(to))
}
