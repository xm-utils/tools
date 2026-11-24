// Package kafka 把 mq/kafka/client 的私有指标（ConsumerMetrics 累计统计与
// 当前时间窗口）适配为统一注册表可收录的 prometheus.Collector。
//
// 拉取式设计：每次抓取时实时读 ConsumerMetrics，不改动消费者本体；
// mq/kafka 组件零依赖本包（适配器模式，见架构路线图 3.1.2 兼容策略）。
package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/xm-utils/tools/metrics"
	"github.com/xm-utils/tools/mq/kafka/client"
)

const component = "kafka_consumer"

// Collector 实现 prometheus.Collector，输出 xmt_kafka_consumer_* 指标。
// consumer 参数作为常量 label 值，用于同进程多消费者实例区分。
type Collector struct {
	m        *client.ConsumerMetrics
	consumer string

	messageDesc    *prometheus.Desc // xmt_kafka_consumer_messages_total{consumer}
	fetchDesc      *prometheus.Desc // xmt_kafka_consumer_fetches_total{consumer}
	errorDesc      *prometheus.Desc
	rateDesc       *prometheus.Desc // xmt_kafka_consumer_rate{consumer,kind}
	processDesc    *prometheus.Desc // xmt_kafka_consumer_process_time_seconds{consumer,stat}
	uptimeDesc     *prometheus.Desc // xmt_kafka_consumer_uptime_seconds{consumer}
	windowDesc     *prometheus.Desc // xmt_kafka_consumer_window_{messages,fetches,errors}
	windowTimeDesc *prometheus.Desc // xmt_kafka_consumer_window_process_time_seconds{consumer,stat}
}

var _ prometheus.Collector = (*Collector)(nil)

// New 为消费者指标收集器建立适配器，consumer 为实例标识（如 topic 或 group 名）。
func New(m *client.ConsumerMetrics, consumer string) *Collector {
	return &Collector{
		m:              m,
		consumer:       consumer,
		messageDesc:    desc("messages_total", "累计消费消息数", "consumer"),
		fetchDesc:      desc("fetches_total", "累计拉取次数", "consumer"),
		errorDesc:      desc("errors_total", "累计错误数", "consumer"),
		rateDesc:       desc("rate", "消费速率（条/秒 或 次/秒）：message_avg/message_recent/fetch_avg/fetch_recent", "consumer", "kind"),
		processDesc:    desc("process_time_seconds", "消息处理耗时（秒），avg 为累计平均", "consumer", "stat"),
		uptimeDesc:     desc("uptime_seconds", "指标统计器运行时长（秒）", "consumer"),
		windowDesc:     desc("window", "当前时间窗口计数：messages/fetches/errors", "consumer", "kind"),
		windowTimeDesc: desc("window_process_time_seconds", "当前窗口处理耗时（秒），total/avg", "consumer", "stat"),
	}
}

func desc(name, help string, labels ...string) *prometheus.Desc {
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
	ch <- c.messageDesc
	ch <- c.fetchDesc
	ch <- c.errorDesc
	ch <- c.rateDesc
	ch <- c.processDesc
	ch <- c.uptimeDesc
	ch <- c.windowDesc
	ch <- c.windowTimeDesc
}

// Collect 实时读取累计统计与当前窗口并产出样本。
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	if c.m == nil {
		return
	}
	o := c.m.GetOverallStats()
	ch <- prometheus.MustNewConstMetric(c.messageDesc, prometheus.CounterValue, float64(o.TotalMessages), c.consumer)
	ch <- prometheus.MustNewConstMetric(c.fetchDesc, prometheus.CounterValue, float64(o.TotalFetches), c.consumer)
	ch <- prometheus.MustNewConstMetric(c.errorDesc, prometheus.CounterValue, float64(o.TotalErrors), c.consumer)
	ch <- prometheus.MustNewConstMetric(c.rateDesc, prometheus.GaugeValue, o.MessageRate, c.consumer, "message_avg")
	ch <- prometheus.MustNewConstMetric(c.rateDesc, prometheus.GaugeValue, o.RecentMessageRate, c.consumer, "message_recent")
	ch <- prometheus.MustNewConstMetric(c.rateDesc, prometheus.GaugeValue, o.FetchRate, c.consumer, "fetch_avg")
	ch <- prometheus.MustNewConstMetric(c.rateDesc, prometheus.GaugeValue, o.RecentFetchRate, c.consumer, "fetch_recent")
	ch <- prometheus.MustNewConstMetric(c.processDesc, prometheus.GaugeValue, o.AvgProcessTime.Seconds(), c.consumer, "avg")
	ch <- prometheus.MustNewConstMetric(c.uptimeDesc, prometheus.GaugeValue, o.Uptime.Seconds(), c.consumer)

	w := c.m.GetCurrentWindow()
	ch <- prometheus.MustNewConstMetric(c.windowDesc, prometheus.GaugeValue, float64(w.MessageCount), c.consumer, "messages")
	ch <- prometheus.MustNewConstMetric(c.windowDesc, prometheus.GaugeValue, float64(w.FetchCount), c.consumer, "fetches")
	ch <- prometheus.MustNewConstMetric(c.windowDesc, prometheus.GaugeValue, float64(w.ErrorCount), c.consumer, "errors")
	ch <- prometheus.MustNewConstMetric(c.windowTimeDesc, prometheus.GaugeValue, w.TotalProcessTime.Seconds(), c.consumer, "total")
	ch <- prometheus.MustNewConstMetric(c.windowTimeDesc, prometheus.GaugeValue, w.AvgProcessTime.Seconds(), c.consumer, "avg")
}
