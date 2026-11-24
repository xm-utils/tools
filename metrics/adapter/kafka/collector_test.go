package kafka_test

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	adapter "github.com/xm-utils/tools/metrics/adapter/kafka"
	"github.com/xm-utils/tools/mq/kafka/client"
)

func gather(t *testing.T, c prometheus.Collector) map[string]*dto.MetricFamily {
	t.Helper()
	reg := prometheus.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("抓取失败: %v", err)
	}
	out := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

func findSample(t *testing.T, mfs map[string]*dto.MetricFamily, family string, labels map[string]string) float64 {
	t.Helper()
	mf, ok := mfs[family]
	if !ok {
		t.Fatalf("缺少指标族 %s", family)
	}
	for _, m := range mf.GetMetric() {
		match := true
		for k, v := range labels {
			found := false
			for _, lp := range m.GetLabel() {
				if lp.GetName() == k && lp.GetValue() == v {
					found = true
					break
				}
			}
			if !found {
				match = false
				break
			}
		}
		if match {
			if g := m.GetGauge(); g != nil {
				return g.GetValue()
			}
			return m.GetCounter().GetValue()
		}
	}
	t.Fatalf("指标 %s 中未找到 label %v 的样本", family, labels)
	return 0
}

func TestCollectorMapping(t *testing.T) {
	cm := client.NewConsumerMetrics(time.Minute)
	cm.RecordFetch(3, 30*time.Millisecond)
	cm.RecordFetch(2, 20*time.Millisecond)
	cm.RecordProcessTime(10 * time.Millisecond)
	cm.RecordError()

	mfs := gather(t, adapter.New(cm, "order-group"))
	name := map[string]string{"consumer": "order-group"}

	if v := findSample(t, mfs, "xmt_kafka_consumer_messages_total", name); v != 5 {
		t.Errorf("累计消息数期望 5, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_kafka_consumer_fetches_total", name); v != 2 {
		t.Errorf("累计拉取数期望 2, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_kafka_consumer_errors_total", name); v != 1 {
		t.Errorf("累计错误数期望 1, 得 %v", v)
	}
	win := merge(name, map[string]string{"kind": "messages"})
	if v := findSample(t, mfs, "xmt_kafka_consumer_window", win); v != 5 {
		t.Errorf("当前窗口消息数期望 5, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_kafka_consumer_uptime_seconds", name); v <= 0 {
		t.Errorf("uptime 应为正, 得 %v", v)
	}
	// 速率与平均耗时值依赖计时，只验证可产出且非负
	if v := findSample(t, mfs, "xmt_kafka_consumer_rate", merge(name, map[string]string{"kind": "message_recent"})); v < 0 {
		t.Errorf("recent 速率不应为负: %v", v)
	}
	if v := findSample(t, mfs, "xmt_kafka_consumer_process_time_seconds", merge(name, map[string]string{"stat": "avg"})); v < 0 {
		t.Errorf("平均耗时应非负: %v", v)
	}
}

func TestCollectorNilSafety(t *testing.T) {
	if mfs := gather(t, adapter.New(nil, "x")); len(mfs) != 0 {
		t.Errorf("nil ConsumerMetrics 应无输出, 得 %d 族", len(mfs))
	}
}

func TestMetricsCollectorsExporter(t *testing.T) {
	c := adapter.New(client.NewConsumerMetrics(time.Minute), "g")
	if got := c.MetricsCollectors(); len(got) != 1 || got[0] != prometheus.Collector(c) {
		t.Errorf("MetricsCollectors 应返回自身, 得 %v", got)
	}
}

func merge(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}
