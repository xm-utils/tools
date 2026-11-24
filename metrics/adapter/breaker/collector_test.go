package breaker_test

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	xmbreaker "github.com/xm-utils/tools/breaker"
	adapter "github.com/xm-utils/tools/metrics/adapter/breaker"
)

// gather 注册采集器到独立注册表并抓取，返回按指标全名索引的 MetricFamily。
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

// findSample 在指定指标族中按 label 精确查找样本值。
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

func TestCollectorSingleBreaker(t *testing.T) {
	cb := xmbreaker.NewCircuitBreaker("order-api", nil)
	m := cb.GetMetrics()
	m.RecordSuccess(120 * time.Millisecond)
	m.RecordSuccess(80 * time.Millisecond)
	m.RecordFailure()
	m.RecordRejected()
	m.RecordStateChange(xmbreaker.StateClosed, xmbreaker.StateOpen)

	mfs := gather(t, adapter.New(cb))

	name := map[string]string{"name": "order-api"}
	if v := findSample(t, mfs, "xmt_breaker_requests_total", merge(name, map[string]string{"result": "success"})); v != 2 {
		t.Errorf("success 计数期望 2, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_breaker_requests_total", merge(name, map[string]string{"result": "failure"})); v != 1 {
		t.Errorf("failure 计数期望 1, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_breaker_requests_total", merge(name, map[string]string{"result": "rejected"})); v != 1 {
		t.Errorf("rejected 计数期望 1, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_breaker_request_duration_seconds", merge(name, map[string]string{"stat": "min"})); v != 0.08 {
		t.Errorf("min 耗时期望 0.08s, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_breaker_state_change_total", merge(name, map[string]string{"transition": "closed_to_open"})); v != 1 {
		t.Errorf("状态变更期望 1, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_breaker_request_rate", merge(name, map[string]string{"kind": "success"})); v <= 0 || v > 1 {
		t.Errorf("成功率期望 (0,1], 得 %v", v)
	}
	if _, ok := mfs["xmt_breaker_last_update_timestamp_seconds"]; !ok {
		t.Error("缺少最近更新时间指标")
	}
}

func TestCollectorNoRequestSkipsDuration(t *testing.T) {
	cb := xmbreaker.NewCircuitBreaker("idle", nil)
	mfs := gather(t, adapter.New(cb))

	// 无成功请求时不产出 min/avg/max（初始哨兵 1h 会误导），但保留零值计数
	if _, ok := mfs["xmt_breaker_request_duration_seconds"]; ok {
		t.Error("空统计不应产出耗时指标")
	}
	if _, ok := mfs["xmt_breaker_request_rate"]; ok {
		t.Error("空统计不应产出占比指标")
	}
	if v := findSample(t, mfs, "xmt_breaker_requests_total", map[string]string{"name": "idle", "result": "success"}); v != 0 {
		t.Errorf("空统计计数期望 0, 得 %v", v)
	}
}

func TestCollectorFromManager(t *testing.T) {
	mgr := xmbreaker.GetManager()
	cb1 := mgr.GetOrCreateBreaker("svc-a", nil)
	cb2 := mgr.GetOrCreateBreaker("svc-b", nil)
	cb1.GetMetrics().RecordSuccess(time.Millisecond)
	cb2.GetMetrics().RecordFailure()

	mfs := gather(t, adapter.NewFromManager(mgr))
	findSample(t, mfs, "xmt_breaker_requests_total", map[string]string{"name": "svc-a", "result": "success"})
	findSample(t, mfs, "xmt_breaker_requests_total", map[string]string{"name": "svc-b", "result": "failure"})
}

func TestCollectorNilSafety(t *testing.T) {
	// nil 入参不 panic，产出空样本
	for _, m := range []map[string]*dto.MetricFamily{
		gather(t, adapter.New(nil)),
		gather(t, adapter.NewFromManager(nil)),
	} {
		if len(m) != 0 {
			t.Errorf("nil 入参应无输出, 得 %d 个指标族", len(m))
		}
	}
}

func TestMetricsCollectorsExporter(t *testing.T) {
	c := adapter.New(xmbreaker.NewCircuitBreaker("e", nil))
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
