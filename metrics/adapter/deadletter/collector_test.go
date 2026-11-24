package deadletter_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	xmdeadletter "github.com/xm-utils/tools/deadletter"
	adapter "github.com/xm-utils/tools/metrics/adapter/deadletter"
	"github.com/xm-utils/tools/redis"
)

// fakeStore 实现 deadletter.PersistenceStore，仅统计接口有效。
type fakeStore struct {
	stats   *xmdeadletter.DatabaseStats
	panicFn func()
}

func (f *fakeStore) Save(ctx context.Context, record *xmdeadletter.QueueMsgRecord) error { return nil }
func (f *fakeStore) BatchSave(ctx context.Context, records []*xmdeadletter.QueueMsgRecord) error {
	return nil
}
func (f *fakeStore) UpdateStatus(ctx context.Context, queueKey, messageID string, status xmdeadletter.QueueStatus, processedTime *time.Time) error {
	return nil
}
func (f *fakeStore) FindByMessageID(ctx context.Context, queueKey, messageID string) (*xmdeadletter.QueueMsgRecord, error) {
	return nil, errors.New("not found")
}
func (f *fakeStore) GetStats(ctx context.Context, queueKey string) (*xmdeadletter.DatabaseStats, error) {
	if f.panicFn != nil {
		f.panicFn()
	}
	return f.stats, nil
}

func gather(t *testing.T, c prometheus.Collector) (map[string]*dto.MetricFamily, error) {
	t.Helper()
	reg := prometheus.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	mfs, err := reg.Gather()
	out := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out, err
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

func TestCollectorMapping(t *testing.T) {
	mr := miniredis.RunT(t)
	if err := redis.InitRedisCache(&redis.Config{Host: mr.Addr(), Prefix: "xp"}); err != nil {
		t.Fatalf("初始化 redis 失败: %v", err)
	}
	// GetStats 读 dead_letter:<queueKey>，经前缀拼装后为 xp:dead_letter:q_ok
	mr.Push("xp:dead_letter:q_ok", "m1", "m2")

	store := &fakeStore{stats: &xmdeadletter.DatabaseStats{
		PendingCount: 7, ProcessingCount: 1, ProcessedCount: 3, AbandonedCount: 2, AvgRetryCount: 1.5,
	}}
	m := xmdeadletter.NewQueueMetrics("q_ok", store)
	m.RecordDeadLetter()
	m.RecordDeadLetter()
	m.RecordRecovery()
	m.RecordAbandoned()

	mfs, err := gather(t, adapter.New(m))
	if err != nil {
		t.Fatalf("抓取失败: %v", err)
	}
	key := map[string]string{"queue_key": "q_ok"}

	if v := findSample(t, mfs, "xmt_deadletter_messages_total", merge(key, map[string]string{"kind": "dead"})); v != 2 {
		t.Errorf("总死信期望 2, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_deadletter_messages_total", merge(key, map[string]string{"kind": "recovered"})); v != 1 {
		t.Errorf("总恢复期望 1, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_deadletter_queue_length", key); v != 2 {
		t.Errorf("队列长度期望 2, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_deadletter_db_messages", merge(key, map[string]string{"status": "pending"})); v != 7 {
		t.Errorf("pending 期望 7, 得 %v", v)
	}
	if v := findSample(t, mfs, "xmt_deadletter_db_avg_retry_count", key); v != 1.5 {
		t.Errorf("平均重试期望 1.5, 得 %v", v)
	}
	// LastCheckTime 未经巡检器更新时为零值，不应产出
	if _, ok := mfs["xmt_deadletter_last_check_timestamp_seconds"]; ok {
		t.Error("零值巡检时间不应产出指标")
	}
}

func TestCollectorPanicSafety(t *testing.T) {
	// 存储层实现 panic 时，适配器须兜底为抓取错误而非击穿抓取方
	store := &fakeStore{panicFn: func() { panic("store down") }}
	m := xmdeadletter.NewQueueMetrics("q_panic", store)

	_, err := gather(t, adapter.New(m))
	if err == nil {
		t.Fatal("GetStats panic 应转为抓取错误")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Errorf("错误信息应含 panic 说明, 得: %v", err)
	}
}

func TestCollectorNilMetrics(t *testing.T) {
	_, err := gather(t, adapter.New(nil))
	if err == nil || !strings.Contains(err.Error(), "nil") {
		t.Errorf("nil QueueMetrics 应报抓取错误, 得: %v", err)
	}
}

func TestMetricsCollectorsExporter(t *testing.T) {
	c := adapter.New(xmdeadletter.NewQueueMetrics("q", &fakeStore{stats: &xmdeadletter.DatabaseStats{}}))
	if got := c.MetricsCollectors(); len(got) != 1 || got[0] != prometheus.Collector(c) {
		t.Errorf("MetricsCollectors 应返回自身, 得 %v", got)
	}
}
