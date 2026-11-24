package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// resetGlobal 测试后恢复默认 namespace 与注册表，避免污染其他用例。
func resetGlobal(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		if err := Init(Config{}); err != nil {
			t.Fatalf("恢复默认配置失败: %v", err)
		}
	})
}

func TestQualifiedName(t *testing.T) {
	resetGlobal(t)

	fq, err := QualifiedName("breaker", "requests_total")
	if err != nil || fq != "xmt_breaker_requests_total" {
		t.Fatalf("QualifiedName 期望 xmt_breaker_requests_total, 得 %q err=%v", fq, err)
	}

	if _, err := QualifiedName("", "x"); err == nil {
		t.Error("空 component 应报错")
	}
	if _, err := QualifiedName("c", ""); err == nil {
		t.Error("空 name 应报错")
	}
	if _, err := QualifiedName("bad-component!", "x"); err == nil {
		t.Error("非法字符应报错")
	}
}

func TestInitNamespaceAndValidation(t *testing.T) {
	resetGlobal(t)

	if err := Init(Config{Namespace: "myapp"}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	if Namespace() != "myapp" {
		t.Fatalf("namespace 期望 myapp, 得 %s", Namespace())
	}
	fq, err := QualifiedName("c", "m")
	if err != nil || fq != "myapp_c_m" {
		t.Fatalf("自定义前缀拼装错误: %q err=%v", fq, err)
	}

	if err := Init(Config{Namespace: "9bad"}); err == nil {
		t.Error("数字开头 namespace 应报错")
	}
	if Namespace() != "myapp" {
		t.Error("非法 Init 不应改变现有配置")
	}
}

func TestInitRebuildsRegistry(t *testing.T) {
	resetGlobal(t)

	c, err := NewCounterVec("test", "before_init_total", "help", "kind")
	if err != nil {
		t.Fatalf("NewCounterVec 失败: %v", err)
	}
	if err := Init(Config{}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	// 旧 collector 不在新注册表中，重新注册应成功且可再次抓取
	if err := Register(c); err != nil {
		t.Fatalf("重建后重新注册失败: %v", err)
	}
	if _, err := Registry().Gather(); err != nil {
		t.Fatalf("Gather 失败: %v", err)
	}
}

func TestConstructorsAndDuplicateRegister(t *testing.T) {
	resetGlobal(t)

	if _, err := NewCounterVec("dup", "x_total", "help", "a"); err != nil {
		t.Fatalf("首次注册 counter 失败: %v", err)
	}
	if _, err := NewCounterVec("dup", "x_total", "help", "a"); err == nil {
		t.Error("重复注册应返回 error 而非 panic")
	}

	if _, err := NewGaugeVec("g", "level", "help", "name"); err != nil {
		t.Errorf("NewGaugeVec 失败: %v", err)
	}
	if _, err := NewHistogramVec("h", "dur_seconds", "help", []float64{0.1, 0.5}, "op"); err != nil {
		t.Errorf("NewHistogramVec 失败: %v", err)
	}
	if _, err := NewHistogramVec("h2", "dur_seconds", "help", nil, "op"); err != nil {
		t.Errorf("nil buckets 应回落 DefBuckets: %v", err)
	}
	if _, err := NewGaugeFunc("f", "snap", "help", func() float64 { return 1 }); err != nil {
		t.Errorf("NewGaugeFunc 失败: %v", err)
	}

	// label 校验
	if _, err := NewGaugeVec("bad", "m", "help"); err == nil {
		t.Error("无 label 的 Vec 构造应报错")
	}
	if _, err := NewGaugeVec("bad", "m", "help", "a", "a"); err == nil {
		t.Error("重复 label 应报错")
	}
	if _, err := NewGaugeFunc("bad", "m", "help", nil); err != ErrNilGaugeFunc {
		t.Errorf("nil func 应返回 ErrNilGaugeFunc, 得 %v", err)
	}
}

type fakeExporter struct {
	collector prometheus.Collector
}

func (f *fakeExporter) MetricsCollectors() []prometheus.Collector {
	if f.collector == nil {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "xmt_fake_value"})
		g.Set(42)
		f.collector = g
	}
	return []prometheus.Collector{f.collector}
}

func TestRegisterExporterAndHandler(t *testing.T) {
	resetGlobal(t)

	if err := RegisterExporter(nil); err != ErrNilExporter {
		t.Errorf("nil exporter 应返回 ErrNilExporter, 得 %v", err)
	}

	e := &fakeExporter{}
	if err := RegisterExporter(e); err != nil {
		t.Fatalf("RegisterExporter 失败: %v", err)
	}

	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("Handler 状态码 %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "xmt_fake_value 42") {
		t.Errorf("输出缺少 exporter 指标:\n%s", rec.Body.String())
	}
}

func TestRegisterNilCollector(t *testing.T) {
	resetGlobal(t)
	if err := Register(nil); err != ErrNilCollector {
		t.Errorf("期望 ErrNilCollector, 得 %v", err)
	}
}
