package metrics

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

var (
	mu       sync.RWMutex
	ns       = DefaultNamespace
	registry = newDefaultRegistry()
)

// newDefaultRegistry 创建带运行时指标的默认注册表（Go collector）。
func newDefaultRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	return reg
}

// Namespace 返回当前指标名前缀（默认 xmt，Init 可覆盖）。
func Namespace() string {
	mu.RLock()
	defer mu.RUnlock()
	return ns
}

// Registry 返回统一注册表，供高级场景（自定义 Gatherer、federate）直接使用。
// 返回值随 Init 重建，调用方不应长期持有。
func Registry() *prometheus.Registry {
	mu.RLock()
	defer mu.RUnlock()
	return registry
}

// Register 把采集器逐个注册进统一注册表；重复注册等同名指标返回 error，不 panic。
// 任一采集器失败即中断并返回该错误（已注册部分不回滚）。
func Register(cs ...prometheus.Collector) error {
	for _, c := range cs {
		if c == nil {
			return ErrNilCollector
		}
		if err := Registry().Register(c); err != nil {
			return err
		}
	}
	return nil
}

// Exporter 是组件暴露 Prometheus 采集器的可选约定接口：
// 组件（或其适配器）实现 MetricsCollectors 后即可被 RegisterExporter 收录，
// 组件本体无需 import 本包——结构类型天然解耦。
// 各组件私有 metrics 的接入一律通过 metrics/adapter/* 下的适配器实现本接口形态。
type Exporter interface {
	MetricsCollectors() []prometheus.Collector
}

// RegisterExporter 收录 Exporter 提供的全部采集器，collector 为 nil 时返回 error。
func RegisterExporter(e Exporter) error {
	if e == nil {
		return ErrNilExporter
	}
	return Register(e.MetricsCollectors()...)
}

// lazyGatherer 每次抓取时实时解析当前注册表，Init 重建后 Handler 无需更换。
type lazyGatherer struct{}

func (lazyGatherer) Gather() ([]*dto.MetricFamily, error) { return Registry().Gather() }

// Handler 返回统一注册表的 /metrics HTTP 处理器（Prometheus 文本格式）。
// 挂载方式：标准库 mux / Gin / grpcx cmux 的 HTTP 端口均可，见 README。
// 每次请求实时读取当前注册表，Init 之后无需重建 Handler。
func Handler() http.Handler {
	return promhttp.HandlerFor(lazyGatherer{}, promhttp.HandlerOpts{})
}

// NewCounterVec 按命名规范创建 CounterVec 并注册进统一注册表。
// labelNames 至少一个；无 label 指标请直接使用 prometheus.NewCounter + Register。
func NewCounterVec(component, name, help string, labelNames ...string) (*prometheus.CounterVec, error) {
	fq, err := QualifiedName(component, name)
	if err != nil {
		return nil, err
	}
	labels, err := normalizeLabels(labelNames)
	if err != nil {
		return nil, err
	}
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: fq,
		Help: help,
	}, labels)
	if err := Register(c); err != nil {
		return nil, err
	}
	return c, nil
}

// NewGaugeVec 按命名规范创建 GaugeVec 并注册进统一注册表。
func NewGaugeVec(component, name, help string, labelNames ...string) (*prometheus.GaugeVec, error) {
	fq, err := QualifiedName(component, name)
	if err != nil {
		return nil, err
	}
	labels, err := normalizeLabels(labelNames)
	if err != nil {
		return nil, err
	}
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fq,
		Help: help,
	}, labels)
	if err := Register(g); err != nil {
		return nil, err
	}
	return g, nil
}

// NewHistogramVec 按命名规范创建 HistogramVec 并注册进统一注册表。
// buckets 为 nil 时使用 prometheus.DefBuckets。
func NewHistogramVec(component, name, help string, buckets []float64, labelNames ...string) (*prometheus.HistogramVec, error) {
	fq, err := QualifiedName(component, name)
	if err != nil {
		return nil, err
	}
	labels, err := normalizeLabels(labelNames)
	if err != nil {
		return nil, err
	}
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    fq,
		Help:    help,
		Buckets: buckets,
	}, labels)
	if err := Register(h); err != nil {
		return nil, err
	}
	return h, nil
}

// NewGaugeFunc 按命名规范创建拉取式 GaugeFunc（无 label）并注册进统一注册表。
// 适合把组件私有快照值（如队列长度）桥接为指标：f 在每次抓取时被调用。
func NewGaugeFunc(component, name, help string, f func() float64) (prometheus.GaugeFunc, error) {
	if f == nil {
		return nil, ErrNilGaugeFunc
	}
	fq, err := QualifiedName(component, name)
	if err != nil {
		return nil, err
	}
	gf := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: fq,
		Help: help,
	}, f)
	if err := Register(gf); err != nil {
		return nil, err
	}
	return gf, nil
}
