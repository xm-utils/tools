package metrics

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ErrNilCollector 传入 nil 采集器
	ErrNilCollector = errors.New("metrics: collector 为 nil")
	// ErrNilExporter 传入 nil Exporter
	ErrNilExporter = errors.New("metrics: exporter 为 nil")
	// ErrNilGaugeFunc GaugeFunc 的取值函数为 nil
	ErrNilGaugeFunc = errors.New("metrics: gauge func 为 nil")
)

// Config 统一指标出口配置，字段与 config.example.yaml 一一对应。
type Config struct {
	// Namespace 指标名前缀，空值取 DefaultNamespace（xmt）。
	Namespace string `yaml:"namespace" json:"namespace"`
	// EnableGoCollector 是否注册 Go 运行时指标（默认 true）。
	EnableGoCollector *bool `yaml:"enable_go_collector" json:"enable_go_collector"`
	// EnableProcessCollector 是否注册进程指标（RSS、启动时间等，默认 false）。
	EnableProcessCollector bool `yaml:"enable_process_collector" json:"enable_process_collector"`
}

// Init 按配置重建统一注册表并应用命名前缀。
// 应在进程启动、注册任何业务指标之前调用一次；重复调用会丢弃旧注册表中
// 已注册的指标（适配器 Collector 实例可重新 Register）。非法前缀返回 error，
// 此时保持原配置不变，不 panic。
func Init(cfg Config) error {
	next := cfg.Namespace
	if next == "" {
		next = DefaultNamespace
	}
	if !isValidMetricName(next + "_x") {
		return fmt.Errorf("metrics: 非法 namespace %q（仅允许字母、数字、下划线、冒号，且不以数字开头）", next)
	}

	reg := prometheus.NewRegistry()
	if cfg.EnableGoCollector == nil || *cfg.EnableGoCollector {
		if err := reg.Register(prometheus.NewGoCollector()); err != nil {
			return fmt.Errorf("metrics: 注册 Go collector 失败: %w", err)
		}
	}
	if cfg.EnableProcessCollector {
		if err := reg.Register(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{})); err != nil {
			return fmt.Errorf("metrics: 注册进程 collector 失败: %w", err)
		}
	}

	mu.Lock()
	ns, registry = next, reg
	mu.Unlock()
	return nil
}
