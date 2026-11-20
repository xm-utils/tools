package trace

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// 导出器类型取值。
const (
	// ExporterNone 不导出：仍注册 SDK TracerProvider（Span/TraceID 正常生成与传播），
	// 仅不外送数据，用于本地开发或未接观测后端的部署。
	ExporterNone = "none"
	// ExporterStdout 标准输出导出（调试用）：Span/指标以 JSON 打印到 stdout。
	ExporterStdout = "stdout"
	// ExporterOTLP OTLP/gRPC 导出：对接 Collector、Jaeger（原生支持 OTLP）等后端。
	ExporterOTLP = "otlp"
)

// Config trace 组件初始化配置。
type Config struct {
	// ServiceName 服务名，写入 resource 属性 service.name。
	// Exporter 为 stdout/otlp 时必填；none 可为空。
	ServiceName string
	// Environment 部署环境（如 prod / staging），可选，写入 deployment.environment。
	Environment string
	// Exporter 导出器类型："none" / "stdout" / "otlp"；空默认 "none"。
	Exporter string
	// Endpoint OTLP 收集器地址（host:port），Exporter=otlp 时必填。
	// 例："localhost:4317"、"otel-collector:4318"。
	Endpoint string
	// Insecure 连接 Endpoint 时是否使用明文 gRPC（不带 TLS）。
	// 采集器与业务同集群内网部署时通常为 true。
	Insecure bool
	// SampleRatio 采样率 (0,1]：>1 报错、<=0 取默认 1.0（全采样）。
	// 采用父级优先（ParentBased）策略：上游已决策的链路不重新采样，
	// 保证同一条 Trace 跨服务后判定一致。
	SampleRatio float64
	// Propagators 传播协议，可选 "tracecontext"、"baggage"；
	// 空默认 ["tracecontext", "baggage"]（W3C 标准，含 traceparent 载体）。
	Propagators []string
}

// 全局装配状态。Init 可重复调用，后一次覆盖前一次并关停旧 Provider。
var (
	mu         sync.Mutex
	tracerProv *sdktrace.TracerProvider
	meterProv  *sdkmetric.MeterProvider
)

// Init 装配全局 TracerProvider / MeterProvider / TextMapPropagator。
//
// 幂等可重入：重复调用会关停上一轮 Provider 后重建（适合配置热更新场景）。
// 所有失败均以 error 返回且不 panic，失败时不改动既有全局注册状态。
// Exporter=none 时可安全用于任何环境，不产生外部网络依赖。
func Init(ctx context.Context, cfg Config) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("trace: init panic 转 error: %v", r)
		}
	}()

	exporter, ratio, err := cfg.normalize()
	if err != nil {
		return err
	}

	res, err := buildResource(ctx, cfg.ServiceName, cfg.Environment)
	if err != nil {
		return err
	}

	// ---- TracerProvider ----
	tpOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	}
	switch exporter {
	case ExporterStdout:
		exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return fmt.Errorf("trace: 构造 stdout span 导出器失败: %w", err)
		}
		tpOpts = append(tpOpts, sdktrace.WithBatcher(exp))
	case ExporterOTLP:
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.Endpoint)}
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exp, err := otlptracegrpc.New(ctx, opts...)
		if err != nil {
			return fmt.Errorf("trace: 构造 otlp span 导出器失败: %w", err)
		}
		tpOpts = append(tpOpts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(tpOpts...)

	// ---- MeterProvider ----
	mpOpts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	switch exporter {
	case ExporterStdout:
		exp, err := stdoutmetric.New(stdoutmetric.WithPrettyPrint())
		if err != nil {
			_ = tp.Shutdown(ctx)
			return fmt.Errorf("trace: 构造 stdout metric 导出器失败: %w", err)
		}
		mpOpts = append(mpOpts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
	case ExporterOTLP:
		opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(cfg.Endpoint)}
		if cfg.Insecure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		exp, err := otlpmetricgrpc.New(ctx, opts...)
		if err != nil {
			_ = tp.Shutdown(ctx)
			return fmt.Errorf("trace: 构造 otlp metric 导出器失败: %w", err)
		}
		mpOpts = append(mpOpts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
	}
	mp := sdkmetric.NewMeterProvider(mpOpts...)

	// ---- 传播器（W3C traceparent 为主）----
	prop, err := buildPropagator(cfg.Propagators)
	if err != nil {
		_ = tp.Shutdown(ctx)
		_ = mp.Shutdown(ctx)
		return err
	}

	mu.Lock()
	oldTP, oldMP := tracerProv, meterProv
	tracerProv, meterProv = tp, mp
	mu.Unlock()

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(prop)

	// 全局替换完成后再关停上一轮 Provider（Provider Shutdown 会级联关停其内部
	// 导出器并 Flush 缓冲），避免出现无 Provider 的窗口；本轮新导出器保持存活。
	if oldTP != nil {
		_ = oldTP.Shutdown(ctx)
	}
	if oldMP != nil {
		_ = oldMP.Shutdown(ctx)
	}
	return nil
}

// Shutdown 刷新并关停当前 Provider，释放导出器后台协程。未 Init 或重复调用均安全。
// 进程退出前调用，保证批量缓冲中残余的 Span / 指标不丢。
func Shutdown(ctx context.Context) error {
	mu.Lock()
	tp, mp := tracerProv, meterProv
	tracerProv, meterProv = nil, nil
	mu.Unlock()

	var errs []string
	if tp != nil {
		appendErr(&errs, tp.Shutdown(ctx))
	}
	if mp != nil {
		appendErr(&errs, mp.Shutdown(ctx))
	}
	return joinErrs("trace: shutdown", errs)
}

// Flush 立即导出已缓冲的 Span 与指标，不关停 Provider。
// 短生命周期任务结束前、以及测试断言导出结果时调用。未 Init 时为 no-op。
func Flush(ctx context.Context) error {
	mu.Lock()
	tp, mp := tracerProv, meterProv
	mu.Unlock()

	var errs []string
	if tp != nil {
		appendErr(&errs, tp.ForceFlush(ctx))
	}
	if mp != nil {
		appendErr(&errs, mp.ForceFlush(ctx))
	}
	return joinErrs("trace: flush", errs)
}

// TracerProvider 返回当前装配的 TracerProvider；未 Init 返回 nil。
// 供 grpcx / httpclient 等组件构造原生 Interceptor、或接入 span 录制器做测试时复用；
// 常规取用 Tracer() 即可。
func TracerProvider() trace.TracerProvider {
	mu.Lock()
	defer mu.Unlock()
	if tracerProv == nil {
		return nil
	}
	return tracerProv
}

// MeterProvider 返回当前装配的 MeterProvider；未 Init 返回 nil。
func MeterProvider() metric.MeterProvider {
	mu.Lock()
	defer mu.Unlock()
	if meterProv == nil {
		return nil
	}
	return meterProv
}

// normalize 校验并归一化配置，返回生效的导出器名与采样率。
func (c Config) normalize() (exporter string, ratio float64, err error) {
	exporter = strings.ToLower(strings.TrimSpace(c.Exporter))
	if exporter == "" {
		exporter = ExporterNone
	}
	switch exporter {
	case ExporterNone, ExporterStdout, ExporterOTLP:
	default:
		return "", 0, fmt.Errorf("trace: 不支持的 Exporter %q（可选 none/stdout/otlp）", c.Exporter)
	}
	if exporter != ExporterNone && strings.TrimSpace(c.ServiceName) == "" {
		return "", 0, fmt.Errorf("trace: Exporter=%s 要求 Config.ServiceName 非空", exporter)
	}
	if exporter == ExporterOTLP && strings.TrimSpace(c.Endpoint) == "" {
		return "", 0, fmt.Errorf("trace: Exporter=otlp 要求 Config.Endpoint 非空")
	}
	ratio = c.SampleRatio
	switch {
	case ratio > 1:
		return "", 0, fmt.Errorf("trace: Config.SampleRatio=%v 超出 [0,1]", c.SampleRatio)
	case ratio <= 0:
		ratio = 1.0
	}
	return exporter, ratio, nil
}

// buildResource 组装 resource 属性；两项均为空时沿用 otel 默认 resource。
func buildResource(ctx context.Context, serviceName, environment string) (*resource.Resource, error) {
	var attrs []attribute.KeyValue
	if name := strings.TrimSpace(serviceName); name != "" {
		attrs = append(attrs, semconv.ServiceName(name))
	}
	if env := strings.TrimSpace(environment); env != "" {
		attrs = append(attrs, semconv.DeploymentEnvironment(env))
	}
	if len(attrs) == 0 {
		return resource.Default(), nil
	}
	res, err := resource.New(ctx, resource.WithAttributes(attrs...),
		resource.WithHost(), resource.WithProcess(), resource.WithTelemetrySDK())
	if err != nil {
		return nil, fmt.Errorf("trace: 构造 resource 失败: %w", err)
	}
	merged, err := resource.Merge(resource.Default(), res)
	if err != nil {
		return nil, fmt.Errorf("trace: 合并 resource 失败: %w", err)
	}
	return merged, nil
}

// buildPropagator 按配置名构造 W3C 组合传播器。
func buildPropagator(names []string) (propagation.TextMapPropagator, error) {
	if len(names) == 0 {
		names = []string{"tracecontext", "baggage"}
	}
	props := make([]propagation.TextMapPropagator, 0, len(names))
	for _, n := range names {
		switch strings.ToLower(strings.TrimSpace(n)) {
		case "tracecontext":
			props = append(props, propagation.TraceContext{})
		case "baggage":
			props = append(props, propagation.Baggage{})
		default:
			return nil, fmt.Errorf("trace: 不支持的传播器 %q（可选 tracecontext/baggage）", n)
		}
	}
	return propagation.NewCompositeTextMapPropagator(props...), nil
}

func appendErr(errs *[]string, err error) {
	if err != nil {
		*errs = append(*errs, err.Error())
	}
}

func joinErrs(prefix string, errs []string) error {
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%s: %s", prefix, strings.Join(errs, "; "))
}
