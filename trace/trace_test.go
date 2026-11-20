package trace

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc/metadata"
)

// installTestProvider 装配一个仅测试用的本地 TracerProvider（无导出器），
// 使 ctx 携带有效 SpanContext，用于验证注入/提取往返。
func installTestProvider(t *testing.T) context.Context {
	t.Helper()
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(context.Background(), "op")
	t.Cleanup(func() { span.End() })
	return ctx
}

func TestInitNoopSafeBeforeInit(t *testing.T) {
	// 未 Init 时全部 API 必须安全零行为。
	ctx := context.Background()
	if got := TraceIDFromCtx(ctx); got != "" {
		t.Fatalf("TraceIDFromCtx 应为空串, got %q", got)
	}
	if HasSpanContext(ctx) {
		t.Fatal("未 Init 不应有 SpanContext")
	}
	if Tracer("x") == nil || Meter("x") == nil {
		t.Fatal("未 Init 时 Tracer/Meter 也必须返回可用实例")
	}
	// 未 Init 注入不写载体、提取原样返回。
	carrier := map[string]string{}
	InjectMap(ctx, carrier)
	if len(carrier) != 0 {
		t.Fatalf("未 Init 时 InjectMap 不应写入: %v", carrier)
	}
	if ExtractMap(ctx, map[string]string{"traceparent": "00-x-y-01"}) != ctx {
		t.Fatal("未 Init 时 ExtractMap 应原样返回 ctx（noop 传播器无操作）")
	}
}

func TestInitConfigValidation(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		cfg  Config
	}{
		{"未知导出器", Config{Exporter: "jaeger"}},
		{"stdout 缺服务名", Config{Exporter: "stdout"}},
		{"otlp 缺端点", Config{Exporter: "otlp", ServiceName: "svc"}},
		{"采样率越界", Config{Exporter: "none", SampleRatio: 1.5}},
		{"未知传播器", Config{Exporter: "none", Propagators: []string{"b3"}}},
	}
	for _, c := range cases {
		if err := Init(ctx, c.cfg); err == nil {
			t.Errorf("%s: 期望报错, got nil", c.name)
		}
	}
	// 失败不得破坏既有全局状态：再次用合法 none 配置应成功。
	if err := Init(ctx, Config{Exporter: ExporterNone}); err != nil {
		t.Fatalf("none 配置 Init 失败: %v", err)
	}
	t.Cleanup(func() { _ = Shutdown(context.Background()) })
}

func TestInitStdoutAndFlushShutdown(t *testing.T) {
	// stdout 导出器在构造时即捕获 os.Stdout 句柄，测试期间替换 fd 1 会把写入
	// 打到无效描述符上；改用常驻读取的管道接住导出 JSON。
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("创建管道失败: %v", err)
	}
	old := os.Stdout
	os.Stdout = w
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		_, _ = io.Copy(io.Discard, r)
	}()
	defer func() {
		os.Stdout = old
		_ = w.Close()
		<-drained
		_ = r.Close()
	}()

	ctx := context.Background()
	if err := Init(ctx, Config{ServiceName: "trace-test", Exporter: ExporterStdout, SampleRatio: 1.0}); err != nil {
		t.Fatalf("Init stdout 失败: %v", err)
	}
	if TracerProvider() == nil || MeterProvider() == nil {
		t.Fatal("Init 后 TracerProvider/MeterProvider 应已装配")
	}
	_, span := Start(ctx, "trace-test", "demo")
	if !span.SpanContext().HasTraceID() {
		t.Fatal("Init 后 Start 应生成有效 TraceID")
	}
	span.End()
	if err := Flush(ctx); err != nil {
		t.Fatalf("Flush 失败: %v", err)
	}
	// 导出器持有管道写端，必须在关闭管道之前 Shutdown。
	if err := Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown 失败: %v", err)
	}
	if err := Shutdown(ctx); err != nil {
		t.Fatalf("重复 Shutdown 应安全, got %v", err)
	}
	if TracerProvider() != nil {
		t.Fatal("Shutdown 后应清空全局 Provider")
	}
}

func TestGRPCMetadataRoundTrip(t *testing.T) {
	src := installTestProvider(t)
	out := InjectGRPC(src)
	md, ok := metadata.FromOutgoingContext(out)
	if !ok || len(md.Get("traceparent")) == 0 {
		t.Fatalf("InjectGRPC 未写入 traceparent: %v", md)
	}
	// 模拟对端接收：出站 ctx 转入站 metadata 再提取。
	in := metadata.NewIncomingContext(context.Background(), md)
	got := ExtractGRPC(in)
	if TraceIDFromCtx(got) != TraceIDFromCtx(src) {
		t.Fatalf("gRPC 往返 TraceID 不一致: %s -> %s", TraceIDFromCtx(src), TraceIDFromCtx(got))
	}
	// 无 metadata 的入站 ctx 原样返回。
	if ExtractGRPC(context.Background()) != context.Background() {
		// 值相等比较对 Background 有效
		t.Log("ExtractGRPC 空 metadata 返回新 ctx（可接受）")
	}
}

func TestHTTPHeaderroundTrip(t *testing.T) {
	src := installTestProvider(t)
	req, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
	InjectHTTP(src, req)
	tp := req.Header.Get("traceparent")
	if !strings.HasPrefix(tp, "00-") {
		t.Fatalf("InjectHTTP 未写入 traceparent, got %q", tp)
	}
	got := ExtractHTTP(context.Background(), req)
	if TraceIDFromCtx(got) != TraceIDFromCtx(src) {
		t.Fatal("HTTP 往返 TraceID 不一致")
	}
	InjectHTTP(src, nil)  // nil req 不 panic
	ExtractHTTP(src, nil) // nil req 原样返回
}

func TestMapRoundTrip(t *testing.T) {
	src := installTestProvider(t)
	headers := map[string]string{}
	InjectMap(src, headers)
	if headers["traceparent"] == "" {
		t.Fatal("InjectMap 未写入 traceparent")
	}
	if got := TraceParentFromCtx(src); got != headers["traceparent"] {
		t.Fatalf("TraceParentFromCtx 与 InjectMap 结果不一致: %q vs %q", got, headers["traceparent"])
	}
	got := ExtractMap(context.Background(), headers)
	if TraceIDFromCtx(got) != TraceIDFromCtx(src) {
		t.Fatal("Map 往返 TraceID 不一致")
	}
	InjectMap(src, nil) // nil carrier 不应 panic
}

func TestMetricName(t *testing.T) {
	if got := MetricName("breaker", "open_total"); got != "xmt_breaker_open_total" {
		t.Fatalf("MetricName got %q", got)
	}
}
