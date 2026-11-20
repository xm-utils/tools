package trace

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// installRecordingProvider 装配一个带内存录制器的 SDK TracerProvider 与 W3C 传播器，
// 返回录制器与恢复函数，供断言拦截器/传输层产生的 Span。
func installRecordingProvider(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	oldTP := otel.GetTracerProvider()
	oldProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(oldTP)
		otel.SetTextMapPropagator(oldProp)
		_ = tp.Shutdown(context.Background())
	})
	return sr
}

func TestUnaryClientInjectsTraceparent(t *testing.T) {
	installRecordingProvider(t)

	var gotMD metadata.MD
	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		gotMD, _ = metadata.FromOutgoingContext(ctx)
		return nil
	}
	inter := UnaryClientInterceptor("test")
	err := inter(context.Background(), "/svc/Method", nil, nil, &grpc.ClientConn{}, invoker)
	if err != nil {
		t.Fatalf("client interceptor 返回错误: %v", err)
	}
	if len(gotMD.Get("traceparent")) == 0 {
		t.Fatal("客户端拦截器未注入 traceparent")
	}
}

func TestUnaryServerExtractsAndSpans(t *testing.T) {
	sr := installRecordingProvider(t)

	// 先由客户端拦截器风格造一个上游 traceparent。
	ctx, span := Tracer("up").Start(context.Background(), "up-op")
	tp := TraceParentFromCtx(ctx)
	span.End()
	if tp == "" {
		t.Fatal("上游 traceparent 为空")
	}

	incoming := metadata.NewIncomingContext(context.Background(), metadata.Pairs("traceparent", tp))
	inter := UnaryServerInterceptor("svc")
	var handlerTraceID string
	_, err := inter(incoming, nil, &grpc.UnaryServerInfo{FullMethod: "/pkg.Svc/Call"},
		func(ctx context.Context, req any) (any, error) {
			handlerTraceID = TraceIDFromCtx(ctx)
			return "resp", nil
		})
	if err != nil {
		t.Fatalf("server interceptor 返回错误: %v", err)
	}

	// handler 内 TraceID 应等于上游 TraceID（链路贯通）。
	want := traceIDFromTraceparent(tp)
	if handlerTraceID == "" || handlerTraceID != want {
		t.Fatalf("handler TraceID=%s, 期望上游 %s", handlerTraceID, want)
	}

	spans := sr.Ended()
	if len(spans) == 0 {
		t.Fatal("未录制到任何 span")
	}
	last := spans[len(spans)-1]
	if last.SpanKind() != oteltrace.SpanKindServer {
		t.Fatalf("span kind = %v, 期望 server", last.SpanKind())
	}
	if last.SpanContext().TraceID().String() != want {
		t.Fatalf("span TraceID=%s, 期望沿用上游 %s", last.SpanContext().TraceID().String(), want)
	}
}

func TestServerInterceptorRecordsError(t *testing.T) {
	sr := installRecordingProvider(t)
	inter := UnaryServerInterceptor("svc")
	myErr := errors.New("boom")
	_, err := inter(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/pkg.Svc/Fail"},
		func(ctx context.Context, req any) (any, error) { return nil, myErr })
	if err != myErr {
		t.Fatalf("原始 error 未透传: %v", err)
	}
	spans := sr.Ended()
	if len(spans) == 0 || spans[len(spans)-1].Status().Code.String() != "Error" {
		t.Fatalf("出错时 span 状态应为 Error, got %+v", spans)
	}
}

func TestHTTPTransportInjects(t *testing.T) {
	installRecordingProvider(t)
	var gotHeader string
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		gotHeader = req.Header.Get("traceparent")
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: req}, nil
	})
	client := &http.Client{Transport: NewTracedTransport("httpclient", base)}
	req, _ := http.NewRequest(http.MethodGet, "http://example.com/api", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	_ = resp.Body.Close()
	if gotHeader == "" {
		t.Fatal("traced transport 未注入 traceparent")
	}
}

func TestHTTPMiddlewareExtracts(t *testing.T) {
	sr := installRecordingProvider(t)
	// 造上游 traceparent。
	ctx, span := Tracer("up").Start(context.Background(), "up")
	tp := TraceParentFromCtx(ctx)
	span.End()

	var handlerTraceID string
	mw := Middleware("server", "GET /x", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerTraceID = TraceIDFromCtx(r.Context())
		w.WriteHeader(http.StatusTeapot)
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("traceparent", tp)
	mw.ServeHTTP(rec, req)

	if want := traceIDFromTraceparent(tp); handlerTraceID != want {
		t.Fatalf("handler TraceID=%s, 期望 %s", handlerTraceID, want)
	}
	if rec.Code != http.StatusTeapot {
		t.Fatalf("状态码未被透传: %d", rec.Code)
	}
	spans := sr.Ended()
	if len(spans) == 0 || spans[len(spans)-1].SpanKind() != oteltrace.SpanKindServer {
		t.Fatalf("未录制到 server span: %+v", spans)
	}
}

func TestSplitGRPCMethod(t *testing.T) {
	cases := map[string][2]string{
		"/pkg.Svc/Call": {"pkg.Svc", "Call"},
		"Call":          {"", "Call"},
		"":              {"", ""},
	}
	for in, want := range cases {
		s, m := splitGRPCMethod(in)
		if s != want[0] || m != want[1] {
			t.Errorf("splitGRPCMethod(%q)=(%q,%q), 期望 (%q,%q)", in, s, m, want[0], want[1])
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// traceIDFromTraceparent 从 "00-<trace>-<span>-<flags>" 提取 trace 段。
func traceIDFromTraceparent(tp string) string {
	parts := splitOn(tp, '-')
	if len(parts) >= 2 {
		return parts[1]
	}
	return ""
}

func splitOn(s string, sep byte) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}
