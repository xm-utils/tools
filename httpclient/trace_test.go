package httpclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xm-utils/tools/trace"
)

// TestTracedTransportInjectsTraceparent 验证开启链路追踪后，出站请求经 traced
// RoundTripper 自动注入 W3C traceparent；trace.Init(Exporter=none) 仅装配全局
// Provider/传播器，不外送数据。
func TestTracedTransportInjectsTraceparent(t *testing.T) {
	if err := trace.Init(context.Background(), trace.Config{Exporter: trace.ExporterNone, SampleRatio: 1.0}); err != nil {
		t.Fatalf("trace.Init 失败: %v", err)
	}
	t.Cleanup(func() { _ = trace.Shutdown(context.Background()) })

	var gotTP string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTP = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	hc := newHttpClient(&Config{EnableTracing: true, TracerName: "test-http"}, nil)
	resp, err := hc.DoRequest(&HttpRequest{Method: http.MethodGet, URL: srv.URL})
	if err != nil {
		t.Fatalf("DoRequest 失败: %v", err)
	}
	_ = resp
	if gotTP == "" {
		t.Fatal("traced transport 未注入 traceparent")
	}
}

// TestTracedHandlerRoundTrip 验证服务端中间件从入站 Header 还原链路，handler 内
// 可取到与上游一致的 TraceID。
func TestTracedHandlerRoundTrip(t *testing.T) {
	if err := trace.Init(context.Background(), trace.Config{Exporter: trace.ExporterNone, SampleRatio: 1.0}); err != nil {
		t.Fatalf("trace.Init 失败: %v", err)
	}
	t.Cleanup(func() { _ = trace.Shutdown(context.Background()) })

	var handlerTrace string
	h := TracedHandler("svc", "GET /x", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerTrace = trace.TraceIDFromCtx(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	// 用真实 span 生成一个上游 traceparent 再打进来。
	ctx, sp := trace.Start(context.Background(), "up", "op")
	incoming := trace.TraceParentFromCtx(ctx)
	sp.End()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("traceparent", incoming)
	h.ServeHTTP(rec, req)

	if handlerTrace == "" || handlerTrace != traceTraceID(incoming) {
		t.Fatalf("handler TraceID=%s, 期望沿用上游 %s", handlerTrace, incoming)
	}
}

// traceTraceID 从 "00-<trace>-<span>-<flags>" 取 trace 段。
func traceTraceID(tp string) string {
	start := -1
	dashes := 0
	for i := 0; i < len(tp); i++ {
		if tp[i] == '-' {
			dashes++
			if dashes == 1 {
				start = i + 1
			}
			if dashes == 2 {
				return tp[start:i]
			}
		}
	}
	return ""
}
