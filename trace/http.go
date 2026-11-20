package trace

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// HTTP 侧贯通：客户端 NewTracedTransport 装饰 RoundTripper，出站注入 traceparent
// 并开 CLIENT span；服务端 Middleware 从入站 Header 还原上下文并开 SERVER span。
// 与 gRPC 拦截器同源，均基于 Init 装配的全局 Provider/传播器，未 Init 时零行为。

// NewTracedTransport 返回带链路追踪的 RoundTripper。base 为 nil 时用 http.DefaultTransport。
// tracerName 建议传组件名（如 "httpclient"）。
func NewTracedTransport(tracerName string, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &tracedTransport{tracerName: tracerName, base: base}
}

type tracedTransport struct {
	tracerName string
	base       http.RoundTripper
}

func (t *tracedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	attrs := []attribute.KeyValue{
		attribute.String("http.request.method", req.Method),
		attribute.String("url.full", req.URL.String()),
	}
	ctx, span := Tracer(t.tracerName).Start(req.Context(), req.Method+" "+req.URL.Host,
		oteltrace.WithSpanKind(oteltrace.SpanKindClient),
		oteltrace.WithAttributes(attrs...))
	// 用携带 span 的 ctx 派生新请求，确保下游 propagator 注入的是本 span。
	req = req.WithContext(ctx)
	InjectHTTP(ctx, req)

	resp, err := t.base.RoundTrip(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return resp, err
	}
	if resp != nil {
		span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
		if resp.StatusCode >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(resp.StatusCode))
		}
	}
	span.End()
	return resp, nil
}

// Middleware 服务端 HTTP 中间件：从入站 Header 还原上游链路并开启 SERVER span，
// 挂到 request ctx 供 handler 使用。operation 为 span 名（如路由/接口名），
// 为空时回退为 "METHOD /path"。tracerName 建议传服务名。
func Middleware(tracerName, operation string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := ExtractHTTP(r.Context(), r)
		name := operation
		if name == "" {
			name = r.Method + " " + r.URL.Path
		}
		ctx, span := Tracer(tracerName).Start(ctx, name,
			oteltrace.WithSpanKind(oteltrace.SpanKindServer),
			oteltrace.WithAttributes(
				attribute.String("http.request.method", r.Method),
				attribute.String("url.path", r.URL.Path),
			))
		defer span.End()

		// 让 handler 内进一步埋点/取 TraceID 能拿到本 span。
		r = r.WithContext(ctx)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		span.SetAttributes(attribute.Int("http.response.status_code", rec.status))
		if rec.status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(rec.status))
		}
	})
}

// statusRecorder 包裹 ResponseWriter 捕获最终状态码。
type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wrote {
		s.status = code
		s.wrote = true
	}
	s.ResponseWriter.WriteHeader(code)
}
