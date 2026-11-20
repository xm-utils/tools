package httpclient

import (
	"net/http"

	"github.com/xm-utils/tools/trace"
)

// TracedHandler 为纯 HTTP 服务端提供链路追踪中间件：从入站 Header 还原上游
// traceparent，开启 SERVER span 并挂到 request ctx，供 handler 内进一步埋点 / 取 TraceID。
// operation 为 span 名（如接口名），为空时回退 "METHOD /path"。
//
// 依赖 app 先调用 trace.Init 装配全局 Provider/传播器；未 Init 时为空操作零开销。
//
//	mux.Handle("/pay", httpclient.TracedHandler("pay-svc", "POST /pay", payHandler))
func TracedHandler(tracerName, operation string, next http.Handler) http.Handler {
	return trace.Middleware(tracerName, operation, next)
}
