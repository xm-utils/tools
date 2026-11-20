package httpclient

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/breaker"
	"github.com/xm-utils/tools/retry"
)

// HttpClient 弹性HTTP客户端（集成重试和熔断）
type HttpClient struct {
	client               *http.Client
	retryExecutor        *retry.Executor
	circuitBreaker       *breaker.CircuitBreaker
	enableCircuitBreaker bool      // 是否启用熔断器
	logKeeper            LogKeeper // 请求日志持久化实现（可选注入）
	log                  *logrus.Entry
}

// Config HTTP客户端配置
type Config struct {
	Timeout              time.Duration `yaml:"timeout"`                // 超时时间
	MaxRetries           int           `yaml:"max_retries"`            // 最大重试次数
	EnableCircuitBreaker bool          `yaml:"enable_circuit_breaker"` // 是否启用熔断器
	ErrorThreshold       float64       `yaml:"error_threshold"`        // 错误率阈值（0-1）
	SlowCallDuration     time.Duration `yaml:"slow_call_duration"`     // 慢调用阈值
	WindowSize           time.Duration `yaml:"window_size"`            // 滑动窗口大小
	MinRequests          int           `yaml:"min_requests"`           // 最小请求数
	// EnableTracing 是否为出站请求接入链路追踪（基于 trace 组件的 traced RoundTripper）。
	// 开启前需由 app 调用 trace.Init 装配全局 Provider；未 Init 时 transport 为透传零行为。
	EnableTracing bool `yaml:"enable_tracing"`
	// TracerName 追踪 span 所属的 tracer 名，默认 "httpclient"。
	TracerName string `yaml:"tracer_name"`
}

// LogKeeper HTTP请求日志持久化接口，由外部注入实现，避免循环依赖
type LogKeeper interface {
	KeepHttpLog(log *HttpRequestLog)
}

// HttpRequestLog HTTP请求日志数据
type HttpRequestLog struct {
	RequestId   string // 请求唯一标识（业务方产生，用于关联排查）
	AppId       int64  // 应用ID
	ServiceType string // 服务类型（pay/risk/sms/ocr/wivlink/adjust/appsflyer）
	Method      string // 请求方法
	URL         string // 请求URL
	ReqHeaders  string // 请求头（JSON）
	ReqBody     string // 请求体
	RespStatus  int    // HTTP响应状态码
	RespBody    string // 响应体
	Duration    int    // 请求耗时（毫秒）
	Attempts    int8   // 实际请求次数（含重试）
	Status      int8   // 结果状态：0-成功 1-失败
	ErrorMsg    string // 错误信息
}

// HttpRequest HTTP请求参数
type HttpRequest struct {
	Method      string
	URL         string
	Headers     map[string]string
	Body        interface{}
	Files       []MultipartField
	ServiceType string          // 服务类型标识，用于日志记录
	AppId       int64           // 应用ID，用于日志记录
	RequestId   string          // 请求唯一标识，由业务方产生并随请求一起持久化；为空时由客户端兜底生成
	Ctx         context.Context // 调用方上下文（可为 nil）；开启链路追踪时用于承接上游 trace 上下文，使出站 span 挂到调用链下
}

// ctxOrBackground 返回请求上下文，为空时回退 context.Background。
func (r *HttpRequest) ctxOrBackground() context.Context {
	if r.Ctx != nil {
		return r.Ctx
	}
	return context.Background()
}

// NewHttpRequest 构建基础的 HttpRequest，预置用于日志记录的 appId 与 serviceType，
// 其余字段（Method/URL/Headers/Body/RequestId 等）由业务方按需设置
func NewHttpRequest(appId int64, serviceType string) *HttpRequest {
	return &HttpRequest{
		AppId:       appId,
		ServiceType: serviceType,
		RequestId:   uuid.NewString(),
	}
}

// HttpResponse HTTP响应结果
type HttpResponse struct {
	StatusCode int
	Body       []byte
	Headers    http.Header
}

// MultipartField multipart表单字段
type MultipartField struct {
	FieldName   string    // 表单字段名
	Value       string    // 普通文本字段的值（FileName为空时使用）
	FileName    string    // 文件名（为空表示普通字段）
	FileData    io.Reader // 文件二进制数据
	ContentType string    // 文件MIME类型（如 image/jpeg）
}

// retryableResponseError 可重试的响应状态码错误
// 携带最后一次有效的HTTP响应，用于在所有重试耗尽后返回给调用方
type retryableResponseError struct {
	response *HttpResponse
	message  string
}

func (e *retryableResponseError) Error() string {
	return e.message
}
