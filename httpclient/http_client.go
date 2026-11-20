package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"sync"
	"time"

	"github.com/google/go-querystring/query"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/breaker"
	"github.com/xm-utils/tools/retry"
	"github.com/xm-utils/tools/trace"
)

// DefaultConfig 默认HTTP客户端配置
func DefaultConfig() *Config {
	return &Config{
		Timeout:              30 * time.Second,
		MaxRetries:           3,
		EnableCircuitBreaker: false, // 默认启用熔断器
		ErrorThreshold:       0.5,
		SlowCallDuration:     5 * time.Second,
		WindowSize:           60 * time.Second,
		MinRequests:          10,
	}
}

var (
	instance *HttpClient
	once     sync.Once
)

func Init(cnf *Config, lk LogKeeper) {
	once.Do(func() {
		instance = newHttpClient(cnf, lk)
	})
}

// GetClient 获取Risk客户端
func GetClient() *HttpClient {
	return instance
}

// newHttpClient 创建弹性HTTP客户端
func newHttpClient(config *Config, lk LogKeeper) *HttpClient {

	log := logrus.WithField("module", "http-client")
	if config == nil {
		config = DefaultConfig()
	}

	// 创建HTTP客户端
	httpClient := &http.Client{
		Timeout: config.Timeout,
	}

	// 按需为出站请求接入链路追踪（trace 未 Init 时 transport 为透传零行为）。
	if config.EnableTracing {
		tracerName := config.TracerName
		if tracerName == "" {
			tracerName = "httpclient"
		}
		httpClient.Transport = trace.NewTracedTransport(tracerName, http.DefaultTransport)
		log.WithField("tracer", tracerName).Info("HTTP Client: 链路追踪已启用")
	}

	// 创建重试配置
	retryConfig := retry.DefaultRetryConfig()
	retryConfig.MaxRetries = config.MaxRetries
	retryConfig.Strategy = &retry.ExponentialBackoffStrategy{
		InitialDelay: 1 * time.Second,
		MaxDelay:     10 * time.Second,
		Multiplier:   2.0,
	}
	retryExecutor := retry.NewRetryExecutor(retryConfig)

	// 按需创建熔断器
	var circuitBreaker *breaker.CircuitBreaker
	if config.EnableCircuitBreaker {
		// 创建熔断器配置
		breakerConfig := breaker.DefaultCircuitBreakerConfig()

		// 如果配置了自定义参数且大于0，则使用自定义值；否则使用默认值
		if config.ErrorThreshold > 0 {
			breakerConfig.ErrorThreshold = config.ErrorThreshold
		}
		if config.SlowCallDuration > 0 {
			breakerConfig.SlowCallDuration = config.SlowCallDuration
		}
		if config.WindowSize > 0 {
			breakerConfig.WindowSize = config.WindowSize
		}
		if config.MinRequests > 0 {
			breakerConfig.MinRequests = config.MinRequests
		}

		breakerConfig.SlowCallThreshold = 0.8
		breakerConfig.WaitDurationInOpenState = 30 * time.Second
		breakerConfig.HalfOpenMaxRequests = 5

		// 状态变更回调
		breakerConfig.OnStateChange = func(oldState, newState breaker.State) {
			log.WithFields(logrus.Fields{
				"oldState": breakerStateToString(oldState),
				"newState": breakerStateToString(newState),
			}).Warn("HTTP Client: 熔断器状态变更")
		}

		circuitBreaker = breaker.NewCircuitBreaker("http-client", breakerConfig)
		log.WithFields(logrus.Fields{
			"enableCircuitBreaker": config.EnableCircuitBreaker,
			"errorThreshold":       breakerConfig.ErrorThreshold,
			"slowCallDuration":     breakerConfig.SlowCallDuration,
			"windowSize":           breakerConfig.WindowSize,
			"minRequests":          breakerConfig.MinRequests,
		}).Info("HTTP Client: 熔断器已启用")
	} else {
		// 禁用熔断器：通过设置极高阈值来禁用
		breakerConfig := breaker.DefaultCircuitBreakerConfig()
		breakerConfig.ErrorThreshold = 1.0             // 100%错误率才触发
		breakerConfig.SlowCallDuration = 1 * time.Hour // 极长的慢调用阈值
		breakerConfig.WindowSize = 1 * time.Hour
		breakerConfig.MinRequests = 999999
		breakerConfig.WaitDurationInOpenState = 1 * time.Hour
		breakerConfig.HalfOpenMaxRequests = 999999

		circuitBreaker = breaker.NewCircuitBreaker("http-client", breakerConfig)
		log.Info("HTTP Client: 熔断器已禁用")
	}

	return &HttpClient{
		client:               httpClient,
		retryExecutor:        retryExecutor,
		circuitBreaker:       circuitBreaker,
		enableCircuitBreaker: config.EnableCircuitBreaker,
		logKeeper:            lk,
		log:                  log,
	}
}

// isRetryableResponseError 检查错误是否为可重试的响应状态码错误，并返回携带的响应
func isRetryableResponseError(err error) (*HttpResponse, bool) {
	if e, ok := err.(*retryableResponseError); ok {
		return e.response, true
	}
	return nil, false
}

// isRetryableStatusCode 判断HTTP状态码是否应该触发重试
// 5xx（服务端错误）、408（请求超时）、429（请求过多）视为瞬时故障，允许重试
// 4xx（除408/429外）视为客户端确定性错误，不重试
func isRetryableStatusCode(statusCode int) bool {
	return statusCode >= 500 || statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooManyRequests
}

// DoRequest 执行HTTP请求（带重试和熔断）
// 重试仅作用于网络IO操作（client.Do），参数序列化、创建请求、组装请求头等前置错误不会触发重试
func (h *HttpClient) DoRequest(req *HttpRequest) (*HttpResponse, error) {
	startTime := time.Now()

	// 0. 生成/透传请求唯一标识：业务方可通过 HttpRequest.RequestId 传入，
	// 未传入时兜底生成，保证每次请求都有唯一标识并随请求记录一起持久化
	if req.RequestId == "" {
		req.RequestId = uuid.NewString()
	}

	// 1. 前置准备：序列化、创建请求、组装请求头（不重试）
	httpReq, prepErr := h.prepareRequest(req)
	if prepErr != nil {
		h.recordLog(req, nil, 0, 1, prepErr, startTime)
		return nil, prepErr
	}

	// 2. 定义重试任务：仅执行真实的IO操作（含状态码校验）
	retryTask := func(ctx context.Context) (any, error) {
		resp, err := h.executeRequest(httpReq)
		if err != nil {
			h.log.WithFields(logrus.Fields{
				"url":    req.URL,
				"method": req.Method,
				"error":  err,
			}).Warn("HTTP Client: IO操作失败，准备重试")
			return nil, err
		}
		return resp, nil
	}

	// 3. 执行重试（+ 熔断）
	var lastErr error
	var httpResponse *HttpResponse
	var attempts int

	if h.enableCircuitBreaker {
		_, err := h.circuitBreaker.ExecuteWithContext(context.Background(), func(ctx context.Context) (interface{}, error) {
			retryResult := h.retryExecutor.ExecuteSync(retryTask)
			attempts = retryResult.Attempts
			if retryResult.Error != nil {
				lastErr = retryResult.Error
				if resp, ok := isRetryableResponseError(retryResult.Error); ok {
					httpResponse = resp
				}
				return nil, lastErr
			}
			httpResponse = retryResult.Data.(*HttpResponse)
			h.log.WithFields(logrus.Fields{
				"url":      req.URL,
				"attempts": retryResult.Attempts,
				"status":   httpResponse.StatusCode,
			}).Debug("HTTP Client: 请求成功")
			return httpResponse, nil
		})
		if err != nil {
			if err == breaker.ErrCircuitOpen {
				lastErr = fmt.Errorf("circuit breaker is open: %w", err)
				h.log.WithFields(logrus.Fields{
					"url":   req.URL,
					"state": breakerStateToString(h.circuitBreaker.GetState()),
				}).Error("HTTP Client: 熔断器已打开，拒绝请求")
				h.recordLog(req, httpResponse, 0, int8(attempts), lastErr, startTime)
				return httpResponse, lastErr
			}
			h.log.WithFields(logrus.Fields{
				"url":   req.URL,
				"state": breakerStateToString(h.circuitBreaker.GetState()),
				"error": err,
			}).Error("HTTP Client: 请求失败")
			h.recordLog(req, httpResponse, 0, int8(attempts), lastErr, startTime)
			return httpResponse, lastErr
		}
	} else {
		retryResult := h.retryExecutor.ExecuteSync(retryTask)
		attempts = retryResult.Attempts
		if retryResult.Error != nil {
			lastErr = retryResult.Error
			if resp, ok := isRetryableResponseError(retryResult.Error); ok {
				httpResponse = resp
			}
			h.log.WithFields(logrus.Fields{
				"url":   req.URL,
				"error": lastErr,
			}).Error("HTTP Client: 请求失败")
			h.recordLog(req, httpResponse, 0, int8(attempts), lastErr, startTime)
			return httpResponse, lastErr
		}
		httpResponse = retryResult.Data.(*HttpResponse)
		h.log.WithFields(logrus.Fields{
			"url":      req.URL,
			"attempts": retryResult.Attempts,
			"status":   httpResponse.StatusCode,
		}).Debug("HTTP Client: 请求成功")
	}

	h.recordLog(req, httpResponse, httpResponse.StatusCode, int8(attempts), nil, startTime)
	return httpResponse, nil
}

// prepareRequest 前置准备：序列化请求体、创建HTTP请求、组装请求头
// 此部分错误不会触发重试
// 序列化方式根据 Content-Type 自动选择：
//   - application/x-www-form-urlencoded：使用 query.Values 构建 form 表单参数
//   - 其他（默认 JSON）：使用 json.Marshal 序列化
func (h *HttpClient) prepareRequest(req *HttpRequest) (*http.Request, error) {
	var reqBytes []byte
	var err error
	var body io.Reader

	// 序列化请求体
	if req.Body != nil {
		contentType := ""
		if req.Headers != nil {
			contentType = req.Headers["Content-Type"]
		}

		if contentType == "application/x-www-form-urlencoded" {
			// form 表单序列化：使用 query.Values 基于 struct 的 url tag 构建参数
			values, queryErr := query.Values(req.Body)
			if queryErr != nil {
				h.log.WithFields(logrus.Fields{
					"error": queryErr,
					"body":  req.Body,
				}).Error("HTTP Client: form参数序列化失败")
				return nil, fmt.Errorf("encode form values failed: %w", queryErr)
			}
			encoded := values.Encode()
			reqBytes = []byte(encoded)
			body = bytes.NewBuffer(reqBytes)
		} else {
			// JSON 序列化（默认）
			reqBytes, err = json.Marshal(req.Body)
			if err != nil {
				h.log.WithFields(logrus.Fields{
					"error": err,
					"body":  req.Body,
				}).Error("HTTP Client: 请求参数序列化失败")
				return nil, fmt.Errorf("marshal request body failed: %w", err)
			}
			body = bytes.NewBuffer(reqBytes)
		}
	}
	if req.Files != nil && len(req.Files) > 0 {
		var ct string
		// 1. 构建multipart请求体
		body, ct, err = h.buildMultipartBody(req.Files)
		if err != nil {
			h.log.WithFields(logrus.Fields{
				"url":   req.URL,
				"error": err,
			}).Error("构建multipart请求体失败")
			return nil, err
		}
		req.Headers["Content-Type"] = ct
	}

	// 记录请求日志
	if reqBytes != nil {
		h.log.WithFields(logrus.Fields{
			"url":     req.URL,
			"method":  req.Method,
			"request": string(reqBytes),
		}).Debug("HTTP Client: 发送请求")
	}

	// 创建HTTP请求（携带调用方 ctx，供链路追踪把出站 span 挂到上游链路下）
	httpReq, err := http.NewRequestWithContext(req.ctxOrBackground(), req.Method, req.URL, body)
	if err != nil {
		h.log.WithFields(logrus.Fields{
			"error": err,
			"url":   req.URL,
		}).Error("HTTP Client: 创建HTTP请求失败")
		return nil, fmt.Errorf("create http request failed: %w", err)
	}

	// 设置请求头
	if req.Headers != nil {
		for k, v := range req.Headers {
			httpReq.Header.Set(k, v)
		}
	}

	return httpReq, nil
}

// buildMultipartBody 构建multipart请求体
func (h *HttpClient) buildMultipartBody(fields []MultipartField) (*bytes.Buffer, string, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	defer func(writer *multipart.Writer) {
		err := writer.Close()
		if err != nil {
			h.log.WithFields(logrus.Fields{
				"error": err,
			}).Error("HTTP Client: multipart writer close failed")
		}
	}(writer)

	for _, field := range fields {
		if field.FileName == "" {
			// 普通文本字段
			if err := writer.WriteField(field.FieldName, field.Value); err != nil {
				return nil, "", err
			}
		} else {
			// 文件字段
			part, err := writer.CreateFormFile(field.FieldName, field.FileName)
			if err != nil {
				return nil, "", err
			}
			if _, err = io.Copy(part, field.FileData); err != nil {
				return nil, "", err
			}
		}
	}

	if err := writer.Close(); err != nil {
		return nil, "", err
	}

	return body, writer.FormDataContentType(), nil
}

// executeRequest 执行真实的网络IO操作：发送请求、读取响应并校验状态码
// 网络错误和可重试状态码（5xx/408/429）会返回error以触发重试
func (h *HttpClient) executeRequest(httpReq *http.Request) (*HttpResponse, error) {
	// 发送请求
	resp, err := h.client.Do(httpReq)
	if err != nil {
		h.log.WithFields(logrus.Fields{
			"error": err,
			"url":   httpReq.URL.String(),
		}).Error("HTTP Client: HTTP请求失败")
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer func(Body io.ReadCloser) {
		if err1 := Body.Close(); err1 != nil {
			h.log.WithFields(logrus.Fields{
				"error": err1,
			}).Error("HTTP Client: response body close failed")
		}
	}(resp.Body)

	// 读取响应
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		h.log.WithFields(logrus.Fields{
			"error": err,
		}).Error("HTTP Client: 读取响应失败")
		return nil, fmt.Errorf("read response failed: %w", err)
	}

	httpResp := &HttpResponse{
		StatusCode: resp.StatusCode,
		Body:       respBody,
		Headers:    resp.Header,
	}

	// 记录响应日志
	h.log.WithFields(logrus.Fields{
		"endpoint": httpReq.URL.String(),
		"status":   resp.StatusCode,
		"response": string(respBody),
	}).Debug("HTTP Client: 收到响应")

	// 校验响应状态码：可重试的状态码返回error触发重试
	if isRetryableStatusCode(resp.StatusCode) {
		h.log.WithFields(logrus.Fields{
			"url":    httpReq.URL.String(),
			"status": resp.StatusCode,
		}).Warn("HTTP Client: 收到可重试的响应状态码")
		return httpResp, &retryableResponseError{
			response: httpResp,
			message:  fmt.Sprintf("retryable http status code: %d", resp.StatusCode),
		}
	}

	return httpResp, nil
}

// ParseJSONResponse 解析JSON响应到目标结构
func (h *HttpClient) ParseJSONResponse(resp *HttpResponse, result interface{}) error {
	// 检查HTTP状态码
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http status code: %d, body: %s", resp.StatusCode, string(resp.Body))
	}

	// 如果result为nil，不需要解析
	if result == nil {
		return nil
	}

	// 解析响应
	if err := json.Unmarshal(resp.Body, result); err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err,
			"body":  string(resp.Body),
		}).Error("HTTP Client: 响应解析失败")
		return fmt.Errorf("unmarshal response failed: %w", err)
	}

	return nil
}

// breakerStateToString 将熔断器状态转换为字符串
func breakerStateToString(state breaker.State) string {
	switch state {
	case breaker.StateClosed:
		return "CLOSED"
	case breaker.StateOpen:
		return "OPEN"
	case breaker.StateHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// GetCircuitBreakerState 获取熔断器状态
func (h *HttpClient) GetCircuitBreakerState() breaker.State {
	if !h.enableCircuitBreaker {
		return breaker.StateClosed // 未启用时返回关闭状态
	}
	return h.circuitBreaker.GetState()
}

// GetMetrics 获取熔断器指标
func (h *HttpClient) GetMetrics() *breaker.CircuitBreakerMetrics {
	if !h.enableCircuitBreaker {
		return nil // 未启用时返回nil
	}
	return h.circuitBreaker.GetMetrics()
}

// ResetCircuitBreaker 重置熔断器
func (h *HttpClient) ResetCircuitBreaker() {
	if !h.enableCircuitBreaker {
		logrus.Warn("HTTP Client: 熔断器未启用，无需重置")
		return
	}
	h.circuitBreaker.Reset()
	logrus.Warn("HTTP Client: 熔断器已手动重置")
}

// recordLog 异步记录HTTP请求日志（参考 sms.RecordKeeper 模式）
func (h *HttpClient) recordLog(req *HttpRequest, resp *HttpResponse, respStatus int, attempts int8, reqErr error, startTime time.Time) {
	if h.logKeeper == nil {
		return
	}

	logData := &HttpRequestLog{
		RequestId:   req.RequestId,
		AppId:       req.AppId,
		ServiceType: req.ServiceType,
		Method:      req.Method,
		URL:         req.URL,
		RespStatus:  respStatus,
		Duration:    int(time.Since(startTime).Milliseconds()),
		Attempts:    attempts,
	}

	// 请求头
	if req.Headers != nil {
		headersJSON, _ := json.Marshal(req.Headers)
		logData.ReqHeaders = string(headersJSON)
	}

	// 请求体
	if req.Body != nil {
		switch v := req.Body.(type) {
		case string:
			logData.ReqBody = v
		case []byte:
			logData.ReqBody = string(v)
		default:
			bodyJSON, _ := json.Marshal(req.Body)
			logData.ReqBody = string(bodyJSON)
		}
	}

	// 响应体
	if resp != nil && resp.Body != nil {
		logData.RespBody = string(resp.Body)
	}

	// 错误信息
	if reqErr != nil {
		logData.Status = 1
		logData.ErrorMsg = reqErr.Error()
		if len(logData.ErrorMsg) > 1000 {
			logData.ErrorMsg = logData.ErrorMsg[:1000]
		}
	}

	// 异步写入，不阻塞主流程
	go h.logKeeper.KeepHttpLog(logData)
}
