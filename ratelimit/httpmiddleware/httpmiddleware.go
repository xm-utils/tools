// Package httpmiddleware 把 ratelimit 挂载为 HTTP 入口中间件(ROADMAP 3.2):
// 基于 net/http 标准 Handler 包装,纯 HTTP 服务直接 Wrap;
// Gin/grpcx cmux HTTP 端口经 engine.Use(gin.WrapH(Wrap(limiter))) 装配,
// httpclient/grpcx 本体零改动。
//
// 拒绝语义:429 Too Many Requests + Retry-After 秒头(向上取整,可预估时)
// + JSON 错误体;限流后端自身故障默认 fail-closed 返回 500,
// FailOpen=true 时放行。
package httpmiddleware

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/xm-utils/tools/ratelimit"
)

// HeaderRetryAfter 标准 Retry-After 响应头(秒)
const HeaderRetryAfter = "Retry-After"

// Config 中间件装配配置(同 grpcmiddleware:两限流维度互斥)
type Config struct {
	// Limiter 服务级固定限流器;与 Local 互斥
	Limiter ratelimit.Limiter
	// Local 本地内存后端,配合 KeyFunc 做多维限流;与 Limiter 互斥
	Local *ratelimit.Local
	// KeyFunc 从请求提取限流 Key(nil 时:Local 模式默认用 r.URL.Path;
	// 租户级可取 r.Header.Get("X-Tenant-Id") 等;返回空串直通不限流)
	KeyFunc func(r *http.Request) (string, error)
	// FailOpen 限流后端故障时放行(默认 false:故障返回 500)
	FailOpen bool
}

// resolve 按请求定位限流器;返回 nil 表示直通
type resolve func(r *http.Request) (ratelimit.Limiter, error)

func newResolve(cfg Config) (resolve, error) {
	if (cfg.Limiter == nil) == (cfg.Local == nil) {
		return nil, fmt.Errorf("%w: Limiter 与 Local 必须二选一", ratelimit.ErrInvalidConfig)
	}
	if cfg.Limiter != nil {
		return func(*http.Request) (ratelimit.Limiter, error) {
			return cfg.Limiter, nil
		}, nil
	}
	keyFn := cfg.KeyFunc
	if keyFn == nil {
		keyFn = func(r *http.Request) (string, error) {
			return r.URL.Path, nil
		}
	}
	return func(r *http.Request) (ratelimit.Limiter, error) {
		key, err := keyFn(r)
		if err != nil {
			return nil, err
		}
		if key == "" {
			return nil, nil
		}
		return cfg.Local.Limiter(key)
	}, nil
}

// Wrap 以默认服务级限流包装 handler,等价 New(Config{Limiter: l}) 的结果应用;
// l 为 nil 时返回 ErrInvalidConfig error,不 panic
func Wrap(l ratelimit.Limiter, next http.Handler) (http.Handler, error) {
	return New(Config{Limiter: l}, next)
}

// New 构造限流中间件 handler;Config 非法返回 error。
// 同一 Config 可复用于多个挂载点(中间件自身无额外状态)。
func New(cfg Config, next http.Handler) (http.Handler, error) {
	r, err := newResolve(cfg)
	if err != nil {
		return nil, err
	}
	return wrap(r, cfg, next), nil
}

// Middleware 返回中间件函数形态,便于链式装配场景一次性构造后多处复用;
// Config 非法时返回 error,由调用方处置
func Middleware(cfg Config) (func(http.Handler) http.Handler, error) {
	r, err := newResolve(cfg)
	if err != nil {
		return nil, err
	}
	return func(next http.Handler) http.Handler {
		return wrap(r, cfg, next)
	}, nil
}

func wrap(r resolve, cfg Config, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		l, err := r(req)
		if err != nil {
			onBackendError(cfg, w, err, next, req)
			return
		}
		if l == nil {
			next.ServeHTTP(w, req)
			return
		}
		aerr := l.Allow(req.Context())
		switch {
		case aerr == nil:
			next.ServeHTTP(w, req)
		case errors.Is(aerr, ratelimit.ErrRateLimited):
			rejected(w, l)
		default:
			onBackendError(cfg, w, aerr, next, req)
		}
	})
}

// rejected 输出 429:Retry-After 头取自判定同批的 Peek 快照(可预估才写),
// JSON 体与 common 统一响应体的 code/message 字段名对齐
func rejected(w http.ResponseWriter, l ratelimit.Limiter) {
	if wait := l.Peek().RetryAfter; wait > 0 {
		w.Header().Set(HeaderRetryAfter, strconv.Itoa(int(math.Ceil(wait.Seconds()))))
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusTooManyRequests)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"code":    http.StatusTooManyRequests,
		"message": "too many requests",
	})
}

func onBackendError(cfg Config, w http.ResponseWriter, cause error, next http.Handler, req *http.Request) {
	if cfg.FailOpen {
		next.ServeHTTP(w, req)
		return
	}
	http.Error(w, "ratelimit backend error", http.StatusInternalServerError)
}
