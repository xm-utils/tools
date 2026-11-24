package httpmiddleware

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xm-utils/tools/ratelimit"
)

var errBoom = errors.New("backend boom")

// stubLimiter 可编程限流器
type stubLimiter struct {
	allowErr error
}

func (s *stubLimiter) Allow(ctx context.Context) error { return s.allowErr }
func (s *stubLimiter) AllowN(ctx context.Context, n int) (bool, error) {
	return false, s.allowErr
}
func (s *stubLimiter) Peek() ratelimit.Result {
	return ratelimit.Result{RetryAfter: 2 * time.Second}
}
func (s *stubLimiter) Close() error { return nil }

func frozenClock() ratelimit.Clock {
	var t atomic.Int64
	t.Store(time.Unix(1700000000, 0).UnixNano())
	return ratelimit.ClockFunc(func() time.Time {
		return time.Unix(0, t.Load())
	})
}

func echoHandler(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte("echo"))
}

func do(h http.Handler, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestConfigValidation(t *testing.T) {
	local, _ := ratelimit.NewLocal(ratelimit.LocalConfig{Rate: 1, Burst: 1})
	stub := &stubLimiter{}

	if _, err := New(Config{}, http.HandlerFunc(echoHandler)); !errors.Is(err, ratelimit.ErrInvalidConfig) {
		t.Fatalf("两者皆 nil 应报 ErrInvalidConfig: %v", err)
	}
	if _, err := New(Config{Limiter: stub, Local: local}, http.HandlerFunc(echoHandler)); !errors.Is(err, ratelimit.ErrInvalidConfig) {
		t.Fatalf("两者同时设置应报错: %v", err)
	}
	if _, err := Wrap(nil, http.HandlerFunc(echoHandler)); !errors.Is(err, ratelimit.ErrInvalidConfig) {
		t.Fatalf("Wrap nil Limiter 应报错: %v", err)
	}
	if _, err := Middleware(Config{}); !errors.Is(err, ratelimit.ErrInvalidConfig) {
		t.Fatalf("Middleware 同契约: %v", err)
	}
}

func TestWrap_AllowThenReject(t *testing.T) {
	tb, _ := ratelimit.NewTokenBucket(ratelimit.TokenBucketConfig{
		Rate: 10, Burst: 3, Clock: frozenClock(),
	})
	h, err := Wrap(tb, http.HandlerFunc(echoHandler))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if rec := do(h, "/api/x"); rec.Code != http.StatusOK {
			t.Fatalf("前 3 次应 200, 实际 %d", rec.Code)
		}
	}
	rec := do(h, "/api/x")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("第 4 次应 429, 实际 %d", rec.Code)
	}
	// Retry-After 向上取整为秒:缺 1 令牌 / 10 每秒 = 100ms → 1
	if ra := rec.Header().Get(HeaderRetryAfter); ra != "1" {
		t.Fatalf("Retry-After 期望 1, 得到 %q", ra)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("429 响应体应为 JSON: %v", err)
	}
	if code, _ := body["code"].(float64); int(code) != http.StatusTooManyRequests {
		t.Fatalf("响应体 code 应为 429: %+v", body)
	}
}

func TestLocal_PerPathIsolation(t *testing.T) {
	local, _ := ratelimit.NewLocal(ratelimit.LocalConfig{
		Rate: 10, Burst: 1, Clock: frozenClock(),
	})
	h, _ := New(Config{Local: local}, http.HandlerFunc(echoHandler))

	if rec := do(h, "/a"); rec.Code != http.StatusOK {
		t.Fatalf("/a 首次应放行: %d", rec.Code)
	}
	if rec := do(h, "/a"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("/a 二次应 429: %d", rec.Code)
	}
	if rec := do(h, "/b"); rec.Code != http.StatusOK {
		t.Fatalf("/b 应有独立份额: %d", rec.Code)
	}
}

func TestKeyFuncEmptyKeyPassthrough(t *testing.T) {
	local, _ := ratelimit.NewLocal(ratelimit.LocalConfig{
		Rate: 10, Burst: 1, Clock: frozenClock(),
	})
	h, _ := New(Config{
		Local:   local,
		KeyFunc: func(r *http.Request) (string, error) { return r.Header.Get("X-Tenant"), nil },
	}, http.HandlerFunc(echoHandler))

	// 无租户头:直通且不经限流判定(反复请求恒 200)
	req := httptest.NewRequest(http.MethodGet, "/public", nil)
	rec := httptest.NewRecorder()
	for i := 0; i < 5; i++ {
		h.ServeHTTP(rec, req)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("空 Key 应直通: %d", rec.Code)
	}
	// 带租户头:正常按租户计数
	req.Header.Set("X-Tenant", "t1")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("t1 首次应放行: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("t1 二次应 429: %d", rec.Code)
	}
}

func TestBackendError(t *testing.T) {
	t.Run("默认 fail-closed 500", func(t *testing.T) {
		h, _ := New(Config{Limiter: &stubLimiter{allowErr: errBoom}}, http.HandlerFunc(echoHandler))
		if rec := do(h, "/x"); rec.Code != http.StatusInternalServerError {
			t.Fatalf("期望 500, 得到 %d", rec.Code)
		}
	})
	t.Run("FailOpen 放行", func(t *testing.T) {
		h, _ := New(Config{Limiter: &stubLimiter{allowErr: errBoom}, FailOpen: true}, http.HandlerFunc(echoHandler))
		rec := do(h, "/x")
		if rec.Code != http.StatusOK || rec.Body.String() != "echo" {
			t.Fatalf("FailOpen 应到达 handler: %d %q", rec.Code, rec.Body.String())
		}
	})
	t.Run("KeyFunc 报错按后端故障处置", func(t *testing.T) {
		local, _ := ratelimit.NewLocal(ratelimit.LocalConfig{Rate: 1, Burst: 1})
		h, _ := New(Config{
			Local:   local,
			KeyFunc: func(r *http.Request) (string, error) { return "", errBoom },
		}, http.HandlerFunc(echoHandler))
		if rec := do(h, "/x"); rec.Code != http.StatusInternalServerError {
			t.Fatalf("KeyFunc 报错期望 500, 得到 %d", rec.Code)
		}
	})
}

func TestMiddleware_FunctionalForm(t *testing.T) {
	tb, _ := ratelimit.NewTokenBucket(ratelimit.TokenBucketConfig{
		Rate: 10, Burst: 1, Clock: frozenClock(),
	})
	mw, err := Middleware(Config{Limiter: tb})
	if err != nil {
		t.Fatal(err)
	}
	h := mw(http.HandlerFunc(echoHandler))
	if rec := do(h, "/x"); rec.Code != http.StatusOK {
		t.Fatalf("首次应放行: %d", rec.Code)
	}
	if rec := do(h, "/x"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("二次应 429: %d", rec.Code)
	}
	// 拒绝时 Retry-After:缺 1 令牌 / 10 每秒 = 100ms → 向上取整 1 秒
	rec := do(h, "/x")
	if ra := rec.Header().Get(HeaderRetryAfter); ra != "1" {
		t.Fatalf("Retry-After 期望 1, 得到 %q", ra)
	}
}
