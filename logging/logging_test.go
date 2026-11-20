//go:build test

package logging_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/logging"
	"github.com/xm-utils/tools/trace"
)

// resetLogger 每个用例结束后恢复标准 logger 到无害默认，避免全局状态串扰。
func resetLogger(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		_ = logging.Init(logging.Config{})
		logrus.SetOutput(os.Stderr)
	})
}

// capture 将标准 logger 输出重定向到缓冲区并返回其字节。
func capture(t *testing.T, f func()) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	logrus.SetOutput(&buf)
	f()
	logrus.SetOutput(io.Discard)

	var out map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("输出非合法 JSON: %v, raw=%q", err, buf.String())
	}
	return out
}

func TestInitValidationKeepsPreviousState(t *testing.T) {
	resetLogger(t)

	if err := logging.Init(logging.Config{Level: "debug", Service: "svc-a"}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	if err := logging.Init(logging.Config{Level: "verbose"}); err == nil {
		t.Error("非法 level 应返回 error")
	}
	if err := logging.Init(logging.Config{TraceKey: "bad key"}); err == nil {
		t.Error("含空白的 trace_key 应返回 error")
	}
	// 失败调用不得改变现配置
	if logging.CurrentLevel() != "debug" || logging.Service() != "svc-a" {
		t.Errorf("非法 Init 后配置被篡改: level=%s service=%s", logging.CurrentLevel(), logging.Service())
	}
}

func TestTraceIDAutoAttached(t *testing.T) {
	resetLogger(t)
	if err := logging.Init(logging.Config{}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}

	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	ctx, span := provider.Tracer("test").Start(context.Background(), "op")
	defer span.End()
	wantTID := trace.TraceIDFromCtx(ctx)
	if wantTID == "" {
		t.Fatal("测试前提：sdk span 应产生有效 TraceID")
	}

	got := capture(t, func() {
		logging.Context(ctx).Info("order created")
	})
	if got["trace_id"] != wantTID {
		t.Errorf("trace_id 期望 %s, 得 %v", wantTID, got["trace_id"])
	}
	// 防回归：Format 内追加字段不得丢失原始 msg/level
	if got["msg"] != "order created" || got["level"] != "info" {
		t.Errorf("msg/level 被篡改: msg=%v level=%v", got["msg"], got["level"])
	}

	// 无链路上下文：不附加 trace_id 字段（零污染）
	got = capture(t, func() {
		logging.Context(context.Background()).Info("no trace")
	})
	if _, ok := got["trace_id"]; ok {
		t.Errorf("无 trace 上下文不应出现 trace_id: %v", got)
	}

	// nil ctx Entry 同样不应 panic 或附加
	got = capture(t, func() {
		logging.For("m").Info("plain")
	})
	if _, ok := got["trace_id"]; ok {
		t.Errorf("无 Context 的 Entry 不应出现 trace_id")
	}
}

func TestCustomTraceKeyAndService(t *testing.T) {
	resetLogger(t)
	if err := logging.Init(logging.Config{Service: "order-svc", TraceKey: "tid"}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}

	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	ctx, span := provider.Tracer("test").Start(context.Background(), "op")
	defer span.End()

	got := capture(t, func() {
		logging.Context(ctx).WithField("k", "v").Info("x")
	})
	if got["tid"] == nil || got["trace_id"] != nil {
		t.Errorf("自定义键名未生效: %v", got)
	}
	if got["service"] != "order-svc" {
		t.Errorf("service 字段期望 order-svc, 得 %v", got["service"])
	}
	if got["k"] != "v" {
		t.Errorf("业务字段丢失: %v", got)
	}
}

func TestForInjectsModule(t *testing.T) {
	resetLogger(t)
	if err := logging.Init(logging.Config{}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	got := capture(t, func() {
		logging.For("deadletter").Warn("x")
	})
	if got["module"] != "deadletter" {
		t.Errorf("For 应注入 module 字段, 得 %v", got["module"])
	}
	if got["msg"] != "x" || got["level"] != "warning" {
		t.Errorf("msg/level 丢失: msg=%v level=%v", got["msg"], got["level"])
	}
}

func TestBusinessFieldWinsOverService(t *testing.T) {
	resetLogger(t)
	if err := logging.Init(logging.Config{Service: "auto-svc"}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	got := capture(t, func() {
		logging.For("m").WithField("service", "biz-svc").Info("x")
	})
	if got["service"] != "biz-svc" {
		t.Errorf("业务自设 service 字段应优先, 得 %v", got["service"])
	}
}

func TestSetLevelAndCurrent(t *testing.T) {
	resetLogger(t)
	if err := logging.SetLevel("DEBUG"); err != nil {
		t.Fatalf("SetLevel 失败: %v", err)
	}
	if logging.CurrentLevel() != "debug" {
		t.Errorf("期望 debug, 得 %s", logging.CurrentLevel())
	}
	if err := logging.SetLevel("verbose"); err == nil {
		t.Error("非法级别应返回 error")
	}
}

func TestTextFormatWhenJSONDisabled(t *testing.T) {
	resetLogger(t)
	text := false
	if err := logging.Init(logging.Config{JSON: &text, Service: "txt-svc"}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	var buf bytes.Buffer
	logrus.SetOutput(&buf)
	logging.For("m").Info("hello")
	out := buf.String()
	if strings.Contains(out, `"level":"info"`) {
		t.Errorf("期望文本格式, 得 JSON: %q", out)
	}
	if !strings.Contains(out, "msg=hello") {
		t.Errorf("文本格式不应丢失消息: %q", out)
	}
	if !strings.Contains(out, "service=txt-svc") {
		t.Errorf("文本模式应追加 service 字段: %q", out)
	}
}
