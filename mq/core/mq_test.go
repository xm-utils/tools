package mq

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestMessageHeaderHelpers(t *testing.T) {
	var nilMsg *Message
	if got := nilMsg.Header("x"); got != "" {
		t.Fatalf("nil 接收器 Header 应返回空串, got %q", got)
	}

	m := &Message{}
	if got := m.Header("missing"); got != "" {
		t.Fatalf("缺失头应返回空串, got %q", got)
	}
	m.SetHeader("k", "v")
	if m.Header("k") != "v" {
		t.Fatalf("SetHeader 后 Header 应可读, got %q", m.Header("k"))
	}
	if m.Headers == nil || len(m.Headers) != 1 {
		t.Fatalf("SetHeader 应惰性初始化 Headers, got %#v", m.Headers)
	}
}

func TestInvokeHandlerNil(t *testing.T) {
	err := InvokeHandler(context.Background(), nil, &Message{})
	if err == nil || !strings.Contains(err.Error(), "handler 不能为 nil") {
		t.Fatalf("nil handler 应返回配置错误, got %v", err)
	}
}

func TestInvokeHandlerPassThrough(t *testing.T) {
	want := errors.New("boom")
	err := InvokeHandler(context.Background(), func(ctx context.Context, msg *Message) error {
		return want
	}, &Message{})
	if !errors.Is(err, want) {
		t.Fatalf("Handler 错误应原样透出, got %v", err)
	}

	if err := InvokeHandler(context.Background(), func(ctx context.Context, msg *Message) error {
		return ErrManualAck
	}, &Message{}); !errors.Is(err, ErrManualAck) {
		t.Fatalf("ErrManualAck 哨兵应可被 errors.Is 识别, got %v", err)
	}
}

func TestInvokeHandlerPanicToError(t *testing.T) {
	err := InvokeHandler(context.Background(), func(ctx context.Context, msg *Message) error {
		panic("panic-from-handler")
	}, &Message{ID: "m1"})
	if err == nil || !strings.Contains(err.Error(), "panic-from-handler") {
		t.Fatalf("Handler panic 应转 error, got %v", err)
	}
}
