package stream

import (
	"bytes"
	"testing"

	rs "github.com/xm-utils/tools/mq/redis/stream/queue"

	"github.com/xm-utils/tools/mq/core"
	envpkg "github.com/xm-utils/tools/mq/redis/internal/envelope"
)

func TestEnvelopeRoundTripBinarySafe(t *testing.T) {
	raw := []byte{0x00, 0xFF, 0x80, 'j', 's', 'o', 'n'}
	src := &mq.Message{
		Key:     "req-1",
		Body:    raw,
		Headers: map[string]string{"trace": "abc"},
	}
	src.SetHeader(HeaderAttempt, "2")

	env := toEnvelope(src)
	payload, err := env.Encode()
	if err != nil {
		t.Fatalf("encode 失败: %v", err)
	}
	decoded, err := decodeEnvelope(payload)
	if err != nil {
		t.Fatalf("decode 失败: %v", err)
	}
	if decoded.Key != "req-1" || decoded.Attempt != 2 {
		t.Fatalf("Key/Attempt 往返不一致: %#v", decoded)
	}
	if !bytes.Equal(decoded.Body, raw) {
		t.Fatalf("二进制 Body 往返损坏: %v", decoded.Body)
	}
	if decoded.Headers["trace"] != "abc" {
		t.Fatalf("Headers 往返丢失: %#v", decoded.Headers)
	}
}

func TestDecodeEnvelopeInvalidJSON(t *testing.T) {
	if _, err := decodeEnvelope("not-a-json"); err == nil {
		t.Fatal("非法信封应返回 error（毒消息由消费侧终态处置）")
	}
}

func TestToEnvelopeAttemptDefaultZero(t *testing.T) {
	env := toEnvelope(&mq.Message{Headers: map[string]string{HeaderAttempt: "abc"}})
	if env.Attempt != 0 {
		t.Fatalf("非法 attempt 头应按 0 计, got %d", env.Attempt)
	}
	env = toEnvelope(&mq.Message{})
	if env.Attempt != 0 {
		t.Fatalf("缺失 attempt 头应按 0 计, got %d", env.Attempt)
	}
}

func TestToMessageSyncsAttemptHeader(t *testing.T) {
	sm := &rs.StreamMessage{MessageID: "1-0", RequestID: "req-1", Payload: "ignored"}
	env := &envpkg.Envelope{
		Key:     "req-1",
		Body:    []byte("v"),
		Headers: map[string]string{"trace": "abc"},
		Attempt: 3,
	}
	msg := toMessage("stream:orders", sm, env)
	if msg.ID != "1-0" || msg.Topic != "stream:orders" || msg.Key != "req-1" {
		t.Fatalf("字段映射错误: %#v", msg)
	}
	if msg.Header(HeaderAttempt) != "3" {
		t.Fatalf("attempt 应同步进消息头, got %q", msg.Header(HeaderAttempt))
	}
	if env.Headers[HeaderAttempt] != "" {
		t.Fatal("同步不得改写信封原始 Headers（共享 map 防护）")
	}

	// Attempt=0 时不应注入头
	msg0 := toMessage("t", sm, &envpkg.Envelope{Body: []byte("v")})
	if msg0.Header(HeaderAttempt) != "" {
		t.Fatalf("attempt=0 不应注入消息头, got %q", msg0.Header(HeaderAttempt))
	}

	// Metadata 保留组件消息
	if _, ok := msg.Metadata.(*rs.StreamMessage); !ok {
		t.Fatalf("Metadata 应为 *rs.StreamMessage, got %T", msg.Metadata)
	}
}
