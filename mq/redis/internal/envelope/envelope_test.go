package envelope

import (
	"bytes"
	"testing"

	"github.com/xm-utils/tools/mq/core"
)

func TestFromMessageReadsAttemptHeader(t *testing.T) {
	msg := &mq.Message{ID: "m-1", Key: "k", Body: []byte("v")}
	msg.SetHeader(HeaderAttempt, "abc") // 非法值按 0
	if e := FromMessage(msg); e.Attempt != 0 {
		t.Fatalf("非法 attempt 头应按 0 处理, got %d", e.Attempt)
	}
	msg.SetHeader(HeaderAttempt, "2")
	e := FromMessage(msg)
	if e.Attempt != 2 {
		t.Fatalf("attempt 头解析错误, got %d", e.Attempt)
	}
	if e.ID != "m-1" {
		t.Fatalf("ID 应透传, got %q", e.ID)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	binary := []byte{0x00, 0xFF, 'a', '\n'}
	e := &Envelope{ID: "m-2", Key: "k", Body: binary, Headers: map[string]string{"t": "1"}, Attempt: 3}
	raw, err := e.Encode()
	if err != nil {
		t.Fatalf("Encode 失败: %v", err)
	}
	got, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode 失败: %v", err)
	}
	if !bytes.Equal(got.Body, binary) {
		t.Fatalf("Body 二进制往返不一致: %v", got.Body)
	}
	if got.ID != "m-2" || got.Key != "k" || got.Attempt != 3 || got.Headers["t"] != "1" {
		t.Fatalf("字段往返不一致: %+v", got)
	}
}

func TestDecodeInvalidJSON(t *testing.T) {
	if _, err := Decode("not-json"); err == nil {
		t.Fatal("非法 JSON 应返回 error")
	}
}

func TestCloneIsolatesHeaders(t *testing.T) {
	e := &Envelope{Headers: map[string]string{"a": "1"}}
	cp := e.Clone()
	cp.Headers["a"] = "2"
	cp.Attempt = 9
	if e.Headers["a"] != "1" || e.Attempt != 0 {
		t.Fatalf("Clone 修改污染了原信封: %+v", e)
	}
	// nil Headers 克隆不应panic
	if (&Envelope{}).Clone() == nil {
		t.Fatal("空信封克隆不应为 nil")
	}
}

func TestToMessageDeliveryIDAndHeaderSync(t *testing.T) {
	e := &Envelope{ID: "persist-1", Key: "k", Body: []byte("v"), Attempt: 2}

	// 持久 ID 场景（zset）：deliveryID 传空，保留信封 ID
	msg := e.ToMessage("topic-a", "", nil)
	if msg.ID != "persist-1" {
		t.Fatalf("deliveryID 为空应保留信封 ID, got %q", msg.ID)
	}
	if msg.Header(HeaderAttempt) != "2" {
		t.Fatalf("attempt 应同步到消息头, got %q", msg.Header(HeaderAttempt))
	}

	// 投递 ID 场景（list/stream）：覆盖信封 ID
	msg2 := e.ToMessage("topic-a", "dlv-9", "meta")
	if msg2.ID != "dlv-9" {
		t.Fatalf("deliveryID 应覆盖信封 ID, got %q", msg2.ID)
	}

	// 返回消息的 Headers 必须是副本：业务改写不得污染信封
	msg.SetHeader("trace", "x")
	if _, ok := e.Headers["trace"]; ok {
		t.Fatal("业务改写消息头污染了信封 Headers")
	}
}
