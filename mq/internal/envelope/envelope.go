// Package envelope 提供 Redis 系 MQ 适配器（redis/stream / redis/list / redis/zset）
// 共享的消息信封编解码。
//
// 统一信封格式的意义：同一条 mq.Message 在三种 Redis 后端间迁移时字节级兼容，
// 与后端选择无关的重投计数（Attempt）、Key、Headers 语义由本包单点维护。
// Body 用 []byte 由 encoding/json 以 base64 编解码，保证二进制安全。
package envelope

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/xm-utils/tools/mq"
)

// HeaderAttempt 重投次数消息头（所有后端适配器同名，信封内计数与消息头双向同步）。
const HeaderAttempt = "X-Mq-Attempt"

// Envelope 消息信封。ID 为消息持久标识（可为空，由后端适配器按能力补全）。
type Envelope struct {
	ID      string            `json:"id,omitempty"`
	Key     string            `json:"key,omitempty"`
	Body    []byte            `json:"body,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Attempt int               `json:"attempt,omitempty"` // 已重投次数（防热循环额度）
}

// FromMessage mq.Message → Envelope（attempt 取自消息头，缺失/非法按 0）。
func FromMessage(msg *mq.Message) *Envelope {
	attempt := 0
	if v := msg.Header(HeaderAttempt); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			attempt = n
		}
	}
	return &Envelope{ID: msg.ID, Key: msg.Key, Body: msg.Body, Headers: msg.Headers, Attempt: attempt}
}

// Clone 深拷贝信封（Headers 与 Body 均隔离），供重投计数变更使用，不得改写原信封。
func (e *Envelope) Clone() *Envelope {
	cp := *e
	if e.Headers != nil {
		cp.Headers = make(map[string]string, len(e.Headers))
		for k, v := range e.Headers {
			cp.Headers[k] = v
		}
	}
	cp.Body = copyBytes(e.Body)
	return &cp
}

// copyBytes 切片深拷贝，nil 保持 nil。
func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

// Encode 序列化信封为 Redis 存储字符串。
func (e *Envelope) Encode() (string, error) {
	raw, err := json.Marshal(e)
	if err != nil {
		return "", fmt.Errorf("mq/envelope: 消息信封序列化失败: %w", err)
	}
	return string(raw), nil
}

// Decode 反序列化信封，非法 JSON 返回 error（消费侧按毒消息进终态处置，不 panic）。
func Decode(raw string) (*Envelope, error) {
	var e Envelope
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		return nil, fmt.Errorf("mq/envelope: 消息信封反序列化失败: %w", err)
	}
	return &e, nil
}

// ToMessage 信封 → mq.Message。deliveryID 为本次投递标识（非空时覆盖信封 ID，
// 用于无持久消息 ID 语义或需逐次投递确认的后端如 List/ZSet 队列）；Headers 深拷贝
// 后同步 attempt 头，Body 副本返回，保证业务对返回消息的改写（含原地修改 Body）
// 不影响信封本体，重投副本始终携带原始内容。
func (e *Envelope) ToMessage(topic, deliveryID string, metadata any) *mq.Message {
	id := e.ID
	if deliveryID != "" {
		id = deliveryID
	}
	var headers map[string]string
	if len(e.Headers) > 0 {
		headers = make(map[string]string, len(e.Headers)+1)
		for k, v := range e.Headers {
			headers[k] = v
		}
	}
	if e.Attempt > 0 {
		if headers == nil {
			headers = make(map[string]string, 1)
		}
		headers[HeaderAttempt] = strconv.Itoa(e.Attempt)
	}
	return &mq.Message{
		ID:       id,
		Key:      e.Key,
		Topic:    topic,
		Body:     copyBytes(e.Body),
		Headers:  headers,
		Metadata: metadata,
	}
}
