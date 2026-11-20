package kafka

import (
	"reflect"
	"testing"
	"time"

	segkafka "github.com/segmentio/kafka-go"

	"github.com/xm-utils/tools/mq/core"
)

func TestToKafkaMessageMapsFields(t *testing.T) {
	msg := &mq.Message{
		Key:     "order-1",
		Body:    []byte(`{"amount":10}`),
		Headers: map[string]string{"trace": "abc", HeaderAttempt: "2"},
	}
	km, err := toKafkaMessage("orders", msg)
	if err != nil {
		t.Fatalf("toKafkaMessage 失败: %v", err)
	}
	if km.Topic != "orders" {
		t.Fatalf("Topic 映射错误: %s", km.Topic)
	}
	if string(km.Key) != "order-1" || string(km.Value) != `{"amount":10}` {
		t.Fatalf("Key/Value 映射错误: key=%s value=%s", km.Key, km.Value)
	}
	if got := fromSegHeaders(km.Headers)[HeaderAttempt]; got != "2" {
		t.Fatalf("Headers 映射丢失: %#v", km.Headers)
	}
	if km.Time.IsZero() {
		t.Fatal("Time 应默认填充当前时间")
	}
}

func TestToKafkaMessageEmptyTopic(t *testing.T) {
	if _, err := toKafkaMessage("", &mq.Message{}); err == nil {
		t.Fatal("空 topic 应返回 error")
	}
}

func TestHeadersRoundTrip(t *testing.T) {
	src := map[string]string{"a": "1", "b": "2", "c": "3"}
	headers := toSegHeaders(src)
	if len(headers) != 3 {
		t.Fatalf("头数量错误: %d", len(headers))
	}
	if got := fromSegHeaders(headers); !reflect.DeepEqual(got, src) {
		t.Fatalf("头往返不一致: %#v", got)
	}
	if toSegHeaders(nil) != nil || fromSegHeaders(nil) != nil {
		t.Fatal("空头双向转换应保持 nil")
	}
}

func TestFromKafkaMessage(t *testing.T) {
	km := segkafka.Message{
		Topic:     "orders",
		Partition: 3,
		Offset:    42,
		Key:       []byte("k1"),
		Value:     []byte("v1"),
		Headers:   []segkafka.Header{{Key: HeaderAttempt, Value: []byte("1")}},
		Time:      time.Now(),
	}
	msg := fromKafkaMessage(km)
	if msg.ID != "orders-3-42" {
		t.Fatalf("ID 生成错误: %s", msg.ID)
	}
	if msg.Key != "k1" || msg.Topic != "orders" || string(msg.Body) != "v1" {
		t.Fatalf("字段映射错误: %#v", msg)
	}
	if _, ok := msg.Metadata.(segkafka.Message); !ok {
		t.Fatalf("Metadata 应保留原始 kafka.Message, got %T", msg.Metadata)
	}
	if attemptOf(msg) != 1 {
		t.Fatalf("attemptOf 解析错误: %d", attemptOf(msg))
	}
	if attemptOf(&mq.Message{}) != 0 {
		t.Fatal("缺失 attempt 头应按 0 计")
	}
	if attemptOf(&mq.Message{Headers: map[string]string{HeaderAttempt: "-5"}}) != 0 {
		t.Fatal("非法 attempt 头应按 0 计")
	}
}

func TestCloneForRequeueKeepsOriginalIntact(t *testing.T) {
	orig := &mq.Message{ID: "m1", Headers: map[string]string{"trace": "abc"}}
	clone := cloneForRequeue(orig, 3)

	if clone.Header(HeaderAttempt) != "3" {
		t.Fatalf("副本 attempt 头错误: %q", clone.Header(HeaderAttempt))
	}
	if orig.Header(HeaderAttempt) != "" {
		t.Fatal("克隆不应改写原消息 Headers")
	}
	if clone.Header("trace") != "abc" {
		t.Fatal("克隆应保留原有 Headers")
	}
	clone.SetHeader("trace", "changed")
	if orig.Header("trace") != "abc" {
		t.Fatal("Headers 必须深拷贝隔离")
	}
}
