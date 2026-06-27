package redis

import (
	"context"
	"fmt"
	"testing"
	"time"
)

const (
	streamKey    = "test_stream"
	groupName    = "test_group"
	consumerName = "test_consumer"
)

func TestXAdd(t *testing.T) {
	ctx := context.Background()

	// 清理测试数据
	_ = Delete(ctx, streamKey)

	// 测试添加消息
	messageID, err := XAdd(ctx, &XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{
			"name":      "test_message",
			"value":     "hello world",
			"timestamp": time.Now().Unix(),
		},
	})
	if err != nil {
		t.Errorf("XAdd error: %v", err)
		return
	}
	t.Logf("Message added with ID: %s", messageID)

	// 验证消息已添加
	length, err := XLen(ctx, streamKey)
	if err != nil {
		t.Errorf("XLen error: %v", err)
		return
	}
	t.Logf("Stream length: %d", length)
	if length != 1 {
		t.Errorf("Expected stream length 1, got %d", length)
	}
}

func TestXRead(t *testing.T) {
	ctx := context.Background()

	// 先添加一些消息
	for i := 0; i < 3; i++ {
		_, err := XAdd(ctx, &XAddArgs{
			Stream: streamKey,
			Values: map[string]interface{}{
				"index": i,
				"data":  fmt.Sprintf("message_%d", i),
			},
		})
		if err != nil {
			t.Errorf("XAdd error: %v", err)
			return
		}
	}

	// 读取消息
	result, err := XRead(ctx, &XReadArgs{
		Streams: []string{streamKey},
		IDs:     []string{"0"}, // 从开始读取
		Count:   10,
	})
	if err != nil {
		t.Errorf("XRead error: %v", err)
		return
	}

	for stream, messages := range result {
		t.Logf("Stream: %s, Messages count: %d", stream, len(messages))
		for _, msg := range messages {
			t.Logf("Message ID: %s, Values: %v", msg.ID, msg.Values)
		}
	}
}

func TestXRange(t *testing.T) {
	ctx := context.Background()

	// 获取范围内的消息
	messages, err := XRange(ctx, streamKey, "-", "+", 10)
	if err != nil {
		t.Errorf("XRange error: %v", err)
		return
	}

	t.Logf("Found %d messages in range", len(messages))
	for _, msg := range messages {
		t.Logf("Message ID: %s, Values: %v", msg.ID, msg.Values)
	}
}

func TestXDel(t *testing.T) {
	ctx := context.Background()

	// 先获取一个消息ID用于删除
	messages, err := XRange(ctx, streamKey, "-", "+", 1)
	if err != nil {
		t.Errorf("XRange error: %v", err)
		return
	}

	if len(messages) > 0 {
		msgID := messages[0].ID
		deleted, err := XDel(ctx, streamKey, msgID)
		if err != nil {
			t.Errorf("XDel error: %v", err)
			return
		}
		t.Logf("Deleted %d messages", deleted)
	}
}

func TestXGroupOperations(t *testing.T) {
	ctx := context.Background()

	// 创建消费者组
	err := XGroupCreate(ctx, streamKey, groupName, "0")
	if err != nil {
		t.Logf("XGroupCreate error (may already exist): %v", err)
	} else {
		t.Log("Consumer group created successfully")
	}

	// 获取消费者组信息
	groups, err := XInfoGroups(ctx, streamKey)
	if err != nil {
		t.Errorf("XInfoGroups error: %v", err)
		return
	}

	for _, group := range groups {
		t.Logf("Group: %s, Consumers: %d, Pending: %d, LastID: %s",
			group.Name, group.Consumers, group.Pending, group.LastID)
	}
}

func TestXReadGroup(t *testing.T) {
	ctx := context.Background()

	// 确保消费者组存在
	_ = XGroupCreate(ctx, streamKey, groupName, "0")

	// 从消费者组读取消息
	result, err := XReadGroup(ctx, &XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamKey},
		IDs:      []string{">"}, // 只读取新消息
		Count:    10,
		Block:    1 * time.Second,
	})
	if err != nil {
		t.Errorf("XReadGroup error: %v", err)
		return
	}

	for stream, messages := range result {
		t.Logf("Stream: %s, Messages count: %d", stream, len(messages))
		for _, msg := range messages {
			t.Logf("Message ID: %s, Values: %v", msg.ID, msg.Values)

			// 确认消息
			acked, err := XAck(ctx, streamKey, groupName, msg.ID)
			if err != nil {
				t.Errorf("XAck error: %v", err)
			} else {
				t.Logf("Acknowledged %d messages", acked)
			}
		}
	}
}

func TestXPending(t *testing.T) {
	ctx := context.Background()

	// 查看待处理的消息
	pending, err := XPending(ctx, &XPendingArgs{
		Stream: streamKey,
		Group:  groupName,
		Start:  "-",
		End:    "+",
		Count:  10,
	})
	if err != nil {
		t.Errorf("XPending error: %v", err)
		return
	}

	t.Logf("Pending messages count: %d", len(pending))
	for _, p := range pending {
		t.Logf("Pending message ID: %s, Consumer: %s, Idle: %v",
			p.ID, p.Consumer, p.Idle)
	}
}

func TestXTrim(t *testing.T) {
	ctx := context.Background()

	// 修剪流，保留最新的5条消息
	trimmed, err := XTrim(ctx, streamKey, 5)
	if err != nil {
		t.Errorf("XTrim error: %v", err)
		return
	}
	t.Logf("Trimmed %d messages", trimmed)

	// 验证修剪后的长度
	length, err := XLen(ctx, streamKey)
	if err != nil {
		t.Errorf("XLen error: %v", err)
		return
	}
	t.Logf("Stream length after trim: %d", length)
}

func TestXAutoClaim(t *testing.T) {
	ctx := context.Background()

	// 自动认领空闲超过1分钟的消息
	start, messages, err := XAutoClaim(ctx, &XAutoClaimArgs{
		Stream:   streamKey,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  1 * time.Minute,
		Start:    "0",
		Count:    10,
	})
	if err != nil {
		t.Errorf("XAutoClaim error: %v", err)
		return
	}

	t.Logf("AutoClaim start ID: %s, Claimed messages: %d", start, len(messages))
	for _, msg := range messages {
		t.Logf("Claimed message ID: %s, Values: %v", msg.ID, msg.Values)
	}
}

func TestStreamWorkflow(t *testing.T) {
	ctx := context.Background()

	// 完整的流工作流程测试
	streamName := "workflow_test_stream"
	testGroup := "workflow_group"
	testConsumer := "workflow_consumer"

	// 清理测试数据
	_ = Delete(ctx, streamName)

	// 1. 添加消息
	t.Log("Step 1: Adding messages to stream")
	var messageIDs []string
	for i := 0; i < 5; i++ {
		id, err := XAdd(ctx, &XAddArgs{
			Stream: streamName,
			Values: map[string]interface{}{
				"task_id":    i,
				"task_name":  fmt.Sprintf("task_%d", i),
				"status":     "pending",
				"created_at": time.Now().Unix(),
			},
		})
		if err != nil {
			t.Errorf("XAdd error: %v", err)
			return
		}
		messageIDs = append(messageIDs, id)
		t.Logf("Added message with ID: %s", id)
	}

	// 2. 创建消费者组
	t.Log("Step 2: Creating consumer group")
	err := XGroupCreate(ctx, streamName, testGroup, "0")
	if err != nil {
		t.Logf("XGroupCreate error (may already exist): %v", err)
	}

	// 3. 从消费者组读取消息
	t.Log("Step 3: Reading messages from consumer group")
	result, err := XReadGroup(ctx, &XReadGroupArgs{
		Group:    testGroup,
		Consumer: testConsumer,
		Streams:  []string{streamName},
		IDs:      []string{">"},
		Count:    10,
	})
	if err != nil {
		t.Errorf("XReadGroup error: %v", err)
		return
	}

	totalMessages := 0
	for stream, messages := range result {
		t.Logf("Stream: %s, Received %d messages", stream, len(messages))
		totalMessages += len(messages)

		for _, msg := range messages {
			t.Logf("Processing message ID: %s, Values: %v", msg.ID, msg.Values)

			// 模拟处理消息
			time.Sleep(100 * time.Millisecond)

			// 确认消息
			acked, err := XAck(ctx, streamName, testGroup, msg.ID)
			if err != nil {
				t.Errorf("XAck error: %v", err)
			} else {
				t.Logf("Acknowledged %d messages", acked)
			}
		}
	}

	t.Logf("Total messages processed: %d", totalMessages)

	// 4. 检查流信息
	t.Log("Step 4: Checking stream information")
	length, err := XLen(ctx, streamName)
	if err != nil {
		t.Errorf("XLen error: %v", err)
		return
	}
	t.Logf("Final stream length: %d", length)

	// 5. 检查消费者组信息
	t.Log("Step 5: Checking consumer group information")
	groups, err := XInfoGroups(ctx, streamName)
	if err != nil {
		t.Errorf("XInfoGroups error: %v", err)
		return
	}

	for _, group := range groups {
		t.Logf("Group: %s, Consumers: %d, Pending: %d, LastID: %s",
			group.Name, group.Consumers, group.Pending, group.LastID)
	}
}
