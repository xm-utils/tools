package redis

import (
	"context"
	"fmt"
	"time"
)

func main() {
	// 初始化 Redis 连接
	err := InitRedisCache(&Config{
		Prefix:   "stream_example",
		Host:     "127.0.0.1:6379",
		Password: "",
		DbNum:    0,
	})
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	// 示例 1: 添加消息到流
	fmt.Println("=== 示例 1: 添加消息到流 ===")
	msgID, err := XAdd(ctx, &XAddArgs{
		Stream: "events",
		Values: map[string]interface{}{
			"event":     "user_login",
			"user_id":   "123",
			"timestamp": time.Now().Unix(),
		},
	})
	if err != nil {
		fmt.Println("XAdd error:", err)
	} else {
		fmt.Println("Message added with ID:", msgID)
	}

	// 添加更多消息
	for i := 0; i < 5; i++ {
		id, err := XAdd(ctx, &XAddArgs{
			Stream: "events",
			Values: map[string]interface{}{
				"event":     fmt.Sprintf("event_%d", i),
				"data":      fmt.Sprintf("data_%d", i),
				"timestamp": time.Now().Unix(),
			},
		})
		if err != nil {
			fmt.Println("XAdd error:", err)
		} else {
			fmt.Printf("Message %d added with ID: %s\n", i, id)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 示例 2: 获取流的长度
	fmt.Println("\n=== 示例 2: 获取流的长度 ===")
	length, err := XLen(ctx, "events")
	if err != nil {
		fmt.Println("XLen error:", err)
	} else {
		fmt.Println("Stream length:", length)
	}

	// 示例 3: 读取流中的消息
	fmt.Println("\n=== 示例 3: 读取流中的消息 ===")
	result, err := XRead(ctx, &XReadArgs{
		Streams: []string{"events"},
		IDs:     []string{"0"}, // 从开始读取
		Count:   10,
	})
	if err != nil {
		fmt.Println("XRead error:", err)
	} else {
		for stream, messages := range result {
			fmt.Printf("Stream: %s, Messages count: %d\n", stream, len(messages))
			for _, msg := range messages {
				fmt.Printf("  Message ID: %s, Values: %v\n", msg.ID, msg.Values)
			}
		}
	}

	// 示例 4: 创建消费者组
	fmt.Println("\n=== 示例 4: 创建消费者组 ===")
	err = XGroupCreate(ctx, "events", "event_processors", "0")
	if err != nil {
		fmt.Println("XGroupCreate error (may already exist):", err)
	} else {
		fmt.Println("Consumer group created successfully")
	}

	// 示例 5: 从消费者组读取消息
	fmt.Println("\n=== 示例 5: 从消费者组读取消息 ===")
	groupResult, err := XReadGroup(ctx, &XReadGroupArgs{
		Group:    "event_processors",
		Consumer: "worker_1",
		Streams:  []string{"events"},
		IDs:      []string{">"}, // 只读取新消息
		Count:    10,
		Block:    2 * time.Second,
	})
	if err != nil {
		fmt.Println("XReadGroup error:", err)
	} else {
		for stream, messages := range groupResult {
			fmt.Printf("Stream: %s, Messages count: %d\n", stream, len(messages))
			for _, msg := range messages {
				fmt.Printf("  Processing message ID: %s, Values: %v\n", msg.ID, msg.Values)

				// 确认消息
				acked, err := XAck(ctx, stream, "event_processors", msg.ID)
				if err != nil {
					fmt.Println("    XAck error:", err)
				} else {
					fmt.Printf("    Acknowledged %d messages\n", acked)
				}
			}
		}
	}

	// 示例 6: 查看消费者组信息
	fmt.Println("\n=== 示例 6: 查看消费者组信息 ===")
	groups, err := XInfoGroups(ctx, "events")
	if err != nil {
		fmt.Println("XInfoGroups error:", err)
	} else {
		for _, group := range groups {
			fmt.Printf("Group: %s, Consumers: %d, Pending: %d, LastID: %s\n",
				group.Name, group.Consumers, group.Pending, group.LastID)
		}
	}

	// 示例 7: 范围查询
	fmt.Println("\n=== 示例 7: 范围查询 ===")
	messages, err := XRange(ctx, "events", "-", "+", 5)
	if err != nil {
		fmt.Println("XRange error:", err)
	} else {
		fmt.Printf("Found %d messages in range\n", len(messages))
		for _, msg := range messages {
			fmt.Printf("  Message ID: %s, Values: %v\n", msg.ID, msg.Values)
		}
	}

	// 示例 8: 修剪流
	fmt.Println("\n=== 示例 8: 修剪流 ===")
	trimmed, err := XTrim(ctx, "events", 3)
	if err != nil {
		fmt.Println("XTrim error:", err)
	} else {
		fmt.Printf("Trimmed %d messages\n", trimmed)
	}

	// 验证修剪后的长度
	length, err = XLen(ctx, "events")
	if err != nil {
		fmt.Println("XLen error:", err)
	} else {
		fmt.Println("Stream length after trim:", length)
	}

	fmt.Println("\n=== 示例完成 ===")
}
