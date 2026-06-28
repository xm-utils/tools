package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// 示例1: 手动提交模式（推荐）
func ExampleManualCommit() {
	// 配置消费者 - 手动提交模式
	config := &Config{
		Brokers:      []string{"localhost:9092"},
		Topic:        "orders",
		GroupID:      "order-processor",
		AutoCommit:   false, // 手动提交（推荐）
		MaxRetries:   3,     // 最大重试3次
		RetryBackoff: 1 * time.Second,
		StartOffset:  kafka.FirstOffset, // 从最早的消息开始
	}

	// 初始化消费者
	if err := InitConsumer(config); err != nil {
		panic(err)
	}

	consumer := GetConsumer()
	defer consumer.Close()

	// 定义消息处理器
	handler := func(ctx context.Context, topic string, msg kafka.Message) error {
		var order Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			fmt.Printf("解析消息失败: %v\n", err)
			return err // 返回错误会触发重试
		}

		fmt.Printf("处理订单: ID=%d, User=%d, Amount=%.2f\n",
			order.ID, order.UserID, order.Amount)

		// 业务逻辑...
		if err := processOrder(order); err != nil {
			fmt.Printf("处理订单失败: %v\n", err)
			return err // 返回错误会触发重试
		}

		// 注意：在手动提交模式下，如果handler返回nil，
		// 消费者会自动调用CommitMessages提交offset
		// 如果需要更细粒度的控制，可以设置 AutoCommit=false 并手动调用 CommitOffset

		return nil
	}

	// 启动消费者
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := consumer.Subscribe(ctx, handler); err != nil {
		fmt.Printf("消费者停止: %v\n", err)
	}
}

// 示例2: 自动提交模式
func ExampleAutoCommit() {
	// 配置消费者 - 自动提交模式
	config := &Config{
		Brokers:        []string{"localhost:9092"},
		Topic:          "logs",
		GroupID:        "log-consumer",
		AutoCommit:     true,            // 自动提交
		CommitInterval: 5 * time.Second, // 每5秒提交一次
		MaxRetries:     3,
		StartOffset:    kafka.LastOffset, // 从最新的消息开始
	}

	if err := InitConsumer(config); err != nil {
		panic(err)
	}

	consumer := GetConsumer()
	defer consumer.Close()

	handler := func(ctx context.Context, topic string, msg kafka.Message) error {
		fmt.Printf("收到日志: %s\n", string(msg.Value))
		// 处理日志...
		return nil
	}

	ctx := context.Background()
	consumer.Subscribe(ctx, handler)
}

// 示例3: 使用去重+手动确认
func ExampleDeduplicationWithManualCommit() {
	config := &Config{
		Brokers:    []string{"localhost:9092"},
		Topic:      "events",
		GroupID:    "event-processor",
		AutoCommit: false, // 手动提交
		MaxRetries: 3,
	}

	if err := InitConsumer(config); err != nil {
		panic(err)
	}

	// 创建Redis去重存储
	// redisStore := NewRedisDeduplicationStore(redisClient)
	// 这里使用内存存储作为示例
	memoryStore := NewMemoryDeduplicationStore()
	defer memoryStore.Close()

	// 创建去重器
	deduplicator := NewMessageDeduplicator(memoryStore, 24*time.Hour)

	consumer := GetConsumer()
	defer consumer.Close()

	// 基础处理器
	baseHandler := func(ctx context.Context, topic string, msg kafka.Message) error {
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return err
		}

		fmt.Printf("处理事件: Type=%s, ID=%s\n", event.Type, event.ID)
		return processEvent(event)
	}

	// 包装处理器添加去重功能
	deduplicatedHandler := deduplicator.WrapHandlerWithDeduplication(baseHandler)

	ctx := context.Background()
	consumer.Subscribe(ctx, deduplicatedHandler)
}

// 示例4: 多主题订阅
func ExampleMultiTopicSubscribe() {
	config := &Config{
		Brokers:    []string{"localhost:9092"},
		Topics:     []string{"orders", "payments", "shipments"}, // 多主题
		GroupID:    "multi-topic-consumer",
		AutoCommit: false,
		MaxRetries: 3,
	}

	if err := InitConsumer(config); err != nil {
		panic(err)
	}

	consumer := GetConsumer()
	defer consumer.Close()

	// 按主题分发处理
	handler := func(ctx context.Context, topic string, msg kafka.Message) error {
		switch topic {
		case "orders":
			return handleOrder(ctx, msg)
		case "payments":
			return handlePayment(ctx, msg)
		case "shipments":
			return handleShipment(ctx, msg)
		default:
			return fmt.Errorf("未知主题: %s", topic)
		}
	}

	ctx := context.Background()
	consumer.SubscribeWithTopicHandler(ctx, handler)
}

// 示例5: 自定义确认逻辑
func ExampleCustomAckLogic() {
	config := &Config{
		Brokers:    []string{"localhost:9092"},
		Topic:      "critical-events",
		GroupID:    "critical-processor",
		AutoCommit: false, // 手动提交
		MaxRetries: 5,     // 更多重试次数
	}

	if err := InitConsumer(config); err != nil {
		panic(err)
	}

	consumer := GetConsumer()
	defer consumer.Close()

	handler := func(ctx context.Context, topic string, msg kafka.Message) error {
		var event CriticalEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			// 解析失败，不重试，直接跳过
			fmt.Printf("解析失败，跳过消息: offset=%d\n", msg.Offset)
			return nil
		}

		// 重要事件，需要确保处理成功
		maxAttempts := 5
		for i := 0; i < maxAttempts; i++ {
			if err := processCriticalEvent(event); err != nil {
				fmt.Printf("处理失败 (尝试 %d/%d): %v\n", i+1, maxAttempts, err)
				if i == maxAttempts-1 {
					// 达到最大重试，记录到死信队列
					sendToDeadLetterQueue(msg, err)
					return nil // 返回nil以提交offset，避免阻塞
				}
				time.Sleep(time.Duration(i+1) * time.Second)
				continue
			}
			break
		}

		// 处理成功后，手动确认
		// 注意：在当前的实现中，handler返回nil后会自动提交
		// 如果需要完全手动控制，可以在consumer中添加一个选项
		return nil
	}

	ctx := context.Background()
	consumer.Subscribe(ctx, handler)
}

// 辅助函数和类型定义

type Order struct {
	ID     int64   `json:"id"`
	UserID int64   `json:"user_id"`
	Amount float64 `json:"amount"`
	Status string  `json:"status"`
}

type Event struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Data string `json:"data"`
}

type CriticalEvent struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
	Payload   string `json:"payload"`
}

func processOrder(order Order) error {
	// 模拟处理订单
	fmt.Printf("正在处理订单 %d...\n", order.ID)
	time.Sleep(100 * time.Millisecond)
	return nil
}

func processEvent(event Event) error {
	// 模拟处理事件
	fmt.Printf("正在处理事件 %s...\n", event.ID)
	return nil
}

func processCriticalEvent(event CriticalEvent) error {
	// 模拟处理重要事件
	fmt.Printf("正在处理重要事件 %s...\n", event.ID)
	// 可能失败的操作
	return nil
}

func sendToDeadLetterQueue(msg kafka.Message, err error) {
	// 将失败的消息发送到死信队列
	fmt.Printf("消息已发送到死信队列: topic=%s, offset=%d, error=%v\n",
		msg.Topic, msg.Offset, err)

	// 实际实现中，这里应该将消息写入死信队列
	// deadLetterMsg := kafka.Message{
	//     Topic: msg.Topic + ".dlq",
	//     Key:   msg.Key,
	//     Value: msg.Value,
	// }
	// Publish(context.Background(), deadLetterMsg.Topic, string(deadLetterMsg.Key), deadLetterMsg.Value)
}

func handleOrder(ctx context.Context, msg kafka.Message) error {
	fmt.Printf("处理订单消息: %s\n", string(msg.Value))
	return nil
}

func handlePayment(ctx context.Context, msg kafka.Message) error {
	fmt.Printf("处理支付消息: %s\n", string(msg.Value))
	return nil
}

func handleShipment(ctx context.Context, msg kafka.Message) error {
	fmt.Printf("处理物流消息: %s\n", string(msg.Value))
	return nil
}
