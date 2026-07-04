package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// ExampleConsumerManager 演示如何使用消费者管理器
func ExampleConsumerManager() {
	// 1. 获取管理器实例
	manager := GetConsumerManager()

	// 2. 注册不同业务的消费者

	// 订单业务 - 高优先级
	orderConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:  []string{"localhost:9092"},
			ClientID: "order-service",
		},
		Topic:      "orders",
		GroupID:    "order-service-group",
		MaxBytes:   1048576, // 1MB
		MaxRetries: 3,
	}

	if err := manager.RegisterConsumer("order", orderConfig); err != nil {
		fmt.Printf("注册订单消费者失败: %v\n", err)
		return
	}

	// 库存业务 - 中等优先级
	inventoryConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:  []string{"localhost:9092"},
			ClientID: "inventory-service",
		},
		Topic:      "inventory-updates",
		GroupID:    "inventory-service-group",
		MaxBytes:   524288, // 512KB
		MaxRetries: 5,
	}

	if err := manager.RegisterConsumer("inventory", inventoryConfig); err != nil {
		fmt.Printf("注册库存消费者失败: %v\n", err)
		return
	}

	// 通知业务 - 低优先级，批量处理
	notificationConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:  []string{"localhost:9092"},
			ClientID: "notification-service",
		},
		Topic:      "notifications",
		GroupID:    "notification-service-group",
		MaxBytes:   2097152, // 2MB
		MaxRetries: 10,
	}

	if err := manager.RegisterConsumer("notification", notificationConfig); err != nil {
		fmt.Printf("注册通知消费者失败: %v\n", err)
		return
	}

	// 3. 定义各业务的消息处理器
	handlers := map[string]TopicHandler{
		"order": func(ctx context.Context, topic string, msg kafka.Message) error {
			fmt.Printf("处理订单消息: key=%s, value=%s\n", string(msg.Key), string(msg.Value))
			// 订单处理逻辑
			return nil
		},
		"inventory": func(ctx context.Context, topic string, msg kafka.Message) error {
			fmt.Printf("处理库存消息: key=%s, value=%s\n", string(msg.Key), string(msg.Value))
			// 库存处理逻辑
			return nil
		},
		"notification": func(ctx context.Context, topic string, msg kafka.Message) error {
			fmt.Printf("处理通知消息: key=%s, value=%s\n", string(msg.Key), string(msg.Value))
			// 通知处理逻辑
			return nil
		},
	}

	// 4. 启动所有消费者
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := manager.StartAll(ctx, handlers); err != nil {
			fmt.Printf("启动消费者失败: %v\n", err)
		}
	}()

	// 5. 健康检查
	health := manager.HealthCheck()
	for _, status := range health {
		fmt.Printf("业务 %s 状态: %s\n", status.Business, status.Status)
	}

	// 6. 查看消费者列表
	fmt.Printf("已注册消费者数量: %d\n", manager.GetConsumerCount())

	// 7. 运行一段时间后关闭
	time.Sleep(10 * time.Second)

	// 8. 优雅关闭所有消费者
	manager.CloseAll()
}

// ExampleSingleConsumer 演示启动单个业务消费者
func ExampleSingleConsumer() {
	manager := GetConsumerManager()

	// 注册消费者
	config := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:  []string{"localhost:9092"},
			ClientID: "user-service",
		},
		Topic:   "user-events",
		GroupID: "user-service-group",
	}

	if err := manager.RegisterConsumer("user", config); err != nil {
		fmt.Printf("注册失败: %v\n", err)
		return
	}

	// 定义处理器
	handler := func(ctx context.Context, topic string, msg kafka.Message) error {
		fmt.Printf("处理用户事件: %s\n", string(msg.Value))
		return nil
	}

	// 启动单个消费者
	ctx := context.Background()
	if err := manager.Start(ctx, "user", handler); err != nil {
		fmt.Printf("启动失败: %v\n", err)
	}
}

// ExampleGracefulShutdown 演示优雅关闭
func ExampleGracefulShutdown() {
	manager := GetConsumerManager()

	// 注册订单消费者
	orderConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:  []string{"localhost:9092"},
			ClientID: "order-service",
		},
		Topic:   "orders",
		GroupID: "order-service-group",
	}

	if err := manager.RegisterConsumer("order", orderConfig); err != nil {
		log.Fatalf("注册失败: %v", err)
	}

	// 注册用户消费者
	userConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:  []string{"localhost:9092"},
			ClientID: "user-service",
		},
		Topic:   "users",
		GroupID: "user-service-group",
	}

	if err := manager.RegisterConsumer("user", userConfig); err != nil {
		log.Fatalf("注册失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 监听系统信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 定义处理器
	handlers := map[string]TopicHandler{
		"order": func(ctx context.Context, topic string, msg kafka.Message) error {
			log.Printf("处理订单: %s", string(msg.Value))
			return nil
		},
		"user": func(ctx context.Context, topic string, msg kafka.Message) error {
			log.Printf("处理用户: %s", string(msg.Value))
			return nil
		},
	}

	// 启动消费者
	go func() {
		if err := manager.StartAll(ctx, handlers); err != nil {
			log.Printf("消费者错误: %v", err)
		}
	}()

	log.Println("消费者已启动，按 Ctrl+C 退出...")

	// 等待关闭信号
	<-sigChan
	log.Println("收到关闭信号，正在优雅关闭...")

	cancel()           // 取消上下文
	manager.CloseAll() // 关闭所有消费者

	log.Println("所有消费者已关闭")
}

// ExampleHealthCheck 演示健康检查
func ExampleHealthCheck() {
	manager := GetConsumerManager()

	// 注册多个消费者
	configs := map[string]*ConsumerConfig{
		"order": {
			CommonConfig: CommonConfig{
				Brokers: []string{"localhost:9092"},
			},
			Topic:   "orders",
			GroupID: "order-group",
		},
		"user": {
			CommonConfig: CommonConfig{
				Brokers: []string{"localhost:9092"},
			},
			Topic:   "users",
			GroupID: "user-group",
		},
	}

	for business, config := range configs {
		if err := manager.RegisterConsumer(business, config); err != nil {
			log.Printf("注册失败 %s: %v", business, err)
		}
	}

	// 定期检查健康状态
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		health := manager.HealthCheck()

		for _, status := range health {
			log.Printf("业务: %s, 主题: %s, 消费组: %s, 状态: %s, 时间: %s",
				status.Business,
				status.Topic,
				status.GroupID,
				status.Status,
				status.Timestamp.Format("2006-01-02 15:04:05"),
			)
		}

		log.Printf("活跃消费者数量: %d", manager.GetConsumerCount())
	}
}

// ExampleDynamicManagement 演示动态管理消费者
func ExampleDynamicManagement() {
	manager := GetConsumerManager()

	// 查看所有已注册的消费者
	consumers := manager.ListConsumers()
	for business := range consumers {
		log.Printf("已注册业务: %s", business)
	}

	// 关闭特定业务的消费者
	if err := manager.Close("old-business"); err != nil {
		log.Printf("关闭失败: %v", err)
	}

	// 注册新业务的消费者
	newConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:  []string{"localhost:9092"},
			ClientID: "new-service",
		},
		Topic:   "new-events",
		GroupID: "new-service-group",
	}

	if err := manager.RegisterConsumer("new-business", newConfig); err != nil {
		log.Printf("注册失败: %v", err)
	}

	log.Printf("当前消费者总数: %d", manager.GetConsumerCount())
}

// ExamplePriorityQueue 演示基于优先级的消费
func ExamplePriorityQueue() {
	manager := GetConsumerManager()

	// 高优先级 - 支付消息
	paymentConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:        "payments",
		GroupID:      "payment-processor",
		MaxRetries:   5,
		RetryBackoff: 1 * time.Second, // 快速重试
	}
	manager.RegisterConsumer("payment", paymentConfig)

	// 中优先级 - 订单消息
	orderConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:        "orders",
		GroupID:      "order-processor",
		MaxRetries:   3,
		RetryBackoff: 2 * time.Second,
	}
	manager.RegisterConsumer("order", orderConfig)

	// 低优先级 - 通知消息
	notificationConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:        "notifications",
		GroupID:      "notification-processor",
		MaxRetries:   10,
		RetryBackoff: 5 * time.Second, // 慢速重试
	}
	manager.RegisterConsumer("notification", notificationConfig)

	// 定义处理器
	handlers := map[string]TopicHandler{
		"payment": func(ctx context.Context, topic string, msg kafka.Message) error {
			log.Printf("[高优先级] 处理支付: %s", string(msg.Value))
			return nil
		},
		"order": func(ctx context.Context, topic string, msg kafka.Message) error {
			log.Printf("[中优先级] 处理订单: %s", string(msg.Value))
			return nil
		},
		"notification": func(ctx context.Context, topic string, msg kafka.Message) error {
			log.Printf("[低优先级] 发送通知: %s", string(msg.Value))
			return nil
		},
	}

	ctx := context.Background()
	manager.StartAll(ctx, handlers)
}

// ExampleMicroservicePattern 演示微服务模式
func ExampleMicroservicePattern() {
	// 在微服务架构中，每个服务独立管理自己的消费者

	// Order Service
	orderManager := NewConsumerManager()
	orderConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:  []string{"kafka:9092"},
			ClientID: "order-service",
		},
		Topics:     []string{"orders-created", "orders-updated"},
		GroupID:    "order-service-group",
		MaxRetries: 3,
	}

	orderManager.RegisterConsumer("order", orderConfig)

	orderHandlers := map[string]TopicHandler{
		"order": func(ctx context.Context, topic string, msg kafka.Message) error {
			log.Printf("[Order Service] 处理订单事件: %s", string(msg.Value))
			return nil
		},
	}

	// User Service
	userManager := NewConsumerManager()
	userConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:  []string{"kafka:9092"},
			ClientID: "user-service",
		},
		Topics:     []string{"users-created", "users-updated"},
		GroupID:    "user-service-group",
		MaxRetries: 5,
	}

	userManager.RegisterConsumer("user", userConfig)

	userHandlers := map[string]TopicHandler{
		"user": func(ctx context.Context, topic string, msg kafka.Message) error {
			log.Printf("[User Service] 处理用户事件: %s", string(msg.Value))
			return nil
		},
	}

	// 分别启动
	ctx := context.Background()
	go orderManager.StartAll(ctx, orderHandlers)
	go userManager.StartAll(ctx, userHandlers)

	// 每个服务可以独立部署和扩展
}

// ExampleSameTopicDifferentGroups 演示同一主题不同消费组
func ExampleSameTopicDifferentGroups() {
	manager := GetConsumerManager()

	// 数据分析 - 大批量，离线处理
	analyticsConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:      "user-events",
		GroupID:    "analytics-group",
		MaxBytes:   5242880, // 5MB
		MaxRetries: 3,
	}
	manager.RegisterConsumer("analytics", analyticsConfig)

	// 实时处理 - 小批量，快速响应
	realtimeConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:      "user-events", // 同一主题
		GroupID:    "realtime-group",
		MaxBytes:   104857, // 100KB
		MaxRetries: 5,
	}
	manager.RegisterConsumer("realtime", realtimeConfig)

	// 两个消费者都会收到相同的消息，但处理方式不同
	handlers := map[string]TopicHandler{
		"analytics": func(ctx context.Context, topic string, msg kafka.Message) error {
			log.Printf("[离线分析] 处理用户事件: %s", string(msg.Value))
			// 存储到数据仓库
			return nil
		},
		"realtime": func(ctx context.Context, topic string, msg kafka.Message) error {
			log.Printf("[实时处理] 处理用户事件: %s", string(msg.Value))
			// 实时更新推荐
			return nil
		},
	}

	ctx := context.Background()
	manager.StartAll(ctx, handlers)
}
