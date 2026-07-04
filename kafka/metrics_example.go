package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// ExampleConsumerMetrics 消费者性能监控示例
func ExampleConsumerMetrics() {
	// 初始化消费者
	config := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:   "test-topic",
		GroupID: "metrics-test-group",
	}

	if err := InitConsumer(config); err != nil {
		log.Fatalf("初始化消费者失败: %v", err)
	}

	consumer := GetConsumer()
	defer consumer.Close()

	// 定义消息处理器
	handler := func(ctx context.Context, topic string, msg kafka.Message) error {
		// 模拟业务处理
		time.Sleep(10 * time.Millisecond)
		fmt.Printf("处理消息: key=%s, value=%s\n", string(msg.Key), string(msg.Value))
		return nil
	}

	// 启动统计打印协程
	go func() {
		ticker := time.NewTicker(30 * time.Second) // 每30秒打印一次统计
		defer ticker.Stop()

		for range ticker.C {
			fmt.Println("\n========== 性能统计 ==========")
			consumer.PrintStats()
		}
	}()

	// 启动消费者
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 在后台启动消费
	go func() {
		if err := consumer.Subscribe(ctx, handler); err != nil {
			log.Printf("消费者错误: %v", err)
		}
	}()

	// 运行一段时间后查看统计
	time.Sleep(2 * time.Minute)

	// 获取详细统计信息
	metrics := consumer.GetMetrics()
	if metrics != nil {
		stats := metrics.GetOverallStats()
		fmt.Printf("\n累计统计:\n")
		fmt.Printf("  总消息数: %d\n", stats.TotalMessages)
		fmt.Printf("  总拉取次数: %d\n", stats.TotalFetches)
		fmt.Printf("  平均速率: %.2f 条/秒\n", stats.MessageRate)
		fmt.Printf("  平均处理耗时: %v\n", stats.AvgProcessTime)

		currentWindow := metrics.GetCurrentWindow()
		fmt.Printf("\n当前窗口统计:\n")
		fmt.Printf("  消息数量: %d\n", currentWindow.MessageCount)
		fmt.Printf("  消息速率: %.2f 条/秒\n", currentWindow.MessageRate)
		fmt.Printf("  平均耗时: %v\n", currentWindow.AvgProcessTime)
	}

	cancel()
	time.Sleep(1 * time.Second)
}

// ExampleConsumerMetricsWithManager 使用 ConsumerManager 的性能监控示例
func ExampleConsumerMetricsWithManager() {
	manager := GetConsumerManager()

	// 注册订单业务消费者
	orderConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:  []string{"localhost:9092"},
			ClientID: "order-service",
		},
		Topic:      "orders",
		GroupID:    "order-service-group",
		MaxRetries: 3,
	}

	if err := manager.RegisterConsumer("order", orderConfig); err != nil {
		log.Fatalf("注册订单消费者失败: %v", err)
	}

	// 注册用户业务消费者
	userConfig := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:  []string{"localhost:9092"},
			ClientID: "user-service",
		},
		Topic:      "users",
		GroupID:    "user-service-group",
		MaxRetries: 5,
	}

	if err := manager.RegisterConsumer("user", userConfig); err != nil {
		log.Fatalf("注册用户消费者失败: %v", err)
	}

	// 定义消息处理器
	handlers := map[string]TopicHandler{
		"order": func(ctx context.Context, topic string, msg kafka.Message) error {
			time.Sleep(20 * time.Millisecond) // 模拟订单处理
			return nil
		},
		"user": func(ctx context.Context, topic string, msg kafka.Message) error {
			time.Sleep(5 * time.Millisecond) // 模拟用户处理
			return nil
		},
	}

	// 启动所有消费者
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := manager.StartAll(ctx, handlers); err != nil {
			log.Printf("消费者运行错误: %v", err)
		}
	}()

	// 定期打印各业务的性能统计
	go func() {
		ticker := time.NewTicker(60 * time.Second) // 每分钟打印一次
		defer ticker.Stop()

		for range ticker.C {
			fmt.Println("\n========== 多业务性能统计 ==========")

			// 获取所有消费者
			consumers := manager.ListConsumers()
			for business, consumer := range consumers {
				fmt.Printf("\n【%s】业务统计:\n", business)
				consumer.PrintStats()
			}
		}
	}()

	// 运行一段时间
	time.Sleep(5 * time.Minute)

	// 获取并显示总体统计
	fmt.Println("\n========== 最终统计报告 ==========")
	consumers := manager.ListConsumers()
	for business, consumer := range consumers {
		metrics := consumer.GetMetrics()
		if metrics != nil {
			stats := metrics.GetOverallStats()
			fmt.Printf("\n【%s】业务:\n", business)
			fmt.Printf("  运行时长: %v\n", stats.Uptime)
			fmt.Printf("  总消息数: %d\n", stats.TotalMessages)
			fmt.Printf("  平均速率: %.2f 条/秒\n", stats.MessageRate)
			fmt.Printf("  平均处理耗时: %v\n", stats.AvgProcessTime)
			fmt.Printf("  错误数量: %d\n", stats.TotalErrors)
		}
	}

	cancel()
	manager.CloseAll()
}

// ExampleCustomMetricsWindow 自定义统计窗口示例
func ExampleCustomMetricsWindow() {
	// 创建自定义窗口的指标收集器（5分钟窗口）
	customMetrics := NewConsumerMetrics(5 * time.Minute)

	// 模拟数据记录
	for i := 0; i < 100; i++ {
		// 模拟拉取消息
		customMetrics.RecordFetch(1, 50*time.Millisecond)

		// 模拟处理耗时
		customMetrics.RecordProcessTime(20 * time.Millisecond)

		time.Sleep(100 * time.Millisecond)
	}

	// 查看统计
	fmt.Println(customMetrics.FormatStats())

	// 获取历史窗口数据
	history := customMetrics.GetHistoryWindows()
	fmt.Printf("\n历史窗口数量: %d\n", len(history))

	for i, window := range history {
		fmt.Printf("窗口 %d: 消息数=%d, 速率=%.2f 条/秒, 平均耗时=%v\n",
			i+1, window.MessageCount, window.MessageRate, window.AvgProcessTime)
	}
}

// ExampleMonitorConsumerLag 监控消费者延迟示例
func ExampleMonitorConsumerLag() {
	manager := GetConsumerManager()

	// 注册消费者
	config := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:   "events",
		GroupID: "event-processor",
	}

	manager.RegisterConsumer("events", config)

	// 获取消费者
	consumer, _ := manager.GetConsumer("events")

	// 定期监控延迟和性能
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			// 获取性能指标
			metrics := consumer.GetMetrics()
			if metrics == nil {
				continue
			}

			stats := metrics.GetOverallStats()
			window := metrics.GetCurrentWindow()

			// 计算消费能力
			fmt.Printf("\n[监控] 时间: %s\n", time.Now().Format("15:04:05"))
			fmt.Printf("  最近1分钟速率: %.2f 条/秒\n", stats.RecentMessageRate)
			fmt.Printf("  当前窗口速率: %.2f 条/秒\n", window.MessageRate)
			fmt.Printf("  平均处理耗时: %v\n", window.AvgProcessTime)
			fmt.Printf("  拉取频率: %.2f 次/秒\n", window.FetchRate)

			// 如果速率过低，发出告警
			if stats.RecentMessageRate < 10 {
				fmt.Println("  ⚠️  警告: 消费速率过低!")
			}

			// 如果处理耗时过长，发出告警
			if window.AvgProcessTime > 1*time.Second {
				fmt.Println("  ⚠️  警告: 处理耗时过长!")
			}
		}
	}()
}
