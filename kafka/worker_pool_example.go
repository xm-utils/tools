package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// WorkerPoolExample 演示如何使用WorkerPool模式
func WorkerPoolExample() {
	// 配置消费者
	config := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:      "test-topic",
		GroupID:    "test-group",
		AutoCommit: false, // 手动提交
		MaxRetries: 3,
	}

	// 初始化消费者
	if err := InitConsumer(config); err != nil {
		fmt.Printf("初始化消费者失败: %v\n", err)
		return
	}

	consumer := GetConsumer()

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 定义消息处理器
	handler := func(ctx context.Context, topic string, msg kafka.Message) error {
		fmt.Printf("收到消息: topic=%s, partition=%d, offset=%d, value=%s\n",
			topic, msg.Partition, msg.Offset, string(msg.Value))

		// 模拟业务处理
		time.Sleep(100 * time.Millisecond)

		// 返回nil表示处理成功，将自动提交offset
		return nil
	}

	// 方式1: 使用传统的单协程异步处理（每条消息一个goroutine）
	go func() {
		fmt.Println("=== 使用传统Subscribe模式 ===")
		if err := Subscribe(ctx, handler); err != nil {
			fmt.Printf("订阅失败: %v\n", err)
		}
	}()

	// 等待一段时间后停止
	time.Sleep(5 * time.Second)
	cancel()
	consumer.Close()
}

// WorkerPoolModeExample 演示如何使用WorkerPool多协程模式
func WorkerPoolModeExample() {
	// 配置消费者
	config := &ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"localhost:9092"},
		},
		Topic:      "test-topic",
		GroupID:    "test-group-pool",
		AutoCommit: false, // 手动提交
		MaxRetries: 3,
	}

	// 初始化消费者
	if err := InitConsumer(config); err != nil {
		fmt.Printf("初始化消费者失败: %v\n", err)
		return
	}

	consumer := GetConsumer()

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 定义消息处理器
	handler := func(ctx context.Context, topic string, msg kafka.Message) error {
		fmt.Printf("[WorkerPool] 收到消息: topic=%s, partition=%d, offset=%d, value=%s\n",
			topic, msg.Partition, msg.Offset, string(msg.Value))

		// 模拟业务处理
		time.Sleep(100 * time.Millisecond)

		// 返回nil表示处理成功，将自动提交offset
		return nil
	}

	// 配置WorkerPool
	poolConfig := &WorkerPoolConfig{
		WorkerCount: 10,               // 10个Worker协程
		QueueSize:   1000,             // 队列容量1000
		Timeout:     30 * time.Second, // 单个任务超时30秒
	}

	// 方式2: 使用WorkerPool多协程异步处理
	go func() {
		fmt.Println("=== 使用WorkerPool模式 ===")
		if err := SubscribeWithWorkerPool(ctx, handler, poolConfig); err != nil {
			fmt.Printf("订阅失败: %v\n", err)
		}
	}()

	// 监控WorkerPool状态
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if consumer.workerPool != nil {
					queueLen := consumer.workerPool.GetQueueLength()
					fmt.Printf("[监控] WorkerPool队列长度: %d\n", queueLen)
				}
			}
		}
	}()

	// 等待一段时间后停止
	time.Sleep(10 * time.Second)
	cancel()

	// Close会自动停止WorkerPool
	consumer.Close()
}

// CompareModesExample 对比两种模式的性能差异
func CompareModesExample() {
	fmt.Println("=== Kafka消费者处理模式对比 ===")
	fmt.Println()

	fmt.Println("1. 传统Subscribe模式:")
	fmt.Println("   - 每条消息启动一个新的goroutine")
	fmt.Println("   - 优点: 实现简单，无队列阻塞风险")
	fmt.Println("   - 缺点: 高并发时goroutine数量不可控，可能导致资源耗尽")
	fmt.Println("   - 适用场景: 低中流量，消息处理时间较短")
	fmt.Println()

	fmt.Println("2. WorkerPool模式:")
	fmt.Println("   - 固定数量的worker协程从任务队列取任务")
	fmt.Println("   - 优点: 协程数量可控，资源使用可预测，支持背压")
	fmt.Println("   - 缺点: 实现稍复杂，队列满时会丢弃消息")
	fmt.Println("   - 适用场景: 高流量，需要控制并发度，消息处理时间较长")
	fmt.Println()

	fmt.Println("选择建议:")
	fmt.Println("   - 如果消息量不大(<1000/s)，使用传统Subscribe模式即可")
	fmt.Println("   - 如果消息量大且处理耗时长，使用WorkerPool模式")
	fmt.Println("   - WorkerPool的WorkerCount建议设置为CPU核心数的2-4倍")
	fmt.Println("   - QueueSize根据内存和消息处理速度调整，避免过大或过小")
}
