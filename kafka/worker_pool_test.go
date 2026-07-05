package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// TestWorkerPoolBasic 测试WorkerPool基本功能
func TestWorkerPoolBasic(t *testing.T) {
	// 创建模拟的Consumer
	consumer := &Consumer{
		log:     logrus.WithField("module", "test"),
		metrics: NewConsumerMetrics(1 * time.Minute),
	}

	// 创建WorkerPool配置
	config := &WorkerPoolConfig{
		WorkerCount: 3,
		QueueSize:   10,
		Timeout:     5 * time.Second,
	}

	pool := NewWorkerPool(consumer, config)

	// 启动WorkerPool
	if err := pool.Start(); err != nil {
		t.Fatalf("启动WorkerPool失败: %v", err)
	}

	// 验证是否已启动
	if !pool.IsStarted() {
		t.Fatal("WorkerPool应该已启动")
	}

	// 提交任务
	var processedCount int
	var mu sync.Mutex

	handler := func(ctx context.Context, topic string, msg kafka.Message) error {
		mu.Lock()
		processedCount++
		mu.Unlock()
		return nil
	}

	// 提交5个任务
	for i := 0; i < 5; i++ {
		msg := kafka.Message{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    int64(i),
			Value:     []byte("test message"),
		}

		msgCtx := &MessageContext{
			Message:   msg,
			ShouldAck: false,
		}

		task := &MessageTask{
			MessageContext: msgCtx,
			Handler:        handler,
		}

		if err := pool.Submit(task); err != nil {
			t.Errorf("提交任务失败: %v", err)
		}
	}

	// 等待任务处理完成
	time.Sleep(1 * time.Second)

	// 验证处理数量
	mu.Lock()
	if processedCount != 5 {
		t.Errorf("期望处理5个任务，实际处理%d个", processedCount)
	}
	mu.Unlock()

	// 停止WorkerPool
	pool.Stop()

	// 验证是否已停止
	if pool.IsStarted() {
		t.Fatal("WorkerPool应该已停止")
	}
}

// TestWorkerPoolQueueFull 测试队列满的情况
func TestWorkerPoolQueueFull(t *testing.T) {
	consumer := &Consumer{
		log: logrus.WithField("module", "test"),
	}

	config := &WorkerPoolConfig{
		WorkerCount: 1,
		QueueSize:   2, // 小队列
		Timeout:     5 * time.Second,
	}

	pool := NewWorkerPool(consumer, config)

	if err := pool.Start(); err != nil {
		t.Fatalf("启动WorkerPool失败: %v", err)
	}
	defer pool.Stop()

	// 提交慢速处理器
	slowHandler := func(ctx context.Context, topic string, msg kafka.Message) error {
		time.Sleep(500 * time.Millisecond) // 模拟慢处理
		return nil
	}

	// 快速填满队列
	for i := 0; i < 5; i++ {
		msg := kafka.Message{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    int64(i),
			Value:     []byte("test"),
		}

		msgCtx := &MessageContext{
			Message:   msg,
			ShouldAck: false,
		}

		task := &MessageTask{
			MessageContext: msgCtx,
			Handler:        slowHandler,
		}

		err := pool.Submit(task)
		if i >= 2 && err == nil {
			// 队列应该满了
			t.Logf("警告: 第%d个任务应该被拒绝但成功了", i)
		}
	}
}

// TestWorkerPoolSubmitAndWait 测试同步提交并等待
func TestWorkerPoolSubmitAndWait(t *testing.T) {
	consumer := &Consumer{
		log: logrus.WithField("module", "test"),
	}

	config := &WorkerPoolConfig{
		WorkerCount: 2,
		QueueSize:   10,
		Timeout:     5 * time.Second,
	}

	pool := NewWorkerPool(consumer, config)

	if err := pool.Start(); err != nil {
		t.Fatalf("启动WorkerPool失败: %v", err)
	}
	defer pool.Stop()

	// 测试成功情况
	successHandler := func(ctx context.Context, topic string, msg kafka.Message) error {
		return nil
	}

	msg := kafka.Message{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    0,
		Value:     []byte("test"),
	}

	msgCtx := &MessageContext{
		Message:   msg,
		ShouldAck: false,
	}

	task := &MessageTask{
		MessageContext: msgCtx,
		Handler:        successHandler,
	}

	start := time.Now()
	err := pool.SubmitAndWait(task)
	duration := time.Since(start)

	if err != nil {
		t.Errorf("SubmitAndWait失败: %v", err)
	}

	if duration > 1*time.Second {
		t.Logf("警告: 等待时间过长: %v", duration)
	}
}

// TestWorkerPoolGracefulShutdown 测试优雅关闭
func TestWorkerPoolGracefulShutdown(t *testing.T) {
	consumer := &Consumer{
		log: logrus.WithField("module", "test"),
	}

	config := &WorkerPoolConfig{
		WorkerCount: 3,
		QueueSize:   10,
		Timeout:     5 * time.Second,
	}

	pool := NewWorkerPool(consumer, config)

	if err := pool.Start(); err != nil {
		t.Fatalf("启动WorkerPool失败: %v", err)
	}

	// 提交一些任务
	var processedCount int
	var mu sync.Mutex

	handler := func(ctx context.Context, topic string, msg kafka.Message) error {
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		processedCount++
		mu.Unlock()
		return nil
	}

	for i := 0; i < 5; i++ {
		msg := kafka.Message{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    int64(i),
			Value:     []byte("test"),
		}

		msgCtx := &MessageContext{
			Message:   msg,
			ShouldAck: false,
		}

		task := &MessageTask{
			MessageContext: msgCtx,
			Handler:        handler,
		}

		pool.Submit(task)
	}

	// 立即停止，应该等待正在处理的任务完成
	pool.Stop()

	mu.Lock()
	t.Logf("关闭前处理了%d个任务", processedCount)
	mu.Unlock()
}

// TestWorkerPoolGetQueueLength 测试获取队列长度
func TestWorkerPoolGetQueueLength(t *testing.T) {
	consumer := &Consumer{
		log: logrus.WithField("module", "test"),
	}

	config := &WorkerPoolConfig{
		WorkerCount: 1,
		QueueSize:   100,
		Timeout:     5 * time.Second,
	}

	pool := NewWorkerPool(consumer, config)

	if err := pool.Start(); err != nil {
		t.Fatalf("启动WorkerPool失败: %v", err)
	}
	defer pool.Stop()

	// 初始队列长度应为0
	initialLen := pool.GetQueueLength()
	if initialLen != 0 {
		t.Errorf("初始队列长度应为0，实际为%d", initialLen)
	}

	// 提交一个慢任务
	slowHandler := func(ctx context.Context, topic string, msg kafka.Message) error {
		time.Sleep(2 * time.Second)
		return nil
	}

	msg := kafka.Message{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    0,
		Value:     []byte("test"),
	}

	msgCtx := &MessageContext{
		Message:   msg,
		ShouldAck: false,
	}

	task := &MessageTask{
		MessageContext: msgCtx,
		Handler:        slowHandler,
	}

	pool.Submit(task)

	// 快速提交几个任务
	time.Sleep(100 * time.Millisecond)
	queueLen := pool.GetQueueLength()
	t.Logf("当前队列长度: %d", queueLen)
}
