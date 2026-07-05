package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// WorkerPoolConfig Worker池配置
type WorkerPoolConfig struct {
	WorkerCount int           // Worker协程数量（默认10）
	QueueSize   int           // 任务队列大小（默认1000）
	Timeout     time.Duration // 单个任务超时时间（默认30秒）
}

// DefaultWorkerPoolConfig 返回默认Worker池配置
func DefaultWorkerPoolConfig() *WorkerPoolConfig {
	return &WorkerPoolConfig{
		WorkerCount: 10,
		QueueSize:   1000,
		Timeout:     30 * time.Second,
	}
}

// MessageTask 消息处理任务
type MessageTask struct {
	MessageContext *MessageContext
	Handler        TopicHandler
	DoneChan       chan error // 用于同步等待处理结果（可选）
}

// WorkerPool Kafka消息处理Worker池
type WorkerPool struct {
	config   *WorkerPoolConfig
	taskChan chan *MessageTask
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	log      *logrus.Entry
	consumer *Consumer
	started  bool
	startMu  sync.Mutex
}

// NewWorkerPool 创建Worker池
func NewWorkerPool(consumer *Consumer, config *WorkerPoolConfig) *WorkerPool {
	if config == nil {
		config = DefaultWorkerPoolConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		config:   config,
		taskChan: make(chan *MessageTask, config.QueueSize),
		ctx:      ctx,
		cancel:   cancel,
		log: logrus.WithFields(logrus.Fields{
			"module":      "Kafka WorkerPool",
			"workerCount": config.WorkerCount,
			"queueSize":   config.QueueSize,
		}),
		consumer: consumer,
		started:  false,
	}
}

// Start 启动Worker池
func (wp *WorkerPool) Start() error {
	wp.startMu.Lock()
	defer wp.startMu.Unlock()

	if wp.started {
		return fmt.Errorf("WorkerPool已经启动")
	}

	wp.started = true
	wp.log.Infof("启动WorkerPool: workers=%d, queueSize=%d", wp.config.WorkerCount, wp.config.QueueSize)

	// 启动Worker协程
	for i := 0; i < wp.config.WorkerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	return nil
}

// worker Worker协程主循环
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	wp.log.Debugf("Worker %d 已启动", id)

	for {
		select {
		case <-wp.ctx.Done():
			wp.log.Debugf("Worker %d 收到停止信号", id)
			return
		case task, ok := <-wp.taskChan:
			if !ok {
				wp.log.Debugf("Worker %d: 任务通道已关闭", id)
				return
			}

			// 处理任务
			wp.executeTask(id, task)
		}
	}
}

// executeTask 执行单个任务
func (wp *WorkerPool) executeTask(workerID int, task *MessageTask) {
	processStart := time.Now()

	// 创建带超时的上下文
	processCtx, cancel := context.WithTimeout(wp.ctx, wp.config.Timeout)
	defer cancel()

	msg := task.MessageContext.Message

	// 执行消息处理
	err := task.Handler(processCtx, msg.Topic, msg)
	processDuration := time.Since(processStart)

	// 记录处理耗时
	if wp.consumer.metrics != nil {
		wp.consumer.metrics.RecordProcessTime(processDuration)
	}

	if err != nil {
		wp.log.Errorf("[Worker %d] 消息处理失败: topic=%s, partition=%d, offset=%d, duration=%v, err=%v",
			workerID, msg.Topic, msg.Partition, msg.Offset, processDuration, err)

		// 记录错误
		if wp.consumer.metrics != nil {
			wp.consumer.metrics.RecordError()
		}
	} else {
		// 处理成功，提交offset
		if task.MessageContext.ShouldAck {
			if commitErr := wp.consumer.commitMessage(msg); commitErr != nil {
				wp.log.Errorf("[Worker %d] 提交offset失败: topic=%s, partition=%d, offset=%d, err=%v",
					workerID, msg.Topic, msg.Partition, msg.Offset, commitErr)
				err = commitErr
			} else {
				wp.log.Debugf("[Worker %d] 消息处理成功并已确认: topic=%s, partition=%d, offset=%d, duration=%v",
					workerID, msg.Topic, msg.Partition, msg.Offset, processDuration)
			}
		}
	}

	// 如果提供了DoneChan，通知调用方
	if task.DoneChan != nil {
		task.DoneChan <- err
		close(task.DoneChan)
	}
}

// Submit 提交任务到Worker池（非阻塞）
func (wp *WorkerPool) Submit(task *MessageTask) error {
	if !wp.started {
		return fmt.Errorf("WorkerPool未启动")
	}

	select {
	case wp.taskChan <- task:
		return nil
	default:
		// 队列已满
		wp.log.Warnf("WorkerPool任务队列已满，丢弃消息: queueSize=%d", wp.config.QueueSize)
		return fmt.Errorf("WorkerPool任务队列已满")
	}
}

// SubmitAndWait 提交任务并等待处理完成（阻塞）
func (wp *WorkerPool) SubmitAndWait(task *MessageTask) error {
	if !wp.started {
		return fmt.Errorf("WorkerPool未启动")
	}

	doneChan := make(chan error, 1)
	task.DoneChan = doneChan

	select {
	case wp.taskChan <- task:
		// 等待处理完成
		select {
		case err := <-doneChan:
			return err
		case <-wp.ctx.Done():
			return wp.ctx.Err()
		}
	default:
		wp.log.Warnf("WorkerPool任务队列已满，无法提交: queueSize=%d", wp.config.QueueSize)
		return fmt.Errorf("WorkerPool任务队列已满")
	}
}

// Stop 停止Worker池
func (wp *WorkerPool) Stop() {
	wp.startMu.Lock()
	defer wp.startMu.Unlock()

	if !wp.started {
		return
	}

	wp.log.Info("正在停止WorkerPool...")

	// 取消上下文
	wp.cancel()

	// 等待所有Worker完成
	wp.wg.Wait()

	// 关闭任务通道
	close(wp.taskChan)

	wp.started = false
	wp.log.Info("WorkerPool已停止")
}

// GetQueueLength 获取当前队列中的任务数
func (wp *WorkerPool) GetQueueLength() int {
	return len(wp.taskChan)
}

// IsStarted 检查WorkerPool是否已启动
func (wp *WorkerPool) IsStarted() bool {
	wp.startMu.Lock()
	defer wp.startMu.Unlock()
	return wp.started
}
