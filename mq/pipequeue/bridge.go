// Package pipequeue 把 mq 门面消费者桥接为 pipeline 引擎的 Queue，
// 使同一套 Step 链消费代码在 Kafka / Redis 各队列形态间零改动切换。
//
// 桥接模型：pipeline.Queue 是拉取语义（Receive/Submit），mq.Consumer 是推送语义
// （Subscribe + Handler）。桥接层内部维护有界缓冲：Subscribe 的 Handler 把消息
// 交接进缓冲（缓冲满即阻塞形成背压，积压留在 MQ 侧），引擎 worker 从 Receive 领取；
// 失败重投的 Submit 默认回灌本地缓冲，可选经 Producer 持久化回投 MQ。
//
// 确认时机两种模式（Config.ManualAck）：
//   - false（默认）：消息成功交接进缓冲即由适配器自动 Commit——进程崩溃时
//     缓冲内未处理消息依赖 MQ 侧重投兜底的窗口最小化，但极端场景可能重复投递；
//   - true：交接后返回 mq.ErrManualAck 挂起确认，业务须把 Queue.Commit /
//     Queue.Reject 接入 pipeline.Config 的终态钩子（OnSuccess/OnExhausted/OnFatal
//     调 Commit，按需 Nack），实现严格 at-least-once；OnInterrupted 不要确认，
//     消息留在途由 MQ 重投。
//
// 后端为 redis/pubsub（广播）时桥接仍可用，但 at-least-once 保证不成立：
// 广播无确认原语，Commit/Reject 为 no-op，ManualAck 与 DurableRetry 均无意义，
// 停机时缓冲内未处理消息直接丢失（需可靠性请改接 list/zset/stream 后端）。
package pipequeue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/xm-utils/tools/mq/core"
	xmpipeline "github.com/xm-utils/tools/pipeline"
)

// 默认缓冲深度与持久回投超时。
const (
	defaultBufferSize    = 100
	defaultProduceTimout = 5 * time.Second
)

// ErrQueueStopped 表示桥接队列已停止，消息无法交付。
var ErrQueueStopped = errors.New("mq/pipequeue: 桥接队列已停止")

// Config 桥接队列装配配置。
type Config struct {
	// Consumer 底层 MQ 消费者（mq/kafka 或 mq/redis/* 适配器）。必填。
	Consumer mq.Consumer
	// Group、Topic 订阅的消费组与主题。必填。
	Group string
	Topic string

	// BufferSize 领取缓冲深度，背压阈值；<=0 取默认 100。
	BufferSize int

	// ManualAck 延迟确认模式，见包注释。
	ManualAck bool

	// Producer 持久回投用的生产者（可选，与 Consumer 指向同一 MQ）。
	// 仅在 DurableRetry=true 时使用。
	Producer mq.Producer
	// DurableRetry Submit 经 Producer 回投 MQ 而非回灌本地缓冲：
	// 重投消息不受本进程停机影响（停机时 Submit 会拒绝并返回 error 交引擎兜底）。
	// 启用后建议同时开启 ManualAck，并按包注释把原消息的 Commit 接入 OnRetried。
	DurableRetry bool
	// ProduceTimeout DurableRetry 同步回投（delay=0）的超时；<=0 取默认 5s。
	ProduceTimeout time.Duration

	// Log 日志出口；nil 时默认 module=mq/pipequeue。
	Log *logrus.Entry
}

// Queue 实现 pipeline.Queue[*mq.Message] 的桥接队列。
type Queue struct {
	cfg Config
	log *logrus.Entry

	ch     chan *mq.Message
	stopCh chan struct{}

	mu        sync.Mutex
	started   bool
	stopped   bool
	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

// 编译期契约锁定：Queue 必须满足 pipeline 引擎的队列抽象。
var _ xmpipeline.Queue[*mq.Message] = (*Queue)(nil)

// New 校验配置并创建桥接队列（未启动）。配置非法返回 error 而非 panic。
func New(cfg Config) (*Queue, error) {
	if cfg.Consumer == nil {
		return nil, fmt.Errorf("mq/pipequeue: Config.Consumer 不能为空")
	}
	if cfg.Group == "" || cfg.Topic == "" {
		return nil, fmt.Errorf("mq/pipequeue: Config.Group 与 Config.Topic 不能为空")
	}
	if cfg.DurableRetry && cfg.Producer == nil {
		return nil, fmt.Errorf("mq/pipequeue: DurableRetry 需要配置 Config.Producer")
	}
	bufSize := cfg.BufferSize
	if bufSize <= 0 {
		bufSize = defaultBufferSize
	}
	timeout := cfg.ProduceTimeout
	if timeout <= 0 {
		timeout = defaultProduceTimout
	}
	log := cfg.Log
	if log == nil {
		log = logrus.WithField("module", "mq/pipequeue")
	}
	cfg.BufferSize, cfg.ProduceTimeout = bufSize, timeout
	return &Queue{
		cfg:    cfg,
		log:    log,
		ch:     make(chan *mq.Message, bufSize),
		stopCh: make(chan struct{}),
	}, nil
}

// Start 拉起订阅转发协程，将 Consumer 推送的消息交接进内部缓冲。
// 幂等保护：重复 Start / Stop 后 Start 返回 error。
func (q *Queue) Start(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return fmt.Errorf("mq/pipequeue: 队列已停止，请重新 New")
	}
	if q.started {
		return fmt.Errorf("mq/pipequeue: 队列已启动，禁止重复 Start")
	}
	q.started = true

	runCtx, cancel := context.WithCancel(ctx)
	q.cancel = cancel
	q.done = make(chan struct{})
	go func() {
		defer close(q.done)
		if err := q.cfg.Consumer.Subscribe(runCtx, q.cfg.Group, q.cfg.Topic, q.handle); err != nil &&
			!errors.Is(err, context.Canceled) {
			q.log.Errorf("订阅退出: group=%s, topic=%s, err=%v", q.cfg.Group, q.cfg.Topic, err)
		}
	}()
	q.log.Infof("桥接队列已启动: group=%s, topic=%s, buffer=%d", q.cfg.Group, q.cfg.Topic, q.cfg.BufferSize)
	return nil
}

// Stop 停止订阅转发并唤醒所有阻塞中的 Receive/交接，幂等。
// 缓冲内未领取消息随进程丢弃：ManualAck 模式下尚未确认，由 MQ 重投兜底；
// 自动确认模式下已由引擎停机语义处理（at-least-once，留待外部补偿）。
func (q *Queue) Stop() {
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.stopped = true
		cancel := q.cancel
		q.mu.Unlock()

		close(q.stopCh)
		if cancel != nil {
			cancel()
		}
		if q.done != nil {
			<-q.done
		}
		q.log.Info("桥接队列已停止")
	})
}

// handle Consumer 推送交接入口：阻塞式写入缓冲（背压）。
// 停机/取消时不返回普通 error——防止失败策略把未处理消息误重投或终态化，
// 而是返回 ErrManualAck 让消息保持在途，由 MQ 重投语义兜底（见各适配器契约）。
func (q *Queue) handle(ctx context.Context, msg *mq.Message) error {
	select {
	case <-q.stopCh:
		return mq.ErrManualAck
	case <-ctx.Done():
		return mq.ErrManualAck
	case q.ch <- msg:
		if q.cfg.ManualAck {
			return mq.ErrManualAck
		}
		return nil
	}
}

// Submit 实现 pipeline.Queue：重投消息回灌。
//
// 契约遵循引擎要求「不得无限阻塞」：delay>0 或缓冲满时登记定时回灌后立即返回；
// delay=0 且缓冲有余量时同步入队。DurableRetry 模式改为经 Producer 回投 MQ：
// delay=0 同步回投（受 ProduceTimeout 约束）并如实返回 error，delay>0 异步回投
// （失败仅记日志，消息已在 MQ 侧留存的语义由 ManualAck 对原消息的确认时机保证）。
// 停止后 Submit 返回 ErrQueueStopped，引擎按「留待外部兜底」处理。
func (q *Queue) Submit(msg *mq.Message, delay time.Duration) error {
	if msg == nil {
		return fmt.Errorf("mq/pipequeue: Submit 消息不能为 nil")
	}
	q.mu.Lock()
	stopped := q.stopped
	q.mu.Unlock()
	if stopped {
		return ErrQueueStopped
	}

	if q.cfg.DurableRetry {
		topic := msg.Topic
		if topic == "" {
			topic = q.cfg.Topic
		}
		if delay <= 0 {
			pctx, cancel := context.WithTimeout(context.Background(), q.cfg.ProduceTimeout)
			defer cancel()
			return q.cfg.Producer.Produce(pctx, topic, msg)
		}
		go q.delayedProduce(msg, topic, delay)
		return nil
	}

	if delay <= 0 {
		select {
		case q.ch <- msg:
			return nil
		default:
			// 缓冲满：转定时回灌，保证 Submit 永不阻塞
			go q.delayedDeliver(msg, 0)
			return nil
		}
	}
	go q.delayedDeliver(msg, delay)
	return nil
}

// delayedDeliver 定时回灌本地缓冲；停机时放弃（返回 error 已由 Submit 语义豁免，
// 未确认消息留在 MQ 侧）。
func (q *Queue) delayedDeliver(msg *mq.Message, delay time.Duration) {
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-q.stopCh:
			return
		}
	}
	select {
	case q.ch <- msg:
	case <-q.stopCh:
	}
}

// delayedProduce 定时持久回投。
func (q *Queue) delayedProduce(msg *mq.Message, topic string, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-q.stopCh:
		return
	}
	pctx, cancel := context.WithTimeout(context.Background(), q.cfg.ProduceTimeout)
	defer cancel()
	if err := q.cfg.Producer.Produce(pctx, topic, msg); err != nil {
		q.log.Errorf("延迟持久回投失败: topic=%s, id=%s, err=%v", topic, msg.ID, err)
	}
}

// Receive 实现 pipeline.Queue：阻塞领取一条消息。
// ctx 结束（引擎 Pause/Stop）时返回 ok=false，且保证未消费任何消息。
func (q *Queue) Receive(ctx context.Context) (*mq.Message, bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case <-q.stopCh:
		return nil, false
	case msg := <-q.ch:
		return msg, true
	}
}

// Commit 确认消息处理完成。ManualAck 模式下接入引擎终态钩子：
// OnSuccess/OnExhausted/OnFatal 中调用（停机中断 OnInterrupted 不要调用）。
func (q *Queue) Commit(ctx context.Context, msg *mq.Message) error {
	return q.cfg.Consumer.Commit(ctx, msg)
}

// Reject 否定消息：requeue=true 重回 MQ 等待再次投递；false 终态丢弃。
// 未启用 ManualAck 模式时底层无在途登记，将返回 mq.ErrNotSubscribed
// （redis/pubsub 除外：广播无确认通道，Commit/Reject 为 no-op 恒返回 nil）。
func (q *Queue) Reject(ctx context.Context, msg *mq.Message, requeue bool) error {
	return q.cfg.Consumer.Reject(ctx, msg, requeue)
}
