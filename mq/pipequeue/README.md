# mq/pipequeue — pipeline 引擎 × MQ 门面桥接队列

`github.com/xm-utils/tools/mq/pipequeue`（package `pipequeue`）

把 [tools/mq](../core/README.md) 门面的**推送式**消费者（`Subscribe` + `Handler`）桥接为
[tools/pipeline](../pipeline/README.md) 引擎所需的**拉取式** `pipeline.Queue[*mq.Message]`
（`Receive` / `Submit`），从而让消息进入引擎后享有步骤链、错误分级、重试退避、有界停机、
同 Key 串行全套能力，而 MQ 侧的确认时机仍由业务终态决定。

桥接层只依赖门面接口，不感知后端类型：`mq/kafka`、`mq/redis` 的 list / zset / stream / pubsub
皆可注入。

## 特性

- **推送→拉取转换**：内部一条 `Subscribe` 协程把消息送入有界缓冲，`Receive` 从缓冲取；
  缓冲满即背压（`Subscribe` 侧阻塞），满足引擎「`Submit` 不得无限阻塞」契约
- **确认时机两档**：`ManualAck=true` 时 `handle` 返回 `mq.ErrManualAck`，MQ 确认完全挂到
  引擎终态钩子（严格 at-least-once）；`false` 时进入引擎即确认（至多一次 + 内存重试）
- **重投回灌或持久回投**：`Submit` 失败消息默认回灌桥接缓冲；`DurableRetry=true` 时经
  `Producer` 回投 MQ 本体，使重投消息跨进程存活
- **停机不吞消息**：`Stop` 后有界停机窗口内收尾在途消息，被中断的消息不确认，留待 MQ 侧重投

## 安装

```bash
go get github.com/xm-utils/tools/mq/pipequeue
```

## API

```go
type Config struct {
    Consumer       mq.Consumer     // 必填：任一适配器
    Group, Topic   string          // 必填：订阅的消费组与主题
    BufferSize     int             // 领取缓冲深度（背压阈值），<=0 取默认 100
    ManualAck      bool            // true：确认挂到引擎终态钩子（严格 at-least-once）
    Producer       mq.Producer     // DurableRetry 时必填（可与 Consumer 指向同一 MQ）
    DurableRetry   bool            // true：Submit 经 Producer 回投 MQ 而非回灌本地缓冲
    ProduceTimeout time.Duration   // 同步回投（delay=0）超时，<=0 取默认 5s
    Log            *logrus.Entry   // nil 时默认 module=mq/pipequeue
}

func New(cfg Config) (*Queue, error)              // 实现 pipeline.Queue[*mq.Message]
func (q *Queue) Start(ctx context.Context) error  // 拉起内部 Subscribe 协程
func (q *Queue) Stop()                            // 有界停机（取消订阅 + 等待收尾）

// Receive 阻塞领取一条消息；ctx 结束（引擎 Pause/Stop）或队列已停时返回 ok=false，
// 且保证未消费任何消息。
func (q *Queue) Receive(ctx context.Context) (*mq.Message, bool)

// Submit 重投消息回灌，遵守引擎「不得无限阻塞」契约：delay>0 或缓冲满时
// 登记定时回灌后立即返回；停止后返回 ErrQueueStopped（引擎按「留待外部兜底」处理）。
func (q *Queue) Submit(msg *mq.Message, delay time.Duration) error

func (q *Queue) Commit(ctx context.Context, msg *mq.Message) error           // 接 OnSuccess / OnExhausted / OnFatal
func (q *Queue) Reject(ctx context.Context, msg *mq.Message, requeue bool) error
```

未启用 `ManualAck` 时底层无在途登记，`Commit`/`Reject` 会返回 `mq.ErrNotSubscribed`
（`mq/redis/pubsub` 除外：广播无确认通道，两者为 no-op 恒返回 nil）。

业务在引擎终态钩子里调用 `queue.Commit` / `queue.Reject` 完成 MQ 侧确认：

| 引擎终态 | 建议调用 | 效果 |
|---|---|---|
| `OnSuccess` | `queue.Commit(ctx, msg)` | 出在途、提交 offset / XACK / LREM |
| `OnExhausted` | `queue.Reject(ctx, msg, false)` | 终态丢弃（死信请业务在调用前自行落 `tools/deadletter`） |
| `OnFatal` | `queue.Reject(ctx, msg, false)` | 同上 |
| `OnInterrupted`（停机中断） | **不要确认** | 保持未确认，由 MQ 侧重投给其他/重启实例 |

## 完整示例

```go
package main

import (
    "context"
    "time"

    mq "github.com/xm-utils/tools/mq/core"
    mqpipequeue "github.com/xm-utils/tools/mq/pipequeue"
    "github.com/xm-utils/tools/pipeline"
)

// 业务上下文：embed 值形态 ExecContext 即满足密封接口
type OrderCtx struct{ pipeline.ExecContext }

func main() {
    ctx := context.Background()

    // consumer / producer 来自任一适配器模块（mq/kafka、mq/redis/{list,zset,stream,pubsub}）
    queue, _ := mqpipequeue.New(mqpipequeue.Config{
        Consumer:  consumer,
        Group:     "order-service",
        Topic:     "orders",
        ManualAck: true, // 确认挂到引擎终态钩子，严格 at-least-once
    })

    engine, _ := pipeline.New(pipeline.Config[*mq.Message, *OrderCtx]{
        Queue:   queue,
        Workers: 4,
        Resolve: func(msg *mq.Message) ([]pipeline.Step[*OrderCtx], *OrderCtx, error) {
            return []pipeline.Step[*OrderCtx]{newPersistStep()}, &OrderCtx{}, nil
        },
        OnSuccess:   func(msg *mq.Message, ctx *OrderCtx) { _ = queue.Commit(context.Background(), msg) },
        OnFatal:     func(msg *mq.Message, reason string) { _ = queue.Reject(context.Background(), msg, false) },
        OnExhausted: func(msg *mq.Message, step string, err error) { _ = queue.Reject(context.Background(), msg, false) },
        Retryer: &pipeline.PolicyRetryer[*mq.Message]{
            Policy: &pipeline.RetryPolicy{MaxAttempt: 3, BaseDelay: time.Second},
            Next:   func(msg *mq.Message) *mq.Message { return msg },
        },
        MsgKeyFunc: func(msg *mq.Message) string { return msg.Key }, // 同 Key 串行保序
    })

    _ = queue.Start(ctx)
    _ = engine.Start()
    // ... 服务退出时：
    engine.Stop() // 先停引擎，等在途步骤收尾
    queue.Stop()  // 再停桥接队列（内部会撤销 Subscribe）
}
```

## 停机与在途消息

`queue.Stop()` 取消内部 `Subscribe` 上下文并等待缓冲清空；`ManualAck=true` 下尚未被终态钩子
确认的消息**不会**被确认 —— 停机中断（`OnInterrupted`）正是要留这个状态，让 Kafka 重平衡 /
Redis PEL / processing 列表把它重投给其他实例。若业务希望停机即丢，配 `ManualAck=false`。

## 后端适配注意

- **`mq/redis/pubsub`（广播）**：桥接可用，但 `ManualAck` 与 `DurableRetry` 均无意义（广播无确认
  原语，`Commit`/`Reject` 为 no-op），建议保持 `ManualAck=false`；停机时缓冲内未处理消息直接丢失。
- **`mq/kafka`**：`Receive` 取到消息即离开 broker 拉取序列，若 `ManualAck=true` 且乱序确认，
  offset 前移会连带覆盖之前未确认消息（见 [../kafka/README.md](../kafka/README.md#offset-粒度须知)）。
  引擎并发 Workers > 1 时建议单分区顺序确认或改用 Redis 队列型后端。
- **`DurableRetry`**：`Submit` 归托 `Producer.Produce`，`delay=0` 同步回投（受 `ProduceTimeout` 约束并如实返回
  error）、`delay>0` 异步回投（失败仅记日志）；启用后建议同时开启 `ManualAck`，并按引擎
  `OnRetried` 钩子处理原消息的确认时机。

## 配置模板

见 [config.example.yaml](config.example.yaml)。

## 测试

```bash
cd mq/pipequeue && go test -race ./...
```

覆盖：门面 → 缓冲 → 引擎交接、缓冲满背压不丢消息、`ManualAck` 下终态钩子回写确认、
停机中断不确认、`DurableRetry` 持久回投路径，以及基于真实 `pipeline` 引擎的端到端链路。

## 相关模块

- 门面契约：[../core/README.md](../core/README.md)
- 引擎本体：[../pipeline/README.md](../pipeline/README.md)
- 后端适配器：[../redis/README.md](../redis/README.md) / [../kafka/README.md](../kafka/README.md)

## 许可证

MIT
