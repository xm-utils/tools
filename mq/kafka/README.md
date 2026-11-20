# mq/kafka — Kafka MQ 适配器

`github.com/xm-utils/tools/mq/kafka`（package `kafka`）

实现 [tools/mq](../core/README.md) 门面契约的 Kafka 适配器（本模块根包），底层为同模块
[client 子包](client/README.md)（原 `tools/kafka` 组件）的薄封装，
走**手动 offset 模式**（`FetchMessage` / `CommitMessages`），
从而把门面的逐消息确认语义（`nil` 自动确认 / `ErrManualAck` 挂起到终态）落到 Kafka 上。

只想直接操作底层客户端（Worker Pool、消息去重、指标）而不走门面时，
import [`client` 子包](client/README.md) 即可，两者同属一个 `go.mod`。

## 特性

- 逐消息 `Commit` / `Reject`，支持 `mq.ErrManualAck` 延迟确认（配合 `mq/pipequeue` 实现严格 at-least-once）
- 失败额度内就地重投：`WithRequeueProducer` 注入 write-only 生产者，回环 Produce 并自增 `X-Mq-Attempt`
- 崩溃/重平衡兜底：未提交 offset 由消费组重平衡后重新投递
- Headers ↔ `kafka.Message` header 双向映射，`msg.Metadata` 保留原始 `kafka.Message`（分区、offset 等专有字段）

## 安装

```bash
go get github.com/xm-utils/tools/mq/kafka
```

## 构造函数与装配项

| 构造函数 | 说明 |
|---|---|
| `kafka.NewProducer(*client.ProducerConfig)` | 生产者（`Produce` / `ProduceBatch` / `Close`） |
| `kafka.NewConsumer(*client.ConsumerConfig, opts...)` | 消费者；`WithRequeueProducer` / `WithMaxRequeue` / `WithRejected` / `WithLogger` |

`client` 为同模块本体子包 `github.com/xm-utils/tools/mq/kafka/client`（包名 `client`）。

## 关键装配约束

适配器会对传入的 `ConsumerConfig` 做强制改写，业务配置项不必（也不应）依赖以下三个字段的原值：

- `AutoCommit = false`、`CommitInterval = 0`：确认时机完全由门面 `Commit`/`Reject` 驱动，
  否则会出现「库已提交、业务未处理完」的假确认。
- `GroupID` / `Topic`：以 `Subscribe(ctx, group, topic, handler)` 参数为准（一个消费者可多订阅）。

## 完整示例

```go
package main

import (
    "context"
    "fmt"

    client "github.com/xm-utils/tools/mq/kafka/client"
    mq "github.com/xm-utils/tools/mq/core"
    mqkafka "github.com/xm-utils/tools/mq/kafka"
)

func main() {
    // 1. 生产
    var producer mq.Producer = mqkafka.NewProducer(&client.ProducerConfig{
        CommonConfig: client.CommonConfig{Brokers: []string{"localhost:9092"}},
    })
    defer producer.Close()

    _ = producer.Produce(context.Background(), "orders", &mq.Message{
        Key:     "order-1001",
        Body:    []byte(`{"amount": 99}`),
        Headers: map[string]string{"source": "api"},
    })

    // 2. 消费（WithRequeueProducer 使 Handler 返回 error 时在额度内重投）
    consumer, err := mqkafka.NewConsumer(&client.ConsumerConfig{
        CommonConfig: client.CommonConfig{Brokers: []string{"localhost:9092"}},
    }, mqkafka.WithRequeueProducer(producer), mqkafka.WithMaxRequeue(3))
    if err != nil {
        panic(err)
    }
    defer consumer.Close()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Subscribe 阻塞式：常驻消费请放入独立 goroutine，退出返回 ctx.Err()
    // （Close 触发则为 context.Canceled），其余非 nil error 仅为初始化失败。
    go func() {
        _ = consumer.Subscribe(ctx, "order-service", "orders", func(ctx context.Context, msg *mq.Message) error {
            fmt.Printf("got %s: %s\n", msg.ID, msg.Body)
            return nil // nil 自动 Commit；mq.ErrManualAck 挂起到业务终态
        })
    }()
}
```

## 失败与终态处置

| 情况 | 行为 |
|---|---|
| Handler 返回 nil | `CommitMessages` 提交该消息 offset（分区级前移） |
| Handler 返回 error 且 attempt < `MaxRequeue`（默认 3） | 经 `WithRequeueProducer` 回环 Produce 新副本，`X-Mq-Attempt` +1；**未注入重投生产者时该消息直接按终态失败处置** |
| Handler panic | `mq.InvokeHandler` recover 为 error，进入上面的失败路径，不击穿消费协程 |
| 达重投额度 | 回调 `WithRejected(ctx, msg, err)`；死信落库由钩子内调用 `tools/deadletter` 完成 |
| Handler 返回 `mq.ErrManualAck` | 不确认，业务终态调用 `consumer.Commit(ctx, msg)` 或 `consumer.Reject(ctx, msg, requeue)` |

## offset 粒度须知

Kafka 的确认单位是**分区内 offset**，不是消息个体，因此：

- 乱序 `Commit` 会把它之前尚未确认的 offset 一并视为已提交（Redis stream/list/zset 逐条登记，无此问题）。
  需要严格「处理完一条确认一条且不漏」，请用单分区 + 顺序确认，或改用 Redis 队列型后端。
- `Reject(msg, requeue=true)` 走回环 Produce（新 offset、attempt+1），不是在原位置重放；
  因此重投副本与原始消息在分区序列上位置不同，同 Key ordering 需业务以 `Key` 自行幂等。

## 配置模板

见 [config.example.yaml](config.example.yaml)：`client` 子包的 producer / consumer 配置字段与
Consumer 装配项逐条注释。

## 测试

```bash
cd mq/kafka && go test -race ./...
```

门面适配（2 个包共 28 用例）：根包覆盖 Kafka `Message` ↔ 门面 `Message` 双向转换（Headers、
Metadata、nil 边界）；`client` 子包覆盖 Manager / Worker Pool / 指标监控。
消费循环与重投路径依赖真实 broker，集成验证请在 CI 中以 kafka 容器驱动。

## 相关模块

- 门面契约与选型矩阵：[../core/README.md](../core/README.md)
- Kafka 客户端本体（Worker Pool / 去重 / 指标）：[client/README.md](client/README.md)、[client/METRICS_README.md](client/METRICS_README.md)
- Redis 四形态适配器：[../redis/README.md](../redis/README.md)
- pipeline 桥接：[../pipequeue/README.md](../pipequeue/README.md)

## 许可证

MIT
