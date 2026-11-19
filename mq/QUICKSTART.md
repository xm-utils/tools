# mq 快速上手（5 分钟）

以订单场景演示：生产 → 消费 → Kafka / Redis 各后端（Stream / List / 有序集合 / 发布订阅）互切 → 对接 pipeline 引擎。

## 1. 生产消息

```go
package main

import (
    "context"
    "fmt"

    xmkafka "github.com/xm-utils/tools/kafka"
    "github.com/xm-utils/tools/mq"
    mqkafka "github.com/xm-utils/tools/mq/kafka"
)

func main() {
    var producer mq.Producer = mqkafka.NewProducer(&xmkafka.ProducerConfig{
        CommonConfig: xmkafka.CommonConfig{Brokers: []string{"localhost:9092"}},
    })
    defer producer.Close()

    err := producer.Produce(context.Background(), "orders", &mq.Message{
        Key:     "order-1001",
        Body:    []byte(`{"amount": 99}`),
        Headers: map[string]string{"source": "api"},
    })
    fmt.Println("produce:", err)
}
```

## 2. 消费消息

```go
consumer, err := mqkafka.NewConsumer(&xmkafka.ConsumerConfig{
    CommonConfig: xmkafka.CommonConfig{Brokers: []string{"localhost:9092"}},
}, mqkafka.WithRequeueProducer(producer)) // 失败额度内重投
if err != nil {
    panic(err) // 实际项目请优雅处理
}

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// Subscribe 是阻塞式：常驻消费请放入独立 goroutine，退出返回 ctx.Err()
// （Close 触发则为 context.Canceled）
go func() {
    err := consumer.Subscribe(ctx, "order-service", "orders", func(ctx context.Context, msg *mq.Message) error {
        fmt.Printf("got %s: %s\n", msg.ID, msg.Body)
        return nil // 返回 nil 自动 Commit；返回 mq.ErrManualAck 则挂起到业务终态确认
    })
    _ = err // 实际项目请区分 ctx.Err()/context.Canceled 与真实异常
}()
```

## 3. 切换 Redis Stream（业务代码零改动）

只替换装配，第 2 步的业务 Handler 一行不动：

```go
import (
    rs "github.com/xm-utils/tools/redis_stream_queue"
    mqredis "github.com/xm-utils/tools/mq/redis/stream"
)

client, _ := rs.CreateRedisClient(rs.DefaultRedisConfig())
var producer mq.Producer = mqredis.NewProducer(client)                      // 第 1 步同款写法替换
consumer, _ := mqredis.NewConsumer(client,
    mqredis.WithDeadLetterKey("stream:orders:dlq"),                          // 终态自动转死信 Stream
    mqredis.WithRejected(func(ctx context.Context, msg *mq.Message, err error) {
        // 告警 / 落 deadletter 组件
    }))
go func() { _ = consumer.Subscribe(ctx, "order-service", "orders", bizHandler) }() // topic 即 stream key
```

体量选型建议：日百万级以下用 Redis 系后端，往上切 Kafka；轻量场景用 List，
需要延迟投递/崩溃自动回收用有序集合，需要多实例消费者组用 Stream，
需要「每个实例都收到」（缓存失效/配置刷新）用发布订阅，
业务侧仅改本节装配代码。

## 4. 切换 Redis List / 有序集合（同样零改动）

两个后端直接接收 `*redis.Client`，队列 key 前缀、重投额度、死信等经 `Options` 装配
（包名 `list` / `zset` 较通用，建议起别名）：

```go
import (
    "time"

    "github.com/go-redis/redis/v8"
    "github.com/xm-utils/tools/mq"
    mqlist "github.com/xm-utils/tools/mq/redis/list"
    mqzset  "github.com/xm-utils/tools/mq/redis/zset"
)

client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

// List 安全队列：适合轻量串行消费，崩溃残留在新实例启动时回补
var producer mq.Producer = mqlist.NewProducer(client, nil)
listConsumer := mqlist.NewConsumer(client, &mqlist.Options{
    MaxRequeue:        3,
    DeadLetterKey:     "mq:list:orders:dlq",
    RecoverProcessing: true, // 单活动消费者假设
})
// topic → mq:list:orders；Subscribe 阻塞，同样放 goroutine
go func() { _ = listConsumer.Subscribe(ctx, "order-service", "orders", bizHandler) }()

// 有序集合延迟队列：score 即可见时间，崩溃残留由可见性超时自动回收重投
zProducer := mqzset.NewProducer(client, nil)
delayed := &mq.Message{Body: []byte(`{"remind":"10:00"}`)}
delayed.SetHeader(mqzset.HeaderDelay, "3600000") // 1 小时后可见（毫秒）
_ = zProducer.Produce(ctx, "reminders", delayed)

zConsumer := mqzset.NewConsumer(client, &mqzset.Options{
    VisibilityTimeout: 5 * time.Minute, // 超过 5 分钟未确认的消息回队重投
})
go func() { _ = zConsumer.Subscribe(ctx, "reminder-service", "reminders", bizHandler) }()
```

注意：List/ZSet 队列无消费者组隔离，group 参数仅作登记维度，多实例订阅同一
topic 共享同一队列（互斥领取由 Lua 原子保证，不会双领）。两类 Subscribe 与其他
适配器一致为阻塞式，ctx 结束或 Close 后退出，退出后同一 group+topic 可重新订阅。

## 5. 切换 Redis 发布/订阅（广播，接口不变但语义降级）

装配写法与队列型完全一致，但 **at-most-once**：无确认、无重投、离线即丢：

```go
import (
    "github.com/go-redis/redis/v8"
    "github.com/xm-utils/tools/mq"
    mqpubsub "github.com/xm-utils/tools/mq/redis/pubsub"
)

client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

publisher := mqpubsub.NewProducer(client, nil) // 频道前缀默认 mq:ps:
_ = publisher.Produce(ctx, "cache:user:1001", &mq.Message{Body: []byte("invalidate")})

for _, instance := range []string{"node-a", "node-b"} {
    subscriber := mqpubsub.NewConsumer(client, &mqpubsub.Options{
        ConsumerName: instance,
        Pattern:      true, // topic 按 glob 匹配频道（PSUBSCRIBE）
    })
    go func(c *mqpubsub.Consumer) {
        _ = c.Subscribe(ctx, "cache-invalidator", "cache:user:*", func(ctx context.Context, msg *mq.Message) error {
            // 广播无确认通道：返回 nil 与返回 mq.ErrManualAck 均为投递完成，
            // 返回 error 不可重投（仅进 Options.Rejected 钩子）
            _ = localCache.Delete(msg.Topic) // msg.Topic 为实际命中频道名
            return nil
        })
    }(subscriber)
}
```

要点：`Options.Pattern: true` 时 topic 作 glob 模式（PSUBSCRIBE），`msg.Topic` 还原为
实际命中频道；不在线的订阅者永久错过该条消息，需要「一条不能丢」请改回
redis/list、redis/zset 或 redis/stream。

## 6. 对接 pipeline 引擎

`mq/pipequeue` 把门面消费者桥接为引擎的 `Queue`，享受步骤链、错误分级、
重试退避、有界停机、同 Key 串行全套能力：

```go
import (
    "context"
    "time"

    "github.com/xm-utils/tools/mq"
    "github.com/xm-utils/tools/mq/pipequeue"
    "github.com/xm-utils/tools/pipeline"
)

// 业务上下文：embed 值形态 ExecContext 即满足密封接口
type OrderCtx struct{ pipeline.ExecContext }

queue, _ := pipequeue.New(pipequeue.Config{
    Consumer:  consumer,       // 任意 mq.Consumer 适配器
    Group:     "order-service",
    Topic:     "orders",
    ManualAck: true,           // 确认挂到引擎终态钩子，严格 at-least-once
})

engine, _ := pipeline.New(pipeline.Config[*mq.Message, *OrderCtx]{
    Queue:   queue,
    Workers: 4,
    Resolve: func(msg *mq.Message) ([]pipeline.Step[*OrderCtx], *OrderCtx, error) {
        return []pipeline.Step[*OrderCtx]{newPersistStep()}, &OrderCtx{}, nil
    },
    // 终态钩子回写 MQ 门面确认（OnInterrupted 停机中断不要确认，留待 MQ 重投）
    OnSuccess: func(msg *mq.Message, ctx *OrderCtx) { _ = queue.Commit(context.Background(), msg) },
    OnFatal:   func(msg *mq.Message, reason string) { _ = queue.Reject(context.Background(), msg, false) },
    OnExhausted: func(msg *mq.Message, step string, err error) {
        _ = queue.Reject(context.Background(), msg, false) // 耗尽转死信后终态丢弃
    },
    Retryer: &pipeline.PolicyRetryer[*mq.Message]{
        Policy: &pipeline.RetryPolicy{MaxAttempt: 3, BaseDelay: time.Second},
        Next:   func(msg *mq.Message) *mq.Message { return msg },
    },
    MsgKeyFunc: func(msg *mq.Message) string { return msg.Key }, // 同 Key 串行保序
})

_ = queue.Start(context.Background())
_ = engine.Start()
// ... 服务退出时：
engine.Stop()
queue.Stop()
```

重试说明：默认（`DurableRetry=false`）重投消息回灌桥接缓冲，进程停机时由
ManualAck 的未确认状态保证 MQ 侧重投；若希望重投消息跨进程存活，配置
`Producer` + `DurableRetry: true`，Submit 将回投 MQ 本体（delay=0 同步、delay>0 异步）。

## 7. 配置模板

适配器直接复用底层组件配置结构体，YAML 字段对应关系见 [config.example.yaml](config.example.yaml)。
