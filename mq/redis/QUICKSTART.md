# mq/redis 快速上手（5 分钟）

以订单场景演示 Redis 四种形态的装配与互切：安全队列（list）→ 延迟队列（zset）→
消费者组队列（stream）→ 广播（pubsub）。业务 Handler 全程零改动，只换装配。

门面契约与跨后端选型矩阵见 [../core/README.md](../core/README.md)。

## 0. 业务 Handler（四种形态共用）

```go
package main

import (
    "context"
    "fmt"

    "github.com/go-redis/redis/v8"
    mq "github.com/xm-utils/tools/mq/core"
)

var bizHandler = func(ctx context.Context, msg *mq.Message) error {
    fmt.Printf("got %s/%s: %s\n", msg.Topic, msg.ID, msg.Body)
    return nil // nil 自动确认；返回 mq.ErrManualAck 则挂起到业务终态显式 Commit/Reject
}

// 下文各形态片段共用同一 ctx 与 client
var ctx = context.Background()
```

## 1. List 安全队列（轻量、无消费者组）

```go
import mqlist "github.com/xm-utils/tools/mq/redis/list"

client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

var producer mq.Producer = mqlist.NewProducer(client, nil)
_ = producer.Produce(ctx, "orders", &mq.Message{
    Key:     "order-1001",
    Body:    []byte(`{"amount": 99}`),
    Headers: map[string]string{"source": "api"},
})

listConsumer := mqlist.NewConsumer(client, &mqlist.Options{
    MaxRequeue:        3,
    DeadLetterKey:     "mq:list:orders:dlq",
    RecoverProcessing: true, // 单活动消费者假设：启动时回补上一实例残留 processing
})
// topic → mq:list:orders；Subscribe 阻塞，常驻消费请放入独立 goroutine
go func() { _ = listConsumer.Subscribe(ctx, "order-service", "orders", bizHandler) }()
```

要点：多实例订阅同一 topic 共享同一队列，领取由 Lua 原子保证互斥（不会双领）；
`group` 参数只是在途登记维度，无队列隔离。

## 2. 有序集合延迟队列（score 即可见时间）

```go
import (
    "time"

    mqzset "github.com/xm-utils/tools/mq/redis/zset"
)

zProducer := mqzset.NewProducer(client, nil)

delayed := &mq.Message{Body: []byte(`{"remind":"10:00"}`)}
delayed.SetHeader(mqzset.HeaderDelay, "3600000")   // 1 小时后可见（相对毫秒）
// 或 delayed.SetHeader(mqzset.HeaderReadyAt, "1737400000000") // 绝对 unix 毫秒，同存时后者优先
_ = zProducer.Produce(ctx, "reminders", delayed)

zConsumer := mqzset.NewConsumer(client, &mqzset.Options{
    VisibilityTimeout: 5 * time.Minute, // 领取后 5 分钟未确认自动回收重投（崩溃兜底）
    ReclaimBatch:      50,
})
go func() { _ = zConsumer.Subscribe(ctx, "reminder-service", "reminders", bizHandler) }()
```

## 3. Stream 消费者组队列（多实例分流）

```go
import (
    queue "github.com/xm-utils/tools/mq/redis/stream/queue"
    mqstream "github.com/xm-utils/tools/mq/redis/stream"
)

// 方式一：已有 client 直接复用（消费者返回 error，主要是建组/参数校验失败）
streamConsumer, _ := mqstream.NewConsumer(client,
    mqstream.WithDeadLetterKey("stream:orders:dlq"), // 达重投额度自动转死信 Stream
    mqstream.WithRejected(func(ctx context.Context, msg *mq.Message, err error) {
        // 告警 / 落 deadletter 组件
    }))

// 方式二：由 stream/queue 本体按 RedisConfig 自建连接（返回 producer、它创建的 *redis.Client 与 error）
streamProducer, _, _ := mqstream.NewProducerWithConfig(queue.DefaultRedisConfig())

// topic 即 stream key，group 即消费者组（首次订阅自动 XGROUP CREATEMKSTREAM）
go func() { _ = streamConsumer.Subscribe(ctx, "order-service", "orders", bizHandler) }()
```

门面只需 `Produce` / `Subscribe` / `Commit` 三个动作；如需直接用队列本体的
装饰器（重试 / 幂等 / 死信）、PendingChecker 巡检或多消费者管理，
直接 import `stream/queue`：见 [stream/queue/README.md](stream/queue/README.md)
与 [stream/queue/USAGE_EXAMPLE.md](stream/queue/USAGE_EXAMPLE.md)。

## 4. 发布/订阅广播（at-most-once，接口不变但语义降级）

```go
import mqpubsub "github.com/xm-utils/tools/mq/redis/pubsub"

publisher := mqpubsub.NewProducer(client, nil) // 频道前缀默认 mq:ps:
_ = publisher.Produce(ctx, "cache:user:1001", &mq.Message{Body: []byte("invalidate")})

for _, instance := range []string{"node-a", "node-b"} {
    subscriber := mqpubsub.NewConsumer(client, &mqpubsub.Options{
        ConsumerName: instance,
        Pattern:      true, // topic 作 glob 模式（PSUBSCRIBE）
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

要点：不在线的订阅者永久错过该条消息；需要「一条不能丢」请改回 list / zset / stream。
Redis 不可达时 `Subscribe` 会直接返回 error（建链前 Ping 预检），不会静默空转。

## 5. 终态确认（ErrManualAck 模式，四种队列形态通用）

```go
handler := func(ctx context.Context, msg *mq.Message) error {
    // 交给 pipeline 引擎等异步终态处置，先挂起确认
    return mq.ErrManualAck
}
// 业务终态时显式回写（msg 原样传递，不要复制后回传）：
_ = listConsumer.Commit(ctx, msg)                    // 成功 → 出 processing
_ = listConsumer.Reject(ctx, msg, true)              // 失败 → 额度内重投
_ = listConsumer.Reject(ctx, msg, false)             // 终态丢弃（死信请业务在调用前自行落）
```

`pubsub` 下这两个方法为 no-op 返回 nil；对未登记（已确认/未知）消息执行会返回
`mq.ErrNotSubscribed`。

## 6. 配置模板

连接与四种形态 Options 字段逐项注释见 [config.example.yaml](config.example.yaml)。

## 许可证

MIT
