# mq — 消息队列统一门面

`github.com/xm-utils/tools/mq`

为业务代码提供与具体 MQ 解耦的 **生产**（`mq.Producer`）与 **消费确认**（`mq.Consumer`）抽象层。业务只依赖门面接口，Kafka 与 Redis 四种形态（Stream 消费者组队列 / List 安全队列 / 有序集合延迟队列 / 发布订阅广播）由子包以适配器形式实现（薄封装 `xm-utils/tools/kafka`、`xm-utils/tools/redis_stream_queue` 组件及 go-redis 直连，不改组件本体）；`mq/pipequeue` 进一步把门面消费者桥接为 `pipeline` 引擎的 `Queue`，同一套 Step 链消费代码可在任意后端间零改动切换。

## 特性

- **统一消息模型**：`mq.Message` 屏蔽 Kafka 分区/offset 与 Redis Stream entry/PEL 差异，底层原始消息保留在 `Metadata`
- **逐消息确认语义**：`Handler` 返回值驱动自动确认；`mq.ErrManualAck` 哨兵支持把确认延迟到业务终态（配合 pipeline 终态钩子实现严格 at-least-once）
- **失败策略对齐**：各适配器共享「attempt 计数 + 额度内重投 + 终态钩子」语义，Kafka 经生产者回环重投，Redis 队列型后端经共享 JSON 信封（`mq/internal/envelope`）重投，消息字节级兼容
- **Redis 四种形态**：Stream（消费者组/多实例分流）、List（轻量安全队列，processing 列表兼做在途）、有序集合（score 即可见时间的延迟队列 + 可见性超时回收）、发布订阅（广播，每个在线实例各收到一份）
- **pipeline 桥接**：推送语义（Subscribe/Handler）→ 拉取语义（Receive/Submit）转换，缓冲满即背压、重投走 `Queue.Submit`，满足引擎「Submit 不得无限阻塞」契约
- **panic 转 error**：`mq.InvokeHandler` 统一收口 Handler panic，适配器内部钩子（Rejected）自带 recover 防线
- **可 mock**：核心接口零外部依赖，单测面向接口写 fake 即可

## 安装

```bash
go get github.com/xm-utils/tools/mq
```

## 包结构

```
mq/                                      # 独立 go.mod
├── mq.go                    # package mq：Message / Handler / Producer / Consumer / InvokeHandler
├── errors.go                # package mq：ErrManualAck / ErrNotSubscribed / ErrConsumerClosed
├── internal/envelope/       # Redis 系适配器共享 JSON 信封（Key/Body base64/Headers/Attempt/ID）
├── kafka/
│   └── producer.go|consumer.go          # Kafka 适配器（手动 offset 模式 FetchMessage/CommitMessages）
├── redis/                               # Redis 系适配器统一目录
│   ├── stream/
│   │   └── producer.go|consumer.go      # Redis Stream 适配器（消费者组 XREADGROUP/XACK）
│   ├── list/
│   │   └── producer.go|consumer.go|scripts.go  # List 安全队列（Lua 原子领取 + processing/LREM 确认）
│   ├── zset/
│   │   └── producer.go|consumer.go|scripts.go  # 有序集合延迟队列（score=可见时间 + 可见性回收）
│   └── pubsub/
│       └── producer.go|consumer.go      # 发布/订阅广播（PUBLISH/SUBSCRIBE/PSUBSCRIBE，at-most-once）
└── pipequeue/
    └── bridge.go                        # pipeline.Queue[*mq.Message] 桥接队列
```

## API 参考

### package mq（核心契约）

```go
type Message struct {
    ID, Key, Topic string
    Body           []byte
    Headers        map[string]string
    Metadata       any          // kafka.Message / *redis_stream.StreamMessage
}
func (m *Message) Header(name string) string
func (m *Message) SetHeader(name, value string)

type Handler func(ctx context.Context, msg *Message) error
// 返回 nil → 适配器自动 Commit
// 返回 ErrManualAck → 挂起确认，业务终态显式 Commit/Reject
// 返回其他 error → 适配器失败策略（额度内重投，超限终态）

type Producer interface {
    Produce(ctx context.Context, topic string, msg *Message) error
    ProduceBatch(ctx context.Context, topic string, msgs []*Message) error
    Close() error
}

type Consumer interface {
    Subscribe(ctx context.Context, group, topic string, handler Handler) error // 阻塞至 ctx 结束或 Close
    Commit(ctx context.Context, msg *Message) error                             // ErrManualAck 模式终态确认
    Reject(ctx context.Context, msg *Message, requeue bool) error
    Close() error
}
```

### 适配器构造

| 构造函数 | 说明 |
|---|---|
| `kafka.NewProducer(*tools/kafka.ProducerConfig)` | Kafka 生产者（消息头映射 Headers） |
| `kafka.NewConsumer(*tools/kafka.ConsumerConfig, opts...)` | Kafka 消费者；`WithRequeueProducer` / `WithMaxRequeue` / `WithRejected` / `WithLogger` |
| `redis/stream.NewProducer(*redis.Client)` | Redis Stream 生产者（也可 `NewProducerWithConfig` 自建连接） |
| `redis/stream.NewConsumer(*redis.Client, opts...)` | Redis Stream 消费者；`WithConsumerName` / `WithMaxRequeue` / `WithDeadLetterKey` / `WithRejected` / `WithLogger` |
| `redis/list.NewProducer(*redis.Client, *Options)` | Redis List 生产者（LPUSH 入队，自动生成消息 ID 并回填 `msg.ID`） |
| `redis/list.NewConsumer(*redis.Client, *Options)` | Redis List 安全队列消费者；Options：`KeyPrefix` / `MaxRequeue` / `DeadLetterKey` / `Rejected` / `PollInterval` / `RecoverProcessing` |
| `redis/zset.NewProducer(*redis.Client, *Options)` | 有序集合延迟队列生产者；延迟经消息头 `X-Mq-Delay`（毫秒）/ `X-Mq-Ready-At`（unix 毫秒） |
| `redis/zset.NewConsumer(*redis.Client, *Options)` | 有序集合消费者；Options 另含 `VisibilityTimeout`（超时未确认自动回收重投）/ `ReclaimBatch` |
| `redis/pubsub.NewProducer(*redis.Client, *Options)` | 发布适配器（PUBLISH，自动生成消息 ID 并回填 `msg.ID`） |
| `redis/pubsub.NewConsumer(*redis.Client, *Options)` | 订阅适配器；Options：`ChannelPrefix` / `Pattern`（PSUBSCRIBE 通配）/ `ChannelBuffer` / `Rejected` / `HealthCheckInterval` |

重投次数消息头：`X-Mq-Attempt`（各适配器同名，单点取自 `mq/internal/envelope`；Redis 后端与共享信封内计数双向同步）。Redis 系适配器均直接接收 `*redis.Client`，不依赖 `tools/redis` 组件的全局连接与 key 前缀约定；key/频道前缀由 `Options.KeyPrefix` / `Options.ChannelPrefix` 控制（默认 `mq:list:` / `mq:zset:` / `mq:ps:`）。

`msg.ID` 为适配器签发的**不透明投递令牌**（kafka / redis-stream 形如 `group|…`，redis-list / redis-zset / redis-pubsub 为领取者+序号形式），仅用于日志追踪与原样回传 `Commit`/`Reject`；业务不要解析其结构或据此做幂等（幂等请用 `Key` / `Headers`）。

包名导入提示：`redis/list`、`redis/zset`、`redis/stream`、`redis/pubsub` 的包名分别为
`list` / `zset` / `stream` / `pubsub`，与业务局部变量容易重名，建议按需起别名：

```go
import (
    mqlist   "github.com/xm-utils/tools/mq/redis/list"
    mqzset   "github.com/xm-utils/tools/mq/redis/zset"
    mqstream "github.com/xm-utils/tools/mq/redis/stream"
    mqpubsub "github.com/xm-utils/tools/mq/redis/pubsub"
)
```

### package pipequeue（pipeline 桥接）

```go
type Config struct {
    Consumer mq.Consumer        // 必填：任一适配器
    Group, Topic string         // 必填
    BufferSize int              // 领取缓冲深度，默认 100；满即背压
    ManualAck bool              // true：确认挂起到业务终态（严格 at-least-once）
    Producer mq.Producer        // DurableRetry 时必填
    DurableRetry bool           // Submit 经 Producer 回投 MQ 而非回灌本地缓冲
    ProduceTimeout time.Duration// 持久回投超时，默认 5s
}

func New(cfg Config) (*Queue, error)          // 实现 pipeline.Queue[*mq.Message]
func (q *Queue) Start(ctx context.Context) error
func (q *Queue) Stop()
func (q *Queue) Commit(ctx, msg) error        // 接入 OnSuccess/OnExhausted/OnFatal
func (q *Queue) Reject(ctx, msg, requeue) error
```

## 多后端差异语义

### 队列型后端（kafka / redis/stream / redis/list / redis/zset）

| 能力 | kafka | redis/stream | redis/list | redis/zset |
|---|---|---|---|---|
| topic / group 映射 | Kafka topic / 消费组 | Stream key / 消费者组（自动 XGROUP CREATEMKSTREAM） | `mq:list:<topic>` + `:processing`；group 仅登记维度，无队列隔离 | `mq:zset:<topic>` + `:processing`；同左 |
| 成功确认 | `CommitMessages`（分区级 offset 前移） | `XACK`（逐条） | `LREM processing`（原文匹配逐条） | `ZREM processing`（逐条） |
| 失败重投 | `WithRequeueProducer` 回环 Produce（attempt+1） | 原 Stream XADD 新 entry | Lua 原子「LREM processing + LPUSH ready」 | 同左（ZADD 回 ready），额度内立即重投 |
| 重投额度 | 默认 3，防热循环 | 同左 | 同左 | 同左 |
| 终态失败 | 回调 `WithRejected`；死信由钩子落 `deadletter` | 可选死信 Stream | 可选 `DeadLetterKey` 死信 List | 可选 `DeadLetterKey` 死信集合（score=失败时刻） |
| ErrManualAck 乱序确认 | offset 粒度粗，乱序 Commit 连同此前 offset 视为已提交 | PEL 逐条，无此问题 | 逐条登记，无此问题；重复 payload 须在 Handler 内完成 Commit | 逐条登记；同内容消息靠持久 ID 保证成员唯一 |
| 在途消息宕机兜底 | 消费组重平衡后按已提交 offset 重投 | PEL 留存，PendingChecker/XCLAIM 巡检接管 | 滞留 processing，新实例 `RecoverProcessing` 启动回补（单活动消费者假设） | `VisibilityTimeout` 到期后台自动回收重投 |
| 延迟投递 | 不支持 | 不支持 | 不支持 | 原生：score 即可见时间 |
| 毒消息（解码失败） | 不存在（Body 原样字节） | 信封非法 JSON → 直接终态 | 同左（死信存原文） | 同左 |

### 发布/订阅（redis/pubsub）适用边界

广播与队列不同构，本适配器有意放弃了部分门面能力（接口仍兼容，业务代码无需改写）：

| 门面语义 | redis/pubsub 行为 |
|---|---|
| 投递保证 | **at-most-once**：无持久化，发布瞬间不在线的订阅者永久错过该消息 |
| 多实例分流 | 不分流：每个在线实例各收到一份（广播本身即是「一消息多消费者」的唯一形态） |
| 成功确认 | 无确认原语：Handler 返回 nil 即投递完成 |
| `mq.ErrManualAck` | 等价于「投递完成」（无在途状态可挂起），Commit/Reject 为 no-op 返回 nil |
| 失败重投 | 不可重投（无存放处）：只能经 `Options.Rejected` 钩子 + 日志观测 |
| 死信 | 不提供（写往另一频道同样会丢）：需可靠性请用 list/zset/stream |
| topic 映射 | 频道名 `mq:ps:<topic>`；`Options.Pattern=true` 时 topic 作 glob 模式（PSUBSCRIBE），此时 `msg.Topic` 还原为实际命中频道 |
| group 参数 | 仅作登记与日志维度（无消费组语义），同一 group+topic 重复订阅返回 error |
| 断线 | go-redis Channel 模式自带探活重连（`HealthCheckInterval`，默认 3s），重连期间消息丢失；首次建链前先 Ping 预检，Redis 不可达时 `Subscribe` 直接返回 error（v8 的 SUBSCRIBE 会吞掉拨号错误） |
| 背压 | 订阅侧缓冲 `ChannelBuffer`（默认 100）满时接收协程阻塞至多 1 分钟（go-redis `chanSendTimeout`，不可配），超时后丢弃该条新消息并打库层告警；慢 Handler 会连带延迟后续投递 |
| 与 pipequeue 桥接 | 可用但 `ManualAck` / `DurableRetry` 无意义（无确认原语），仅建议自动确认模式；停机时缓冲内未处理消息直接丢失 |

适用：配置刷新、缓存失效通知、在线事件广播；不适用：任何「一条也不能丢」的业务流水。

### 消费者生命周期语义（各适配器一致，pubsub 仅无确认环节）

- `Subscribe` 阻塞式：常驻消费请放入独立 goroutine；返回值为退出原因 —— ctx 结束为
  `ctx.Err()`，`Close` 触发为 `context.Canceled`，其余非 nil error 仅为初始化失败。
- 循环退出后自动撤销该 group+topic 登记，可原地重新订阅；`Close` 则终止全部订阅。
- 拉取/领取持续失败按 500ms 退避，空队列按 `PollInterval`（List/ZSet 默认 100ms）轮询；
  pubsub 为长连接推送，无轮询。
- 提交/重投失败的在途登记会回滚，业务可重试确认（至多一次语义由幂等性兜住）。

## 错误约定

| 错误 | 场景 |
|---|---|
| `mq.ErrManualAck` | Handler 返回哨兵：挂起确认（非失败） |
| `mq.ErrNotSubscribed` | 对未登记（已确认/未知）消息执行 Commit/Reject（redis/pubsub 不适用，恒返回 nil） |
| `mq.ErrConsumerClosed` | 消费者 Close 后 Subscribe/Commit/Reject |
| `pipequeue.ErrQueueStopped` | 桥接队列停止后 Submit（引擎按「留待外部兜底」处理） |

## 依赖与构建说明

- 基座依赖：`tools/kafka`（`kafka/v1.0.4`）、`tools/redis_stream_queue`（`redis_stream_queue/v1.0.0`）、`tools/pipeline`（`pipeline/v0.0.1`，仅 pipequeue 编译期契约引用），均按已发布 tag 引用，无仓内 replace；本地联动由根目录 `go.work` 负责（不提交）。
- Redis 连接基座：`go-redis/v8`（与 `redis_stream_queue` 组件同版本）；`miniredis` 仅测试依赖，用于四种 Redis 形态适配器的真实协议层集成测试（含 Lua 脚本、SUBSCRIBE/PUBLISH 与 Stream 消费者组命令）。
- 适配为薄封装：不修改 MQ 组件本体，符合「三方库改动在调用方模块完成」约定。
- go 指令与仓库所有组件统一为 1.24.2。

## 测试

```bash
cd mq
go test -race ./...   # 49 用例：核心契约/panic 防线、适配器映射与信封二进制安全、桥接交接/背压/停机/持久回灌、真实 pipeline 引擎端到端，四种 Redis 形态适配器基于 miniredis 的真实协议层集成测试（XREADGROUP/重投、Lua 领取/回收、广播与通配订阅、阻塞订阅退出与建链预检语义）
```

## 快速上手

见 [QUICKSTART.md](QUICKSTART.md)（5 分钟：生产 → 消费 → 切换 Redis 队列/延迟/广播形态 → pipeline 对接）。

## 许可证

MIT
