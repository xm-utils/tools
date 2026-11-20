# mq/redis — Redis 四种形态 MQ 适配器

`github.com/xm-utils/tools/mq/redis`

实现 [tools/mq](../core/README.md) 门面契约的 Redis 侧适配器，按数据结构与投递语义分为四个子包，
互不依赖，按需 import（只用 list / zset / pubsub 不会把 `stream/queue` 队列本体拉入构建）：

| 子包 | 底层结构 | 投递语义 | 典型场景 |
|---|---|---|---|
| `list` | List + Lua（ready / processing 双列表） | at-least-once，安全队列 | 轻量串行消费、无需消费者组的任务领取 |
| `zset` | 有序集合（score = 可见时间） | at-least-once + 延迟投递 + 可见性超时回收 | 定时提醒、退避重试、崩溃自动回收 |
| `stream` | Stream + 消费者组（XREADGROUP / PEL / XACK） | at-least-once，多实例分流 | 主流水队列、需要 PEL 巡检接管 |
| `pubsub` | PUBLISH / SUBSCRIBE / PSUBSCRIBE | **at-most-once** 广播 | 缓存失效、配置刷新、在线事件 |

## 特性

- **同一信封跨形态字节兼容**：三种队列型形态共享 JSON 信封（`internal/envelope`），同一条 `mq.Message` 在 list / zset / stream 间迁移无需转换，重投计数、Key、Headers 语义单点维护
- **额度内重投 + 终态钩子**：Handler 返回 error 时按 `MaxRequeue`（默认 3）就地重投，超限走 `Rejected` 钩子 / 死信 key，防热循环打爆下游
- **崩溃残留兜底**：list 支持启动时把 processing 残留原子回补 ready；zset 由 `VisibilityTimeout` 后台自动回收；stream 由 PEL 留存，可经 XCLAIM 手动接管（本体 `PendingChecker` 当前处于注释状态，见下文「本体启用范围」）
- **零全局状态耦合**：适配器直接接收 `*redis.Client`（`go-redis/v8`），不依赖 `tools/redis` 组件的全局连接与 key 前缀约定（`stream/queue` 队列本体除外，它自带连接工厂）
- **miniredis 真实协议层集成测试**：含 Lua 脚本、消费者组命令、SUBSCRIBE/PUBLISH，无需外部 Redis

## 安装

```bash
go get github.com/xm-utils/tools/mq/redis
```

## 包结构与导入别名

```
mq/redis/                       # 独立 go.mod
├── list/       producer.go|consumer.go|scripts.go|list_test.go
├── zset/       producer.go|consumer.go|scripts.go|zset_test.go
├── stream/     producer.go|consumer.go|envelope_test.go|stream_test.go
│   └── queue/  package queue    # Stream 队列本体（启用范围见下）
├── pubsub/     producer.go|consumer.go|pubsub_test.go
└── internal/envelope/          # 队列型三形态共享信封（模块内部，不对外）
```

子包包名为 `list` / `zset` / `stream` / `pubsub`（较通用，易与业务局部变量重名），建议起别名；
stream 适配器的队列能力在 `stream/queue`（包名 `queue`，仓库内旧别名 `rs` 仍可用）：

```go
import (
    mqlist   "github.com/xm-utils/tools/mq/redis/list"
    mqzset   "github.com/xm-utils/tools/mq/redis/zset"
    mqstream "github.com/xm-utils/tools/mq/redis/stream"
    mqpubsub "github.com/xm-utils/tools/mq/redis/pubsub"
)
```

## 本体启用范围

`stream/queue` 当前只保留门面适配器实际调用的能力：`CreateRedisClient`、`RedisConfig` /
`QueueConfig`、`StreamMessage` 与 `StreamQueue` 的 `InitStream` / `EnqueueMessage` /
`ReadMessages` / `AckMessage`（+内部 `DelMessage`）。

原组件的以下能力**已在各自文件内整体注释保留**（去掉 `/* */` 即恢复）：`Consumer`
封装与 `ConsumerBuilder`、`DecoratorChain` 与重试 / 幂等 / 死信装饰器、
`MultiStreamConsumer`、`PendingChecker`、`MetricsMonitor` 与 `AlertConfig`，以及
`GetPendingMessages` / `ClaimMessage` / `GetStreamLength`。因此 `mq/redis` 不再引入
`tools/redis`、`tools/deadletter`、`tools/retry`、`tools/database`、`ants/v2`。

## 构造函数与装配项

| 构造函数 | 说明 |
|---|---|
| `stream.NewProducer(*redis.Client)` | Stream 生产者（XADD） |
| `stream.NewProducerWithConfig(*queue.RedisConfig)` | 自建连接变体，返回 `(*Producer, *redis.Client, error)` |
| `stream.NewConsumer(*redis.Client, opts...)` | Stream 消费者（返回 `(*Consumer, error)`）；`WithConsumerName` / `WithMaxRequeue` / `WithDeadLetterKey` / `WithRejected` / `WithLogger` |
| `list.NewProducer(*redis.Client, *Options)` | LPUSH 入队（自动生成消息 ID 并回填 `msg.ID`） |
| `list.NewConsumer(*redis.Client, *Options)` | Lua 原子领取 + processing 在途登记；Options：`KeyPrefix` / `ConsumerName` / `MaxRequeue` / `DeadLetterKey` / `PollInterval` / `Rejected` / `RecoverProcessing` |
| `zset.NewProducer(*redis.Client, *Options)` | ZADD 入队；延迟经消息头 `X-Mq-Delay`（相对毫秒）/ `X-Mq-Ready-At`（unix 毫秒，同存后者优先） |
| `zset.NewConsumer(*redis.Client, *Options)` | 按 score 到期领取；Options 另含 `VisibilityTimeout`（超时未确认自动回收重投）/ `ReclaimBatch`（单轮上限） |
| `pubsub.NewProducer(*redis.Client, *Options)` | PUBLISH（自动生成消息 ID 并回填 `msg.ID`） |
| `pubsub.NewConsumer(*redis.Client, *Options)` | SUBSCRIBE / PSUBSCRIBE；Options：`ChannelPrefix` / `ConsumerName` / `Pattern` / `ChannelBuffer` / `HealthCheckInterval` / `Rejected` |

Options 传 `nil` 取全部默认值（key 前缀 `mq:list:` / `mq:zset:` / `mq:ps:`，`MaxRequeue` 3，`PollInterval` 100ms）。
重投次数消息头 `X-Mq-Attempt` 由门面 `mq.HeaderAttempt` 单点定义，各子包导出同值常量，与信封内计数双向同步。

## 共享 JSON 信封

`internal/envelope` 为 list / zset / stream 三形态的统一载荷编码（`pubsub` 直发 `mq.Message.Body`，
不经信封，因其无重投与在途概念）：

```json
{"id":"...","key":"...","body":"<base64>","headers":{"k":"v"},"attempt":1}
```

`body` 由 `encoding/json` 对 `[]byte` base64 编解码，二进制安全；`id` 为业务侧持久标识，
重投副本沿用原信封 ID（**不会**被投递令牌 `msg.ID` 的 group 前缀污染），`attempt` 为重投计数。

## 各形态语义细节

- **list**：topic → `mq:list:<topic>`（ready）与 `<key>:processing`。领取由 Lua 脚本原子完成
  （RPOP ready + LPUSH processing + 返回 payload），确认即 `LREM processing 1 payload`。
  无消费者组隔离，group 参数仅作在途登记维度；多实例订阅同一 topic 共享队列，互斥领取不会双领。
  进程崩溃残留可由新实例 `RecoverProcessing=true` 启动时分批回补 ready（**单活动消费者假设**，
  多实例同时开启会互相回补对方在途消息）。
- **zset**：topic → `mq:zset:<topic>`，score 即「何时可见」；领取用 Lua 把到期成员搬到 `:processing`。
  `VisibilityTimeout > 0` 时后台协程周期性把 processing 中 score 超期的成员 ZADD 回 ready（崩溃兜底首选）。
  死信集合成员 score 为失败时刻，便于按时间扫描。同内容消息靠持久 ID 保证 processing 成员唯一。
- **stream**：topic → stream key，group → 消费者组（首次订阅自动 `XGROUP CREATEMKSTREAM`）。
  消费走同模块 `stream/queue` 本体的读取/ack 能力，失败重投为原 stream XADD 新 entry（attempt+1），
  达额度后可选转死信 Stream。`msg.ID` 形如 `group|entry-id`，PEL 逐条 XACK，无 offset 粗粒度问题。
- **pubsub**：topic → 频道 `mq:ps:<topic>`；`Pattern=true` 时 topic 作 glob 模式（PSUBSCRIBE），
  `msg.Topic` 还原为实际命中频道名。

## 发布/订阅适用边界

广播与队列不同构，本适配器有意放弃部分门面能力（接口仍兼容，业务代码无需改写）：

| 门面语义 | pubsub 行为 |
|---|---|
| 投递保证 | **at-most-once**：无持久化，发布瞬间不在线的订阅者永久错过该消息 |
| 多实例分流 | 不分流：每个在线实例各收到一份（广播本身即是「一消息多消费者」的唯一形态） |
| 成功确认 | 无确认原语：Handler 返回 nil 即投递完成 |
| `mq.ErrManualAck` | 等价于「投递完成」（无在途状态可挂起），`Commit`/`Reject` 为 no-op 返回 nil |
| 失败重投 | 不可重投（无存放处）：只能经 `Options.Rejected` 钩子 + 日志观测 |
| 死信 | 不提供（写往另一频道同样会丢）：需可靠性请用 list / zset / stream |
| group 参数 | 仅作登记与日志维度，同一 group+topic 重复订阅返回 error |
| 断线 | go-redis Channel 模式自带探活重连（`HealthCheckInterval`，默认 3s），重连期间消息丢失；首次建链前先 `PING` 预检，Redis 不可达时 `Subscribe` 直接返回 error（v8 的 SUBSCRIBE 会吞掉拨号错误、静默空转） |
| 背压 | `ChannelBuffer`（默认 100）满时接收协程阻塞至多 1 分钟（go-redis `chanSendTimeout`，不可配），超时后**丢弃该条新消息**并打库层告警；慢 Handler 会连带延迟后续投递 |
| 与 pipequeue 桥接 | 可用但 `ManualAck` / `DurableRetry` 无意义（无确认原语），仅建议自动确认模式；停机时缓冲内未处理消息直接丢失 |

适用：配置刷新、缓存失效通知、在线事件广播；不适用：任何「一条也不能丢」的业务流水。

## 配置模板

见 [config.example.yaml](config.example.yaml)：Redis 连接与四种形态的 Options 字段逐项注释。
stream 形态直接复用 `stream/queue` 包的 `RedisConfig`，其余三形态对应本模块各 `Options` 结构体字段。

## 测试

```bash
cd mq/redis && go test -race ./...
```

覆盖：信封编解码与二进制安全 / attempt 双向同步 / ID 不被投递令牌污染；
list 领取-确认-重投-死信-`RecoverProcessing` 回补与 Lua 原子性；
zset score 到期、延迟头解析、可见性超时回收；
stream 消费者组消费、就地重投、达额度转死信、毒消息终态；
pubsub 广播到达每个在线订阅者、通配订阅、缓冲满降级、Redis 不可达时 `Subscribe` 快速失败、
`Close` 后同一 group+topic 可重订阅且取消登记不累积。

## 相关模块

- 门面契约：[../core/README.md](../core/README.md)
- Stream 队列本体：[stream/queue/README.md](stream/queue/README.md)、[USAGE_EXAMPLE.md](stream/queue/USAGE_EXAMPLE.md)（文档描述的是组件全量能力，当前启用范围见上文[本体启用范围](#本体启用范围)）
- Kafka 适配器：[../kafka/README.md](../kafka/README.md)
- pipeline 桥接：[../pipequeue/README.md](../pipequeue/README.md)
- 快速上手：[QUICKSTART.md](QUICKSTART.md)

## 许可证

MIT
