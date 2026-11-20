# mq — 消息队列统一门面（抽象契约）

`github.com/xm-utils/tools/mq/core`

为业务代码提供与具体 MQ 解耦的 **生产**（`mq.Producer`）与 **消费确认**（`mq.Consumer`）抽象。
本模块**只含契约与公共设施**（`Message` / `Handler` / 哨兵错误 / `InvokeHandler`），零第三方依赖，
可单独被 mock 与引用；各平台适配器为 `mq/` 顶层目录下的同级独立模块，按部署目标择一或多数引入：

| 模块 | import path | 提供内容 |
|---|---|---|
| 门面（本模块） | `github.com/xm-utils/tools/mq/core` | Message/Handler/Producer/Consumer/ErrManualAck 等契约（包名仍为 `mq`） |
| Redis 适配器 | `github.com/xm-utils/tools/mq/redis` | list 安全队列 / zset 延迟队列 / stream 消费者组 / pubsub 广播 四形态（含共享 JSON 信封与 `stream/queue` 队列本体） |
| Kafka 适配器 | `github.com/xm-utils/tools/mq/kafka` | 门面适配器根包 + `client` 子包（Kafka 客户端本体：生产者 / 消费者 / Worker Pool / 去重 / 指标） |
| pipeline 桥接 | `github.com/xm-utils/tools/mq/pipequeue` | 把 `mq.Consumer` 桥接为 `pipeline.Queue[*mq.Message]` |

```
业务代码 ──依赖──▶ mq/core（门面，纯 stdlib）
                     ▲                    ▲
      mq/kafka ──────┘                    └────── mq/redis（list / zset / stream / pubsub）
        └── client（Kafka 客户端本体）              └── stream/queue（Redis Stream 可靠队列本体）
                     ▲
                     └── mq/pipequeue（只面向门面，任一后端皆可接入）
```

切换 MQ = 换装配，业务代码零改动；只需 Redis 广播时不会把 `segmentio/kafka-go` 拉进依赖树，
反之亦然。

## 安装

```bash
go get github.com/xm-utils/tools/mq/core              # 门面（业务代码与单测 mock 只需这个）
go get github.com/xm-utils/tools/mq/redis        # Redis 四形态适配器
go get github.com/xm-utils/tools/mq/kafka        # Kafka 适配器
go get github.com/xm-utils/tools/mq/pipequeue    # pipeline 引擎桥接
```

## 包结构

```
mq/                              # 顶层容器目录（自身不是模块，无 go.mod）
├── core/       go.mod  package mq
│   ├── mq.go            # Message / Handler / Producer / Consumer / InvokeHandler / HeaderAttempt
│   └── errors.go        # ErrManualAck / ErrNotSubscribed / ErrConsumerClosed
├── redis/      go.mod  package list / zset / stream / pubsub
│   ├── internal/envelope/          # 队列型三形态共享信封（模块内部）
│   └── stream/queue/  package queue # Redis Stream 可靠队列本体（消费者组/装饰器/Pending 巡检/指标）
├── kafka/      go.mod  package kafka（门面适配器）
│   └── client/         package client # Kafka 客户端本体（生产者/消费者/Worker Pool/去重/指标）
└── pipequeue/  go.mod  package pipequeue
```

包名注意：门面目录为 `core` 但包名仍为 `mq`，import 后写出来还是 `mq.Message`；
若偏好路径末段与包名一致，可自行加别名 `mq "github.com/xm-utils/tools/mq/core"`。

## API 参考（package mq）

```go
// HeaderAttempt 重投次数消息头（全后端同名，单点由本包定义，值 "X-Mq-Attempt"）。
const HeaderAttempt = "X-Mq-Attempt"

type Message struct {
    ID, Key, Topic string
    Body           []byte
    Headers        map[string]string
    Metadata       any          // client.Message / *queue.StreamMessage / *redis.Message
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

// InvokeHandler 适配器消费循环必须经本函数调用 Handler：panic 统一 recover 转 error，
// 不击穿消费协程（与仓库「panic 转 error」规范一致）。
func InvokeHandler(ctx context.Context, handler Handler, msg *Message) error
```

`msg.ID` 为适配器签发的**不透明投递令牌**（kafka / redis-stream 形如 `group|…`，redis-list / redis-zset / redis-pubsub 为领取者+序号形式），仅用于日志追踪与原样回传 `Commit`/`Reject`；业务不要解析其结构或据此做幂等（幂等请用 `Key` / `Headers`）。

## 多后端差异语义（选型矩阵）

队列型四后端能力对比（Redis 形态细节与调参见 [../redis/README.md](../redis/README.md)，Kafka 见 [../kafka/README.md](../kafka/README.md)）：

| 能力 | mq/kafka | mq/redis/stream | mq/redis/list | mq/redis/zset | mq/redis/pubsub |
|---|---|---|---|---|---|
| topic / group 映射 | Kafka topic / 消费组 | Stream key / 消费者组（自动 XGROUP CREATEMKSTREAM） | `mq:list:<topic>` + `:processing`；group 仅登记维度 | `mq:zset:<topic>` + `:processing`；同左 | 频道 `mq:ps:<topic>`；group 仅登记 |
| 成功确认 | `CommitMessages`（分区级 offset 前移） | `XACK`（逐条） | `LREM processing`（逐条） | `ZREM processing`（逐条） | 无确认原语（no-op） |
| 失败重投 | `WithRequeueProducer` 回环 Produce（attempt+1） | 原 Stream XADD 新 entry | Lua 原子「LREM processing + LPUSH ready」 | ZADD 回 ready | 不可重投 |
| 重投额度 | 默认 3，防热循环 | 同左 | 同左 | 同左 | 无（恒 0） |
| 终态失败 | 回调 `WithRejected`；死信由钩子落 `deadletter` | 可选死信 Stream | 可选 `DeadLetterKey` 死信 List | 可选死信集合（score=失败时刻） | 仅 `Rejected` 钩子 + 日志 |
| ErrManualAck 乱序确认 | offset 粒度粗，乱序 Commit 连同此前 offset 视为已提交 | PEL 逐条，无此问题 | 逐条登记；重复 payload 须在 Handler 内完成 Commit | 逐条登记；同内容消息靠持久 ID 保成员唯一 | 不适用（等价投递完成） |
| 在途消息宕机兜底 | 消费组重平衡后按已提交 offset 重投 | PEL 留存，PendingChecker/XCLAIM 巡检接管 | 滞留 processing，`RecoverProcessing` 启动回补（单活动消费者假设） | `VisibilityTimeout` 到期后台自动回收 | 不适用（无在途） |
| 延迟投递 | 不支持 | 不支持 | 不支持 | 原生：score 即可见时间 | 不支持 |
| 多实例分流 | 消费组分摊分区 | 消费者组分摊 entry | 共享同一队列、互斥领取（Lua 原子，不双领） | 同左 | **不分流**：每个在线实例各收到一份 |

体量选型建议：日百万级以下用 Redis 系后端，往上切 Kafka；轻量串行消费用 List，
需要延迟投递或崩溃自动回收用有序集合，需要多实例消费者组用 Stream，
需要「每个实例都收到」（缓存失效 / 配置刷新 / 在线事件）用发布订阅 ——
业务侧只改装配代码。

发布/订阅与其他四者不同构（at-most-once、失败不可重投、离线即丢），
完整边界表见 [../redis/README.md](../redis/README.md#发布订阅适用边界)。

## 消费者生命周期语义（各适配器一致，pubsub 仅无确认环节）

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
| `mq.ErrNotSubscribed` | 对未登记（已确认/未知）消息执行 Commit/Reject（pubsub 不适用，恒返回 nil） |
| `mq.ErrConsumerClosed` | 消费者 Close 后 Subscribe/Commit/Reject |
| `pipequeue.ErrQueueStopped` | 桥接队列停止后 Submit（引擎按「留待外部兜底」处理） |

错误信息带模块前缀（`mq/kafka:`、`mq/redis/list:`、`mq/pipequeue:` 等）便于定位实现归属。

## 依赖与构建说明

- 门面 `mq/core`：零第三方依赖（仅 stdlib），无 go.sum；被三个平台模块引用。
- `mq/redis`：`go-redis/v8` + `logrus` + `stream/queue` 队列本体带来的仓内依赖（`tools/redis`、`tools/deadletter`、`tools/retry`、`tools/database`（GORM）、`ants/v2`）；`miniredis` 为测试依赖，四形态均做真实协议层集成测试（含 Lua 脚本、SUBSCRIBE/PUBLISH、Stream 消费者组命令）。
- `mq/kafka`：`segmentio/kafka-go` + `logrus`（客户端本体与适配器同模块，不再有仓内跨模块依赖）。
- `mq/pipequeue`：`tools/pipeline`（`pipeline/v0.0.1`，编译期契约引用）+ `logrus`。
- 本体与适配器同模块共存：只用门面语义时 import 适配器根包，需要直接操作底层客户端（Worker Pool、去重、指标）时 import `client` / `queue` 子包，两者编译期同步演进。
- go 指令与仓库所有组件统一为 1.24.2；各模块独立 `go.mod`，本地联动由根目录 `go.work` 负责（不提交）。

### 路径迁移说明

原顶层独立组件 `tools/kafka`（已发布 `kafka/v1.0.4`）与 `tools/redis_stream_queue`（已发布
`redis_stream_queue/v1.0.0`）已分别并入 `mq/kafka/client`（包名 `kafka` → `client`）与
`mq/redis/stream/queue`（包名 `redis_stream` → `queue`），旧目录不再存在。

- 旧 import path 在新版本中不可用；历史 tag 仍可从代理解析，存目项业务可继续钉在旧版本上，但新功能与修复只在新路径演进。
- 同模块内不再有跨模块依赖，因此 `client` / `queue` 与适配器根的代码在同一 `go.mod` 下版本锁定，不存在版本漂移。

## 测试

```bash
cd mq/core       && go test -race ./...   # 门面契约 / InvokeHandler panic 防线（4 用例）
cd mq/redis      && go test -race ./...   # 信封 + list / zset / stream / pubsub 四形态 miniredis 集成（31 用例）
cd mq/kafka      && go test -race ./...   # 门面转换 5 用例 + client 客户端本体（Manager / Worker Pool / 指标）23 用例
cd mq/pipequeue  && go test -race ./...   # 桥接交接/背压/停机/持久回灌 + 真实引擎端到端（9 用例）
```

合计 72 个顶层用例（十个包，其中 `stream/queue` 本体无自带测试、由 stream 适配器的集成测试覆盖）；
Redis 系基于 miniredis 真实协议层，无需外部服务，Kafka 本体用例不依赖真实 broker。

## 快速上手

- [../redis/QUICKSTART.md](../redis/QUICKSTART.md)：Redis 四形态装配与互切（含延迟投递、广播通配订阅）
- [../kafka/README.md](../kafka/README.md)：Kafka 生产 / 消费 / 失败重投（客户端本体见 [../kafka/client/README.md](../kafka/client/README.md)）
- [../pipequeue/README.md](../pipequeue/README.md)：对接 pipeline 引擎（步骤链 + 终态钩子回写确认）

## 许可证

MIT
