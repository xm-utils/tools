# Redis Stream 组件架构设计文档

## 📋 目录

- [1. 组件概述](#1-组件概述)
- [2. 架构设计](#2-架构设计)
- [3. 核心组件](#3-核心组件)
- [4. 装饰器模式](#4-装饰器模式)
- [5. 使用指南](#5-使用指南)
- [6. 最佳实践](#6-最佳实践)
- [7. 监控与告警](#7-监控与告警)
- [8. 故障排查](#8-故障排查)

---

## 1. 组件概述

### 1.1 设计理念

Redis Stream 组件是基于 **装饰器模式** 和 **构建器模式** 设计的消息队列解决方案，提供以下核心能力：

- ✅ **高可靠性**: ACK 确认机制 + Pending 消息检查
- ✅ **幂等性保证**: 基于 Redis 的分布式幂等控制
- ✅ **自动重试**: 可配置的重试策略
- ✅ **死信队列**: 失败消息持久化到数据库
- ✅ **协程池优化**: 基于 ants 的高性能异步处理
- ✅ **可插拔架构**: 灵活组合装饰器功能
- ✅ **监控告警**: 实时监控关键指标

### 1.2 技术栈

| 技术 | 用途 |
|------|------|
| Redis Stream | 消息队列底层存储 |
| ants | Go 协程池管理 |
| xm-utils/retry | 重试策略执行器 |
| xm-utils/deadletter | 死信队列持久化 |
| GORM | 死信队列数据库存储 |

### 1.3 适用场景

- 订单状态变更通知
- 支付回调处理
- 商户余额更新
- 风险控制事件
- 异步任务调度

---

## 2. 架构设计

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────┐
│                      Producer (生产者)                        │
│                     EnqueueMessage()                         │
└──────────────────────┬──────────────────────────────────────┘
                       │ XADD
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                   Redis Stream                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Stream Key: stream:callback:order                   │   │
│  │  Consumer Group: callback-group                      │   │
│  │  Max Length: 1,000,000                               │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────┬──────────────────────────────────────┘
                       │ XREADGROUP
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    Consumer (消费者)                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  consumeLoop() - 消息拉取循环                         │   │
│  │  ↓                                                    │   │
│  │  EnhancedHandler (装饰器链)                           │   │
│  │    ├─ DeadLetterDecorator (死信队列)                  │   │
│  │    ├─ RetryDecorator (重试)                           │   │
│  │    ├─ IdempotentDecorator (幂等性)                    │   │
│  │    └─ Business Handler (业务逻辑)                     │   │
│  │  ↓                                                    │   │
│  │  Callback (可选回调)                                  │   │
│  │  ↓                                                    │   │
│  │  XACK (消息确认)                                      │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  PendingChecker - Pending 消息检查器                  │   │
│  │  - 每 60 秒检查超时消息                               │   │
│  │  - XCLAIM 认领超时消息                                │   │
│  │  - 重新处理                                           │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  业务处理结果                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Success     │  │  Retry       │  │  Dead Letter │      │
│  │  (XACK)      │  │  (重试)      │  │  (DB存储)    │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 数据流

```
1. 生产者发布消息
   ↓
2. Redis Stream 存储 (XADD)
   ↓
3. 消费者组拉取消息 (XREADGROUP)
   ↓
4. 进入装饰器链处理
   ├─ 幂等性检查 (跳过已处理消息)
   ├─ 重试包装 (失败时自动重试)
   └─ 死信队列 (最终失败移入死信)
   ↓
5. 业务处理器执行
   ↓
6. 同步回调通知 (如果配置)
   ↓
7. 消息确认 (XACK + XDEL)
   ↓
8. Pending 检查器监控 (XCLAIM 超时消息)
```

---

## 3. 核心组件

### 3.1 StreamQueue - 队列管理器

**职责**: 管理 Redis Stream 的基础操作

**位置**: `internal/redis_stream/stream_queue.go`

**核心方法**:

```go
// 创建队列管理器
config := DefaultStreamConfig("stream:callback:order", "callback-group")
queue := NewStreamQueue(config)

// 初始化 Stream 和消费者组
err := queue.InitStream(ctx)

// 发布消息
msg := &StreamMessage{
    RequestID: "req-123456",
    Payload:   `{"orderId":"ORD001","status":"paid"}`,
}
messageID, err := queue.EnqueueMessage(ctx, msg)

// 读取消息 (由 Consumer 内部调用)
messages, err := queue.ReadMessages(ctx, "consumer-1")

// 确认消息 (由 Consumer 内部调用)
err := queue.AckMessage(ctx, messageID)
```

**配置参数**:

| 参数 | 默认值 | 说明 |
|------|--------|------|
| StreamKey | - | Stream 键名 |
| ConsumerGroup | - | 消费者组名称 |
| ReadBlockMs | 5s | 阻塞读取超时时间 |
| ReadBatchCount | 100 | 每次拉取的消息数量 |
| StreamMaxLen | 1,000,000 | Stream 最大长度 |

### 3.2 Consumer - 消费者

**职责**: 负责消息拉取、协程池管理、Pending 检查

**位置**: `internal/redis_stream/consumer.go`

**核心特性**:

1. **协程池支持**: 基于 ants 实现高性能异步处理
2. **Pending 检查**: 定时检查并认领超时消息
3. **回调机制**: 支持同步回调通知应用层
4. **优雅停止**: 支持上下文取消和资源释放

**使用方法**:

```go
// 创建消费者
handler := func(ctx context.Context, msg *StreamMessage) (interface{}, error) {
    // 业务逻辑
    log.Infof("处理消息: %s", msg.Payload)
    return nil, nil
}

consumer := NewConsumer(queue, "consumer-1", handler).
    WithPool(10).  // 协程池大小
    WithCallback(func(ctx context.Context, msg *StreamMessage, result *MessageResult) {
        // 回调通知
        log.Infof("消息处理完成: success=%v", result.Success)
    })

// 启动消费者
consumer.Start()

// 停止消费者 (优雅关闭)
defer consumer.Stop()
```

**协程池配置**:

```go
// 推荐配置
poolSize := runtime.NumCPU() * 2  // CPU 核心数 * 2

// 自定义配置
consumer.WithPool(20)  // 固定 20 个协程
```

### 3.3 MessageProcessor - 消息处理器接口

**职责**: 定义消息处理的标准接口

**位置**: `internal/redis_stream/message_processor.go`

**接口定义**:

```go
type MessageProcessor interface {
    // HasCallback 判断是否配置了回调函数
    HasCallback() bool
    
    // Handler 消息处理函数（必需）
    Handler(ctx context.Context, msg *StreamMessage) (interface{}, error)
    
    // Callback 消息回调函数（可选）
    Callback(ctx context.Context, msg *StreamMessage, result *MessageResult)
}
```

**实现示例**:

```go
type OrderCallbackProcessor struct{}

func (p *OrderCallbackProcessor) HasCallback() bool {
    return true
}

func (p *OrderCallbackProcessor) Handler(ctx context.Context, msg *StreamMessage) (interface{}, error) {
    var orderData map[string]interface{}
    json.Unmarshal([]byte(msg.Payload), &orderData)
    
    // 业务逻辑
    log.Infof("处理订单回调: orderId=%s", orderData["orderId"])
    
    return map[string]string{"status": "processed"}, nil
}

func (p *OrderCallbackProcessor) Callback(ctx context.Context, msg *StreamMessage, result *MessageResult) {
    if result.Success {
        log.Infof("订单处理成功: %s", msg.RequestID)
    } else {
        log.Errorf("订单处理失败: %s, error=%s", msg.RequestID, result.Error)
    }
}
```

### 3.4 MessageResult - 消息处理结果

**职责**: 统一的消息处理返回格式

**位置**: `internal/redis_stream/message_result.go`

**结构定义**:

```go
type MessageResult struct {
    Success bool        `json:"success"` // 是否成功
    Data    interface{} `json:"data"`    // 返回数据
    Error   string      `json:"error"`   // 错误信息
}
```

**便捷函数**:

```go
// 创建成功结果
result := NewSuccessResult(map[string]string{"status": "ok"})

// 创建错误结果
result := NewErrorResult(errors.New("处理失败"))
result := NewErrorResultWithMsg("参数无效")

// 检查结果
if result.IsSuccess() {
    data := result.GetData()
} else {
    errMsg := result.GetError()
}
```

### 3.5 PendingChecker - Pending 消息检查器

**职责**: 定时检查并认领超时未确认的消息

**位置**: `internal/redis_stream/pending_checker.go`

**工作机制**:

1. 每 60 秒检查一次 Pending 消息
2. 发现超时消息 (>5 分钟) 后通过 XCLAIM 认领
3. 重新提交给消费者处理

**配置常量**:

```go
const PendingCheckInterval = 60 * time.Second  // 检查间隔
```

**注意**: PendingChecker 作为 Consumer 的内部组件自动启动，无需手动配置。

---

## 4. 装饰器模式

### 4.1 装饰器架构

组件采用**责任链模式**实现装饰器，支持灵活组合：

```
请求流程 (从外到内):
DeadLetter → Retry → Idempotent → Business Handler

执行流程 (从内到外):
Business Handler → Idempotent → Retry → DeadLetter
```

**装饰器接口**:

```go
type HandlerDecorator interface {
    Decorate(handler MessageHandler) MessageHandler
}
```

### 4.2 IdempotentDecorator - 幂等性装饰器

**职责**: 防止消息重复处理

**位置**: `internal/redis_stream/decorator_idempotent.go`

**工作原理**:

1. 基于 RequestID 生成 Redis Key
2. 处理前检查 Key 是否存在
3. 处理成功后设置 Key (带过期时间)

**配置示例**:

```go
// 在 ConsumerBuilder 中配置
builder.WithIdempotent("idempotent:callback", 24*time.Hour)
```

**Redis Key 格式**:

```
{idempotentPrefix}:{requestID}
例如: idempotent:callback:req-123456
```

**注意事项**:

- ✅ RequestID 必须唯一且稳定
- ✅ 过期时间应大于消息处理超时时间
- ⚠️ 幂等检查仅对成功消息生效

### 4.3 RetryDecorator - 重试装饰器

**职责**: 失败消息自动重试

**位置**: `internal/redis_stream/decorator_retry.go`

**依赖**: `github.com/xm-utils/tools/retry`

**配置示例**:

```go
import "github.com/xm-utils/tools/retry"

// 自定义重试配置
retryConfig := &retry.Config{
    MaxRetries:    3,           // 最大重试次数
    InitialDelay:  1 * time.Second,  // 初始延迟
    MaxDelay:      30 * time.Second, // 最大延迟
    Multiplier:    2.0,         // 延迟倍数 (指数退避)
}

builder.WithRetry(retryConfig)

// 或使用默认配置
builder.WithRetry(retry.DefaultRetryConfig())
```

**默认配置**:

```go
DefaultRetryConfig() {
    MaxRetries:   3,
    InitialDelay: 1s,
    MaxDelay:     30s,
    Multiplier:   2.0,
}
```

**重试策略**:

```
第1次失败 → 等待 1s  → 第2次尝试
第2次失败 → 等待 2s  → 第3次尝试
第3次失败 → 等待 4s  → 第4次尝试
第4次失败 → 进入死信队列
```

### 4.4 DeadLetterDecorator - 死信队列装饰器

**职责**: 将最终失败的消息持久化到数据库

**位置**: `internal/redis_stream/decorator_deadletter.go`

**依赖**: 
- `github.com/xm-utils/tools/deadletter`
- GORM 数据库连接

**工作流程**:

1. 捕获所有处理失败的异常
2. 序列化消息体
3. 存入 `dead_letter_queue` 表
4. 记录失败原因和重试次数

**配置示例**:

```go
builder.WithDeadLetter("callback:deadletter")
```

**数据库表结构**:

```sql
CREATE TABLE dead_letter_queue (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    message_id VARCHAR(100) NOT NULL,
    payload TEXT NOT NULL,
    reason VARCHAR(500),
    retry_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL,
    status VARCHAR(20) DEFAULT 'pending'
);
```

**死信消息处理**:

```go
// 查询死信消息
var deadLetters []DeadLetterQueue
db.Where("status = ?", "pending").Find(&deadLetters)

// 重新处理
for _, dl := range deadLetters {
    var msg StreamMessage
    json.Unmarshal([]byte(dl.Payload), &msg)
    // 重新入队或手动处理
}
```

### 4.5 DecoratorChain - 装饰器链构建器

**职责**: 管理和组装装饰器链

**位置**: `internal/redis_stream/decorator.go`

**使用方式**:

```go
// 手动构建装饰器链
chain := NewDecoratorChain()

// 添加装饰器 (从外到内)
chain.Add(NewDeadLetterDecorator(ctx, manager, "dead:letter"))
chain.Add(NewRetryDecorator(ctx, manager, retryConfig))
chain.Add(NewIdempotentDecorator(ctx, "idempotent", 24*time.Hour))

// 构建增强处理器
enhancedHandler := chain.Build(baseHandler)
```

**注意**: 装饰器添加顺序与执行顺序相反（后添加的先执行）。

---

## 5. 使用指南

### 5.1 快速开始

#### 方式一: 使用 ConsumerBuilder (推荐)

```go
package main

import (
    "context"
    "time"
    "gitlab.novgate.com/xm/pay/internal/redis_stream"
    "github.com/xm-utils/tools/retry"
)

func main() {
    ctx := context.Background()
    
    // 1. 创建队列管理器
    config := redis_stream.DefaultStreamConfig("stream:callback:order", "callback-group")
    queue := redis_stream.NewStreamQueue(config)
    
    // 2. 初始化 Stream
    if err := queue.InitStream(ctx); err != nil {
        panic(err)
    }
    
    // 3. 定义消息处理器
    processor := &OrderCallbackProcessor{}
    
    // 4. 构建消费者 (链式调用)
    consumer := redis_stream.NewConsumerBuilder(queue, "consumer-1", processor).
        WithIdempotent("idempotent:callback", 24*time.Hour).
        WithRetry(retry.DefaultRetryConfig()).
        WithDeadLetter("callback:deadletter").
        WithPool(10).
        Build()
    
    // 5. 启动消费者
    consumer.Start()
    
    // 6. 优雅关闭
    defer consumer.Stop()
    
    // 保持运行
    select {}
}
```

#### 方式二: 手动组装装饰器

```go
// 1. 创建基础消费者
baseHandler := func(ctx context.Context, msg *StreamMessage) (interface{}, error) {
    // 业务逻辑
    return nil, nil
}

// 2. 创建装饰器
idempotent := NewIdempotentDecorator(ctx, "idempotent", 24*time.Hour)
retry := NewRetryDecorator(ctx, queue, retryConfig)
deadLetter := NewDeadLetterDecorator(ctx, queue, "dead:letter")

// 3. 构建装饰器链
chain := NewDecoratorChain()
chain.Add(deadLetter)
chain.Add(retry)
chain.Add(idempotent)

enhancedHandler := chain.Build(baseHandler)

// 4. 创建消费者
consumer := NewConsumer(queue, "consumer-1", enhancedHandler).
    WithPool(10).
    WithCallback(callback)

consumer.Start()
```

#### 方式三: 便捷函数

```go
// 运行完整功能消费者 (包含所有装饰器)
processor := &OrderCallbackProcessor{}
consumer := redis_stream.RunFullConsumer(queue, "consumer-1", processor)

// 或运行自定义配置消费者
consumer := redis_stream.RunConsumer(queue, "consumer-1", processor)
```

### 5.2 生产者示例

```go
func PublishOrderCallback(orderId string, status string) error {
    ctx := context.Background()
    
    // 1. 创建队列
    config := redis_stream.DefaultStreamConfig("stream:callback:order", "callback-group")
    queue := redis_stream.NewStreamQueue(config)
    
    // 2. 构建消息
    msg := &redis_stream.StreamMessage{
        RequestID: fmt.Sprintf("ORDER:%s:%d", orderId, time.Now().UnixMilli()),
        Payload:   fmt.Sprintf(`{"orderId":"%s","status":"%s"}`, orderId, status),
    }
    
    // 3. 发布消息
    messageID, err := queue.EnqueueMessage(ctx, msg)
    if err != nil {
        log.Errorf("消息发布失败: %v", err)
        return err
    }
    
    log.Infof("消息发布成功: messageId=%s", messageID)
    return nil
}
```

### 5.3 多消费者并行消费

```go
// 启动多个消费者实例
for i := 1; i <= 3; i++ {
    consumerName := fmt.Sprintf("consumer-%d", i)
    
    consumer := redis_stream.NewConsumerBuilder(queue, consumerName, processor).
        WithIdempotent("idempotent:callback", 24*time.Hour).
        WithRetry(retry.DefaultRetryConfig()).
        WithDeadLetter("callback:deadletter").
        WithPool(10).
        Build()
    
    consumer.Start()
    defer consumer.Stop()
}
```

**负载均衡**: Redis Stream 消费者组会自动在多个消费者之间分配消息，确保每条消息只被一个消费者处理。

### 5.4 批量消息发布

```go
func BatchPublishOrders(orders []Order) error {
    ctx := context.Background()
    config := redis_stream.DefaultStreamConfig("stream:callback:order", "callback-group")
    queue := redis_stream.NewStreamQueue(config)
    
    for _, order := range orders {
        msg := &redis_stream.StreamMessage{
            RequestID: fmt.Sprintf("ORDER:%s", order.ID),
            Payload:   order.ToJSON(),
        }
        
        _, err := queue.EnqueueMessage(ctx, msg)
        if err != nil {
            log.Errorf("批量发布失败: orderId=%s, err=%v", order.ID, err)
            continue
        }
    }
    
    return nil
}
```

---

## 6. 最佳实践

### 6.1 幂等性设计

**原则**: 每个消息必须有唯一的 RequestID

```go
// ✅ 正确: 使用业务唯一标识
msg.RequestID = fmt.Sprintf("ORDER:%s:%d", orderId, timestamp)

// ❌ 错误: 使用随机 ID
msg.RequestID = uuid.New().String()  // 无法保证幂等
```

**RequestID 生成策略**:

| 场景 | 推荐格式 |
|------|----------|
| 订单回调 | `ORDER:{orderId}:{timestamp}` |
| 支付通知 | `PAY:{transactionId}:{timestamp}` |
| 商户余额更新 | `BALANCE:{merchantId}:{changeId}` |

### 6.2 重试策略配置

**推荐配置**:

```go
// 重要业务 (如支付)
retryConfig := &retry.Config{
    MaxRetries:   5,           // 更多重试次数
    InitialDelay: 2 * time.Second,
    MaxDelay:     60 * time.Second,
    Multiplier:   2.0,
}

// 普通业务 (如通知)
retryConfig := &retry.Config{
    MaxRetries:   3,
    InitialDelay: 1 * time.Second,
    MaxDelay:     30 * time.Second,
    Multiplier:   2.0,
}

// 实时性要求高的业务
retryConfig := &retry.Config{
    MaxRetries:   2,           // 减少重试次数
    InitialDelay: 500 * time.Millisecond,
    MaxDelay:     5 * time.Second,
    Multiplier:   2.0,
}
```

### 6.3 协程池调优

**计算公式**:

```
poolSize = CPU核心数 × 2 ~ 4

考虑因素:
- CPU 密集型: poolSize = CPU核心数 × 1~2
- IO 密集型: poolSize = CPU核心数 × 3~4
- 混合型:   poolSize = CPU核心数 × 2
```

**监控协程池**:

```go
// 获取运行中的协程数量
runningWorkers := consumer.GetRunningWorkers()
log.Infof("当前工作协程数: %d", runningWorkers)

// 获取配置的池大小
poolSize := consumer.GetPoolSize()
log.Infof("协程池大小: %d", poolSize)
```

### 6.4 消息体设计

**StreamMessage 字段说明**:

```go
type StreamMessage struct {
    MessageID        string `json:"messageId"`   // Redis 自动生成，无需手动设置
    RequestID        string `json:"requestId"`   // ⭐ 必填，用于幂等性
    Payload          string `json:"payload"`     // ⭐ 必填，业务数据 (JSON)
    EnqueueTime      int64  `json:"enqueueTime"` // 自动填充
    RetryCount       int    `json:"retryCount"`  // 自动填充
    Status           string `json:"status"`      // 自动填充
}
```

**Payload 最佳实践**:

```json
{
  "orderId": "ORD20260706001",
  "merchantId": "M10001",
  "amount": 100.50,
  "status": "paid",
  "timestamp": 1720252800000,
  "metadata": {
    "channel": "alipay",
    "currency": "CNY"
  }
}
```

### 6.5 错误处理

**业务处理器错误返回**:

```go
func (p *OrderCallbackProcessor) Handler(ctx context.Context, msg *StreamMessage) (interface{}, error) {
    var orderData OrderData
    if err := json.Unmarshal([]byte(msg.Payload), &orderData); err != nil {
        // ❌ 参数错误，不应重试
        return nil, fmt.Errorf("参数解析失败: %w", err)
    }
    
    if err := p.validateOrder(orderData); err != nil {
        // ❌ 业务校验失败，不应重试
        return nil, fmt.Errorf("订单校验失败: %w", err)
    }
    
    if err := p.processOrder(orderData); err != nil {
        // ✅ 系统错误，应该重试
        return nil, fmt.Errorf("订单处理失败: %w", err)
    }
    
    return map[string]string{"status": "success"}, nil
}
```

**区分错误类型**:

| 错误类型 | 是否重试 | 示例 |
|---------|---------|------|
| 参数错误 | ❌ | JSON 解析失败、字段缺失 |
| 业务校验失败 | ❌ | 余额不足、订单不存在 |
| 系统临时错误 | ✅ | 数据库连接超时、网络抖动 |
| 第三方服务错误 | ✅ | 支付网关超时、API 限流 |

### 6.6 资源清理

**优雅关闭**:

```go
func main() {
    consumer := NewConsumerBuilder(queue, "consumer-1", processor).Build()
    consumer.Start()
    
    // 监听系统信号
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    
    <-sigChan
    log.Info("收到退出信号，开始优雅关闭...")
    
    // 停止消费者 (等待正在处理的消息完成)
    consumer.Stop()
    log.Info("消费者已停止")
}
```

---

## 7. 监控与告警

### 7.1 监控指标

**内置指标**:

| 指标名称 | Key 格式 | 说明 |
|---------|----------|------|
| Stream 长度 | `metric:stream:length:{streamKey}` | 当前积压消息数 |
| 消费延迟 | `metric:consumer:lag:{streamKey}:{group}` | Pending 消息数 |
| 死信增长 | `metric:dead_letter:growth:{streamKey}` | 1小时死信增长数 |
| 回调成功 | `metric:callback:success:{streamKey}` | 回调成功计数 |
| 回调失败 | `metric:callback:failure:{streamKey}` | 回调失败计数 |

**获取监控数据**:

```go
monitor := redis_stream.NewMetricsMonitor(nil)
metrics := monitor.GetMetrics(ctx, "stream:callback:order")

log.Infof("Stream 长度: %v", metrics["stream_length"])
log.Infof("回调成功率: %.2f%%", metrics["callback_success_rate"])
log.Infof("死信队列长度: %v", metrics["dead_letter_length"])
```

### 7.2 告警阈值

**默认告警配置**:

```go
DefaultAlertConfig = &AlertConfig{
    StreamLengthThreshold: 10000,    // Stream 长度 > 1万
    ConsumerLagThreshold:  1000,     // Pending > 1000
    DeadLetterGrowthRate:  100,      // 1小时死信增长 > 100
    CallbackFailureRate:   0.1,      // 回调失败率 > 10%
    AlertCheckInterval:    1 * time.Minute,  // 每分钟检查
    AlertCooldown:         10 * 60,  // 告警冷却 10 分钟
}
```

**自定义告警配置**:

```go
customConfig := &redis_stream.AlertConfig{
    StreamLengthThreshold: 5000,
    ConsumerLagThreshold:  500,
    DeadLetterGrowthRate:  50,
    CallbackFailureRate:   0.05,
    AlertCheckInterval:    30 * time.Second,
    AlertCooldown:         5 * 60,
}

monitor := redis_stream.NewMetricsMonitor(customConfig)
monitor.StartMonitoring(ctx, []string{"stream:callback:order"})
```

### 7.3 告警集成

**当前实现**: 告警仅记录日志

**TODO 扩展**: 集成实际告警渠道

```go
// 选项1: 邮件告警
func sendEmailAlert(message string) {
    // 集成 SMTP 发送邮件
}

// 选项2: 钉钉 Webhook
func sendDingTalkAlert(message string) {
    webhook := "https://oapi.dingtalk.com/robot/send?access_token=xxx"
    payload := map[string]interface{}{
        "msgtype": "text",
        "text": map[string]string{
            "content": message,
        },
    }
    http.Post(webhook, "application/json", toJSON(payload))
}

// 选项3: Prometheus + Grafana
func recordPrometheusMetric(name string, value float64) {
    // 集成 Prometheus Client
}
```

---

## 8. 故障排查

### 8.1 常见问题

#### 问题1: 消息重复消费

**现象**: 同一条消息被多次处理

**原因**:
1. RequestID 不唯一
2. 幂等性未启用
3. 幂等 Key 过期时间过短

**解决方案**:

```go
// ✅ 确保 RequestID 唯一
msg.RequestID = fmt.Sprintf("ORDER:%s:%d", orderId, time.Now().UnixMilli())

// ✅ 启用幂等性
builder.WithIdempotent("idempotent:callback", 24*time.Hour)

// ✅ 延长过期时间 (如果消息可能延迟处理)
builder.WithIdempotent("idempotent:callback", 72*time.Hour)
```

#### 问题2: 消息丢失

**现象**: 消息发布后未被消费

**排查步骤**:

```bash
# 1. 检查 Stream 是否存在
redis-cli XINFO STREAM stream:callback:order

# 2. 检查消费者组
redis-cli XINFO GROUPS stream:callback:order

# 3. 检查 Pending 消息
redis-cli XPENDING stream:callback:order callback-group

# 4. 查看 Stream 长度
redis-cli XLEN stream:callback:order
```

**常见原因**:
1. 消费者未启动
2. 消费者组名称错误
3. 消费者崩溃后未重启

**解决方案**:

```go
// 确保 InitStream 成功
if err := queue.InitStream(ctx); err != nil {
    log.Fatalf("Stream 初始化失败: %v", err)
}

// 确保消费者持续运行
consumer.Start()
defer consumer.Stop()
select {}  // 阻塞主协程
```

#### 问题3: 消费延迟过高

**现象**: Pending 消息数量持续增长

**原因**:
1. 消费者处理能力不足
2. 业务逻辑耗时过长
3. 消费者崩溃

**解决方案**:

```go
// 方案1: 增加消费者实例
for i := 1; i <= 5; i++ {
    consumer := NewConsumerBuilder(queue, fmt.Sprintf("consumer-%d", i), processor).Build()
    consumer.Start()
}

// 方案2: 增大协程池
consumer.WithPool(20)

// 方案3: 优化业务逻辑 (异步化处理)
func (p *Processor) Handler(ctx context.Context, msg *StreamMessage) (interface{}, error) {
    // 快速返回，异步处理耗时操作
    go p.asyncProcess(msg)
    return nil, nil
}
```

#### 问题4: 死信队列堆积

**现象**: `dead_letter_queue` 表数据快速增长

**原因**:
1. 业务逻辑存在 Bug
2. 第三方服务持续不可用
3. 重试次数不足

**解决方案**:

```go
// 1. 分析死信原因
var deadLetters []DeadLetterQueue
db.Where("created_at > ?", time.Now().Add(-24*time.Hour)).
   Group("reason").
   Select("reason, COUNT(*) as count").
   Find(&deadLetters)

// 2. 修复 Bug 后重新处理
for _, dl := range deadLetters {
    var msg StreamMessage
    json.Unmarshal([]byte(dl.Payload), &msg)
    
    // 重新入队
    queue.EnqueueMessage(ctx, &msg)
    
    // 标记为已处理
    db.Model(&dl).Update("status", "reprocessed")
}

// 3. 调整重试策略
retryConfig := &retry.Config{
    MaxRetries: 5,  // 增加重试次数
}
```

#### 问题5: 协程池耗尽

**现象**: 日志中出现 "提交消息到协程池失败"

**原因**: 并发消息数超过协程池容量

**解决方案**:

```go
// 方案1: 增大协程池
consumer.WithPool(50)

// 方案2: 监控协程池使用情况
ticker := time.NewTicker(10 * time.Second)
go func() {
    for range ticker.C {
        running := consumer.GetRunningWorkers()
        poolSize := consumer.GetPoolSize()
        log.Infof("协程池使用率: %d/%d (%.2f%%)", 
            running, poolSize, float64(running)/float64(poolSize)*100)
    }
}()

// 方案3: 降级处理 (代码已内置)
// 当协程池满时，自动降级为同步处理
```

### 8.2 调试技巧

#### 启用详细日志

```go
// 设置日志级别为 DEBUG
logger.SetLevel(logrus.DebugLevel)
```

#### 查看 Redis Stream 状态

```bash
# 查看 Stream 信息
redis-cli XINFO STREAM stream:callback:order

# 查看消费者组详情
redis-cli XINFO GROUPS stream:callback:order

# 查看消费者详情
redis-cli XINFO CONSUMERS stream:callback:order callback-group

# 查看 Pending 消息
redis-cli XPENDING stream:callback:order callback-group - + 100

# 查看最近的消息
redis-cli XRANGE stream:callback:order - + COUNT 10
```

#### 模拟消息发布

```bash
# 手动添加测试消息
redis-cli XADD stream:callback:order * data '{"requestId":"test-001","payload":"{\"orderId\":\"ORD001\"}"}'

# 查看消息是否被消费
redis-cli XPENDING stream:callback:order callback-group
```

### 8.3 性能优化建议

1. **批量操作**: 尽量批量发布消息，减少网络往返
2. **索引优化**: 为 `dead_letter_queue` 表添加索引
3. **Redis 集群**: 高吞吐场景使用 Redis Cluster
4. **消息压缩**: Payload 较大时使用 gzip 压缩
5. **异步回调**: 回调逻辑尽量异步化，避免阻塞主流程

---

## 附录

### A. 配置常量汇总

```go
const (
    StreamMaxLen           = 1000000        // Stream 最大长度
    DeadLetterKey          = "stream:dead_letter"
    PendingCheckInterval   = 60 * time.Second
    ReadBatchCount         = 100
    ReadBlockMs            = 5 * time.Second
)
```

### B. 错误码说明

| 错误信息 | 含义 | 解决方案 |
|---------|------|---------|
| BUSYGROUP | 消费者组已存在 | 正常，忽略即可 |
| NOGROUP | 消费者组不存在 | 检查 InitStream 是否成功 |
| NOENTRIES | 没有新消息 | 正常，继续等待 |
| ERR syntax | 命令语法错误 | 检查 Redis 版本 (需要 >= 5.0) |

### C. 相关文档

- [Redis Stream 官方文档](https://redis.io/docs/data-types/streams/)
- [ants 协程池文档](https://github.com/panjf2000/ants)
- [xm-utils/retry 文档](https://gitlab.novgate.com/xm/utils/tools/retry)
- [xm-utils/deadletter 文档](https://gitlab.novgate.com/xm/utils/tools/deadletter)

### D. 版本历史

| 版本 | 日期 | 变更说明 |
|------|------|---------|
| v2.0 | 2026-07-06 | 重构为装饰器模式，支持可插拔架构 |
| v1.0 | 2026-06-01 | 初始版本，单体消费者实现 |

---

**文档维护**: 如有疑问或建议，请联系开发团队。

**最后更新**: 2026-07-06
