# Kafka 客户端工具库

[![Go Version](https://img.shields.io/badge/go-1.24.2+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Kafka SDK](https://img.shields.io/badge/kafka--go-v0.4.51-blue.svg)](https://github.com/segmentio/kafka-go)

一个基于 [segmentio/kafka-go](https://github.com/segmentio/kafka-go) 封装的高性能 Go 语言 Kafka 客户端工具库，提供简洁易用的消息生产和消费功能，支持单主题/多主题模式、消息去重、批量处理等高级特性。

## 📋 目录

- [项目简介](#项目简介)
- [主要特性](#主要特性)
- [安装](#安装)
- [快速开始](#快速开始)
  - [基础初始化](#基础初始化)
  - [YAML 配置加载](#yaml-配置加载)
- [核心功能](#核心功能)
  - [消息生产](#消息生产)
  - [消息消费](#消息消费)
  - [单主题模式](#单主题模式)
  - [多主题模式](#多主题模式)
- [高级功能](#高级功能)
  - [消息去重](#消息去重)
  - [内存去重存储](#内存去重存储)
  - [Redis 去重存储](#redis-去重存储)
  - [自定义去重处理器](#自定义去重处理器)
- [配置说明](#配置说明)
  - [Config 配置结构](#config-配置结构)
  - [配置参数详解](#配置参数详解)
- [使用示例](#使用示例)
  - [基础生产者示例](#基础生产者示例)
  - [基础消费者示例](#基础消费者示例)
  - [多主题消费者示例](#多主题消费者示例)
  - [带去重的消费者示例](#带去重的消费者示例)
  - [完整的微服务示例](#完整的微服务示例)
- [API 参考](#api-参考)
  - [生产者 API](#生产者-api)
  - [消费者 API](#消费者-api)
  - [去重 API](#去重-api)
  - [数据结构](#数据结构)
- [最佳实践](#最佳实践)
  - [生产环境配置建议](#生产环境配置建议)
  - [消息可靠性保证](#消息可靠性保证)
  - [性能优化建议](#性能优化建议)
  - [去重策略选择](#去重策略选择)
- [常见问题](#常见问题)
- [依赖](#依赖)
- [许可证](#许可证)

## 📖 项目简介

本项目是对 segmentio/kafka-go 的二次封装，旨在简化 Kafka 在 Go 微服务项目中的使用。提供了以下核心功能：

- **消息生产**：支持单条消息发布、批量消息发布、自动重试
- **消息消费**：支持单主题/多主题订阅、异步消息处理、优雅退出
- **消息去重**：支持基于内存或 Redis 的消息去重，防止重复处理
- **灵活配置**：支持丰富的配置选项，满足不同场景需求

## ✨ 主要特性

- ✅ **简洁的 API**：封装复杂的 kafka-go SDK，提供简单易用的接口
- ✅ **单/多主题支持**：灵活支持单主题和多主题消费模式
- ✅ **消息去重**：内置消息去重机制，支持内存和 Redis 两种存储方式
- ✅ **批量处理**：支持批量消息发送，提高吞吐量
- ✅ **自动重试**：内置重试机制，提高消息投递成功率
- ✅ **异步处理**：消费者采用异步处理方式，提高并发能力
- ✅ **优雅退出**：支持 context 控制的优雅关闭
- ✅ **日志集成**：集成 logrus，提供详细的运行日志
- ✅ **默认值优化**：合理的默认配置，开箱即用

## 🚀 安装

```bash
go get github.com/xm-utils/tools/kafka
```

### 依赖要求

- Go 1.24.2+
- github.com/segmentio/kafka-go v0.4.51+
- github.com/sirupsen/logrus v1.9.4+
- github.com/go-redis/redis/v8 v8.11.5+（可选，用于 Redis 去重）

### 前置条件

使用前请确保已部署并运行 Kafka 集群。推荐使用 Kafka 2.x 或更高版本。

## 🎯 快速开始

### 基础初始化

#### 生产者初始化

```go
package main

import (
    "context"
    "fmt"
    "log"
    "github.com/xm-utils/tools/kafka"
)

func main() {
    // 创建配置
    config := &kafka.Config{
        Brokers: []string{"localhost:9092"},
        Topic:   "my-topic",
    }

    // 初始化生产者
    if err := kafka.InitProducer(config); err != nil {
        log.Fatalf("Failed to init producer: %v", err)
    }

    // 获取生产者实例
    producer := kafka.GetProducer()
    defer producer.Close()

    // 发布消息
    ctx := context.Background()
    err := producer.Publish(ctx, "my-topic", "key1", []byte("Hello Kafka!"))
    if err != nil {
        log.Printf("Failed to publish message: %v", err)
    } else {
        fmt.Println("Message published successfully")
    }
}
```

#### 消费者初始化

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "github.com/xm-utils/tools/kafka"
    "github.com/segmentio/kafka-go"
)

func main() {
    // 创建配置
    config := &kafka.Config{
        Brokers: []string{"localhost:9092"},
        Topic:   "my-topic",
        GroupID: "my-consumer-group",
    }

    // 初始化消费者
    if err := kafka.InitConsumer(config); err != nil {
        log.Fatalf("Failed to init consumer: %v", err)
    }

    // 获取消费者实例
    consumer := kafka.GetConsumer()
    defer consumer.Close()

    // 定义消息处理器
    handler := func(ctx context.Context, topic string, msg kafka.Message) error {
        log.Printf("Received message: topic=%s, key=%s, value=%s", 
            topic, string(msg.Key), string(msg.Value))
        return nil
    }

    // 启动消费
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // 监听信号，优雅退出
    go func() {
        sigChan := make(chan os.Signal, 1)
        signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
        <-sigChan
        cancel()
    }()

    log.Println("Starting consumer...")
    if err := consumer.Subscribe(ctx, handler); err != nil {
        log.Printf("Consumer stopped: %v", err)
    }
}
```

### YAML 配置加载

1. 创建配置文件 `kafka.yaml`：

```yaml
brokers:
  - "localhost:9092"
  - "localhost:9093"
  - "localhost:9094"

# 单主题模式
topic: "my-topic"

# 多主题模式（与 topic 二选一）
# topics:
#   - "topic-1"
#   - "topic-2"
#   - "topic-3"

group_id: "my-consumer-group"
client_id: "my-app"

# 生产者和消费者通用配置
max_attempts: 10
dial_timeout: 10s
read_timeout: 10s
write_timeout: 10s

# 生产者配置
batch_size: 1000
batch_bytes: 1048576  # 1MB

# 消费者配置
min_bytes: 1
max_bytes: 1048576  # 1MB
queue_capacity: 1000
```

2. 在代码中加载配置（需配合配置加载库如 viper）：

```go
package main

import (
    "log"
    "github.com/spf13/viper"
    "github.com/xm-utils/tools/kafka"
)

func main() {
    // 使用 viper 加载 YAML 配置
    viper.SetConfigFile("kafka.yaml")
    if err := viper.ReadInConfig(); err != nil {
        log.Fatalf("Failed to read config: %v", err)
    }

    // 解析为 kafka.Config 结构
    var config kafka.Config
    if err := viper.Unmarshal(&config); err != nil {
        log.Fatalf("Failed to unmarshal config: %v", err)
    }

    // 初始化生产者和消费者
    if err := kafka.InitProducer(&config); err != nil {
        log.Fatalf("Failed to init producer: %v", err)
    }

    if err := kafka.InitConsumer(&config); err != nil {
        log.Fatalf("Failed to init consumer: %v", err)
    }

    log.Println("Kafka client initialized successfully")
}
```

## 🔧 核心功能

### 消息生产

#### 单条消息发布

```go
// 使用默认生产者
ctx := context.Background()
err := kafka.Publish(ctx, "my-topic", "order-123", []byte(`{"order_id":123}`))
if err != nil {
    log.Printf("Publish failed: %v", err)
}
```

#### 使用生产者实例发布

```go
producer := kafka.GetProducer()

// 发布 JSON 消息
message := map[string]interface{}{
    "user_id":   1001,
    "action":    "login",
    "timestamp": time.Now().Unix(),
}

jsonData, _ := json.Marshal(message)
err := producer.Publish(ctx, "user-events", "user-1001", jsonData)
if err != nil {
    log.Printf("Publish failed: %v", err)
}
```

#### 批量消息发布

```go
producer := kafka.GetProducer()

messages := []kafka.Message{
    {
        Topic: "orders",
        Key:   []byte("order-1"),
        Value: []byte(`{"order_id":1,"amount":100}`),
        Time:  time.Now(),
    },
    {
        Topic: "orders",
        Key:   []byte("order-2"),
        Value: []byte(`{"order_id":2,"amount":200}`),
        Time:  time.Now(),
    },
    {
        Topic: "orders",
        Key:   []byte("order-3"),
        Value: []byte(`{"order_id":3,"amount":300}`),
        Time:  time.Now(),
    },
}

err := producer.PublishBatch(ctx, messages)
if err != nil {
    log.Printf("Batch publish failed: %v", err)
} else {
    fmt.Printf("Successfully published %d messages\n", len(messages))
}
```

### 消息消费

#### 基础消息消费

```go
consumer := kafka.GetConsumer()

handler := func(ctx context.Context, topic string, msg kafka.Message) error {
    log.Printf("Received: topic=%s, key=%s, value=%s", 
        topic, string(msg.Key), string(msg.Value))
    
    // 处理业务逻辑
    // ...
    
    return nil
}

ctx := context.Background()
if err := consumer.Subscribe(ctx, handler); err != nil {
    log.Printf("Subscribe failed: %v", err)
}
```

#### 异步消息处理

消费者内部已经实现了异步处理，每个消息会在独立的 goroutine 中处理：

```go
handler := func(ctx context.Context, topic string, msg kafka.Message) error {
    // 模拟耗时处理
    time.Sleep(100 * time.Millisecond)
    
    // 处理业务逻辑
    processOrder(msg.Value)
    
    return nil
}
```

### 单主题模式

单主题模式适用于只关注单一业务场景的消费者：

```go
config := &kafka.Config{
    Brokers: []string{"localhost:9092"},
    Topic:   "user-events",  // 单个主题
    GroupID: "user-event-processor",
}

if err := kafka.InitConsumer(config); err != nil {
    log.Fatal(err)
}

handler := func(ctx context.Context, topic string, msg kafka.Message) error {
    // 只需要处理 user-events 主题的消息
    log.Printf("User event: %s", string(msg.Value))
    return nil
}

kafka.Subscribe(ctx, handler)
```

### 多主题模式

多主题模式适用于需要同时监听多个相关主题的场景：

```go
config := &kafka.Config{
    Brokers: []string{"localhost:9092"},
    Topics:  []string{
        "order-created",
        "order-updated",
        "order-cancelled",
    },
    GroupID: "order-processor",
}

if err := kafka.InitConsumer(config); err != nil {
    log.Fatal(err)
}

// 根据主题进行不同的处理逻辑
handler := func(ctx context.Context, topic string, msg kafka.Message) error {
    switch topic {
    case "order-created":
        log.Printf("New order: %s", string(msg.Value))
        handleNewOrder(msg.Value)
        
    case "order-updated":
        log.Printf("Order updated: %s", string(msg.Value))
        handleOrderUpdate(msg.Value)
        
    case "order-cancelled":
        log.Printf("Order cancelled: %s", string(msg.Value))
        handleOrderCancel(msg.Value)
        
    default:
        log.Printf("Unknown topic: %s", topic)
    }
    
    return nil
}

kafka.Subscribe(ctx, handler)
```

## 🚀 高级功能

### 消息去重

在高并发或网络重试场景下，可能会出现消息重复。本库提供了完善的消息去重机制。

#### 去重原理

1. **唯一标识生成**：优先使用消息 Key，否则使用 `topic:partition:offset`
2. **去重检查**：在处理消息前检查是否已处理过
3. **标记已处理**：处理成功后标记消息，设置过期时间
4. **容错设计**：如果去重检查失败，继续处理消息（宁可重复也不丢失）

### 内存去重存储

适用于单机应用或测试环境：

```go
package main

import (
    "context"
    "log"
    "time"
    "github.com/xm-utils/tools/kafka"
    "github.com/segmentio/kafka-go"
)

func main() {
    // 初始化消费者
    config := &kafka.Config{
        Brokers: []string{"localhost:9092"},
        Topic:   "orders",
        GroupID: "order-processor",
    }
    kafka.InitConsumer(config)

    // 创建内存去重存储
    memoryStore := kafka.NewMemoryDeduplicationStore()
    defer memoryStore.Close()

    // 创建去重器（TTL 24小时）
    deduplicator := kafka.NewMessageDeduplicator(memoryStore, 24*time.Hour)

    // 原始消息处理器
    orderHandler := func(ctx context.Context, topic string, msg kafka.Message) error {
        log.Printf("Processing order: %s", string(msg.Value))
        // 处理订单逻辑
        return nil
    }

    // 包装处理器，添加去重功能
    deduplicatedHandler := deduplicator.WrapHandlerWithDeduplication(orderHandler)

    // 启动消费（自动去重）
    ctx := context.Background()
    kafka.Subscribe(ctx, deduplicatedHandler)
}
```

**内存去重特点：**
- ✅ 速度快，无网络开销
- ✅ 无需额外依赖
- ❌ 重启后去重记录丢失
- ❌ 不支持分布式环境
- ❌ 占用应用内存

**监控去重记录数：**

```go
ticker := time.NewTicker(1 * time.Minute)
defer ticker.Stop()

for range ticker.C {
    count := memoryStore.GetRecordCount()
    log.Printf("Current deduplication records: %d", count)
}
```

### Redis 去重存储

适用于分布式环境或多实例部署：

```go
package main

import (
    "context"
    "log"
    "time"
    "github.com/xm-utils/tools/kafka"
    "github.com/segmentio/kafka-go"
    "github.com/go-redis/redis/v8"
)

func main() {
    // 初始化 Redis 客户端
    redisClient := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "",
        DB:       0,
    })
    defer redisClient.Close()

    // 初始化消费者
    config := &kafka.Config{
        Brokers: []string{"localhost:9092"},
        Topic:   "payments",
        GroupID: "payment-processor",
    }
    kafka.InitConsumer(config)

    // 创建 Redis 去重存储
    redisStore := kafka.NewRedisDeduplicationStore(redisClient)
    defer redisStore.Close()

    // 创建去重器（TTL 48小时）
    deduplicator := kafka.NewMessageDeduplicator(redisStore, 48*time.Hour)

    // 原始消息处理器
    paymentHandler := func(ctx context.Context, topic string, msg kafka.Message) error {
        log.Printf("Processing payment: %s", string(msg.Value))
        // 处理支付逻辑
        return nil
    }

    // 包装处理器，添加去重功能
    deduplicatedHandler := deduplicator.WrapHandlerWithDeduplication(paymentHandler)

    // 启动消费（自动去重）
    ctx := context.Background()
    kafka.Subscribe(ctx, deduplicatedHandler)
}
```

**Redis 去重特点：**
- ✅ 支持分布式环境
- ✅ 持久化存储，重启不丢失
- ✅ 多实例共享去重状态
- ❌ 需要 Redis 依赖
- ❌ 有网络开销

**Redis Key 格式：**
```
kafka:duplicate:{topic}:{key}
kafka:duplicate:orders:order-123
kafka:duplicate:payments:payment-456
```

### 自定义去重处理器

如果需要更精细的控制，可以手动实现去重逻辑：

```go
// 自定义去重处理器
customHandler := func(ctx context.Context, topic string, msg kafka.Message) error {
    deduplicator := getDeduplicator() // 获取去重器
    
    // 检查是否重复
    isDup, err := deduplicator.IsDuplicate(msg)
    if err != nil {
        log.Printf("Check duplicate failed: %v", err)
        // 继续处理，避免消息丢失
    } else if isDup {
        log.Printf("Skip duplicate message: key=%s", string(msg.Key))
        return nil
    }
    
    // 处理消息
    if err := processMessage(msg); err != nil {
        return err
    }
    
    // 标记为已处理
    if err := deduplicator.MarkProcessed(msg); err != nil {
        log.Printf("Mark processed failed: %v", err)
        // 不影响业务逻辑
    }
    
    return nil
}

kafka.Subscribe(ctx, customHandler)
```

## ⚙️ 配置说明

### Config 配置结构

```go
type Config struct {
    Brokers       []string      // Kafka broker地址列表
    Topic         string        // 默认主题（单主题模式）
    Topics        []string      // 多主题列表（多主题模式）
    GroupID       string        // 消费者组ID
    ClientID      string        // 客户端ID
    MaxAttempts   int           // 最大重试次数
    DialTimeout   time.Duration // 连接超时时间
    ReadTimeout   time.Duration // 读取超时时间
    WriteTimeout  time.Duration // 写入超时时间
    BatchSize     int           // 批量大小
    BatchBytes    int64         // 批量字节数
    MinBytes      int           // 最小字节数
    MaxBytes      int           // 最大字节数
    QueueCapacity int           // 队列容量
}
```

### 配置参数详解

#### 基础配置

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| Brokers | []string | 是 | - | Kafka broker 地址列表，如 `["localhost:9092"]` |
| Topic | string | 否* | - | 单主题模式下的主题名称 |
| Topics | []string | 否* | - | 多主题模式下的主题列表 |
| GroupID | string | 消费时必填 | - | 消费者组 ID，用于负载均衡和 offset 管理 |
| ClientID | string | 否 | "" | 客户端 ID，用于标识应用 |

**注意**：Topic 和 Topics 至少需要配置一个。

#### 重试和超时配置

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| MaxAttempts | int | 10 | 最大重试次数（生产者） |
| DialTimeout | time.Duration | 10s | 连接超时时间 |
| ReadTimeout | time.Duration | 10s | 读取超时时间 |
| WriteTimeout | time.Duration | 10s | 写入超时时间 |

#### 生产者配置

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| BatchSize | int | 1000 | 批量发送的消息数量 |
| BatchBytes | int64 | 1MB (1048576) | 批量发送的最大字节数 |

**批量配置说明：**
- `BatchSize`: 达到此数量后立即发送
- `BatchBytes`: 达到此字节数后立即发送
- `BatchTimeout`: 固定为 10ms，超时后也会发送

#### 消费者配置

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| MinBytes | int | 1 | 每次读取的最小字节数 |
| MaxBytes | int | 1MB (1048576) | 每次读取的最大字节数 |
| QueueCapacity | int | 1000 | 队列容量（预留配置） |

**消费者内部配置（不可配置）：**
- `MaxWait`: 1s - 最大等待时间
- `HeartbeatInterval`: 3s - 心跳间隔
- `SessionTimeout`: 30s - 会话超时
- `RebalanceTimeout`: 30s - 重平衡超时
- `StartOffset`: FirstOffset - 从最早的消息开始消费
- `ReadBackoffMin`: 100ms - 最小退避时间
- `ReadBackoffMax`: 1s - 最大退避时间

## 💡 使用示例

### 基础生产者示例

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"
    "github.com/xm-utils/tools/kafka"
)

// Order 订单结构
type Order struct {
    OrderID   int64   `json:"order_id"`
    UserID    int64   `json:"user_id"`
    Amount    float64 `json:"amount"`
    CreatedAt int64   `json:"created_at"`
}

func main() {
    // 初始化生产者
    config := &kafka.Config{
        Brokers: []string{"localhost:9092"},
    }

    if err := kafka.InitProducer(config); err != nil {
        log.Fatalf("Failed to init producer: %v", err)
    }
    defer kafka.GetProducer().Close()

    log.Println("✓ Producer initialized")

    // 发布订单消息
    ctx := context.Background()
    
    for i := 1; i <= 10; i++ {
        order := Order{
            OrderID:   int64(i),
            UserID:    1000 + int64(i),
            Amount:    float64(i * 100),
            CreatedAt: time.Now().Unix(),
        }

        jsonData, _ := json.Marshal(order)
        key := fmt.Sprintf("order-%d", order.OrderID)

        if err := kafka.Publish(ctx, "orders", key, jsonData); err != nil {
            log.Printf("Failed to publish order %d: %v", order.OrderID, err)
        } else {
            fmt.Printf("✓ Published order %d\n", order.OrderID)
        }

        time.Sleep(100 * time.Millisecond)
    }
}
```

### 基础消费者示例

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "os"
    "os/signal"
    "syscall"
    "github.com/xm-utils/tools/kafka"
    "github.com/segmentio/kafka-go"
)

// Order 订单结构
type Order struct {
    OrderID   int64   `json:"order_id"`
    UserID    int64   `json:"user_id"`
    Amount    float64 `json:"amount"`
    CreatedAt int64   `json:"created_at"`
}

func main() {
    // 初始化消费者
    config := &kafka.Config{
        Brokers: []string{"localhost:9092"},
        Topic:   "orders",
        GroupID: "order-processor",
    }

    if err := kafka.InitConsumer(config); err != nil {
        log.Fatalf("Failed to init consumer: %v", err)
    }
    defer kafka.GetConsumer().Close()

    log.Println("✓ Consumer initialized")

    // 定义消息处理器
    handler := func(ctx context.Context, topic string, msg kafka.Message) error {
        var order Order
        if err := json.Unmarshal(msg.Value, &order); err != nil {
            return fmt.Errorf("unmarshal order failed: %w", err)
        }

        log.Printf("Received order: ID=%d, User=%d, Amount=%.2f", 
            order.OrderID, order.UserID, order.Amount)

        // 处理订单业务逻辑
        processOrder(order)

        return nil
    }

    // 创建可取消的 context
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // 监听信号，优雅退出
    go func() {
        sigChan := make(chan os.Signal, 1)
        signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
        sig := <-sigChan
        log.Printf("Received signal: %v, shutting down...", sig)
        cancel()
    }()

    // 启动消费
    log.Println("Starting to consume messages...")
    if err := kafka.Subscribe(ctx, handler); err != nil {
        if err == context.Canceled {
            log.Println("Consumer stopped gracefully")
        } else {
            log.Printf("Consumer error: %v", err)
        }
    }
}

func processOrder(order Order) {
    // 订单处理逻辑
    log.Printf("Processing order %d...", order.OrderID)
}
```

### 多主题消费者示例

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "github.com/xm-utils/tools/kafka"
    "github.com/segmentio/kafka-go"
)

// 不同主题的消息结构
type OrderEvent struct {
    OrderID int64  `json:"order_id"`
    Status  string `json:"status"`
}

type PaymentEvent struct {
    PaymentID int64   `json:"payment_id"`
    OrderID   int64   `json:"order_id"`
    Amount    float64 `json:"amount"`
}

type ShippingEvent struct {
    ShippingID int64  `json:"shipping_id"`
    OrderID    int64  `json:"order_id"`
    Location   string `json:"location"`
}

func main() {
    // 多主题配置
    config := &kafka.Config{
        Brokers: []string{"localhost:9092"},
        Topics: []string{
            "order-events",
            "payment-events",
            "shipping-events",
        },
        GroupID: "ecommerce-processor",
    }

    if err := kafka.InitConsumer(config); err != nil {
        log.Fatalf("Failed to init consumer: %v", err)
    }
    defer kafka.GetConsumer().Close()

    // 统一消息处理器，根据主题分发
    handler := func(ctx context.Context, topic string, msg kafka.Message) error {
        switch topic {
        case "order-events":
            return handleOrderEvent(msg)
            
        case "payment-events":
            return handlePaymentEvent(msg)
            
        case "shipping-events":
            return handleShippingEvent(msg)
            
        default:
            log.Printf("Unknown topic: %s", topic)
            return nil
        }
    }

    ctx := context.Background()
    log.Println("Starting multi-topic consumer...")
    kafka.Subscribe(ctx, handler)
}

func handleOrderEvent(msg kafka.Message) error {
    var event OrderEvent
    if err := json.Unmarshal(msg.Value, &event); err != nil {
        return err
    }
    
    log.Printf("Order event: ID=%d, Status=%s", event.OrderID, event.Status)
    // 处理订单事件
    return nil
}

func handlePaymentEvent(msg kafka.Message) error {
    var event PaymentEvent
    if err := json.Unmarshal(msg.Value, &event); err != nil {
        return err
    }
    
    log.Printf("Payment event: ID=%d, Order=%d, Amount=%.2f", 
        event.PaymentID, event.OrderID, event.Amount)
    // 处理支付事件
    return nil
}

func handleShippingEvent(msg kafka.Message) error {
    var event ShippingEvent
    if err := json.Unmarshal(msg.Value, &event); err != nil {
        return err
    }
    
    log.Printf("Shipping event: ID=%d, Order=%d, Location=%s", 
        event.ShippingID, event.OrderID, event.Location)
    // 处理物流事件
    return nil
}
```

### 带去重的消费者示例

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "time"
    "github.com/xm-utils/tools/kafka"
    "github.com/segmentio/kafka-go"
    "github.com/go-redis/redis/v8"
)

// Payment 支付消息
type Payment struct {
    PaymentID int64   `json:"payment_id"`
    OrderID   int64   `json:"order_id"`
    Amount    float64 `json:"amount"`
    Status    string  `json:"status"`
}

func main() {
    // 初始化 Redis
    redisClient := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })
    defer redisClient.Close()

    // 初始化消费者
    config := &kafka.Config{
        Brokers: []string{"localhost:9092"},
        Topic:   "payments",
        GroupID: "payment-processor",
    }

    if err := kafka.InitConsumer(config); err != nil {
        log.Fatalf("Failed to init consumer: %v", err)
    }
    defer kafka.GetConsumer().Close()

    // 创建 Redis 去重存储（TTL 48小时）
    redisStore := kafka.NewRedisDeduplicationStore(redisClient)
    defer redisStore.Close()

    deduplicator := kafka.NewMessageDeduplicator(redisStore, 48*time.Hour)

    // 原始支付处理器
    paymentHandler := func(ctx context.Context, topic string, msg kafka.Message) error {
        var payment Payment
        if err := json.Unmarshal(msg.Value, &payment); err != nil {
            return err
        }

        log.Printf("Processing payment: ID=%d, Order=%d, Amount=%.2f", 
            payment.PaymentID, payment.OrderID, payment.Amount)

        // 模拟处理延迟
        time.Sleep(100 * time.Millisecond)

        // 处理支付业务逻辑
        return processPayment(payment)
    }

    // 包装处理器，添加去重功能
    deduplicatedHandler := deduplicator.WrapHandlerWithDeduplication(paymentHandler)

    // 启动消费
    ctx := context.Background()
    log.Println("Starting consumer with deduplication...")
    kafka.Subscribe(ctx, deduplicatedHandler)
}

func processPayment(payment Payment) error {
    // 支付处理逻辑
    log.Printf("Payment processed successfully: ID=%d", payment.PaymentID)
    return nil
}
```

### 完整的微服务示例

一个完整的订单处理微服务，包含生产者和消费者：

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"
    "github.com/xm-utils/tools/kafka"
    "github.com/segmentio/kafka-go"
    "github.com/go-redis/redis/v8"
)

// OrderService 订单服务
type OrderService struct {
    deduplicator *kafka.MessageDeduplicator
}

// NewOrderService 创建订单服务
func NewOrderService() *OrderService {
    // 初始化 Redis 去重
    redisClient := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })
    
    redisStore := kafka.NewRedisDeduplicationStore(redisClient)
    deduplicator := kafka.NewMessageDeduplicator(redisStore, 24*time.Hour)
    
    return &OrderService{
        deduplicator: deduplicator,
    }
}

// CreateOrder 创建订单并发布到 Kafka
func (s *OrderService) CreateOrder(w http.ResponseWriter, r *http.Request) {
    var order map[string]interface{}
    if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    order["created_at"] = time.Now().Unix()
    jsonData, _ := json.Marshal(order)
    
    orderID := fmt.Sprintf("%v", order["order_id"])
    ctx := context.Background()
    
    if err := kafka.Publish(ctx, "orders", orderID, jsonData); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{
        "status": "success",
        "message": "Order created",
    })
}

// StartConsumer 启动订单消费者
func (s *OrderService) StartConsumer(ctx context.Context) {
    handler := func(ctx context.Context, topic string, msg kafka.Message) error {
        log.Printf("Processing order: %s", string(msg.Value))
        
        // 这里可以添加业务逻辑
        // 例如：更新数据库、发送通知等
        
        return nil
    }

    // 使用去重处理器
    deduplicatedHandler := s.deduplicator.WrapHandlerWithDeduplication(handler)
    
    log.Println("Starting order consumer...")
    kafka.Subscribe(ctx, deduplicatedHandler)
}

func main() {
    // Kafka 配置
    kafkaConfig := &kafka.Config{
        Brokers: []string{"localhost:9092"},
        Topic:   "orders",
        GroupID: "order-service",
    }

    // 初始化生产者和消费者
    if err := kafka.InitProducer(kafkaConfig); err != nil {
        log.Fatalf("Failed to init producer: %v", err)
    }
    defer kafka.GetProducer().Close()

    if err := kafka.InitConsumer(kafkaConfig); err != nil {
        log.Fatalf("Failed to init consumer: %v", err)
    }
    defer kafka.GetConsumer().Close()

    log.Println("✓ Kafka initialized")

    // 创建订单服务
    service := NewOrderService()

    // 启动 HTTP 服务器
    http.HandleFunc("/api/orders", service.CreateOrder)
    
    server := &http.Server{Addr: ":8080"}
    go func() {
        log.Println("✓ HTTP server starting on :8080")
        if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Fatalf("HTTP server error: %v", err)
        }
    }()

    // 启动消费者
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    go service.StartConsumer(ctx)

    // 优雅退出
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan

    log.Println("Shutting down...")
    
    // 关闭 HTTP 服务器
    shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer shutdownCancel()
    server.Shutdown(shutdownCtx)
    
    // 取消消费者 context
    cancel()
    
    log.Println("Service stopped")
}
```

## 📚 API 参考

### 生产者 API

#### 初始化和获取

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `InitProducer(config *Config) error` | 初始化生产者 | error: 初始化错误 |
| `GetProducer() *Producer` | 获取默认生产者实例 | producer: 生产者实例 |

#### 消息发布

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `Publish(ctx context.Context, topic string, key string, value []byte) error` | 发布单条消息（包级别） | error: 发布错误 |
| `(p *Producer) Publish(ctx context.Context, topic string, key string, value []byte) error` | 发布单条消息（实例方法） | error: 发布错误 |
| `(p *Producer) PublishBatch(ctx context.Context, messages []kafka.Message) error` | 批量发布消息 | error: 发布错误 |

#### 生命周期管理

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `(p *Producer) Close()` | 关闭生产者 | - |

### 消费者 API

#### 初始化和获取

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `InitConsumer(config *Config) error` | 初始化消费者 | error: 初始化错误 |
| `GetConsumer() *Consumer` | 获取默认消费者实例 | consumer: 消费者实例 |

#### 消息订阅

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `Subscribe(ctx context.Context, handler TopicHandler) error` | 订阅消息（包级别） | error: 订阅错误 |
| `(c *Consumer) Subscribe(ctx context.Context, handler TopicHandler) error` | 订阅消息（实例方法） | error: 订阅错误 |
| `SubscribeWithTopicHandler(ctx context.Context, handler TopicHandler) error` | 订阅消息并支持主题分发（包级别） | error: 订阅错误 |
| `(c *Consumer) SubscribeWithTopicHandler(ctx context.Context, handler TopicHandler) error` | 订阅消息并支持主题分发（实例方法） | error: 订阅错误 |

#### 生命周期管理

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `(c *Consumer) Close()` | 关闭消费者 | - |

### 去重 API

#### DeduplicationStore 接口

```go
type DeduplicationStore interface {
    IsDuplicate(key string) (bool, error)
    MarkProcessed(key string, ttl time.Duration) error
    Close() error
}
```

#### MessageDeduplicator

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `NewMessageDeduplicator(store DeduplicationStore, ttl time.Duration) *MessageDeduplicator` | 创建消息去重器 | deduplicator: 去重器实例 |
| `(d *MessageDeduplicator) GenerateKey(msg kafka.Message) string` | 生成去重 key | key: 唯一标识 |
| `(d *MessageDeduplicator) IsDuplicate(msg kafka.Message) (bool, error)` | 检查消息是否重复 | isDuplicate: 是否重复, error: 错误 |
| `(d *MessageDeduplicator) MarkProcessed(msg kafka.Message) error` | 标记消息已处理 | error: 错误 |
| `(d *MessageDeduplicator) WrapHandlerWithDeduplication(handler TopicHandler) TopicHandler` | 包装处理器添加去重功能 | handler: 包装后的处理器 |

#### MemoryDeduplicationStore

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `NewMemoryDeduplicationStore() *MemoryDeduplicationStore` | 创建内存去重存储 | store: 存储实例 |
| `(s *MemoryDeduplicationStore) GetRecordCount() int` | 获取当前记录数 | count: 记录数量 |

#### RedisDeduplicationStore

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `NewRedisDeduplicationStore(client *redis.Client) *RedisDeduplicationStore` | 创建 Redis 去重存储 | store: 存储实例 |

### 数据结构

#### Config

Kafka 配置结构：

```go
type Config struct {
    Brokers       []string      // Kafka broker地址列表
    Topic         string        // 默认主题（单主题模式）
    Topics        []string      // 多主题列表（多主题模式）
    GroupID       string        // 消费者组ID
    ClientID      string        // 客户端ID
    MaxAttempts   int           // 最大重试次数
    DialTimeout   time.Duration // 连接超时时间
    ReadTimeout   time.Duration // 读取超时时间
    WriteTimeout  time.Duration // 写入超时时间
    BatchSize     int           // 批量大小
    BatchBytes    int64         // 批量字节数
    MinBytes      int           // 最小字节数
    MaxBytes      int           // 最大字节数
    QueueCapacity int           // 队列容量
}
```

#### TopicHandler

消息处理函数类型：

```go
type TopicHandler func(ctx context.Context, topic string, msg kafka.Message) error
```

**参数说明：**
- `ctx`: 上下文，用于控制取消
- `topic`: 消息所属的主题
- `msg`: Kafka 消息对象

**返回值：**
- `error`: 处理错误，返回非 nil 表示处理失败

#### kafka.Message

Kafka 消息结构（来自 segmentio/kafka-go）：

```go
type Message struct {
    Topic     string            // 主题
    Partition int               // 分区
    Offset    int64             // 偏移量
    Key       []byte            // 消息键
    Value     []byte            // 消息值
    Headers   []Header          // 消息头
    Time      time.Time         // 时间戳
}
```

## 📖 最佳实践

### 生产环境配置建议

#### 1. Broker 配置

```go
config := &kafka.Config{
    Brokers: []string{
        "kafka-1.example.com:9092",
        "kafka-2.example.com:9092",
        "kafka-3.example.com:9092",
    },
}
```

**建议：**
- 至少配置 3 个 broker 以实现高可用
- 使用域名而非 IP，便于运维切换
- 确保网络连通性和低延迟

#### 2. 重试和超时配置

```go
config := &kafka.Config{
    MaxAttempts:  10,           // 适当增加重试次数
    DialTimeout:  10 * time.Second,
    ReadTimeout:  10 * time.Second,
    WriteTimeout: 10 * time.Second,
}
```

**建议：**
- 生产环境适当增加超时时间
- 根据网络状况调整重试次数
- 监控重试率，过高时需排查问题

#### 3. 批量配置优化

```go
config := &kafka.Config{
    BatchSize:  1000,    // 根据消息大小调整
    BatchBytes: 1048576, // 1MB
}
```

**建议：**
- 小消息可以增加 BatchSize
- 大消息需要减小 BatchSize
- 监控批量发送频率和大小

#### 4. 消费者组管理

```go
config := &kafka.Config{
    GroupID: "production-order-processor-v1",
}
```

**建议：**
- 使用有意义的 GroupID，包含环境和版本信息
- 不同功能的消费者使用不同的 GroupID
- 升级时考虑使用新的 GroupID 实现蓝绿部署

### 消息可靠性保证

#### 1. 生产者可靠性

```go
// kafka-go 默认配置已经保证高可靠性：
// - RequiredAcks: kafka.RequireAll（等待所有副本确认）
// - Async: false（同步发送）
// - Completion: 回调记录失败
```

**建议：**
- 不要修改为异步模式（Async=true）
- 保持 RequireAll 配置
- 实现重试逻辑处理临时失败

#### 2. 消费者可靠性

```go
handler := func(ctx context.Context, topic string, msg kafka.Message) error {
    // 1. 先处理业务逻辑
    if err := processBusinessLogic(msg); err != nil {
        return err // 返回错误，不提交 offset
    }
    
    // 2. 再标记去重（如果使用）
    if err := deduplicator.MarkProcessed(msg); err != nil {
        log.Printf("Mark processed failed: %v", err)
        // 不影响 offset 提交
    }
    
    return nil // 返回 nil，提交 offset
}
```

**建议：**
- 业务处理失败时返回 error
- 幂等处理，允许消息重复消费
- 使用去重机制防止重复处理

#### 3. 消息持久化

```yaml
# Kafka 服务端配置建议
log.retention.hours: 168  # 保留 7 天
log.replication.factor: 3  # 3 副本
min.insync.replicas: 2     # 最少 2 个副本确认
```

### 性能优化建议

#### 1. 生产者性能

```go
// 批量发送优于单条发送
messages := make([]kafka.Message, 0, 100)
for _, data := range dataList {
    messages = append(messages, kafka.Message{
        Topic: "my-topic",
        Key:   []byte(data.Key),
        Value: data.Value,
    })
}
producer.PublishBatch(ctx, messages)
```

**优化点：**
- 使用批量发送减少网络往返
- 预分配切片容量
- 合理设置 BatchSize 和 BatchBytes

#### 2. 消费者性能

```go
// 异步处理提高并发度
handler := func(ctx context.Context, topic string, msg kafka.Message) error {
    go func(m kafka.Message) {
        // 异步处理
        processMessage(m)
    }(msg)
    return nil
}
```

**注意：** 消费者内部已经实现了异步处理，无需再次 goroutine

**优化点：**
- 控制并发度，避免资源耗尽
- 使用连接池访问外部资源
- 监控消费延迟

#### 3. 资源配置

```go
config := &kafka.Config{
    MinBytes: 1,
    MaxBytes: 1048576, // 根据消息大小调整
}
```

**建议：**
- MaxBytes 设置为预期消息大小的倍数
- 监控内存使用情况
- 根据吞吐量调整批处理参数

### 去重策略选择

#### 场景对比

| 场景 | 推荐方案 | 原因 |
|------|---------|------|
| 单机应用 | 内存去重 | 简单高效 |
| 多实例部署 | Redis 去重 | 共享状态 |
| 测试环境 | 内存去重 | 无需额外依赖 |
| 金融交易 | Redis 去重 | 数据可靠 |
| 日志收集 | 不需要去重 | 允许重复 |
| 订单处理 | Redis 去重 | 业务敏感 |

#### TTL 设置建议

```go
// 短时效场景（实时性要求高）
deduplicator := kafka.NewMessageDeduplicator(store, 1*time.Hour)

// 中等时效场景（一般业务）
deduplicator := kafka.NewMessageDeduplicator(store, 24*time.Hour)

// 长时效场景（对账、审计）
deduplicator := kafka.NewMessageDeduplicator(store, 7*24*time.Hour)
```

**建议：**
- 根据业务重试窗口设置 TTL
- 考虑 Kafka 消息保留时间
- 平衡去重效果和存储成本

## ❓ 常见问题

### Q1: 如何选择单主题还是多主题模式？

**单主题模式**适用于：
- 只关注单一业务领域
- 简单的消息处理逻辑
- 独立的消费者组

**多主题模式**适用于：
- 需要关联多个业务事件
- 统一的消息处理流程
- 事件溯源场景

### Q2: 消息重复消费怎么办？

1. **业务层幂等**：确保多次处理结果一致
2. **使用去重机制**：本库提供的去重功能
3. **数据库唯一约束**：利用数据库防止重复插入

```go
// 幂等处理示例
func processOrder(order Order) error {
    // 使用唯一键插入，如果已存在则忽略
    _, err := db.Exec(
        "INSERT INTO orders (order_id, ...) VALUES (?, ...) ON DUPLICATE KEY UPDATE ...",
        order.OrderID, ...,
    )
    return err
}
```

### Q3: 如何保证消息顺序性？

Kafka 只能保证**分区内有序**：

```go
// 相同 key 的消息会发送到同一分区
producer.Publish(ctx, "orders", "user-123", order1Data)
producer.Publish(ctx, "orders", "user-123", order2Data)
// 这两个消息会按顺序被消费
```

**建议：**
- 需要顺序的消息使用相同的 key
- 避免跨分区的顺序依赖
- 消费者单线程处理同一分区

### Q4: 消费者 lag 过大如何处理？

1. **增加消费者实例**：横向扩展
2. **优化处理逻辑**：减少单条消息处理时间
3. **增加分区数**：提高并行度
4. **批量处理**：减少 IO 次数

```go
// 监控 lag
ticker := time.NewTicker(1 * time.Minute)
for range ticker.C {
    stats := consumer.reader.Stats()
    log.Printf("Consumer lag: %d", stats.Lag)
}
```

### Q5: 如何处理大消息？

```go
config := &kafka.Config{
    MaxBytes: 10485760, // 增加到 10MB
}
```

**建议：**
- Kafka 不适合传输超大消息
- 大文件存储到 OSS，消息中传 URL
- 考虑使用专门的文件传输服务

### Q6: 如何优雅关闭消费者？

```go
ctx, cancel := context.WithCancel(context.Background())

// 监听信号
go func() {
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan
    cancel() // 取消 context
}()

// 阻塞等待
if err := consumer.Subscribe(ctx, handler); err != nil {
    if err == context.Canceled {
        log.Println("Graceful shutdown")
    }
}
```

### Q7: 内存去重会占用多少内存？

假设：
- 每条记录约 100 bytes
- 100万条未过期记录

内存占用 ≈ 100 MB

**监控方法：**
```go
count := memoryStore.GetRecordCount()
log.Printf("Records: %d, Estimated memory: %.2f MB", count, float64(count)*100/1024/1024)
```

### Q8: Redis 去重失败会影响业务吗？

不会。去重失败只会记录日志，不会阻断消息处理：

```go
isDup, err := d.IsDuplicate(msg)
if err != nil {
    log.Warnf("检查消息重复失败: %v，继续处理", err)
    // 继续处理，避免消息丢失
}
```

**设计理念：** 宁可重复处理，也不能丢失消息。

## 📦 依赖

```go
require (
    github.com/segmentio/kafka-go v0.4.51
    github.com/sirupsen/logrus v1.9.4
    github.com/go-redis/redis/v8 v8.11.5  // 可选，用于 Redis 去重
)
```

**间接依赖：**
- github.com/klauspost/compress v1.15.9
- github.com/pierrec/lz4/v4 v4.1.15
- golang.org/x/sys v0.13.0

## 📄 许可证

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

### 贡献指南

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

### 开发建议

- 遵循 Go 代码规范
- 添加必要的单元测试
- 更新文档
- 保持 API 向后兼容

## 📧 联系方式

如有问题或建议，请通过以下方式联系：

- 提交 [Issue](../../issues)
- 发送邮件至项目维护者

## 🔗 相关链接

- [Kafka 官方文档](https://kafka.apache.org/documentation/)
- [segmentio/kafka-go](https://github.com/segmentio/kafka-go)
- [Kafka 最佳实践](https://github.com/apache/kafka)

---

**注意**：使用前请确保已部署并运行 Kafka 集群。推荐使用 Kafka 2.x 或更高版本。

**版本信息**：
- 当前版本：v1.0.0
- Kafka SDK 版本：v0.4.51
- Go 版本要求：1.24.2+
