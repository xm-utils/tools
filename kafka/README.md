# Kafka 客户端工具库

[![Go Version](https://img.shields.io/badge/go-1.24.2+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Kafka SDK](https://img.shields.io/badge/kafka--go-v0.4.51-blue.svg)](https://github.com/segmentio/kafka-go)

一个基于 [segmentio/kafka-go](https://github.com/segmentio/kafka-go) 封装的高性能 Go 语言 Kafka 客户端工具库，提供简洁易用的消息生产和消费功能，支持消费者管理器、单主题/多主题模式、消息去重、批量处理等高级特性。

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
  - [消息确认机制](#消息确认机制)
  - [单主题模式](#单主题模式)
  - [多主题模式](#多主题模式)
- [消费者管理器](#消费者管理器) 
  - [为什么需要消费者管理器](#为什么需要消费者管理器)
  - [快速开始](#快速开始-1)
  - [使用模式](#使用模式)
  - [健康检查与监控](#健康检查与监控)
  - [动态管理消费者](#动态管理消费者)
  - [API 参考](#api-参考-1)
  - [最佳实践](#最佳实践-1)
  - [常见问题](#常见问题-1)
- [高级功能](#高级功能)
  - [消息去重](#消息去重)
  - [内存去重存储](#内存去重存储)
  - [Redis 去重存储](#redis-去重存储)
- [配置说明](#配置说明)
  - [ProducerConfig 生产者配置](#producerconfig-生产者配置)
  - [ConsumerConfig 消费者配置](#consumerconfig-消费者配置)
  - [配置参数详解](#配置参数详解)
- [使用示例](#使用示例)
- [最佳实践](#最佳实践)
- [常见问题](#常见问题)
- [依赖](#依赖)
- [许可证](#许可证)

## 📖 项目简介

本项目是对 segmentio/kafka-go 的二次封装，旨在简化 Kafka 在 Go 微服务项目中的使用。提供了以下核心功能：

- **消息生产**：支持单条消息发布、批量消息发布、自动重试
- **消息消费**：支持单主题/多主题订阅、异步消息处理、优雅退出
- **消费者管理器**：支持多业务消费者实例统一管理，提供更好的隔离性和灵活性 
- **消息确认**：支持手动/自动提交 offset、内置重试机制、指数退避策略
- **消息去重**：支持基于内存或 Redis 的消息去重，防止重复处理
- **灵活配置**：支持丰富的配置选项，满足不同场景需求

## ✨ 主要特性

- ✅ **简洁的 API**：封装复杂的 kafka-go SDK，提供简单易用的接口
- ✅ **消费者管理器**：支持多业务消费者实例统一管理，隔离性更好 
- ✅ **单/多主题支持**：灵活支持单主题和多主题消费模式
- ✅ **消息确认**：支持手动/自动提交 offset，保证消息可靠性
- ✅ **消息重试**：内置指数退避重试机制，自动处理临时故障
- ✅ **消息去重**：内置消息去重机制，支持内存和 Redis 两种存储方式
- ✅ **批量处理**：支持批量消息发送，提高吞吐量
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
    config := &kafka.ProducerConfig{
        CommonConfig: kafka.CommonConfig{
            Brokers: []string{"localhost:9092"},
        },
        Topic: "my-topic",
    }

    // 应用默认值
    defaults := kafka.GetProducerDefaults()
    if config.MaxAttempts == 0 {
        config.MaxAttempts = defaults.MaxAttempts
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

#### 消费者初始化（传统方式）

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
    config := &kafka.ConsumerConfig{
        CommonConfig: kafka.CommonConfig{
            Brokers: []string{"localhost:9092"},
        },
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

# 生产者配置
topic: "my-topic"
max_attempts: 10
batch_size: 1000
batch_bytes: 1048576  # 1MB

# 消费者配置
group_id: "my-consumer-group"
min_bytes: 1
max_bytes: 1048576  # 1MB
```

2. 在代码中加载配置：

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

    // 解析为 kafka.ProducerConfig 结构
    var config kafka.ProducerConfig
    if err := viper.Unmarshal(&config); err != nil {
        log.Fatalf("Failed to unmarshal config: %v", err)
    }

    // 初始化生产者
    if err := kafka.InitProducer(&config); err != nil {
        log.Fatalf("Failed to init producer: %v", err)
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
}

err := producer.PublishBatch(ctx, messages)
if err != nil {
    log.Printf("Batch publish failed: %v", err)
}
```

### 消息消费

#### 基础消息消费

```go
consumer := kafka.GetConsumer()

handler := func(ctx context.Context, topic string, msg kafka.Message) error {
    log.Printf("Received: topic=%s, key=%s, value=%s", 
        topic, string(msg.Key), string(msg.Value))
    return nil
}

ctx := context.Background()
if err := consumer.Subscribe(ctx, handler); err != nil {
    log.Printf("Subscribe failed: %v", err)
}
```

### 消息确认机制

Kafka 消费者支持完整的消息确认功能，包括手动/自动提交 offset、内置重试机制和指数退避策略。

#### 手动提交模式（推荐）

```go
config := &kafka.ConsumerConfig{
    CommonConfig: kafka.CommonConfig{
        Brokers: []string{"localhost:9092"},
    },
    Topic:      "orders",
    GroupID:    "order-processor",
    AutoCommit: false,  // 手动提交
    MaxRetries: 3,
}

handler := func(ctx context.Context, topic string, msg kafka.Message) error {
    // 处理业务逻辑
    if err := processOrder(msg.Value); err != nil {
        return err  // 返回错误会触发重试
    }
    return nil  // 返回 nil 表示成功，自动提交 offset
}
```

#### 自动提交模式

```go
config := &kafka.ConsumerConfig{
    CommonConfig: kafka.CommonConfig{
        Brokers: []string{"localhost:9092"},
    },
    Topic:          "logs",
    GroupID:        "log-collector",
    AutoCommit:     true,   // 自动提交
    CommitInterval: 5 * time.Second,  // 每5秒提交一次
}
```

### 单主题模式

```go
config := &kafka.ConsumerConfig{
    CommonConfig: kafka.CommonConfig{
        Brokers: []string{"localhost:9092"},
    },
    Topic:   "user-events",
    GroupID: "user-event-processor",
}

handler := func(ctx context.Context, topic string, msg kafka.Message) error {
    log.Printf("User event: %s", string(msg.Value))
    return nil
}

kafka.Subscribe(ctx, handler)
```

### 多主题模式

```go
config := &kafka.ConsumerConfig{
    CommonConfig: kafka.CommonConfig{
        Brokers: []string{"localhost:9092"},
    },
    Topics: []string{
        "order-created",
        "order-updated",
        "order-cancelled",
    },
    GroupID: "order-processor",
}

// 根据主题进行不同的处理逻辑
handler := func(ctx context.Context, topic string, msg kafka.Message) error {
    switch topic {
    case "order-created":
        handleNewOrder(msg.Value)
    case "order-updated":
        handleOrderUpdate(msg.Value)
    case "order-cancelled":
        handleOrderCancel(msg.Value)
    }
    return nil
}

kafka.Subscribe(ctx, handler)
```

## 🎯 消费者管理器 (ConsumerManager)

消费者管理器是 Kafka 工具库的核心功能之一，用于管理多个业务消费者实例，提供更好的隔离性、灵活性和可维护性。

### 为什么需要消费者管理器

在生产环境中，一个应用通常需要处理多个业务领域的消息。使用消费者管理器可以：

**❌ 不使用管理器的问题：**
- 所有业务共享同一个 Consumer Group，无法独立控制 offset
- 不同业务的处理速度相互影响
- 某个业务处理失败会影响其他业务
- 无法针对不同业务设置不同的配置
- 扩展性差，难以水平扩展特定业务

**✅ 使用管理器的好处：**
- ✅ **隔离性好**：每个业务有独立的 Consumer Group，offset 互不影响
- ✅ **灵活配置**：不同业务可以有不同的配置（批量大小、超时、重试等）
- ✅ **故障隔离**：某个业务的问题不会影响其他业务
- ✅ **独立扩展**：可以针对高流量业务单独增加消费者实例
- ✅ **独立监控**：可以分别监控每个业务的消费延迟、处理速度
- ✅ **便于维护**：可以单独重启某个业务的消费者而不影响其他业务

### 快速开始

#### 1. 基础用法

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    
    "github.com/segmentio/kafka-go"
    "github.com/xm-utils/tools/kafka"
)

func main() {
    // 获取管理器实例（单例模式）
    manager := kafka.GetConsumerManager()
    
    // 注册订单业务消费者
    orderConfig := &kafka.ConsumerConfig{
        CommonConfig: kafka.CommonConfig{
            Brokers:  []string{"localhost:9092"},
            ClientID: "order-service",
        },
        Topic:      "orders",
        GroupID:    "order-service-group",
        MaxRetries: 3,
    }
    
    if err := manager.RegisterConsumer("order", orderConfig); err != nil {
        log.Fatalf("注册订单消费者失败: %v", err)
    }
    
    // 注册用户业务消费者
    userConfig := &kafka.ConsumerConfig{
        CommonConfig: kafka.CommonConfig{
            Brokers:  []string{"localhost:9092"},
            ClientID: "user-service",
        },
        Topic:      "users",
        GroupID:    "user-service-group",
        MaxRetries: 5,
    }
    
    if err := manager.RegisterConsumer("user", userConfig); err != nil {
        log.Fatalf("注册用户消费者失败: %v", err)
    }
    
    // 定义各业务的消息处理器
    handlers := map[string]kafka.TopicHandler{
        "order": func(ctx context.Context, topic string, msg kafka.Message) error {
            log.Printf("处理订单消息: key=%s, value=%s", 
                string(msg.Key), string(msg.Value))
            return nil
        },
        "user": func(ctx context.Context, topic string, msg kafka.Message) error {
            log.Printf("处理用户消息: key=%s, value=%s", 
                string(msg.Key), string(msg.Value))
            return nil
        },
    }
    
    // 启动所有消费者
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    go func() {
        if err := manager.StartAll(ctx, handlers); err != nil {
            log.Printf("消费者运行错误: %v", err)
        }
    }()
    
    log.Println("所有消费者已启动")
    
    // 等待关闭信号
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan
    
    log.Println("收到关闭信号，正在优雅关闭...")
    cancel()
    manager.CloseAll()
    log.Println("所有消费者已关闭")
}
```

### 使用模式

#### 微服务架构模式

在微服务架构中，每个微服务独立管理自己的消费者：

```go
// order-service/main.go
func main() {
    manager := kafka.GetConsumerManager()
    
    config := &kafka.ConsumerConfig{
        CommonConfig: kafka.CommonConfig{
            Brokers:  []string{"kafka:9092"},
            ClientID: "order-service",
        },
        Topics:     []string{"orders-created", "orders-updated"},
        GroupID:    "order-service-group",
        MaxRetries: 3,
    }
    
    manager.RegisterConsumer("order", config)
    
    handlers := map[string]kafka.TopicHandler{
        "order": OrderMessageHandler,
    }
    
    ctx := context.Background()
    manager.StartAll(ctx, handlers)
}
```

**优势：**
- ✅ 服务间完全隔离
- ✅ 可以独立部署和扩展
- ✅ 故障不会传播

#### 单服务多业务模式

在一个服务中处理多个业务领域的消息：

```go
func main() {
    manager := kafka.GetConsumerManager()
    
    businesses := map[string]*kafka.ConsumerConfig{
        "analytics": {
            CommonConfig: kafka.CommonConfig{
                Brokers: []string{"kafka:9092"},
            },
            Topic:      "user-events",
            GroupID:    "analytics-group",
            MaxRetries: 10,
            MaxBytes:   2097152, // 2MB 批量处理
        },
        "realtime": {
            CommonConfig: kafka.CommonConfig{
                Brokers: []string{"kafka:9092"},
            },
            Topic:      "user-events",  // 同一主题，不同消费组
            GroupID:    "realtime-group",
            MaxRetries: 3,
            MaxBytes:   1048576,        // 1MB 快速响应
        },
    }
    
    for business, config := range businesses {
        manager.RegisterConsumer(business, config)
    }
    
    handlers := map[string]kafka.TopicHandler{
        "analytics": AnalyticsHandler,
        "realtime":  RealtimeHandler,
    }
    
    ctx := context.Background()
    manager.StartAll(ctx, handlers)
}
```

**优势：**
- ✅ 同一数据源，不同处理策略
- ✅ 资源利用率高
- ✅ 便于统一管理

### 健康检查与监控

```go
func MonitorConsumers() {
    manager := kafka.GetConsumerManager()
    
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        health := manager.HealthCheck()
        
        for _, status := range health {
            log.Printf("业务: %s, 主题: %s, 消费组: %s, 状态: %s",
                status.Business,
                status.Topic,
                status.GroupID,
                status.Status,
            )
        }
        
        log.Printf("活跃消费者数量: %d", manager.GetConsumerCount())
    }
}
```

### API 参考

#### ConsumerManager 主要方法

| 方法 | 说明 | 参数 | 返回值 |
|------|------|------|--------|
| `GetConsumerManager()` | 获取默认管理器实例（单例） | 无 | `*ConsumerManager` |
| `NewConsumerManager()` | 创建新的管理器实例 | 无 | `*ConsumerManager` |
| `RegisterConsumer()` | 注册业务消费者 | business, config | `error` |
| `GetConsumer()` | 获取指定业务的消费者 | business | `(*Consumer, error)` |
| `StartAll()` | 启动所有消费者 | ctx, handlers | `error` |
| `Start()` | 启动单个业务消费者 | ctx, business, handler | `error` |
| `CloseAll()` | 关闭所有消费者 | 无 | 无 |
| `Close()` | 关闭指定业务消费者 | business | `error` |
| `ListConsumers()` | 列出所有消费者 | 无 | `map[string]*Consumer` |
| `GetConsumerCount()` | 获取消费者数量 | 无 | `int` |
| `HealthCheck()` | 健康检查 | 无 | `map[string]ConsumerHealth` |

### 最佳实践

#### 1. 配置建议

```go
// 高吞吐场景（日志收集、数据分析）
highThroughputConfig := &kafka.ConsumerConfig{
    CommonConfig: kafka.CommonConfig{
        Brokers: []string{"kafka:9092"},
    },
    Topic:      "logs",
    GroupID:    "log-collector",
    MaxBytes:   5242880,      // 5MB 大批量
    MaxWait:    5 * time.Second,
    MaxRetries: 3,
}

// 低延迟场景（实时通知、即时消息）
lowLatencyConfig := &kafka.ConsumerConfig{
    CommonConfig: kafka.CommonConfig{
        Brokers: []string{"kafka:9092"},
    },
    Topic:        "instant-messages",
    GroupID:      "message-delivery",
    MaxBytes:     104857,
    MaxWait:      100 * time.Millisecond,
    MaxRetries:   5,
    RetryBackoff: 500 * time.Millisecond,
}
```

#### 2. 优雅关闭

```go
func GracefulShutdown() {
    manager := kafka.GetConsumerManager()
    
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    
    select {
    case sig := <-sigChan:
        log.Printf("收到信号: %v，开始优雅关闭", sig)
        cancel()
        time.Sleep(2 * time.Second)
        manager.CloseAll()
    case <-ctx.Done():
        log.Println("超时，强制关闭")
        manager.CloseAll()
    }
}
```

### 常见问题

#### Q1: 什么时候使用单消费者，什么时候使用多消费者？

**A:** 
- **使用单消费者**：只有一个业务场景，或者所有消息的处理逻辑相同
- **使用多消费者**：有多个业务场景，需要独立的 offset、配置或故障隔离

#### Q2: 不同消费者可以订阅同一个主题吗？

**A:** 可以！只要使用不同的 `GroupID`，它们就会独立消费。

```go
analyticsConfig.GroupID = "analytics-group"     // 用于数据分析
realtimeConfig.GroupID = "realtime-group"       // 用于实时推送
```

#### Q3: 如何保证消息顺序？

**A:** Kafka 只保证**分区内**的顺序。如果需要严格顺序：
1. 使用相同的 key 将相关消息发送到同一分区
2. 消费者单线程处理该分区

### 总结对比

| 特性 | 单消费者 | 多消费者（管理器） |
|------|---------|------------------|
| 隔离性 | ❌ 差 | ✅ 好 |
| 灵活性 | ❌ 低 | ✅ 高 |
| 可扩展性 | ❌ 差 | ✅ 好 |
| 管理复杂度 | ✅ 简单 | ⚠️ 中等 |
| 资源占用 | ✅ 少 | ⚠️ 较多 |
| 适用场景 | 简单应用 | 生产环境推荐 |

**推荐：** 在生产环境中始终使用 ConsumerManager 管理多个独立的消费者实例。

## 🔧 高级功能

### 消息去重

Kafka 工具库内置了消息去重机制，支持内存和 Redis 两种存储方式。

#### 启用去重

```go
// 初始化去重器
deduplicator := kafka.NewMemoryDeduplicator(10 * time.Minute)
consumer.SetDeduplicator(deduplicator)

// 或者使用 Redis 去重
redisClient := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})
deduplicator := kafka.NewRedisDeduplicator(redisClient, 10*time.Minute)
consumer.SetDeduplicator(deduplicator)
```

### 内存去重存储

适用于单机应用或测试环境：

```go
// 创建内存去重器，过期时间 10 分钟
deduplicator := kafka.NewMemoryDeduplicator(10 * time.Minute)

// 在消费者中使用
handler := func(ctx context.Context, topic string, msg kafka.Message) error {
    // 检查是否重复
    isDup, err := deduplicator.IsDuplicate(msg)
    if err != nil {
        log.Printf("Check duplicate failed: %v", err)
        return err
    }
    
    if isDup {
        log.Printf("Skip duplicate message: key=%s", string(msg.Key))
        return nil
    }
    
    // 处理消息
    if err := processMessage(msg); err != nil {
        return err
    }
    
    // 标记为已处理
    return deduplicator.MarkProcessed(msg)
}
```

**优点：**
- ✅ 速度快，无网络开销
- ✅ 无需外部依赖

**缺点：**
- ❌ 重启后数据丢失
- ❌ 不支持分布式

### Redis 去重存储

适用于生产环境和分布式系统：

```go
import "github.com/go-redis/redis/v8"

// 创建 Redis 客户端
redisClient := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
    Password: "",
    DB:       0,
})

// 创建 Redis 去重器，过期时间 10 分钟
deduplicator := kafka.NewRedisDeduplicator(redisClient, 10*time.Minute)

// 在消费者中使用
handler := func(ctx context.Context, topic string, msg kafka.Message) error {
    // 检查是否重复
    isDup, err := deduplicator.IsDuplicate(msg)
    if err != nil {
        log.Printf("Check duplicate failed: %v", err)
        return err
    }
    
    if isDup {
        log.Printf("Skip duplicate message: key=%s", string(msg.Key))
        return nil
    }
    
    // 处理消息
    if err := processMessage(msg); err != nil {
        return err
    }
    
    // 标记为已处理
    return deduplicator.MarkProcessed(msg)
}
```

**优点：**
- ✅ 持久化存储
- ✅ 支持分布式
- ✅ 重启不丢失

**缺点：**
- ❌ 需要 Redis 依赖
- ❌ 有网络开销

**Redis Key 格式：**
```
kafka:duplicate:{topic}:{key}
kafka:duplicate:orders:order-123
kafka:duplicate:payments:payment-456
```

## ⚙️ 配置说明

### ProducerConfig 生产者配置

```go
type ProducerConfig struct {
    CommonConfig           `yaml:",inline"`         // 公共配置
    Topic                  string                   `yaml:"topic"`                    // 默认主题
    MaxAttempts            int                      `yaml:"max_attempts"`             // 最大重试次数
    DialTimeout            time.Duration            `yaml:"dial_timeout"`             // 连接超时
    ReadTimeout            time.Duration            `yaml:"read_timeout"`             // 读取超时
    WriteTimeout           time.Duration            `yaml:"write_timeout"`            // 写入超时
    BatchSize              int                      `yaml:"batch_size"`               // 批量大小
    BatchBytes             int64                    `yaml:"batch_bytes"`              // 批量字节数
    BatchTimeout           time.Duration            `yaml:"batch_timeout"`            // 批量超时
    RequiredAcks           kafka.RequiredAcks       `yaml:"required_acks"`            // 确认级别
    Async                  bool                     `yaml:"async"`                    // 异步发送
    AllowAutoTopicCreation bool                     `yaml:"allow_auto_topic_creation"` // 允许自动创建主题
}
```

### ConsumerConfig 消费者配置

```go
type ConsumerConfig struct {
    CommonConfig   `yaml:",inline"`    // 公共配置
    Topics         []string            `yaml:"topics"`          // 多主题列表
    Topic          string              `yaml:"topic"`           // 单主题
    GroupID        string              `yaml:"group_id"`        // 消费组 ID
    StartOffset    int64               `yaml:"start_offset"`    // 起始 offset
    MinBytes       int                 `yaml:"min_bytes"`       // 最小字节数
    MaxBytes       int                 `yaml:"max_bytes"`       // 最大字节数
    MaxWait        time.Duration       `yaml:"max_wait"`        // 最大等待时间
    QueueCapacity  int                 `yaml:"queue_capacity"`  // 队列容量
    AutoCommit     bool                `yaml:"auto_commit"`     // 自动提交
    CommitInterval time.Duration       `yaml:"commit_interval"` // 提交间隔
    MaxRetries     int                 `yaml:"max_retries"`     // 最大重试次数
    RetryBackoff   time.Duration       `yaml:"retry_backoff"`   // 重试退避
}
```

### CommonConfig 公共配置

```go
type CommonConfig struct {
    Brokers  []string `yaml:"brokers"`   // Kafka broker 地址列表
    ClientID string   `yaml:"client_id"` // 客户端 ID
}
```

### 配置参数详解

#### 基础配置

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| Brokers | []string | 是 | - | Kafka broker 地址列表，如 `["localhost:9092"]` |
| ClientID | string | 否 | "" | 客户端 ID，用于标识应用 |
| Topic | string | 否* | - | 单主题模式下的主题名称 |
| Topics | []string | 否* | - | 多主题模式下的主题列表 |
| GroupID | string | 消费时必填 | - | 消费者组 ID，用于负载均衡和 offset 管理 |

**注意**：Topic 和 Topics 至少需要配置一个。

#### 生产者配置

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| MaxAttempts | int | 3 | 最大重试次数 |
| DialTimeout | time.Duration | 10s | 连接超时时间 |
| ReadTimeout | time.Duration | 10s | 读取超时时间 |
| WriteTimeout | time.Duration | 10s | 写入超时时间 |
| BatchSize | int | 1000 | 批量发送的消息数量 |
| BatchBytes | int64 | 10MB | 批量发送的最大字节数 |
| BatchTimeout | time.Duration | 10ms | 批量超时时间 |
| RequiredAcks | kafka.RequiredAcks | RequireAll | 确认策略 |
| Async | bool | true | 是否异步发送 |
| AllowAutoTopicCreation | bool | false | 是否允许自动创建主题 |

**RequiredAcks 可选值：**
- `kafka.RequireNone (0)`: 不需要任何确认，性能最高但可靠性最低
- `kafka.WaitForLocal (1)`: 只需要 leader 副本确认
- `kafka.RequireAll (-1)`: 需要所有同步副本确认，可靠性最高

#### 消费者配置

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| StartOffset | int64 | FirstOffset (-2) | 起始 offset：-2=FirstOffset, -1=LastOffset |
| MinBytes | int | 1 | 每次读取的最小字节数 |
| MaxBytes | int | 10MB | 每次读取的最大字节数 |
| MaxWait | time.Duration | 1s | 最大等待时间 |
| QueueCapacity | int | 1000 | 队列容量 |
| AutoCommit | bool | false | 是否自动提交 offset（推荐手动提交） |
| CommitInterval | time.Duration | 0 | 自动提交间隔（AutoCommit=true 时有效） |
| MaxRetries | int | 3 | 消息处理最大重试次数 |
| RetryBackoff | time.Duration | 1s | 重试退避时间（指数退避） |

**消费者内部配置（不可配置）：**
- `HeartbeatInterval`: 3s - 心跳间隔
- `SessionTimeout`: 30s - 会话超时
- `RebalanceTimeout`: 30s - 重平衡超时
- `ReadBackoffMin`: 100ms - 最小退避时间
- `ReadBackoffMax`: 1s - 最大退避时间

## 💡 使用示例

### 完整的微服务示例

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"
    
    "github.com/segmentio/kafka-go"
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
    // 获取消费者管理器
    manager := kafka.GetConsumerManager()
    
    // 注册订单业务消费者
    orderConfig := &kafka.ConsumerConfig{
        CommonConfig: kafka.CommonConfig{
            Brokers:  []string{"localhost:9092"},
            ClientID: "order-service",
        },
        Topic:      "orders",
        GroupID:    "order-service-group",
        MaxRetries: 3,
        RetryBackoff: 2 * time.Second,
    }
    
    if err := manager.RegisterConsumer("order", orderConfig); err != nil {
        log.Fatalf("注册订单消费者失败: %v", err)
    }
    
    // 定义消息处理器
    handlers := map[string]kafka.TopicHandler{
        "order": func(ctx context.Context, topic string, msg kafka.Message) error {
            var order Order
            if err := json.Unmarshal(msg.Value, &order); err != nil {
                log.Printf("解析订单消息失败: %v", err)
                return nil // 解析失败不重试
            }
            
            log.Printf("收到订单: ID=%d, User=%d, Amount=%.2f", 
                order.OrderID, order.UserID, order.Amount)
            
            // TODO: 处理订单业务逻辑
            return nil
        },
    }
    
    // 启动消费者
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    go func() {
        if err := manager.StartAll(ctx, handlers); err != nil {
            log.Printf("消费者运行错误: %v", err)
        }
    }()
    
    log.Println("订单服务已启动")
    
    // 优雅关闭
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan
    
    log.Println("正在关闭...")
    cancel()
    manager.CloseAll()
    log.Println("订单服务已停止")
}
```

## 🎯 最佳实践

### 生产环境配置建议

```go
// 生产者配置
producerConfig := &kafka.ProducerConfig{
    CommonConfig: kafka.CommonConfig{
        Brokers: []string{"kafka1:9092", "kafka2:9092", "kafka3:9092"},
        ClientID: "my-service",
    },
    MaxAttempts:            5,
    BatchSize:              1000,
    BatchBytes:             10e6,  // 10MB
    RequiredAcks:           kafka.RequireAll,  // 最高可靠性
    AllowAutoTopicCreation: false,  // 生产环境禁止自动创建主题
}

// 消费者配置
consumerConfig := &kafka.ConsumerConfig{
    CommonConfig: kafka.CommonConfig{
        Brokers: []string{"kafka1:9092", "kafka2:9092", "kafka3:9092"},
        ClientID: "my-service",
    },
    Topic:      "orders",
    GroupID:    "my-service-group",
    MaxRetries: 3,
    RetryBackoff: 2 * time.Second,
    AutoCommit: false,  // 手动提交保证可靠性
}
```

### 消息可靠性保证

1. **生产者端**
   - 设置 `RequiredAcks = kafka.RequireAll`
   - 启用重试机制 `MaxAttempts >= 3`
   - 监控发送失败率

2. **消费者端**
   - 使用手动提交 `AutoCommit = false`
   - 设置合理的重试次数 `MaxRetries = 3-5`
   - 记录处理失败的消息到死信队列

### 性能优化建议

1. **批量处理**
   - 增大 `BatchSize` 和 `BatchBytes`
   - 适当增加 `BatchTimeout`

2. **并行处理**
   - 增加分区数提高并行度
   - 使用多个消费者实例

3. **资源控制**
   - 合理设置 `MaxBytes` 避免 OOM
   - 监控内存使用情况

## ❓ 常见问题

### Q1: 如何保证消息不丢失？

**A:** 
1. 生产者使用 `RequiredAcks = kafka.RequireAll`
2. 消费者使用手动提交 `AutoCommit = false`
3. 消息处理成功后再提交 offset
4. 设置合理的重试次数

### Q2: 如何处理消息积压？

**A:**
1. 增加消费者实例数量
2. 增加主题分区数
3. 优化消息处理逻辑，提高处理速度
4. 临时增加消费者的 `MaxBytes` 和 `BatchSize`

### Q3: 如何实现消息的顺序性？

**A:**
1. 使用相同的 key 将相关消息发送到同一分区
2. 消费者单线程处理该分区
3. 避免使用多线程并发处理同一分区的消息

### Q4: 消费者重启后从哪里开始消费？

**A:** 取决于 `StartOffset` 配置：
- `kafka.FirstOffset`: 从最早的消息开始
- `kafka.LastOffset`: 从最新的消息开始
- 如果已有提交的 offset，从上次提交的位置继续

### Q5: 如何监控消费者状态？

**A:** 使用 ConsumerManager 的健康检查功能：

```go
manager := kafka.GetConsumerManager()
health := manager.HealthCheck()
for business, status := range health {
    log.Printf("%s: %s", business, status.Status)
}
```

## 📦 依赖

- Go 1.24.2+
- github.com/segmentio/kafka-go v0.4.51+
- github.com/sirupsen/logrus v1.9.4+
- github.com/go-redis/redis/v8 v8.11.5+（可选，用于 Redis 去重）

## 📄 许可证

MIT License

---

**更多问题？** 欢迎提 Issue 或 PR！


