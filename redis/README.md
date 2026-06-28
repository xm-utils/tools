# Redis 客户端工具库

[![Go Version](https://img.shields.io/badge/go-1.24.2+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Redis SDK](https://img.shields.io/badge/redis--go-v8.11.5-blue.svg)](https://github.com/go-redis/redis)

一个基于 [go-redis/redis v8](https://github.com/go-redis/redis) 封装的高性能 Go 语言 Redis 客户端工具库，提供简洁易用的 API 来操作 Redis 的各种数据结构，支持 String、Hash、List、Set、Sorted Set、Stream 等完整功能，并内置泛型支持和自动序列化/反序列化。

## 📋 目录

- [项目简介](#项目简介)
- [主要特性](#主要特性)
- [安装](#安装)
- [快速开始](#快速开始)
  - [基础初始化](#基础初始化)
  - [YAML 配置加载](#yaml-配置加载)
- [核心功能](#核心功能)
  - [String 字符串](#string-字符串)
  - [Hash 哈希](#hash-哈希)
  - [List 列表](#list-列表)
  - [Set 集合](#set-集合)
  - [Sorted Set 有序集合](#sorted-set-有序集合)
  - [Stream 流](#stream-流)
  - [发布订阅](#发布订阅)
- [高级功能](#高级功能)
  - [泛型支持](#泛型支持)
  - [自动序列化](#自动序列化)
  - [Key 前缀管理](#key-前缀管理)
  - [Lua 脚本执行](#lua-脚本执行)
- [配置说明](#配置说明)
  - [Config 配置结构](#config-配置结构)
  - [配置参数详解](#配置参数详解)
- [使用示例](#使用示例)
  - [缓存用户信息](#缓存用户信息)
  - [实现排行榜](#实现排行榜)
  - [消息队列](#消息队列)
  - [分布式锁](#分布式锁)
  - [Stream 消费者组](#stream-消费者组)
  - [发布订阅模式](#发布订阅模式)
- [API 参考](#api-参考)
  - [初始化和通用 API](#初始化和通用-api)
  - [String API](#string-api)
  - [Hash API](#hash-api)
  - [List API](#list-api)
  - [Set API](#set-api)
  - [Sorted Set API](#sorted-set-api)
  - [Stream API](#stream-api)
  - [发布订阅 API](#发布订阅-api)
- [最佳实践](#最佳实践)
  - [Key 命名规范](#key-命名规范)
  - [缓存策略](#缓存策略)
  - [性能优化](#性能优化)
  - [内存管理](#内存管理)
- [常见问题](#常见问题)
- [依赖](#依赖)
- [许可证](#许可证)

## 📖 项目简介

本项目是对 go-redis/redis v8 的二次封装，旨在简化 Redis 在 Go 项目中的使用。提供了以下核心功能：

- **完整的数据结构支持**：String、Hash、List、Set、Sorted Set、Stream
- **泛型支持**：类型安全的数据获取，自动反序列化
- **自动序列化**：支持基本类型、结构体、切片等的自动编解码
- **Key 前缀管理**：统一的命名空间隔离
- **Stream 完整支持**：消费者组、消息确认、待处理消息管理
- **发布订阅**：支持普通订阅和模式订阅

## ✨ 主要特性

- ✅ **简洁的 API**：封装复杂的 redis-go SDK，提供简单易用的接口
- ✅ **泛型支持**：利用 Go 泛型特性，类型安全的数据获取
- ✅ **自动序列化**：支持 string、int、float、bool、struct、slice 等自动编解码
- ✅ **Key 前缀**：统一的 Key 前缀管理，避免命名冲突
- ✅ **完整数据结构**：支持 Redis 所有主要数据结构
- ✅ **Stream 支持**：完整的 Stream 功能，包括消费者组和消息确认
- ✅ **发布订阅**：支持 Publish/Subscribe 模式
- ✅ **Lua 脚本**：支持执行 Lua 脚本
- ✅ **连接管理**：自动连接检测和超时控制

## 🚀 安装

```bash
go get github.com/xm-utils/tools/redis
```

### 依赖要求

- Go 1.24.2+
- github.com/go-redis/redis/v8 v8.11.5+

### 前置条件

使用前请确保已部署并运行 Redis 服务器。推荐使用 Redis 6.x 或更高版本以支持 Stream 功能。

## 🎯 快速开始

### 基础初始化

```go
package main

import (
    "context"
    "fmt"
    "log"
    "github.com/xm-utils/tools/redis"
)

func main() {
    // 初始化 Redis 连接
    err := redis.InitRedisCache(&redis.Config{
        Prefix:   "myapp",
        Host:     "127.0.0.1:6379",
        Password: "",
        DbNum:    0,
    })
    if err != nil {
        log.Fatalf("Failed to init redis: %v", err)
    }

    ctx := context.Background()

    // 检查连接
    if redis.IsExist(ctx, "test") {
        fmt.Println("Redis connected successfully")
    }
}
```

### YAML 配置加载

1. 创建配置文件 `redis.yaml`：

```yaml
prefix: "myapp"
host: "127.0.0.1:6379"
password: ""
dbNum: 0
```

2. 在代码中加载配置（需配合配置加载库如 viper）：

```go
package main

import (
    "log"
    "github.com/spf13/viper"
    "github.com/xm-utils/tools/redis"
)

func main() {
    // 使用 viper 加载 YAML 配置
    viper.SetConfigFile("redis.yaml")
    if err := viper.ReadInConfig(); err != nil {
        log.Fatalf("Failed to read config: %v", err)
    }

    // 解析为 redis.Config 结构
    var config redis.Config
    if err := viper.Unmarshal(&config); err != nil {
        log.Fatalf("Failed to unmarshal config: %v", err)
    }

    // 初始化 Redis
    if err := redis.InitRedisCache(&config); err != nil {
        log.Fatalf("Failed to init redis: %v", err)
    }

    log.Println("Redis initialized successfully")
}
```

## 🔧 核心功能

### String 字符串

#### 设置和获取

```go
ctx := context.Background()

// 设置字符串（带过期时间，单位：秒）
err := redis.Set(ctx, "user:1001:name", "张三", 3600)
if err != nil {
    log.Printf("Set failed: %v", err)
}

// 获取字符串（泛型支持）
name, err := redis.Get[string](ctx, "user:1001:name")
if err != nil {
    if err == redis.Nil {
        fmt.Println("Key not found")
    } else {
        log.Printf("Get failed: %v", err)
    }
} else {
    fmt.Printf("Name: %s\n", name)
}
```

#### 批量操作

```go
// 批量设置
err := redis.MSet(ctx, map[string]interface{}{
    "user:1001:name":  "张三",
    "user:1001:age":   25,
    "user:1001:email": "zhangsan@example.com",
})

// 批量获取
names, err := redis.MGet[string](ctx, 
    "user:1001:name", 
    "user:1002:name",
    "user:1003:name",
)
```

#### 原子递增

```go
// 计数器
count, err := redis.Incr(ctx, "page:view:1001")
if err != nil {
    log.Printf("Incr failed: %v", err)
}
fmt.Printf("View count: %d\n", count)

// 自定义增量
newCount, err := redis.IncrBy(ctx, "score:1001", 10)
```

#### 其他字符串操作

```go
// 设置并返回旧值
oldValue, err := redis.GetSet(ctx, "counter", 100)

// 只在键不存在时设置
success, err := redis.Setnx(ctx, "lock:order:1001", "locked")

// 追加字符串
newLen, err := redis.Append(ctx, "message:1001", " additional text")

// 获取字符串长度
length, err := redis.Strlen(ctx, "message:1001")

// 位操作
err := redis.SetBit(ctx, "bitmap:user:1001", 0, 1)
bit, err := redis.GetBit(ctx, "bitmap:user:1001", 0)
```

### Hash 哈希

#### 基本操作

```go
ctx := context.Background()

// 设置单个字段
err := redis.HSet(ctx, "user:1001", "name", "张三")
err = redis.HSet(ctx, "user:1001", "age", 25)
err = redis.HSet(ctx, "user:1001", "email", "zhangsan@example.com")

// 获取单个字段
name, err := redis.HGet[string](ctx, "user:1001", "name")
age, err := redis.HGet[int](ctx, "user:1001", "age")

// 批量设置
err := redis.HMSet(ctx, "user:1001", map[string]interface{}{
    "name":  "李四",
    "age":   30,
    "city":  "北京",
})

// 批量获取
values := redis.HMGet[string](ctx, "user:1001", "name", "age", "city")
```

#### 获取所有字段

```go
// 获取所有字段和值
err, profile := redis.HGetAll[string](ctx, "user:1001")
if err != nil {
    log.Printf("HGetAll failed: %v", err)
} else {
    for field, value := range profile {
        fmt.Printf("%s: %s\n", field, value)
    }
}

// 获取所有字段名
fields, err := redis.HKeys(ctx, "user:1001")

// 获取所有值
values, err := redis.HVals[string](ctx, "user:1001")

// 获取字段数量
count := redis.HLen(ctx, "user:1001")
```

#### 字段操作

```go
// 检查字段是否存在
exists, err := redis.HExists(ctx, "user:1001", "name")

// 删除字段
err := redis.HDel(ctx, "user:1001", "temp_field")

// 字段值递增
newVal, err := redis.HIncrBy(ctx, "user:1001", "login_count", 1)
newFloatVal, err := redis.HIncrByFloat(ctx, "user:1001", "score", 0.5)

// 只在字段不存在时设置
success, err := redis.HSetnx(ctx, "user:1001", "created_at", time.Now().Unix())
```

### List 列表

#### 推入和弹出

```go
ctx := context.Background()

// 左侧推入（头部）
err := redis.LPush(ctx, "queue:tasks", "task1", "task2", "task3")

// 右侧推入（尾部）
err = redis.RPush(ctx, "queue:tasks", "task4", "task5")

// 左侧弹出
task, err := redis.LPop[string](ctx, "queue:tasks")

// 右侧弹出
task, err = redis.RPop[string](ctx, "queue:tasks")
```

#### 阻塞操作

```go
// 阻塞式左侧弹出（超时时间：秒）
key, task, err := redis.BLPop[string](ctx, 30, "queue:tasks", "queue:backup")
if err != nil {
    if err == redis.Nil {
        fmt.Println("Timeout")
    }
} else {
    fmt.Printf("Got task from %s: %s\n", key, task)
}

// 阻塞式右侧弹出
key, task, err = redis.BRPop[string](ctx, 30, "queue:tasks")
```

#### 范围查询

```go
// 获取指定范围的元素
tasks, err := redis.LRange[string](ctx, "queue:tasks", 0, 9) // 前10个

// 获取列表长度
length, err := redis.LLen(ctx, "queue:tasks")

// 通过索引获取元素
task, err := redis.LIndex[string](ctx, "queue:tasks", 0)

// 通过索引设置元素
err = redis.LSet(ctx, "queue:tasks", 0, "updated_task")
```

#### 其他列表操作

```go
// 移除元素（count > 0: 从头到尾; count < 0: 从尾到头; count = 0: 全部）
removed, err := redis.LRem(ctx, "queue:tasks", 1, "task_to_remove")

// 修剪列表（只保留指定范围内的元素）
err = redis.Ltrim(ctx, "queue:tasks", 0, 99) // 只保留前100个

// 在某个元素前后插入
err = redis.LInsert(ctx, "queue:tasks", "BEFORE", "pivot_value", "new_value")

// 只在列表存在时推入
err = redis.LPushX(ctx, "queue:tasks", "task")
err = redis.RPushX(ctx, "queue:tasks", "task")

// 从一个列表弹出并推入到另一个列表
task, err := redis.RPopLPush[string](ctx, "queue:source", "queue:dest")
task, err = redis.BRPopLPush(ctx, "queue:source", "queue:dest", 30)
```

### Set 集合

#### 基本操作

```go
ctx := context.Background()

// 添加成员
added, err := redis.SAdd(ctx, "tags:article:1001", "Go", "Redis", "Database")

// 获取所有成员
members, err := redis.SMembers[string](ctx, "tags:article:1001")

// 获取成员数量
count := redis.SCard(ctx, "tags:article:1001")

// 检查成员是否存在
isMember, err := redis.SIsMember(ctx, "tags:article:1001", "Go")

// 移除成员
removed, err := redis.SRem(ctx, "tags:article:1001", "Database")
```

#### 集合运算

```go
// 差集（在 key1 但不在其他集合中）
diff, err := redis.SDiff[string](ctx, "set1", "set2", "set3")
// 将差集存储到新集合
count, err := redis.SDiffStore(ctx, "diff_result", "set1", "set2")

// 交集
inter, err := redis.SInter[string](ctx, "set1", "set2")
count, err = redis.SInterStore(ctx, "inter_result", "set1", "set2")

// 并集
union, err := redis.SUnion[string](ctx, "set1", "set2")
count, err = redis.SUnionStore(ctx, "union_result", "set1", "set2")
```

#### 随机操作

```go
// 随机弹出一个成员
member, err := redis.SPop[string](ctx, "tags:article:1001")

// 随机获取成员（不删除）
members, err := redis.SRandMember[string](ctx, "tags:article:1001", 5) // 获取5个

// 移动成员到另一个集合
moved, err := redis.SMove(ctx, "set1", "set2", "member_to_move")
```

### Sorted Set 有序集合

#### 添加和查询

```go
ctx := context.Background()

// 添加成员及分数
err := redis.ZAdd(ctx, "leaderboard:game1", map[interface{}]float64{
    "player1": 100.5,
    "player2": 200.3,
    "player3": 150.7,
})

// 添加单个成员
err = redis.ZAddByScore(ctx, "leaderboard:game1", "player4", 180.0)

// 获取成员数量
count, err := redis.ZCard(ctx, "leaderboard:game1")

// 获取成员的分数
score, err := redis.ZScore(ctx, "leaderboard:game1", "player1")

// 增加成员的分数
newScore, err := redis.ZIncrBy(ctx, "leaderboard:game1", "player1", 10.5)
```

#### 范围查询

```go
// 按索引范围获取（从低分到高分）
players, err := redis.ZRange[string](ctx, "leaderboard:game1", 0, 9) // Top 10

// 按分数范围获取
players, err = redis.ZRangeByScore[string](ctx, "leaderboard:game1", "100", "200")

// 获取带分数的结果
results, err := redis.ZRangeWithScores(ctx, "leaderboard:game1", 0, 9)
for _, z := range results {
    fmt.Printf("Member: %v, Score: %f\n", z.Member, z.Score)
}

// 反向查询（从高分到低分）
players, err = redis.ZRevRange[string](ctx, "leaderboard:game1", 0, 9)
players, err = redis.ZRevRangeByScore[string](ctx, "leaderboard:game1", "200", "100")
```

#### 排名操作

```go
// 获取排名（从低分到高分，0-based）
rank, err := redis.ZRank(ctx, "leaderboard:game1", "player1")

// 获取排名（从高分到低分，0-based）
revRank, err := redis.ZRevRank(ctx, "leaderboard:game1", "player1")
```

#### 统计和删除

```go
// 统计分数范围内的成员数
count, err := redis.ZCount(ctx, "leaderboard:game1", "100", "200")

// 统计字典区间内的成员数
count, err = redis.ZLexCount(ctx, "zset", "[a", "[z")

// 删除成员
removed, err := redis.ZRem(ctx, "leaderboard:game1", "player1", "player2")

// 按排名范围删除
removed, err = redis.ZRemRangeByRank(ctx, "leaderboard:game1", 0, 9)

// 按分数范围删除
removed, err = redis.ZRemRangeByScore(ctx, "leaderboard:game1", 0, 100)

// 按字典范围删除
removed, err = redis.ZRemRangeByLex(ctx, "zset", "[a", "[c")
```

#### 集合运算

```go
// 交集并存储
count, err := redis.ZInterStore(ctx, "result_zset", "zset1", "zset2")

// 并集并存储
count, err = redis.ZUnionStore(ctx, "result_zset", "zset1", "zset2")
```

### Stream 流

Redis Stream 是一个强大的消息队列功能，支持消费者组、消息确认等高级特性。

#### 添加消息

```go
ctx := context.Background()

// 添加简单消息
msgID, err := redis.XAdd(ctx, &redis.XAddArgs{
    Stream: "events",
    Values: map[string]interface{}{
        "event":     "user_login",
        "user_id":   "123",
        "timestamp": time.Now().Unix(),
    },
})
if err != nil {
    log.Printf("XAdd failed: %v", err)
}
fmt.Printf("Message added with ID: %s\n", msgID)

// 添加消息并限制最大长度（近似修剪）
msgID, err = redis.XAdd(ctx, &redis.XAddArgs{
    Stream: "logs",
    MaxLen: 10000, // 最多保留10000条消息
    Values: map[string]string{
        "level":   "INFO",
        "message": "Application started",
    },
})
```

#### 读取消息

```go
// 读取消息（非阻塞）
result, err := redis.XRead(ctx, &redis.XReadArgs{
    Streams: []string{"events"},
    IDs:     []string{"0"}, // 从头开始读取
    Count:   10,
})

// 阻塞读取
result, err = redis.XRead(ctx, &redis.XReadArgs{
    Streams: []string{"events"},
    IDs:     []string{"$"}, // 只读取新消息
    Count:   10,
    Block:   5 * time.Second, // 阻塞5秒
})

// 处理读取结果
for stream, messages := range result {
    fmt.Printf("Stream: %s, Messages: %d\n", stream, len(messages))
    for _, msg := range messages {
        fmt.Printf("  ID: %s, Values: %v\n", msg.ID, msg.Values)
    }
}
```

#### 消费者组

```go
// 创建消费者组
err := redis.XGroupCreate(ctx, "events", "event_processors", "0")
if err != nil {
    fmt.Println("Group may already exist:", err)
}

// 创建消费者组（如果流不存在则创建）
err = redis.XGroupCreateMkStream(ctx, "new_stream", "group1", "0")

// 从消费者组读取消息
result, err := redis.XReadGroup(ctx, &redis.XReadGroupArgs{
    Group:    "event_processors",
    Consumer: "worker_1",
    Streams:  []string{"events"},
    IDs:      []string{">"}, // 只读取未分配的消息
    Count:    10,
    Block:    5 * time.Second,
})

// 确认消息处理完成
for stream, messages := range result {
    for _, msg := range messages {
        // 处理消息
        processMessage(msg)
        
        // 确认消息
        acked, err := redis.XAck(ctx, stream, "event_processors", msg.ID)
        if err != nil {
            log.Printf("XAck failed: %v", err)
        }
    }
}
```

#### 待处理消息

```go
// 查看待处理消息
pending, err := redis.XPending(ctx, &redis.XPendingArgs{
    Stream:   "events",
    Group:    "event_processors",
    Start:    "-",
    End:      "+",
    Count:    10,
    Consumer: "worker_1", // 可选
})

for _, p := range pending {
    fmt.Printf("ID: %s, Consumer: %s, Idle: %v, Retry: %d\n",
        p.ID, p.Consumer, p.Idle, p.RetryCount)
}

// 认领超时消息
claimed, err := redis.XClaim(ctx, "events", "event_processors", "worker_2", 
    5*time.Minute, "message_id_1", "message_id_2")

// 自动认领
nextID, claimed, err := redis.XAutoClaim(ctx, &redis.XAutoClaimArgs{
    Stream:   "events",
    Group:    "event_processors",
    Consumer: "worker_2",
    MinIdle:  5 * time.Minute,
    Start:    "0",
    Count:    10,
})
```

#### 管理操作

```go
// 删除消息
deleted, err := redis.XDel(ctx, "events", "msg_id_1", "msg_id_2")

// 获取流长度
length, err := redis.XLen(ctx, "events")

// 修剪流
trimmed, err := redis.XTrim(ctx, "events", 1000) // 保留最后1000条
trimmed, err = redis.XTrimApprox(ctx, "events", 1000) // 近似修剪（更快）

// 范围查询
messages, err := redis.XRange(ctx, "events", "-", "+", 10) // 最新10条
messages, err = redis.XRevRange(ctx, "events", "+", "-", 10) // 反向查询

// 销毁消费者组
destroyed, err := redis.XGroupDestroy(ctx, "events", "event_processors")

// 删除消费者
deleted, err := redis.XGroupDelConsumer(ctx, "events", "event_processors", "worker_1")

// 设置消费者组的ID
err = redis.XGroupSetID(ctx, "events", "event_processors", "0")
```

#### 信息查询

```go
// 获取消费者组信息
groups, err := redis.XInfoGroups(ctx, "events")
for _, g := range groups {
    fmt.Printf("Group: %s, Consumers: %d, Pending: %d, LastID: %s\n",
        g.Name, g.Consumers, g.Pending, g.LastID)
}

// 获取消费者信息
consumers, err := redis.XInfoConsumers(ctx, "events", "event_processors")
for _, c := range consumers {
    fmt.Printf("Consumer: %s, Pending: %d, Idle: %v\n",
        c.Name, c.Pending, c.Idle)
}

// 获取流信息
info, err := redis.XInfoStream(ctx, "events")
```

### 发布订阅

#### 发布消息

```go
ctx := context.Background()

// 发布消息
err := redis.Publish(ctx, "channel:notifications", map[string]interface{}{
    "type":    "order_created",
    "order_id": 12345,
    "message": "New order received",
})
```

#### 订阅频道

```go
// 订阅频道
pubsub := redis.Subscribe(ctx, "channel:notifications", "channel:updates")
defer pubsub.Close()

// 接收消息
for {
    msg, err := redis.ReceiveMessage(ctx, pubsub)
    if err != nil {
        log.Printf("Receive error: %v", err)
        continue
    }
    
    fmt.Printf("Received from %s: %s\n", msg.Channel, msg.Payload)
}
```

#### 模式订阅

```go
// 模式订阅（支持通配符）
pubsub := redis.PSubscribe(ctx, "channel:*", "events:*")
defer pubsub.Close()

// 接收消息
for {
    msg, err := redis.ReceiveMessage(ctx, pubsub)
    if err != nil {
        log.Printf("Receive error: %v", err)
        continue
    }
    
    fmt.Printf("Pattern: %s, Channel: %s, Message: %s\n",
        msg.Pattern, msg.Channel, msg.Payload)
}
```

## 🚀 高级功能

### 泛型支持

本库充分利用 Go 泛型特性，提供类型安全的数据获取：

```go
// 自动反序列化为指定类型
type User struct {
    ID    int    `json:"id"`
    Name  string `json:"name"`
    Email string `json:"email"`
}

// String 类型
userJSON, _ := redis.Get[string](ctx, "user:1001")

// 结构体类型
var user User
err := json.Unmarshal([]byte(userJSON), &user)

// 或者直接使用 HGetAll
err, userData := redis.HGetAll[string](ctx, "user:1001")

// List 类型
scores, _ := redis.LRange[float64](ctx, "scores:game1", 0, -1)

// Set 类型
tags, _ := redis.SMembers[string](ctx, "tags:article:1001")

// Sorted Set 类型
leaders, _ := redis.ZRange[string](ctx, "leaderboard", 0, 9)
```

### 自动序列化

内置的 encode/decode 函数支持多种类型的自动序列化：

```go
// 支持的类型：
// - string: 直接存储
// - int/int64: 转换为字符串
// - float32/float64: 转换为字符串
// - bool: 转换为 "true"/"false"
// - []byte: 直接存储
// - struct/slice/map: JSON 序列化

// 示例
redis.Set(ctx, "config", map[string]interface{}{
    "max_conn": 100,
    "timeout":  30,
    "enabled":  true,
}, 3600)

config, _ := redis.Get[map[string]interface{}](ctx, "config")
```

### Key 前缀管理

所有操作都会自动添加配置的 Key 前缀，实现命名空间隔离：

```go
// 配置前缀为 "myapp"
redis.InitRedisCache(&redis.Config{
    Prefix: "myapp",
    // ...
})

// 实际操作的是 "myapp:user:1001"
redis.Set(ctx, "user:1001", "John", 3600)

// 不带前缀的操作（特殊场景）
redis.SetForNoPrefix(ctx, "global:config", "...", 3600)
val, _ := redis.GetForNoPrefix[string](ctx, "global:config")
```

### Lua 脚本执行

```go
// 执行 Lua 脚本
result, err := redis.Eval(ctx, `
    local current = redis.call('GET', KEYS[1])
    if not current then
        return nil
    end
    if tonumber(current) < tonumber(ARGV[1]) then
        return redis.call('INCR', KEYS[1])
    end
    return current
`, []string{"counter"}, 100)
```

## ⚙️ 配置说明

### Config 配置结构

```go
type Config struct {
    Prefix   string // KEY前缀，用于命名空间隔离
    Host     string // Redis 主机地址 (host:port)
    Password string // 密码（可选）
    DbNum    int    // 数据库编号 (默认 0)
}
```

### 配置参数详解

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| Prefix | string | 否 | "" | Key 前缀，用于命名空间隔离 |
| Host | string | 是 | - | Redis 服务器地址，格式：`host:port` |
| Password | string | 否 | "" | Redis 密码，无密码留空 |
| DbNum | int | 否 | 0 | 数据库编号，范围 0-15 |

**Host 格式示例：**
- `"127.0.0.1:6379"` - 本地 Redis
- `"redis.example.com:6379"` - 远程 Redis
- `"192.168.1.100:6380"` - 自定义端口

## 💡 使用示例

### 缓存用户信息

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"
    "github.com/xm-utils/tools/redis"
)

type User struct {
    ID        int       `json:"id"`
    Name      string    `json:"name"`
    Email     string    `json:"email"`
    CreatedAt time.Time `json:"created_at"`
}

func main() {
    // 初始化 Redis
    redis.InitRedisCache(&redis.Config{
        Prefix: "myapp",
        Host:   "127.0.0.1:6379",
    })

    ctx := context.Background()

    // 缓存用户
    user := User{
        ID:        1001,
        Name:      "张三",
        Email:     "zhangsan@example.com",
        CreatedAt: time.Now(),
    }

    userData, _ := json.Marshal(user)
    redis.Set(ctx, fmt.Sprintf("user:%d", user.ID), string(userData), 3600)

    // 获取用户
    cached, err := redis.Get[string](ctx, "user:1001")
    if err != nil {
        if err == redis.Nil {
            fmt.Println("Cache miss")
            // 从数据库加载...
        } else {
            log.Printf("Get failed: %v", err)
        }
    } else {
        var cachedUser User
        json.Unmarshal([]byte(cached), &cachedUser)
        fmt.Printf("Cached user: %+v\n", cachedUser)
    }

    // 使用 Hash 存储用户属性
    redis.HSet(ctx, "user:1001:profile", "name", "张三")
    redis.HSet(ctx, "user:1001:profile", "age", 25)
    redis.HSet(ctx, "user:1001:profile", "city", "北京")

    name, _ := redis.HGet[string](ctx, "user:1001:profile", "name")
    fmt.Printf("User name: %s\n", name)
}
```

### 实现排行榜

```go
package main

import (
    "context"
    "fmt"
    "github.com/xm-utils/tools/redis"
)

func main() {
    redis.InitRedisCache(&redis.Config{
        Prefix: "game",
        Host:   "127.0.0.1:6379",
    })

    ctx := context.Background()
    leaderboard := "leaderboard:game1"

    // 添加玩家分数
    scores := map[interface{}]float64{
        "player_alex":   1500.5,
        "player_bob":    2300.0,
        "player_charlie": 1800.3,
        "player_david":  2100.7,
        "player_eve":    1950.2,
    }
    redis.ZAdd(ctx, leaderboard, scores)

    // 更新玩家分数
    redis.ZIncrBy(ctx, leaderboard, "player_alex", 100)

    // 获取 Top 10
    top10, err := redis.ZRevRange[string](ctx, leaderboard, 0, 9)
    if err != nil {
        panic(err)
    }

    fmt.Println("=== Top 10 Players ===")
    for i, player := range top10 {
        score, _ := redis.ZScore(ctx, leaderboard, player)
        rank, _ := redis.ZRevRank(ctx, leaderboard, player)
        fmt.Printf("%d. %s: %.1f points\n", rank+1, player, score)
    }

    // 获取分数范围
    highScorers, _ := redis.ZRangeByScore[string](ctx, leaderboard, "2000", "+inf")
    fmt.Printf("\nPlayers with 2000+ points: %v\n", highScorers)

    // 获取玩家数量
    count, _ := redis.ZCard(ctx, leaderboard)
    fmt.Printf("\nTotal players: %d\n", count)
}
```

### 消息队列

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    "github.com/xm-utils/tools/redis"
)

func main() {
    redis.InitRedisCache(&redis.Config{
        Prefix: "mq",
        Host:   "127.0.0.1:6379",
    })

    ctx := context.Background()
    queue := "queue:tasks"

    // 生产者：添加任务
    go func() {
        for i := 1; i <= 10; i++ {
            task := fmt.Sprintf("task_%d", i)
            redis.RPush(ctx, queue, task)
            fmt.Printf("Produced: %s\n", task)
            time.Sleep(100 * time.Millisecond)
        }
    }()

    // 消费者：处理任务
    time.Sleep(500 * time.Millisecond)
    for {
        task, err := redis.BLPop[string](ctx, 5, queue)
        if err != nil {
            if err == redis.Nil {
                fmt.Println("Queue empty, exiting")
                break
            }
            log.Printf("BLPop error: %v", err)
            continue
        }
        
        fmt.Printf("Consumed: %s\n", task)
        // 处理任务...
        time.Sleep(200 * time.Millisecond)
    }
}
```

### 分布式锁

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/xm-utils/tools/redis"
)

// DistributedLock 分布式锁
type DistributedLock struct {
    key     string
    owner   string
    timeout int64
}

// NewDistributedLock 创建分布式锁
func NewDistributedLock(key, owner string, timeout int64) *DistributedLock {
    return &DistributedLock{
        key:     key,
        owner:   owner,
        timeout: timeout,
    }
}

// Acquire 获取锁
func (l *DistributedLock) Acquire(ctx context.Context) (bool, error) {
    success, err := redis.SetnxExpire(ctx, l.key, l.owner, l.timeout)
    if err != nil {
        return false, err
    }
    return success, nil
}

// Release 释放锁
func (l *DistributedLock) Release(ctx context.Context) error {
    owner, err := redis.Get[string](ctx, l.key)
    if err != nil {
        return err
    }
    
    if owner == l.owner {
        return redis.Delete(ctx, l.key)
    }
    
    return fmt.Errorf("not lock owner")
}

func main() {
    redis.InitRedisCache(&redis.Config{
        Prefix: "lock",
        Host:   "127.0.0.1:6379",
    })

    ctx := context.Background()

    // 使用分布式锁
    lock := NewDistributedLock("order:12345", "worker-1", 10)
    
    acquired, err := lock.Acquire(ctx)
    if err != nil {
        panic(err)
    }
    
    if !acquired {
        fmt.Println("Failed to acquire lock")
        return
    }
    
    defer lock.Release(ctx)
    
    fmt.Println("Lock acquired, processing order...")
    time.Sleep(2 * time.Second)
    fmt.Println("Order processed")
}
```

### Stream 消费者组

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    "github.com/xm-utils/tools/redis"
)

func main() {
    redis.InitRedisCache(&redis.Config{
        Prefix: "stream",
        Host:   "127.0.0.1:6379",
    })

    ctx := context.Background()
    stream := "orders"
    group := "order-processors"
    consumer := "worker-1"

    // 创建消费者组（忽略错误，可能已存在）
    redis.XGroupCreate(ctx, stream, group, "0")

    // 生产者：添加订单消息
    go func() {
        for i := 1; i <= 5; i++ {
            msgID, err := redis.XAdd(ctx, &redis.XAddArgs{
                Stream: stream,
                Values: map[string]interface{}{
                    "order_id": i,
                    "product":  fmt.Sprintf("Product %d", i),
                    "amount":   i * 100,
                },
            })
            if err != nil {
                log.Printf("XAdd error: %v", err)
            } else {
                fmt.Printf("Order added: %s\n", msgID)
            }
            time.Sleep(1 * time.Second)
        }
    }()

    // 消费者：处理订单
    time.Sleep(500 * time.Millisecond)
    for {
        result, err := redis.XReadGroup(ctx, &redis.XReadGroupArgs{
            Group:    group,
            Consumer: consumer,
            Streams:  []string{stream},
            IDs:      []string{">"},
            Count:    10,
            Block:    5 * time.Second,
        })
        
        if err != nil {
            log.Printf("XReadGroup error: %v", err)
            continue
        }

        for streamName, messages := range result {
            for _, msg := range messages {
                fmt.Printf("Processing order: ID=%s, Values=%v\n", msg.ID, msg.Values)
                
                // 模拟处理
                time.Sleep(500 * time.Millisecond)
                
                // 确认消息
                acked, err := redis.XAck(ctx, streamName, group, msg.ID)
                if err != nil {
                    log.Printf("XAck error: %v", err)
                } else {
                    fmt.Printf("Acknowledged: %d messages\n", acked)
                }
            }
        }
    }
}
```

### 发布订阅模式

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"
    "github.com/xm-utils/tools/redis"
)

type Notification struct {
    Type    string `json:"type"`
    UserID  int    `json:"user_id"`
    Message string `json:"message"`
}

func main() {
    redis.InitRedisCache(&redis.Config{
        Prefix: "pubsub",
        Host:   "127.0.0.1:6379",
    })

    ctx := context.Background()
    channel := "notifications"

    // 订阅者
    go func() {
        pubsub := redis.Subscribe(ctx, channel)
        defer pubsub.Close()

        fmt.Println("Subscriber started...")
        for {
            msg, err := redis.ReceiveMessage(ctx, pubsub)
            if err != nil {
                log.Printf("Receive error: %v", err)
                continue
            }

            var notif Notification
            json.Unmarshal([]byte(msg.Payload), &notif)
            fmt.Printf("Received notification: Type=%s, User=%d, Message=%s\n",
                notif.Type, notif.UserID, notif.Message)
        }
    }()

    // 发布者
    time.Sleep(1 * time.Second)
    notifications := []Notification{
        {Type: "order", UserID: 1001, Message: "Order #12345 shipped"},
        {Type: "payment", UserID: 1002, Message: "Payment received"},
        {Type: "review", UserID: 1003, Message: "New review posted"},
    }

    for _, notif := range notifications {
        data, _ := json.Marshal(notif)
        err := redis.Publish(ctx, channel, string(data))
        if err != nil {
            log.Printf("Publish error: %v", err)
        } else {
            fmt.Printf("Published: %+v\n", notif)
        }
        time.Sleep(1 * time.Second)
    }

    time.Sleep(2 * time.Second)
}
```

## 📚 API 参考

### 初始化和通用 API

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `InitRedisCache(config *Config) error` | 初始化 Redis 连接 | error: 初始化错误 |
| `IsExist(ctx context.Context, key string) bool` | 检查键是否存在 | bool: 是否存在 |
| `Delete(ctx context.Context, key string) error` | 删除键 | error: 删除错误 |
| `ClearAll(ctx context.Context) error` | 清空所有数据 | error: 清空错误 |
| `ExpireAt(ctx context.Context, key string, t time.Time) error` | 设置过期时间点 | error: 设置错误 |
| `ExpireIn(ctx context.Context, key string, d time.Duration) error` | 设置过期时长 | error: 设置错误 |
| `Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error)` | 执行 Lua 脚本 | result: 脚本返回值, error: 执行错误 |

### String API

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `Set(ctx, key string, val interface{}, timeout int64) error` | 设置键值对 | error: 设置错误 |
| `Get[T any](ctx, key string) (T, error)` | 获取值（泛型） | value: 值, error: 获取错误 |
| `GetRange(ctx, key string, start, end int64) (string, error)` | 获取子字符串 | string: 子串, error: 错误 |
| `GetSet(ctx, key string, val interface{}) (string, error)` | 设置并返回旧值 | oldValue: 旧值, error: 错误 |
| `GetBit(ctx, key string, offset int64) (int64, error)` | 获取位值 | bit: 位值, error: 错误 |
| `MGet[T any](ctx, keys ...string) ([]T, error)` | 批量获取 | values: 值列表, error: 错误 |
| `SetBit(ctx, key string, offset int64, value int) (int64, error)` | 设置位值 | oldBit: 原位值, error: 错误 |
| `SetEX(ctx, key string, val interface{}, timeout int64) error` | 设置键值及过期时间 | error: 设置错误 |
| `Setnx(ctx, key string, val interface{}) (bool, error)` | 只在键不存在时设置 | success: 是否成功, error: 错误 |
| `SetnxExpire(ctx, key string, val interface{}, expire int64) (bool, error)` | 设置键值及过期时间（不存在时） | success: 是否成功, error: 错误 |
| `SetRange(ctx, key string, offset int64, value interface{}) error` | 覆盖部分字符串 | error: 设置错误 |
| `Strlen(ctx, key string) (int64, error)` | 获取字符串长度 | length: 长度, error: 错误 |
| `MSet(ctx, keysAndValues map[string]interface{}) error` | 批量设置 | error: 设置错误 |
| `MSetnx(ctx, keysAndValues map[string]interface{}) (bool, error)` | 批量设置（不存在时） | success: 是否成功, error: 错误 |
| `PSetEX(ctx, key string, val interface{}, timeout int64) error` | 设置键值及毫秒级过期时间 | error: 设置错误 |
| `Incr(ctx, key string) (int64, error)` | 自增 1 | newValue: 新值, error: 错误 |
| `IncrBy(ctx, key string, val int64) (int64, error)` | 自增指定值 | newValue: 新值, error: 错误 |
| `IncrByFloat(ctx, key string, val float64) (float64, error)` | 自增浮点数 | newValue: 新值, error: 错误 |
| `Decr(ctx, key string) (int64, error)` | 自减 1 | newValue: 新值, error: 错误 |
| `DecrBy(ctx, key string, val int64) (int64, error)` | 自减指定值 | newValue: 新值, error: 错误 |
| `Append(ctx, key string, val string) (int64, error)` | 追加字符串 | newLength: 新长度, error: 错误 |

### Hash API

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `HDel(ctx, key string, fields ...string) error` | 删除字段 | error: 删除错误 |
| `HExists(ctx, key, field string) (bool, error)` | 检查字段是否存在 | exists: 是否存在, error: 错误 |
| `HGet[T any](ctx, key, field string) (T, error)` | 获取字段值（泛型） | value: 值, error: 错误 |
| `HGetAll[T any](ctx, key string) (error, map[string]T)` | 获取所有字段 | error: 错误, fields: 字段映射 |
| `HIncrBy(ctx, key, field string, incr int64) (int64, error)` | 字段值自增整数 | newValue: 新值, error: 错误 |
| `HIncrByFloat(ctx, key, field string, incr float64) (float64, error)` | 字段值自增浮点数 | newValue: 新值, error: 错误 |
| `HKeys(ctx, key string) ([]string, error)` | 获取所有字段名 | fields: 字段名列表, error: 错误 |
| `HLen(ctx, key string) int64` | 获取字段数量 | count: 数量 |
| `HMGet[T any](ctx, key string, fields ...string) []T` | 批量获取字段值 | values: 值列表 |
| `HMSet(ctx, key string, fields map[string]interface{}) error` | 批量设置字段 | error: 设置错误 |
| `HSet(ctx, key, field string, val interface{}) error` | 设置字段值 | error: 设置错误 |
| `HSetnx(ctx, key, field string, val interface{}) (bool, error)` | 只在字段不存在时设置 | success: 是否成功, error: 错误 |
| `HVals[T any](ctx, key string) ([]T, error)` | 获取所有字段值 | values: 值列表, error: 错误 |

### List API

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `BLPop[T any](ctx, timeout int, keys ...string) (string, T, error)` | 阻塞式左侧弹出 | key: 键名, value: 值, error: 错误 |
| `BRPop[T any](ctx, timeout int, keys ...string) (string, T, error)` | 阻塞式右侧弹出 | key: 键名, value: 值, error: 错误 |
| `BRPopLPush(ctx, source, destination string, timeout int) (string, error)` | 阻塞式弹出并推入 | value: 值, error: 错误 |
| `LIndex[T any](ctx, key string, index int64) (T, error)` | 通过索引获取元素 | value: 值, error: 错误 |
| `LInsert(ctx, key, before string, pivot, val interface{}) error` | 在元素前后插入 | error: 插入错误 |
| `LLen(ctx, key string) (int64, error)` | 获取列表长度 | length: 长度, error: 错误 |
| `LPop[T any](ctx, key string) (T, error)` | 左侧弹出 | value: 值, error: 错误 |
| `LPush(ctx, key string, vals ...interface{}) error` | 左侧推入 | error: 推入错误 |
| `LPushX(ctx, key string, vals ...interface{}) error` | 左侧推入（列表存在时） | error: 推入错误 |
| `LRange[T any](ctx, key string, start, stop int64) ([]T, error)` | 获取范围元素 | values: 值列表, error: 错误 |
| `LRem(ctx, key string, index int64, val interface{}) (int64, error)` | 移除元素 | count: 移除数量, error: 错误 |
| `LSet(ctx, key string, index int64, val interface{}) error` | 通过索引设置值 | error: 设置错误 |
| `Ltrim(ctx, key string, start, stop int64) error` | 修剪列表 | error: 修剪错误 |
| `RPop[T any](ctx, key string) (T, error)` | 右侧弹出 | value: 值, error: 错误 |
| `RPopLPush[T any](ctx, source, destination string) (T, error)` | 弹出并推入 | value: 值, error: 错误 |
| `RPush(ctx, key string, vals ...any) error` | 右侧推入 | error: 推入错误 |
| `RPushX(ctx, key string, vals ...interface{}) error` | 右侧推入（列表存在时） | error: 推入错误 |

### Set API

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `SAdd(ctx, key string, members ...interface{}) (int64, error)` | 添加成员 | count: 添加数量, error: 错误 |
| `SCard(ctx, key string) int64` | 获取成员数量 | count: 数量 |
| `SDiff[T any](ctx, keys ...string) ([]T, error)` | 差集 | members: 成员列表, error: 错误 |
| `SDiffStore(ctx, destination string, keys ...string) (int64, error)` | 差集并存储 | count: 结果数量, error: 错误 |
| `SInter[T any](ctx, keys ...string) ([]T, error)` | 交集 | members: 成员列表, error: 错误 |
| `SInterStore(ctx, destination string, keys ...string) (int64, error)` | 交集并存储 | count: 结果数量, error: 错误 |
| `SIsMember(ctx, key string, member interface{}) (bool, error)` | 检查成员是否存在 | isMember: 是否存在, error: 错误 |
| `SMembers[T any](ctx, key string) ([]T, error)` | 获取所有成员 | members: 成员列表, error: 错误 |
| `SMove(ctx, source, destination string, member interface{}) (bool, error)` | 移动成员 | success: 是否成功, error: 错误 |
| `SPop[T any](ctx, key string) (T, error)` | 随机弹出成员 | value: 值, error: 错误 |
| `SRandMember[T any](ctx, key string, count int) ([]T, error)` | 随机获取成员 | members: 成员列表, error: 错误 |
| `SRem(ctx, key string, members ...interface{}) (int64, error)` | 移除成员 | count: 移除数量, error: 错误 |
| `SUnion[T any](ctx, keys ...string) ([]T, error)` | 并集 | members: 成员列表, error: 错误 |
| `SUnionStore(ctx, destination string, keys ...string) (int64, error)` | 并集并存储 | count: 结果数量, error: 错误 |

### Sorted Set API

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `ZAdd(ctx, key string, pairs map[interface{}]float64) error` | 添加成员及分数 | error: 添加错误 |
| `ZAddByScore(ctx, key string, member interface{}, score float64) error` | 添加单个成员及分数 | error: 添加错误 |
| `ZCard(ctx, key string) (int64, error)` | 获取成员数量 | count: 数量, error: 错误 |
| `ZCount(ctx, key, min, max string) (int64, error)` | 统计分数范围内的成员数 | count: 数量, error: 错误 |
| `ZScore(ctx, key, member interface{}) (float64, error)` | 获取成员分数 | score: 分数, error: 错误 |
| `ZIncrBy(ctx, key, member interface{}, increment float64) (float64, error)` | 增加成员分数 | newScore: 新分数, error: 错误 |
| `ZInterStore(ctx, destination string, keys ...string) (int64, error)` | 交集并存储 | count: 结果数量, error: 错误 |
| `ZLexCount(ctx, key, min, max string) (int64, error)` | 统计字典区间内的成员数 | count: 数量, error: 错误 |
| `ZRange[T any](ctx, key string, start, stop int64) ([]T, error)` | 按索引范围获取 | members: 成员列表, error: 错误 |
| `ZRangeWithScores(ctx, key string, start, stop int64) ([]redis.Z, error)` | 按索引范围获取（带分数） | results: 结果列表, error: 错误 |
| `ZRangeByLex(ctx, key, min, max string) ([]string, error)` | 按字典范围获取 | members: 成员列表, error: 错误 |
| `ZRangeByScore[T any](ctx, key, min, max string) ([]T, error)` | 按分数范围获取 | members: 成员列表, error: 错误 |
| `ZRangeByScoreWithScores(ctx, key, min, max string) ([]redis.Z, error)` | 按分数范围获取（带分数） | results: 结果列表, error: 错误 |
| `ZRank(ctx, key, member interface{}) (int64, error)` | 获取排名（从低到高） | rank: 排名, error: 错误 |
| `ZRevRank(ctx, key, member interface{}) (int64, error)` | 获取排名（从高到低） | rank: 排名, error: 错误 |
| `ZRevRange[T any](ctx, key string, start, stop int64) ([]T, error)` | 反向按索引范围获取 | members: 成员列表, error: 错误 |
| `ZRevRangeByScore[T any](ctx, key, max, min string) ([]T, error)` | 反向按分数范围获取 | members: 成员列表, error: 错误 |
| `ZRevRangeByScoreWithScores(ctx, key, max, min string) ([]redis.Z, error)` | 反向按分数范围获取（带分数） | results: 结果列表, error: 错误 |
| `ZRem(ctx, key string, members ...interface{}) (int64, error)` | 移除成员 | count: 移除数量, error: 错误 |
| `ZRemRangeByLex(ctx, key, min, max string) (int64, error)` | 按字典范围移除 | count: 移除数量, error: 错误 |
| `ZRemRangeByRank(ctx, key string, start, stop int64) (int64, error)` | 按排名范围移除 | count: 移除数量, error: 错误 |
| `ZRemRangeByScore(ctx, key string, min, max int64) (int64, error)` | 按分数范围移除 | count: 移除数量, error: 错误 |
| `ZUnionStore(ctx, destination string, keys ...string) (int64, error)` | 并集并存储 | count: 结果数量, error: 错误 |

### Stream API

#### 数据结构

```go
type XAddArgs struct {
    Stream string      // 流名称
    MaxLen int64       // 最大长度
    MinID  string      // 最小ID
    Limit  int64       // 限制数量
    ID     string      // 消息ID
    Values interface{} // 消息内容
}

type XReadArgs struct {
    Streams []string      // 流名称列表
    IDs     []string      // ID列表
    Count   int64         // 最大消息数
    Block   time.Duration // 阻塞时间
}

type XReadGroupArgs struct {
    Group    string        // 消费者组名称
    Consumer string        // 消费者名称
    Streams  []string      // 流名称列表
    IDs      []string      // ID列表
    Count    int64         // 最大消息数
    Block    time.Duration // 阻塞时间
    NoAck    bool          // 是否不自动确认
}

type StreamMessage struct {
    ID     string                 // 消息ID
    Values map[string]interface{} // 消息内容
}
```

#### Stream 函数

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `XAdd(ctx, args *XAddArgs) (string, error)` | 添加消息 | msgID: 消息ID, error: 错误 |
| `XDel(ctx, stream string, ids ...string) (int64, error)` | 删除消息 | count: 删除数量, error: 错误 |
| `XLen(ctx, stream string) (int64, error)` | 获取流长度 | length: 长度, error: 错误 |
| `XRange(ctx, stream, start, stop string, count int64) ([]StreamMessage, error)` | 范围查询 | messages: 消息列表, error: 错误 |
| `XRevRange(ctx, stream, start, stop string, count int64) ([]StreamMessage, error)` | 反向范围查询 | messages: 消息列表, error: 错误 |
| `XRead(ctx, args *XReadArgs) (map[string][]StreamMessage, error)` | 读取消息 | streams: 流消息映射, error: 错误 |
| `XTrim(ctx, stream string, maxLen int64) (int64, error)` | 修剪流 | count: 修剪数量, error: 错误 |
| `XTrimApprox(ctx, stream string, maxLen int64) (int64, error)` | 近似修剪流 | count: 修剪数量, error: 错误 |
| `XGroupCreate(ctx, stream, group, id string) error` | 创建消费者组 | error: 错误 |
| `XGroupCreateMkStream(ctx, stream, group, id string) error` | 创建消费者组（创建流） | error: 错误 |
| `XGroupDestroy(ctx, stream, group string) (int64, error)` | 销毁消费者组 | count: 是否成功, error: 错误 |
| `XGroupDelConsumer(ctx, stream, group, consumer string) (int64, error)` | 删除消费者 | count: 移除数量, error: 错误 |
| `XGroupSetID(ctx, stream, group, id string) error` | 设置消费者组ID | error: 错误 |
| `XReadGroup(ctx, args *XReadGroupArgs) (map[string][]StreamMessage, error)` | 从消费者组读取 | streams: 流消息映射, error: 错误 |
| `XAck(ctx, stream, group string, ids ...string) (int64, error)` | 确认消息 | count: 确认数量, error: 错误 |
| `XPending(ctx, args *XPendingArgs) ([]XPendingExt, error)` | 查看待处理消息 | pending: 待处理列表, error: 错误 |
| `XClaim(ctx, stream, group, consumer string, minIdle time.Duration, ids ...string) ([]StreamMessage, error)` | 认领消息 | messages: 消息列表, error: 错误 |
| `XAutoClaim(ctx, args *XAutoClaimArgs) (string, []StreamMessage, error)` | 自动认领消息 | nextID: 下一个ID, messages: 消息列表, error: 错误 |
| `XInfoGroups(ctx, stream string) ([]StreamGroup, error)` | 获取消费者组信息 | groups: 组信息列表, error: 错误 |
| `XInfoConsumers(ctx, stream, group string) ([]StreamConsumer, error)` | 获取消费者信息 | consumers: 消费者列表, error: 错误 |
| `XInfoStream(ctx, stream string) (*XStreamInfo, error)` | 获取流信息 | info: 流信息, error: 错误 |

### 发布订阅 API

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `Publish(ctx, channel string, msg interface{}) error` | 发布消息 | error: 发布错误 |
| `Subscribe(ctx, channel ...string) *redis.PubSub` | 订阅频道 | pubsub: 订阅对象 |
| `PSubscribe(ctx, channel ...string) *redis.PubSub` | 模式订阅 | pubsub: 订阅对象 |
| `ReceiveMessage(ctx, pubSub *redis.PubSub) (*redis.Message, error)` | 接收消息 | message: 消息, error: 错误 |

## 📖 最佳实践

### Key 命名规范

推荐使用冒号分隔的层级结构：

```
{业务}:{模块}:{实体}:{ID}:{属性}

示例：
user:1001:name
user:1001:profile
order:20231201:status
cache:api:user_list
queue:email_tasks
leaderboard:game1
```

**建议：**
- 使用小写字母和数字
- 用冒号分隔层级
- 包含业务前缀便于管理
- 避免特殊字符和空格

### 缓存策略

#### 1. 缓存过期时间

```go
// 根据数据特性设置不同的过期时间
redis.Set(ctx, "session:user:1001", sessionData, 1800)        // 30分钟
redis.Set(ctx, "config:app", appConfig, 86400)                // 24小时
redis.Set(ctx, "counter:daily:views", 0, 86400)               // 每天重置
```

#### 2. 缓存穿透保护

```go
// 使用 Setnx 实现互斥锁
func GetUserWithCache(userID int) (*User, error) {
    key := fmt.Sprintf("user:%d", userID)
    
    // 尝试从缓存获取
    userJSON, err := redis.Get[string](ctx, key)
    if err == nil {
        var user User
        json.Unmarshal([]byte(userJSON), &user)
        return &user, nil
    }
    
    // 缓存未命中，使用分布式锁防止缓存穿透
    lockKey := key + ":lock"
    locked, _ := redis.SetnxExpire(ctx, lockKey, "1", 10)
    if locked {
        defer redis.Delete(ctx, lockKey)
        
        // 从数据库加载
        user := loadFromDB(userID)
        
        // 写入缓存
        userData, _ := json.Marshal(user)
        redis.Set(ctx, key, string(userData), 3600)
        
        return user, nil
    }
    
    // 等待后重试
    time.Sleep(100 * time.Millisecond)
    return GetUserWithCache(userID)
}
```

#### 3. 缓存更新策略

```go
// Cache-Aside 模式（推荐）
func UpdateUser(user *User) error {
    // 1. 更新数据库
    if err := db.Update(user); err != nil {
        return err
    }
    
    // 2. 删除缓存（而非更新）
    redis.Delete(ctx, fmt.Sprintf("user:%d", user.ID))
    
    return nil
}
```

### 性能优化

#### 1. 批量操作

```go
// 使用 MSet 代替多次 Set
data := make(map[string]interface{})
for i := 1; i <= 100; i++ {
    data[fmt.Sprintf("user:%d", i)] = getUserData(i)
}
redis.MSet(ctx, data)

// 使用 Pipeline（需要原生客户端）
pipe := redis.GetClient().Pipeline()
for i := 1; i <= 100; i++ {
    pipe.Set(ctx, fmt.Sprintf("key:%d", i), i, 0)
}
pipe.Exec(ctx)
```

#### 2. 合理设置过期时间

```go
// 避免大量 key 同时过期（缓存雪崩）
baseTTL := 3600
jitter := rand.Intn(300) // 0-300秒随机抖动
redis.Set(ctx, key, value, int64(baseTTL+jitter))
```

#### 3. 使用合适的数据结构

```go
// 计数器：使用 String 的 Incr
redis.Incr(ctx, "counter:page_views")

// 排行榜：使用 Sorted Set
redis.ZAdd(ctx, "leaderboard", map[interface{}]float64{"player1": 100})

// 好友关系：使用 Set
redis.SAdd(ctx, "user:1001:friends", 1002, 1003, 1004)

// 最新消息列表：使用 List + Ltrim
redis.LPush(ctx, "user:1001:messages", msg)
redis.Ltrim(ctx, "user:1001:messages", 0, 99) // 只保留最新100条
```

### 内存管理

#### 1. 监控内存使用

```go
// 定期检查 Redis 内存
info := client.Info(ctx, "memory")
fmt.Println(info.Val())
```

#### 2. 及时清理无用数据

```go
// 设置合理的过期时间
redis.Set(ctx, "temp:data", data, 300) // 5分钟

// 使用 Stream 的自动修剪
redis.XAdd(ctx, &redis.XAddArgs{
    Stream: "logs",
    MaxLen: 10000,
    Values: logData,
})
```

#### 3. 避免大 Key

```go
// ❌ 避免存储超大 Hash
redis.HMSet(ctx, "huge_hash", hugeMap) // 可能包含百万级字段

// ✅ 拆分成多个小 Hash
for i, chunk := range chunks {
    redis.HMSet(ctx, fmt.Sprintf("data:chunk:%d", i), chunk)
}
```

## ❓ 常见问题

### Q1: 如何处理 Redis.Nil 错误？

```go
value, err := redis.Get[string](ctx, "key")
if err != nil {
    if err == redis.Nil {
        // Key 不存在
        fmt.Println("Key not found")
        return nil
    }
    // 其他错误
    log.Printf("Error: %v", err)
    return err
}
```

### Q2: 如何实现分布式锁？

参考上面的中的"分布式锁"示例。关键点：
- 使用 `SetnxExpire` 保证原子性
- 设置合理的超时时间防止死锁
- 释放锁时验证所有者

### Q3: Stream 和 List 作为队列的区别？

| 特性 | Stream | List |
|------|--------|------|
| 消息确认 | ✅ 支持 XAck | ❌ 不支持 |
| 消费者组 | ✅ 支持 | ❌ 不支持 |
| 消息持久化 | ✅ 永久保存 | ✅ 永久保存 |
| 消息回溯 | ✅ 可以 | ❌ 弹出即删除 |
| 复杂度 | 较高 | 简单 |
| 适用场景 | 可靠消息队列 | 简单任务队列 |

**建议：**
- 需要可靠投递：使用 Stream
- 简单任务队列：使用 List + BLPop

### Q4: 如何批量操作提高效率？

```go
// 1. 使用 MSet/MGet
redis.MSet(ctx, data)
values := redis.MGet[string](ctx, keys...)

// 2. 使用 Pipeline（需要获取原生客户端）
pipe := client.Pipeline()
for _, cmd := range commands {
    pipe.Exec(ctx, cmd)
}
results, _ := pipe.Exec(ctx)
```

### Q5: 如何处理连接超时？

```go
// InitRedisCache 已经内置了 10 秒超时检测
err := redis.InitRedisCache(&redis.Config{
    Host: "127.0.0.1:6379",
})
if err != nil {
    log.Fatalf("Connection failed: %v", err)
}

// 如果运行中出现超时，检查：
// 1. Redis 服务器是否正常
// 2. 网络是否通畅
// 3. 防火墙配置
// 4. Redis 最大连接数
```

### Q6: Key 前缀如何工作？

```go
// 配置前缀
redis.InitRedisCache(&redis.Config{
    Prefix: "myapp",
})

// 所有操作自动添加前缀
redis.Set(ctx, "user:1001", "John", 3600)
// 实际操作的 key 是 "myapp:user:1001"

// 如果需要跳过前缀（特殊场景）
redis.SetForNoPrefix(ctx, "global:key", "value", 3600)
```

### Q7: 泛型支持哪些类型？

支持所有可 JSON 序列化的类型：
- 基本类型：`string`, `int`, `int64`, `float64`, `bool`
- 复合类型：`[]byte`, `[]string`, `[]int` 等切片
- 复杂类型：`struct`, `map[string]interface{}` 等

```go
// 基本类型
name, _ := redis.Get[string](ctx, "name")
age, _ := redis.Get[int](ctx, "age")

// 结构体
var user User
userJSON, _ := redis.Get[string](ctx, "user")
json.Unmarshal([]byte(userJSON), &user)

// 列表
scores, _ := redis.LRange[float64](ctx, "scores", 0, -1)
```

### Q8: 如何监控 Redis 状态？

```go
// 获取统计信息
info := client.Info(ctx)
fmt.Println(info.Val())

// 获取特定部分
memoryInfo := client.Info(ctx, "memory")
serverInfo := client.Info(ctx, "server")

// 监控 Stream 消费者组
groups, _ := redis.XInfoGroups(ctx, "events")
for _, g := range groups {
    fmt.Printf("Group: %s, Pending: %d\n", g.Name, g.Pending)
}
```

## 📦 依赖

```go
require github.com/go-redis/redis/v8 v8.11.5
```

**间接依赖：**
- github.com/cespare/xxhash/v2 v2.1.2
- github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f

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

- [Redis 官方文档](https://redis.io/documentation)
- [go-redis/redis](https://github.com/go-redis/redis)
- [Redis 命令参考](https://redis.io/commands/)

---

**注意**：使用前请确保已部署并运行 Redis 服务器。推荐使用 Redis 6.x 或更高版本以支持完整的 Stream 功能。

**版本信息**：
- 当前版本：v1.0.0
- Redis SDK 版本：v8.11.5
- Go 版本要求：1.24.2+ 