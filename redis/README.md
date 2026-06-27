# Redis Utility Library

一个基于 [go-redis/redis/v8](https://github.com/go-redis/redis) 的 Go 语言 Redis 客户端封装库，提供了简洁易用的 API 来操作 Redis 的各种数据结构。

## 特性

- 🚀 支持 Redis 主要数据结构：String、Hash、List、Set、Sorted Set、Stream
- 🔧 提供统一的初始化接口和配置管理
- 📦 内置 JSON 序列化/反序列化支持
- 🎯 泛型支持，类型安全的数据获取
- ⏰ 内置过期时间管理
- 📡 支持发布/订阅模式
- 🌊 完整的 Redis Stream 支持，包括消费者组和消息确认

## 安装

```bash
go get github.com/XingMenTech/utils/redis
```

## 快速开始

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/XingMenTech/utils/redis"
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
        panic(err)
    }

    // 使用 String 操作
    redis.Set("user:1", "John Doe", 3600) // 设置键值对，过期时间 1 小时
    name, _ := redis.Get[string]("user:1")
    fmt.Println(name) // 输出: John Doe

    // 使用 Hash 操作
    redis.HSet("user:profile:1", "name", "John Doe")
    redis.HSet("user:profile:1", "age", 25)
    profileName, _ := redis.HGet[string]("user:profile:1", "name")
    fmt.Println(profileName) // 输出: John Doe

    // 使用 Stream 操作
    ctx := context.Background()
    
    // 添加消息到流
    msgID, err := redis.XAdd(ctx, &redis.XAddArgs{
        Stream: "events",
        Values: map[string]interface{}{
            "event": "user_login",
            "user_id": "123",
            "timestamp": time.Now().Unix(),
        },
    })
    if err != nil {
        panic(err)
    }
    fmt.Println("Message ID:", msgID)
    
    // 创建消费者组
    err = redis.XGroupCreate(ctx, "events", "event_processors", "0")
    if err != nil {
        fmt.Println("Group may already exist:", err)
    }
    
    // 从消费者组读取消息
    result, err := redis.XReadGroup(ctx, &redis.XReadGroupArgs{
        Group:    "event_processors",
        Consumer: "worker_1",
        Streams:  []string{"events"},
        IDs:      []string{">"}, // 只读取新消息
        Count:    10,
        Block:    5 * time.Second,
    })
    if err != nil {
        panic(err)
    }
    
    for stream, messages := range result {
        fmt.Printf("Stream: %s, Messages: %d\n", stream, len(messages))
        for _, msg := range messages {
            fmt.Printf("  Message ID: %s, Values: %v\n", msg.ID, msg.Values)
            // 处理消息后确认
            redis.XAck(ctx, stream, "event_processors", msg.ID)
        }
    }
}
```

## API 概览

### 核心功能

- `InitRedisCache(config *Config)` - 初始化 Redis 连接
- `IsExist(key string)` - 检查键是否存在
- `Delete(key string)` - 删除键
- `ClearAll()` - 清空所有数据
- `ExpireAt(key string, t time.Time)` - 设置过期时间点
- `ExpireIn(key string, d time.Duration)` - 设置过期时长

### String 操作

- `Set(key string, val interface{}, timeout int64)` - 设置键值对
- `Get[T any](key string)` - 获取值（泛型支持）
- `MGet[T any](keys ...string)` - 批量获取
- `MSet(keysAndValues map[string]interface{})` - 批量设置
- `Incr(key string)` / `Decr(key string)` - 自增/自减
- `Append(key string, val string)` - 追加字符串

### Hash 操作

- `HSet(key string, field string, val interface{})` - 设置哈希字段
- `HGet[T any](key string, field string)` - 获取哈希字段值
- `HMSet(key string, fields map[string]interface{})` - 批量设置哈希字段
- `HGetAll[T any](key string)` - 获取所有哈希字段
- `HDel(key string, fields ...string)` - 删除哈希字段
- `HExists(key, field string)` - 检查哈希字段是否存在

### List 操作

- `LPush(key string, vals ...interface{})` - 左侧推入
- `RPush(key string, vals ...any)` - 右侧推入
- `LPop[T any](key string)` - 左侧弹出
- `RPop[T any](key string)` - 右侧弹出
- `LRange[T any](key string, start, stop int64)` - 获取范围元素
- `LLen(key string)` - 获取列表长度

### Set 操作

- `SAdd(key string, members ...interface{})` - 添加成员
- `SMembers[T any](key string)` - 获取所有成员
- `SRem(key string, members ...interface{})` - 移除成员
- `SCard(key string)` - 获取成员数量
- `SIsMember(key string, member interface{})` - 检查成员是否存在
- `SInter[T any](keys ...string)` - 交集操作

### Sorted Set 操作

- `ZAdd(key string, pairs map[interface{}]float64)` - 添加成员及分数
- `ZRange[T any](key string, start, stop int64)` - 按索引范围获取
- `ZRangeByScore[T any](key, min, max string)` - 按分数范围获取
- `ZRem(key string, members ...interface{})` - 移除成员
- `ZCard(key string)` - 获取成员数量
- `ZRank(key, member interface{})` - 获取排名

### 发布/订阅

- `Publish(channel string, msg interface{})` - 发布消息
- `Subscribe(channel ...string)` - 订阅频道
- `PSubscribe(channel ...string)` - 模式订阅

### Stream 操作

- `XAdd(args *XAddArgs)` - 向流中添加消息
- `XRead(args *XReadArgs)` - 读取流中的消息
- `XDel(stream string, ids ...string)` - 删除流中的消息
- `XLen(stream string)` - 获取流的长度
- `XRange(stream, start, stop string, count int64)` - 范围查询消息
- `XRevRange(stream, start, stop string, count int64)` - 反向范围查询
- `XTrim(stream string, maxLen int64)` - 修剪流
- `XGroupCreate(stream, group, id string)` - 创建消费者组
- `XGroupDestroy(stream, group string)` - 销毁消费者组
- `XReadGroup(args *XReadGroupArgs)` - 从消费者组读取消息
- `XAck(stream, group string, ids ...string)` - 确认消息处理
- `XPending(args *XPendingArgs)` - 查看待处理消息
- `XClaim(stream, group, consumer string, minIdle time.Duration, ids ...string)` - 认领消息
- `XAutoClaim(args *XAutoClaimArgs)` - 自动认领消息
- `XInfoGroups(stream string)` - 获取消费者组信息
- `XInfoConsumers(stream, group string)` - 获取消费者信息

## 配置说明

```go
type Config struct {
    Prefix   string // KEY前缀，用于命名空间隔离
    Host     string // Redis 主机地址 (host:port)
    Password string // 密码（可选）
    DbNum    int    // 数据库编号 (默认 0)
}
```

## 泛型支持

本库充分利用 Go 泛型特性，提供类型安全的数据获取：

```go
// 自动反序列化为指定类型
user, err := redis.Get[User]("user:1")
profiles, err := redis.HGetAll[Profile]("users:profiles")
items, err := redis.LRange[string]("mylist", 0, -1)
```

## 测试

运行测试前请确保本地 Redis 服务正在运行：

```bash
go test -v
```

## 依赖

- [go-redis/redis/v8](https://github.com/go-redis/redis) - Redis Go 客户端
- Go 1.22.10+

## License

MIT License