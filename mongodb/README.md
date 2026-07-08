# MongoDB 客户端工具库

[![Go Version](https://img.shields.io/badge/go-1.24.2+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![MongoDB Driver](https://img.shields.io/badge/mongo--driver-v2-blue.svg)](https://github.com/mongodb/mongo-go-driver)

一个基于 [MongoDB Go Driver v2](https://github.com/mongodb/mongo-go-driver) 封装的高可用 Go 语言 MongoDB 客户端工具库，提供简洁易用的 CRUD API、连接池管理、事务支持等完整功能。

## 📋 目录

- [项目简介](#项目简介)
- [主要特性](#主要特性)
- [安装](#安装)
- [快速开始](#快速开始)
  - [基础初始化](#基础初始化)
  - [配置加载](#配置加载)
- [核心功能](#核心功能)
  - [基础 CRUD 操作](#基础-crud-操作)
  - [批量操作](#批量操作)
  - [高级查询](#高级查询)
  - [索引管理](#索引管理)
  - [事务支持](#事务支持)
  - [聚合查询](#聚合查询)
- [高可用特性](#高可用特性)
  - [连接池管理](#连接池管理)
  - [副本集支持](#副本集支持)
  - [TLS/SSL 加密](#tlsssl-加密)
  - [自动重连](#自动重连)
- [配置说明](#配置说明)
  - [Config 配置结构](#config-配置结构)
  - [配置参数详解](#配置参数详解)
- [使用示例](#使用示例)
  - [单文档操作](#单文档操作)
  - [批量操作](#批量操作-1)
  - [分页查询](#分页查询)
  - [事务处理](#事务处理)
  - [索引管理](#索引管理-1)
- [API 参考](#api-参考)
  - [初始化和通用 API](#初始化和通用-api)
  - [CRUD API](#crud-api)
  - [索引管理 API](#索引管理-api)
  - [事务 API](#事务-api)
- [最佳实践](#最佳实践)
  - [连接池调优](#连接池调优)
  - [索引优化](#索引优化)
  - [查询优化](#查询优化)
- [常见问题](#常见问题)
- [依赖](#依赖)
- [许可证](#许可证)

## 📖 项目简介

本项目是对 MongoDB Go Driver v2 的二次封装，旨在简化 MongoDB 在 Go 项目中的使用。提供了以下核心功能：

- **完整的 CRUD 操作**：插入、查询、更新、删除
- **批量操作支持**：批量插入、更新、删除
- **高级查询功能**：分页、排序、投影、聚合
- **索引管理**：创建、删除、列出索引
- **事务支持**：支持多文档 ACID 事务
- **高可用特性**：连接池、副本集、自动重连
- **类型安全**：支持泛型和 BSON 编解码

## ✨ 主要特性

- ✅ **简洁的 API**：封装复杂的 MongoDB Driver，提供简单易用的接口
- ✅ **完整的 CRUD**：支持所有基本的数据库操作
- ✅ **批量操作**：高效的批量数据处理
- ✅ **高级查询**：分页、排序、聚合、投影
- ✅ **索引管理**：完整的索引生命周期管理
- ✅ **事务支持**：支持多文档 ACID 事务
- ✅ **连接池**：可配置的连接池管理
- ✅ **高可用**：支持副本集和自动故障转移
- ✅ **安全性**：支持 TLS/SSL 加密连接
- ✅ **全局客户端**：支持单例模式的全局客户端

## 🚀 安装

```bash
go get github.com/xm-utils/tools/mongodb
```

### 依赖要求

- Go 1.24.2+
- go.mongodb.org/mongo-driver/v2

### 前置条件

使用前请确保已部署并运行 MongoDB 服务器。推荐使用 MongoDB 4.4 或更高版本以支持完整的事务功能。

## 🎯 快速开始

### 基础初始化

```go
package main

import (
    "context"
    "fmt"
    "log"
    "github.com/xm-utils/tools/mongodb"
)

func main() {
    // 创建配置
    cfg := &mongodb.Config{
        URI:      "mongodb://localhost:27017",
        Database: "test_db",
    }

    // 初始化客户端
    client, err := mongodb.NewClient(cfg)
    if err != nil {
        log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close(context.Background())

    // 测试连接
    ctx := context.Background()
    if err := client.Ping(ctx); err != nil {
        log.Fatalf("Failed to ping MongoDB: %v", err)
    }

    fmt.Println("Connected to MongoDB successfully")
}
```

### 全局客户端模式

```go
package main

import (
    "context"
    "fmt"
    "log"
    "github.com/xm-utils/tools/mongodb"
)

func main() {
    // 初始化全局客户端
    cfg := &mongodb.Config{
        URI:      "mongodb://localhost:27017",
        Database: "test_db",
    }

    if err := mongodb.InitClient(cfg); err != nil {
        log.Fatalf("Failed to init client: %v", err)
    }

    // 获取全局客户端
    client := mongodb.GetClient()
    defer client.Close(context.Background())

    fmt.Println("Global client initialized")
}
```

## 🔧 核心功能

### 基础 CRUD 操作

#### 插入文档

```go
ctx := context.Background()

// 定义数据结构
type User struct {
    Name  string `bson:"name"`
    Email string `bson:"email"`
    Age   int    `bson:"age"`
}

// 插入单个文档
user := User{
    Name:  "张三",
    Email: "zhangsan@example.com",
    Age:   25,
}

result, err := client.InsertOne(ctx, "users", user)
if err != nil {
    log.Printf("Insert failed: %v", err)
} else {
    fmt.Printf("Inserted ID: %v\n", result.InsertedID)
}

// 批量插入
var users []interface{}
for i := 1; i <= 10; i++ {
    users = append(users, User{
        Name:  fmt.Sprintf("用户%d", i),
        Email: fmt.Sprintf("user%d@example.com", i),
        Age:   20 + i,
    })
}

insertResult, err := client.InsertMany(ctx, "users", users)
if err != nil {
    log.Printf("Batch insert failed: %v", err)
} else {
    fmt.Printf("Inserted %d documents\n", len(insertResult.InsertedIDs))
}
```

#### 查询文档

```go
ctx := context.Background()

// 查询单个文档
var user User
err := client.FindOne(ctx, "users", bson.M{"name": "张三"}, &user)
if err != nil {
    if err.Error() == "document not found" {
        fmt.Println("User not found")
    } else {
        log.Printf("Find failed: %v", err)
    }
} else {
    fmt.Printf("Found user: %+v\n", user)
}

// 根据 ID 查询
var userByID User
err = client.FindByID(ctx, "users", objectID, &userByID)

// 查询多个文档
results, err := client.Find(ctx, "users", bson.M{"age": bson.M{"$gte": 25}})
if err != nil {
    log.Printf("Find failed: %v", err)
} else {
    fmt.Printf("Found %d users\n", len(results))
}

// 查询并解码到指定类型
var users []User
err = client.FindWithDecode(ctx, "users", bson.M{}, &users)
```

#### 更新文档

```go
ctx := context.Background()

// 更新单个文档
updateResult, err := client.UpdateOne(
    ctx,
    "users",
    bson.M{"name": "张三"},
    bson.M{"$set": bson.M{"age": 26}},
)
if err != nil {
    log.Printf("Update failed: %v", err)
} else {
    fmt.Printf("Modified count: %d\n", updateResult.ModifiedCount)
}

// 更新多个文档
updateManyResult, err := client.UpdateMany(
    ctx,
    "users",
    bson.M{"age": bson.M{"$lt": 25}},
    bson.M{"$set": bson.M{"updated_at": time.Now()}},
)

// 根据 ID 更新
updateResult, err = client.UpdateByID(ctx, "users", objectID, 
    bson.M{"$set": bson.M{"email": "newemail@example.com"}})

// Upsert（插入或更新）
upsertResult, err := client.UpsertOne(
    ctx,
    "users",
    bson.M{"email": "zhangsan@example.com"},
    bson.M{"$set": bson.M{"name": "张三", "age": 25}},
)
```

#### 删除文档

```go
ctx := context.Background()

// 删除单个文档
deleteResult, err := client.DeleteOne(ctx, "users", bson.M{"name": "张三"})
if err != nil {
    log.Printf("Delete failed: %v", err)
} else {
    fmt.Printf("Deleted count: %d\n", deleteResult.DeletedCount)
}

// 删除多个文档
deleteManyResult, err := client.DeleteMany(
    ctx,
    "users",
    bson.M{"age": bson.M{"$gt": 30}},
)

// 根据 ID 删除
deleteResult, err = client.DeleteByID(ctx, "users", objectID)
```

### 批量操作

```go
ctx := context.Background()

// 批量写操作
models := []mongo.WriteModel{
    mongo.NewInsertOneModel().SetDocument(User{Name: "用户1", Age: 20}),
    mongo.NewInsertOneModel().SetDocument(User{Name: "用户2", Age: 25}),
    mongo.NewUpdateOneModel().
        SetFilter(bson.M{"name": "用户1"}).
        SetUpdate(bson.M{"$set": bson.M{"age": 21}}),
    mongo.NewDeleteOneModel().SetFilter(bson.M{"name": "旧用户"}),
}

bulkResult, err := client.BulkWrite(ctx, "users", models)
if err != nil {
    log.Printf("Bulk write failed: %v", err)
} else {
    fmt.Printf("Inserted: %d, Modified: %d, Deleted: %d\n",
        bulkResult.InsertedCount,
        bulkResult.ModifiedCount,
        bulkResult.DeletedCount)
}
```

### 高级查询

#### 分页查询

```go
ctx := context.Background()

// 分页查询
page := int64(1)
pageSize := int64(10)
results, total, err := client.FindWithPagination(ctx, "users", bson.M{}, page, pageSize)
if err != nil {
    log.Printf("Pagination failed: %v", err)
} else {
    fmt.Printf("Total: %d, Page: %d, Results: %d\n", total, page, len(results))
}
```

#### 排序和投影

```go
ctx := context.Background()

// 排序查询
opts := options.Find().
    SetSort(bson.M{"age": -1}).  // 按年龄降序
    SetLimit(10)                  // 限制 10 条

topUsers, err := client.FindWithOptions(ctx, "users", bson.M{}, opts)

// 投影查询（只返回指定字段）
projOpts := options.Find().
    SetProjection(bson.M{"name": 1, "email": 1, "_id": 0}).
    SetLimit(10)

projectedUsers, err := client.FindWithOptions(ctx, "users", bson.M{}, projOpts)
```

#### 统计查询

```go
ctx := context.Background()

// 统计文档数量
count, err := client.CountDocuments(ctx, "users", bson.M{"age": bson.M{"$gte": 25}})
if err != nil {
    log.Printf("Count failed: %v", err)
} else {
    fmt.Printf("Users over 25: %d\n", count)
}

// 估算文档数量（更快）
estimatedCount, err := client.EstimatedDocumentCount(ctx, "users")

// Distinct 查询
distinctAges, err := client.Distinct(ctx, "users", "age", bson.M{})
fmt.Printf("Distinct ages: %v\n", distinctAges)
```

### 聚合查询

```go
ctx := context.Background()

// 聚合管道
pipeline := mongo.Pipeline{
    {{Key: "$match", Value: bson.M{"age": bson.M{"$gte": 25}}}},
    {{Key: "$group", Value: bson.M{
        "_id":    bson.M{"$substr": bson.A{"$name", 0, 1}},
        "count":  bson.M{"$sum": 1},
        "avgAge": bson.M{"$avg": "$age"},
    }}},
    {{Key: "$sort", Value: bson.M{"count": -1}}},
}

results, err := client.Aggregate(ctx, "users", pipeline)
if err != nil {
    log.Printf("Aggregation failed: %v", err)
} else {
    for _, result := range results {
        fmt.Printf("%+v\n", result)
    }
}

// 聚合并解码到指定类型
type AggResult struct {
    Letter string  `bson:"_id"`
    Count  int     `bson:"count"`
    AvgAge float64 `bson:"avgAge"`
}

var aggResults []AggResult
err = client.AggregateWithDecode(ctx, "users", pipeline, &aggResults)
```

### 索引管理

```go
ctx := context.Background()

// 创建唯一索引
indexModel := mongo.IndexModel{
    Keys: bson.M{"email": 1},
    Options: options.Index().
        SetUnique(true).
        SetName("idx_email"),
}

indexName, err := client.CreateIndex(ctx, "users", indexModel)

// 创建复合索引
compoundIndex := mongo.IndexModel{
    Keys: bson.M{"age": 1, "name": 1},
    Options: options.Index().SetName("idx_age_name"),
}

_, err = client.CreateIndex(ctx, "users", compoundIndex)

// 创建 TTL 索引（自动过期）
ttlIndex := mongo.IndexModel{
    Keys: bson.M{"expire_at": 1},
    Options: options.Index().
        SetExpireAfterSeconds(3600).
        SetName("idx_expire"),
}

_, err = client.CreateIndex(ctx, "sessions", ttlIndex)

// 列出所有索引
indexes, err := client.ListIndexes(ctx, "users")
for _, idx := range indexes {
    fmt.Printf("Index: %v\n", idx)
}

// 删除索引
err = client.DropIndex(ctx, "users", "idx_age_name")

// 删除所有索引
err = client.DropAllIndexes(ctx, "users")
```

### 事务支持

```go
ctx := context.Background()

// 在事务中执行多个操作（需要副本集）
result, err := client.WithTransaction(ctx, func(sc mongo.SessionContext) (interface{}, error) {
    // 操作 1: 插入用户
    user := User{
        Name:  "事务用户",
        Email: "transaction@example.com",
        Age:   30,
    }
    _, err := client.InsertOne(sc, "users", user)
    if err != nil {
        return nil, err
    }

    // 操作 2: 更新统计信息
    _, err = client.UpdateOne(
        sc,
        "statistics",
        bson.M{"_id": "user_count"},
        bson.M{"$inc": bson.M{"count": 1}},
    )
    if err != nil {
        return nil, err
    }

    return "success", nil
})

if err != nil {
    log.Printf("Transaction failed: %v", err)
} else {
    fmt.Printf("Transaction result: %v\n", result)
}
```

## 🛡️ 高可用特性

### 连接池管理

```go
cfg := &mongodb.Config{
    URI:                  "mongodb://localhost:27017",
    Database:             "test_db",
    MaxPoolSize:          100,  // 最大连接数
    MinPoolSize:          10,   // 最小连接数
    ConnectTimeout:       10 * time.Second,
    SocketTimeout:        30 * time.Second,
    ServerSelectionTimeout: 30 * time.Second,
    HeartbeatInterval:    10 * time.Second,
}

client, err := mongodb.NewClient(cfg)
```

### 副本集支持

```go
cfg := &mongodb.Config{
    Hosts: []string{
        "mongo1.example.com:27017",
        "mongo2.example.com:27017",
        "mongo3.example.com:27017",
    },
    Database:   "test_db",
    Username:   "admin",
    Password:   "password",
    AuthSource: "admin",
    ReplicaSet: "myReplicaSet",
}

client, err := mongodb.NewClient(cfg)
```

### TLS/SSL 加密

```go
cfg := &mongodb.Config{
    URI:         "mongodb://localhost:27017",
    Database:    "test_db",
    TLS:         true,
    TLSCAFile:   "/path/to/ca.pem",
    TLSCertFile: "/path/to/client-cert.pem",
    TLSKeyFile:  "/path/to/client-key.pem",
}

client, err := mongodb.NewClient(cfg)
```

### 自动重连

MongoDB Go Driver 内置了自动重连机制。通过配置以下参数可以优化重连行为：

```go
cfg := &mongodb.Config{
    URI:                  "mongodb://localhost:27017",
    MaxPoolSize:          100,
    ConnectTimeout:       10 * time.Second,
    SocketTimeout:        30 * time.Second,
    ServerSelectionTimeout: 30 * time.Second,  // 服务器选择超时
    HeartbeatInterval:    10 * time.Second,     // 心跳检测间隔
}
```

## ⚙️ 配置说明

### Config 配置结构

```go
type Config struct {
    URI                    string        // MongoDB 连接字符串
    Hosts                  []string      // 服务器地址列表
    Database               string        // 默认数据库名
    Username               string        // 用户名
    Password               string        // 密码
    AuthSource             string        // 认证数据库
    MaxPoolSize            uint64        // 最大连接池大小
    MinPoolSize            uint64        // 最小连接池大小
    ConnectTimeout         time.Duration // 连接超时时间
    SocketTimeout          time.Duration // Socket 超时时间
    ServerSelectionTimeout time.Duration // 服务器选择超时时间
    HeartbeatInterval      time.Duration // 心跳检测间隔
    ReplicaSet             string        // 副本集名称
    Direct                 bool          // 是否直连
    TLS                    bool          // 是否启用 TLS/SSL
    TLSCAFile              string        // CA 证书文件
    TLSCertFile            string        // 客户端证书文件
    TLSKeyFile             string        // 客户端私钥文件
}
```

### 配置参数详解

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| URI | string | 是* | - | MongoDB 连接字符串，与 Hosts 二选一 |
| Hosts | []string | 是* | - | 服务器地址列表，与 URI 二选一 |
| Database | string | 否 | "" | 默认数据库名 |
| Username | string | 否 | "" | 用户名 |
| Password | string | 否 | "" | 密码 |
| AuthSource | string | 否 | "admin" | 认证数据库 |
| MaxPoolSize | uint64 | 否 | 100 | 最大连接池大小 |
| MinPoolSize | uint64 | 否 | 0 | 最小连接池大小 |
| ConnectTimeout | time.Duration | 否 | 10s | 连接超时时间 |
| SocketTimeout | time.Duration | 否 | 30s | Socket 超时时间 |
| ServerSelectionTimeout | time.Duration | 否 | 30s | 服务器选择超时时间 |
| HeartbeatInterval | time.Duration | 否 | 10s | 心跳检测间隔 |
| ReplicaSet | string | 否 | "" | 副本集名称 |
| Direct | bool | 否 | false | 是否直连模式 |
| TLS | bool | 否 | false | 是否启用 TLS/SSL |
| TLSCAFile | string | 否 | "" | CA 证书文件路径 |
| TLSCertFile | string | 否 | "" | 客户端证书文件路径 |
| TLSKeyFile | string | 否 | "" | 客户端私钥文件路径 |

\* URI 和 Hosts 至少需要提供一个

## 💡 使用示例

### 单文档操作

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    "github.com/xm-utils/tools/mongodb"
    "go.mongodb.org/mongo-driver/v2/bson"
)

type Product struct {
    ID        string    `bson:"_id,omitempty"`
    Name      string    `bson:"name"`
    Price     float64   `bson:"price"`
    Stock     int       `bson:"stock"`
    CreatedAt time.Time `bson:"created_at"`
}

func main() {
    cfg := &mongodb.Config{
        URI:      "mongodb://localhost:27017",
        Database: "shop",
    }

    client, err := mongodb.NewClient(cfg)
    if err != nil {
        log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close(context.Background())

    ctx := context.Background()

    // 插入商品
    product := Product{
        Name:      "iPhone 15",
        Price:     7999.00,
        Stock:     100,
        CreatedAt: time.Now(),
    }

    result, err := client.InsertOne(ctx, "products", product)
    if err != nil {
        log.Printf("Insert failed: %v", err)
        return
    }
    fmt.Printf("Product inserted with ID: %v\n", result.InsertedID)

    // 查询商品
    var found Product
    err = client.FindOne(ctx, "products", bson.M{"name": "iPhone 15"}, &found)
    if err != nil {
        log.Printf("Find failed: %v", err)
    } else {
        fmt.Printf("Found product: %+v\n", found)
    }

    // 更新库存
    _, err = client.UpdateOne(
        ctx,
        "products",
        bson.M{"name": "iPhone 15"},
        bson.M{"$set": bson.M{"stock": 95}},
    )

    // 删除商品
    _, err = client.DeleteOne(ctx, "products", bson.M{"name": "iPhone 15"})
}
```

### 分页查询

```go
package main

import (
    "context"
    "fmt"
    "log"
    "github.com/xm-utils/tools/mongodb"
    "go.mongodb.org/mongo-driver/v2/bson"
)

func main() {
    cfg := &mongodb.Config{
        URI:      "mongodb://localhost:27017",
        Database: "blog",
    }

    client, err := mongodb.NewClient(cfg)
    if err != nil {
        log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close(context.Background())

    ctx := context.Background()

    // 分页查询文章
    page := int64(1)
    pageSize := int64(20)
    
    articles, total, err := client.FindWithPagination(
        ctx,
        "articles",
        bson.M{"status": "published"},
        page,
        pageSize,
    )
    if err != nil {
        log.Printf("Query failed: %v", err)
        return
    }

    fmt.Printf("Total articles: %d\n", total)
    fmt.Printf("Current page: %d, Articles: %d\n", page, len(articles))

    // 显示文章
    for i, article := range articles {
        fmt.Printf("%d. %v\n", i+1, article["title"])
    }
}
```

### 事务处理

```go
package main

import (
    "context"
    "fmt"
    "log"
    "github.com/xm-utils/tools/mongodb"
    "go.mongodb.org/mongo-driver/v2/bson"
    "go.mongodb.org/mongo-driver/v2/mongo"
)

type Order struct {
    OrderID   string  `bson:"order_id"`
    UserID    string  `bson:"user_id"`
    Amount    float64 `bson:"amount"`
    Status    string  `bson:"status"`
}

func main() {
    cfg := &mongodb.Config{
        URI:        "mongodb://localhost:27017/?replicaSet=myReplicaSet",
        Database:   "ecommerce",
        MaxPoolSize: 50,
    }

    client, err := mongodb.NewClient(cfg)
    if err != nil {
        log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close(context.Background())

    ctx := context.Background()

    // 创建订单事务
    order := Order{
        OrderID: "ORD123456",
        UserID:  "USER001",
        Amount:  299.99,
        Status:  "pending",
    }

    result, err := client.WithTransaction(ctx, func(sc mongo.SessionContext) (interface{}, error) {
        // 1. 创建订单
        _, err := client.InsertOne(sc, "orders", order)
        if err != nil {
            return nil, fmt.Errorf("failed to create order: %w", err)
        }

        // 2. 扣减库存
        _, err = client.UpdateOne(
            sc,
            "products",
            bson.M{"product_id": "PROD001", "stock": bson.M{"$gte": 1}},
            bson.M{"$inc": bson.M{"stock": -1}},
        )
        if err != nil {
            return nil, fmt.Errorf("failed to update stock: %w", err)
        }

        // 3. 更新用户余额
        _, err = client.UpdateOne(
            sc,
            "users",
            bson.M{"user_id": order.UserID, "balance": bson.M{"$gte": order.Amount}},
            bson.M{"$inc": bson.M{"balance": -order.Amount}},
        )
        if err != nil {
            return nil, fmt.Errorf("failed to update balance: %w", err)
        }

        return order.OrderID, nil
    })

    if err != nil {
        log.Printf("Transaction failed: %v", err)
        return
    }

    fmt.Printf("Order created successfully: %v\n", result)
}
```

### 索引优化

```go
package main

import (
    "context"
    "fmt"
    "log"
    "github.com/xm-utils/tools/mongodb"
    "go.mongodb.org/mongo-driver/v2/bson"
    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
    cfg := &mongodb.Config{
        URI:      "mongodb://localhost:27017",
        Database: "analytics",
    }

    client, err := mongodb.NewClient(cfg)
    if err != nil {
        log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close(context.Background())

    ctx := context.Background()

    // 创建常用查询索引
    indexes := []mongo.IndexModel{
        // 用户事件查询索引
        {
            Keys: bson.M{"user_id": 1, "event_type": 1, "timestamp": -1},
            Options: options.Index().SetName("idx_user_events"),
        },
        // 日期范围查询索引
        {
            Keys: bson.M{"timestamp": -1},
            Options: options.Index().SetName("idx_timestamp"),
        },
        // 唯一约束索引
        {
            Keys: bson.M{"session_id": 1},
            Options: options.Index().SetUnique(true).SetName("idx_session_unique"),
        },
    }

    names, err := client.CreateManyIndexes(ctx, "events", indexes)
    if err != nil {
        log.Printf("Create indexes failed: %v", err)
        return
    }

    fmt.Printf("Created indexes: %v\n", names)

    // 查看索引使用情况
    explainResult, err := client.Collection("events").Find(
        ctx,
        bson.M{"user_id": "USER001"},
    ).Explain(ctx)
    
    fmt.Printf("Query plan: %+v\n", explainResult)
}
```

## 📚 API 参考

### 初始化和通用 API

```go
// 创建新客户端
func NewClient(cfg *Config) (*Client, error)

// 初始化全局客户端
func InitClient(cfg *Config) error

// 获取全局客户端
func GetClient() *Client

// 获取数据库实例
func (c *Client) GetDatabase(dbName ...string) *mongo.Database

// 获取集合实例
func (c *Client) Collection(collectionName string, dbName ...string) *mongo.Collection

// 检查连接状态
func (c *Client) Ping(ctx context.Context) error

// 关闭连接
func (c *Client) Close(ctx context.Context) error
```

### CRUD API

```go
// 插入单个文档
func (c *Client) InsertOne(ctx context.Context, collectionName string, document interface{}, dbName ...string) (*mongo.InsertOneResult, error)

// 批量插入
func (c *Client) InsertMany(ctx context.Context, collectionName string, documents []interface{}, dbName ...string) (*mongo.InsertManyResult, error)

// 查询单个文档
func (c *Client) FindOne(ctx context.Context, collectionName string, filter interface{}, result interface{}, dbName ...string) error

// 查询多个文档
func (c *Client) Find(ctx context.Context, collectionName string, filter interface{}, dbName ...string) ([]map[string]interface{}, error)

// 更新单个文档
func (c *Client) UpdateOne(ctx context.Context, collectionName string, filter interface{}, update interface{}, dbName ...string) (*mongo.UpdateResult, error)

// 更新多个文档
func (c *Client) UpdateMany(ctx context.Context, collectionName string, filter interface{}, update interface{}, dbName ...string) (*mongo.UpdateResult, error)

// 删除单个文档
func (c *Client) DeleteOne(ctx context.Context, collectionName string, filter interface{}, dbName ...string) (*mongo.DeleteResult, error)

// 删除多个文档
func (c *Client) DeleteMany(ctx context.Context, collectionName string, filter interface{}, dbName ...string) (*mongo.DeleteResult, error)

// 统计文档数量
func (c *Client) CountDocuments(ctx context.Context, collectionName string, filter interface{}, dbName ...string) (int64, error)

// 分页查询
func (c *Client) FindWithPagination(ctx context.Context, collectionName string, filter interface{}, page int64, pageSize int64, dbName ...string) ([]map[string]interface{}, int64, error)
```

### 索引管理 API

```go
// 创建索引
func (c *Client) CreateIndex(ctx context.Context, collectionName string, model mongo.IndexModel, dbName ...string) (string, error)

// 创建多个索引
func (c *Client) CreateManyIndexes(ctx context.Context, collectionName string, models []mongo.IndexModel, dbName ...string) ([]string, error)

// 删除索引
func (c *Client) DropIndex(ctx context.Context, collectionName string, indexName string, dbName ...string) error

// 删除所有索引
func (c *Client) DropAllIndexes(ctx context.Context, collectionName string, dbName ...string) error

// 列出所有索引
func (c *Client) ListIndexes(ctx context.Context, collectionName string, dbName ...string) ([]bson.M, error)
```

### 事务 API

```go
// 启动会话
func (c *Client) StartSession(ctx context.Context) (mongo.Session, error)

// 在事务中执行操作
func (c *Client) WithTransaction(ctx context.Context, fn func(mongo.SessionContext) (interface{}, error), dbName ...string) (interface{}, error)
```

## 🎯 最佳实践

### 连接池调优

```go
// 高并发场景配置
cfg := &mongodb.Config{
    URI:                  "mongodb://localhost:27017",
    Database:             "production_db",
    MaxPoolSize:          200,  // 根据并发量调整
    MinPoolSize:          20,   // 保持一定数量的空闲连接
    ConnectTimeout:       5 * time.Second,
    SocketTimeout:        15 * time.Second,
    ServerSelectionTimeout: 5 * time.Second,
    HeartbeatInterval:    5 * time.Second,
}
```

**调优建议：**
- `MaxPoolSize`：根据应用并发量和 MongoDB 服务器性能调整，通常 100-500
- `MinPoolSize`：设置为预期并发量的 10-20%，避免频繁创建连接
- `ConnectTimeout`：内网环境可设置为 3-5 秒，外网环境 10-15 秒
- `SocketTimeout`：根据查询复杂度设置，一般 15-30 秒

### 索引优化

1. **为常用查询字段创建索引**
   ```go
   // 为 frequently queried fields 创建索引
   index := mongo.IndexModel{
       Keys: bson.M{"user_id": 1, "created_at": -1},
   }
   ```

2. **使用复合索引优化多字段查询**
   ```go
   // 根据查询顺序设计复合索引
   index := mongo.IndexModel{
       Keys: bson.M{"status": 1, "category": 1, "created_at": -1},
   }
   ```

3. **避免过多索引**
   - 每个索引都会占用存储空间
   - 写入操作需要更新所有索引
   - 定期审查和优化索引

### 查询优化

1. **使用投影减少数据传输**
   ```go
   opts := options.Find().
       SetProjection(bson.M{"name": 1, "email": 1, "_id": 0})
   ```

2. **使用分页避免大量数据返回**
   ```go
   results, total, _ := client.FindWithPagination(ctx, "collection", filter, page, 100)
   ```

3. **使用合适的查询操作符**
   ```go
   // 使用 $in 代替多个 $or
   filter := bson.M{"status": bson.M{"$in": []string{"active", "pending"}}}
   ```

## ❓ 常见问题

### 1. 如何处理连接超时？

```go
cfg := &mongodb.Config{
    ConnectTimeout:       10 * time.Second,
    SocketTimeout:        30 * time.Second,
    ServerSelectionTimeout: 30 * time.Second,
}

client, err := mongodb.NewClient(cfg)
if err != nil {
    log.Printf("Connection failed: %v", err)
}

// 定期检查连接状态
ctx := context.Background()
if err := client.Ping(ctx); err != nil {
    log.Printf("Connection lost: %v", err)
}
```

### 2. 如何实现优雅关闭？

```go
client, err := mongodb.NewClient(cfg)
if err != nil {
    panic(err)
}

// 使用 defer 确保连接关闭
defer func() {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    if err := client.Close(ctx); err != nil {
        log.Printf("Failed to close connection: %v", err)
    }
}()
```

### 3. 如何处理大规模数据插入？

```go
// 使用批量插入提高性能
batchSize := 1000
var batch []interface{}

for i, doc := range documents {
    batch = append(batch, doc)
    
    if len(batch) >= batchSize || i == len(documents)-1 {
        _, err := client.InsertMany(ctx, "collection", batch)
        if err != nil {
            log.Printf("Batch insert failed: %v", err)
        }
        batch = batch[:0] // 清空批次
    }
}
```

### 4. 事务失败如何处理？

```go
result, err := client.WithTransaction(ctx, func(sc mongo.SessionContext) (interface{}, error) {
    // 事务操作
    return result, nil
})

if err != nil {
    // 事务已自动回滚，可以重试
    log.Printf("Transaction failed: %v", err)
    // 实现重试逻辑
}
```

## 🔗 依赖

- Go 1.24.2+
- go.mongodb.org/mongo-driver/v2

## 📄 许可证

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📧 联系方式

如有问题或建议，请提交 Issue。

