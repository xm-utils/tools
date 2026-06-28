# Database 数据库工具库

[![Go Version](https://img.shields.io/badge/go-1.24.2+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![GORM](https://img.shields.io/badge/gorm-v1.31.2-blue.svg)](https://gorm.io/)

一个基于 [GORM](https://gorm.io/) 封装的高性能 Go 语言数据库工具库，提供简洁易用的 CRUD 操作、事务管理、条件构建器等功能，并包含强大的分库分表插件支持取模分片和时间范围分片两种策略。

## 📋 目录

- [项目简介](#项目简介)
- [主要特性](#主要特性)
- [安装](#安装)
- [快速开始](#快速开始)
  - [基础初始化](#基础初始化)
  - [YAML 配置加载](#yaml-配置加载)
- [核心功能](#核心功能)
  - [CRUD 操作](#crud-操作)
  - [事务管理](#事务管理)
  - [条件构建器](#条件构建器)
  - [分页查询](#分页查询)
  - [批量操作](#批量操作)
- [高级功能](#高级功能)
  - [泛型支持](#泛型支持)
  - [原生 SQL](#原生-sql)
  - [上下文支持](#上下文支持)
  - [表名前缀](#表名前缀)
- [分库分表](#分库分表)
  - [取模分片](#取模分片)
  - [时间范围分片](#时间范围分片)
  - [双写模式](#双写模式)
  - [主键生成](#主键生成)
- [配置说明](#配置说明)
  - [MysqlConfig 配置结构](#mysqlconfig-配置结构)
  - [连接池配置](#连接池配置)
- [使用示例](#使用示例)
  - [用户管理系统](#用户管理系统)
  - [订单系统（分片）](#订单系统分片)
  - [日志系统（时间分片）](#日志系统时间分片)
  - [事务示例](#事务示例)
- [API 参考](#api-参考)
  - [初始化和连接](#初始化和连接)
  - [查询 API](#查询-api)
  - [写入 API](#写入-api)
  - [事务 API](#事务-api)
  - [条件构建器 API](#条件构建器-api)
  - [分片 API](#分片-api)
- [最佳实践](#最佳实践)
  - [连接池调优](#连接池调优)
  - [索引优化](#索引优化)
  - [查询优化](#查询优化)
  - [分片策略选择](#分片策略选择)
- [常见问题](#常见问题)
- [依赖](#依赖)
- [许可证](#许可证)

## 📖 项目简介

本项目是对 GORM 的二次封装，旨在简化数据库在 Go 项目中的使用。提供了以下核心功能：

- **通用 CRUD**：基于泛型的增删改查操作，类型安全
- **事务管理**：简化的事务处理，支持嵌套事务
- **条件构建器**：链式调用构建复杂查询条件
- **分页查询**：内置分页和排序支持
- **批量操作**：高效的批量插入和更新
- **分库分表**：强大的分片插件，支持取模和时间范围两种策略
- **连接池管理**：自动优化连接池参数

## ✨ 主要特性

- ✅ **简洁的 API**：封装复杂的 GORM，提供简单易用的接口
- ✅ **泛型支持**：利用 Go 泛型特性，类型安全的数据库操作
- ✅ **条件构建器**：链式调用构建 WHERE 条件，支持 Eq、In、Like、Between 等
- ✅ **事务管理**：简化的事务处理，自动回滚和提交
- ✅ **分页查询**：内置分页、排序、时间范围查询
- ✅ **批量操作**：高效的批量插入，自动分批处理
- ✅ **分库分表**：支持取模分片和时间范围分片
- ✅ **主键生成**：支持 Snowflake、PostgreSQL Sequence、MySQL Sequence
- ✅ **连接池优化**：自动设置合理的连接池参数
- ✅ **多数据库支持**：支持多数据库实例，通过 alias 区分

## 🚀 安装

```bash
go get github.com/xm-utils/tools/database
```

### 依赖要求

- Go 1.24.2+
- gorm.io/gorm v1.31.2+
- gorm.io/driver/mysql v1.6.0+
- github.com/bwmarrin/snowflake v0.3.0（分片功能）
- github.com/longbridgeapp/sqlparser v0.3.2（分片功能）

### 前置条件

使用前请确保已部署并运行 MySQL 数据库。推荐使用 MySQL 5.7 或更高版本。

## 🎯 快速开始

### 基础初始化

```go
package main

import (
    "fmt"
    "log"
    "github.com/xm-utils/tools/database"
)

func main() {
    // 创建配置
    config := &database.MysqlConfig{
        Alias:    "default",
        Name:     "mydb",
        User:     "root",
        Password: "password",
        Host:     "127.0.0.1",
        Port:     "3306",
        Debug:    "true",
        
        // 连接池配置
        MaxIdleConns:    10,
        MaxOpenConns:    100,
        ConnMaxLifetime: 3600,
        ConnMaxIdleTime: 600,
    }

    // 初始化数据库连接
    if err := database.InitGorm(config); err != nil {
        log.Fatalf("Failed to init database: %v", err)
    }

    fmt.Println("Database initialized successfully")
}
```

### YAML 配置加载

1. 创建配置文件 `database.yaml`：

```yaml
db_alias: "default"
db_name: "mydb"
db_user: "root"
db_pwd: "password"
db_host: "127.0.0.1"
db_port: "3306"
db_debug: "true"
db_table_prefix: "app_"
db_charset: "utf8mb4"
db_location: "Asia/Shanghai"

# 连接池配置
db_max_idle_conns: 10
db_max_open_conns: 100
db_conn_max_lifetime: 3600
db_conn_max_idle_time: 600
```

2. 在代码中加载配置（需配合配置加载库如 viper）：

```go
package main

import (
    "log"
    "github.com/spf13/viper"
    "github.com/xm-utils/tools/database"
)

func main() {
    // 使用 viper 加载 YAML 配置
    viper.SetConfigFile("database.yaml")
    if err := viper.ReadInConfig(); err != nil {
        log.Fatalf("Failed to read config: %v", err)
    }

    // 解析为 database.MysqlConfig 结构
    var config database.MysqlConfig
    if err := viper.Unmarshal(&config); err != nil {
        log.Fatalf("Failed to unmarshal config: %v", err)
    }

    // 初始化数据库
    if err := database.InitGorm(&config); err != nil {
        log.Fatalf("Failed to init database: %v", err)
    }

    log.Println("Database initialized successfully")
}
```

## 🔧 核心功能

### CRUD 操作

#### 定义模型

```go
package model

import "time"

type User struct {
    ID        int64     `gorm:"primaryKey;autoIncrement"`
    Username  string    `gorm:"size:50;not null;uniqueIndex"`
    Email     string    `gorm:"size:100;not null"`
    Age       int       `gorm:"default:0"`
    Status    int       `gorm:"default:1"`
    CreatedAt time.Time `gorm:"autoCreateTime"`
    UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

func (User) TableName() string {
    return database.TableName("users")
}
```

#### 插入记录

```go
ctx := context.Background()

// 单条插入
user := &User{
    Username: "zhangsan",
    Email:    "zhangsan@example.com",
    Age:      25,
}

err := database.Insert[*User](nil, user)
if err != nil {
    log.Printf("Insert failed: %v", err)
} else {
    fmt.Printf("User created with ID: %d\n", user.ID)
}
```

#### 批量插入

```go
// 批量插入（自动分批，每批100条）
users := []*User{
    {Username: "user1", Email: "user1@example.com"},
    {Username: "user2", Email: "user2@example.com"},
    {Username: "user3", Email: "user3@example.com"},
    // ... 更多用户
}

err := database.InsertBatch[*User](nil, 100, users)
if err != nil {
    log.Printf("Batch insert failed: %v", err)
}
```

#### 查询记录

```go
// 根据 ID 查询
user := database.FindByID[*User](1001)
if user != nil {
    fmt.Printf("User: %+v\n", *user)
}

// 根据条件查询单条
user = database.FindOne[*User]("username = ?", "zhangsan")
if user != nil {
    fmt.Printf("Found user: %s\n", user.Username)
}

// 根据主键查询
var queryUser User
queryUser.Username = "zhangsan"
err := database.ReadOne(&queryUser)
if err != nil {
    if err == gorm.ErrRecordNotFound {
        fmt.Println("User not found")
    }
}
```

#### 分页查询

```go
// 构建查询参数
param := database.ListParam{
    Cond: database.NewCondition().
        Eq(true, "status", 1).
        Like(true, "username", "zhang").
        In(true, "age", []interface{}{20, 25, 30}),
    
    Page: &PageParam{
        PageNum:  1,
        PageSize: 20,
    },
    
    Order: []string{"-created_at", "id"}, // -表示DESC
    
    Time: &TimeRangeParam{
        Column: "created_at",
        Start:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
        End:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
    },
}

// 执行查询
users, total, err := database.FindAll[*User](param)
if err != nil {
    log.Printf("Query failed: %v", err)
} else {
    fmt.Printf("Total: %d, Users: %d\n", total, len(users))
    for _, user := range users {
        fmt.Printf("  - %s (%s)\n", user.Username, user.Email)
    }
}
```

#### 更新记录

```go
// 根据主键更新
user.Age = 26
err := database.Update[*User](nil, user)

// 更新指定字段
err = database.Update[*User](nil, user, "age", "status")

// 根据条件更新
condition := map[string]interface{}{
    "username": "zhangsan",
}
updates := map[string]interface{}{
    "age":    27,
    "status": 0,
}
err = database.UpdateByCondition[*User](nil, condition, updates)
```

#### 删除记录

```go
// 根据主键删除
err := database.Delete[*User](nil, user)

// 根据条件删除
condition := map[string]interface{}{
    "status": 0,
}
err = database.DeleteByCondition[*User](nil, condition)
```

### 事务管理

#### 方式一：Transaction 函数（推荐）

```go
err := database.Transaction(func(tx *gorm.DB) error {
    // 插入用户
    user := &User{
        Username: "lisi",
        Email:    "lisi@example.com",
    }
    if err := database.Insert[*User](tx, user); err != nil {
        return err
    }
    
    // 创建订单
    order := &Order{
        UserID: user.ID,
        Amount: 99.99,
    }
    if err := database.Insert[*Order](tx, order); err != nil {
        return err
    }
    
    return nil
})

if err != nil {
    log.Printf("Transaction failed: %v", err)
} else {
    fmt.Println("Transaction committed")
}
```

#### 方式二：Tx 对象

```go
tx := database.NewTx()

err := tx.Execute(func(tx *gorm.DB) error {
    // 业务逻辑
    user := &User{Username: "wangwu", Email: "wangwu@example.com"}
    if err := database.Insert[*User](tx, user); err != nil {
        return err
    }
    
    return nil
})

if err != nil {
    log.Printf("Transaction failed: %v", err)
}
```

### 条件构建器

条件构建器提供链式调用来构建复杂的 WHERE 条件：

```go
cond := database.NewCondition()

// 等于条件
cond.Eq(true, "status", 1)

// IN 条件
cond.In(true, "age", []interface{}{20, 25, 30})

// LIKE 条件（前后模糊）
cond.Like(true, "username", "zhang")

// 左模糊
cond.LLike(true, "email", "@example.com")

// 右模糊
cond.RLike(true, "phone", "138")

// NOT LIKE
cond.NotLike(true, "username", "test")

// BETWEEN
cond.Between(true, "age", "20", "30")

// 使用条件查询
users, err := database.FindList[*User](cond)
```

**条件说明：**
- 第一个参数 `bool`：是否启用该条件（便于动态构建）
- 第二个参数：字段名
- 后续参数：值或范围

### 分页查询

实现分页接口：

```go
type PageParam struct {
    PageNum  int32 `json:"page_num"`
    PageSize int32 `json:"page_size"`
}

func (p *PageParam) IsValid() bool {
    return p.PageNum > 0 && p.PageSize > 0 && p.PageSize <= 100
}

func (p *PageParam) GetLimit() (limit, offset int32) {
    limit = p.PageSize
    offset = (p.PageNum - 1) * p.PageSize
    return
}

// 使用时间范围接口
type TimeRangeParam struct {
    Column string
    Start  time.Time
    End    time.Time
}

func (t *TimeRangeParam) IsValid() bool {
    return !t.Start.IsZero() && !t.End.IsZero()
}

func (t *TimeRangeParam) GetColumn() string {
    return t.Column
}

func (t *TimeRangeParam) GetTime() (start, end time.Time) {
    return t.Start, t.End
}
```

### 批量操作

#### 批量插入

```go
// 插入1000条数据，每批100条
var users []*User
for i := 0; i < 1000; i++ {
    users = append(users, &User{
        Username: fmt.Sprintf("user%d", i),
        Email:    fmt.Sprintf("user%d@example.com", i),
    })
}

err := database.InsertBatch[*User](nil, 100, users)
```

#### 批量更新

```go
// 批量更新状态
condition := map[string]interface{}{
    "status": 1,
}
updates := map[string]interface{}{
    "status": 0,
}
err = database.UpdateByCondition[*User](nil, condition, updates)
```

## 🚀 高级功能

### 泛型支持

所有查询和写入操作都支持泛型，提供类型安全：

```go
// 查询返回强类型
users, total, err := database.FindAll[*User](param)
// users 类型为 []*User

user := database.FindByID[*User](1001)
// user 类型为 *User

count, err := database.Count[*User](cond)
```

### 原生 SQL

```go
// 执行原生 SQL
err := database.Exec("UPDATE users SET status = ? WHERE age > ?", 0, 18)

// 查询原生 SQL
type UserSummary struct {
    Age   int   `json:"age"`
    Count int64 `json:"count"`
}

summaries, err := database.Raw[*UserSummary](
    "SELECT age, COUNT(*) as count FROM users GROUP BY age",
)
```

### 上下文支持

```go
// 带上下文的数据库操作
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

db := database.WithContext(ctx)
var users []*User
db.Where("status = ?", 1).Find(&users)
```

### 表名前缀

```go
// 设置全局表名前缀
database.SetPrefix("app_")

// 在模型中使用
func (User) TableName() string {
    return database.TableName("users") // 实际表名：app_users
}
```

## 🗂️ 分库分表

本库提供了强大的分片插件，支持两种分片策略：**取模分片**和**时间范围分片**。

### 取模分片

适用于数据均匀分布的场景，如用户表、订单表等。

#### 配置示例

```go
package main

import (
    "github.com/xm-utils/tools/database/sharding"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
)

type User struct {
    ID       int64  `gorm:"primaryKey"`
    Username string `gorm:"size:50"`
    Email    string `gorm:"size:100"`
}

func main() {
    db, err := gorm.Open(mysql.Open("root:password@tcp(127.0.0.1:3306)/mydb"), &gorm.Config{})
    if err != nil {
        panic(err)
    }

    // 配置取模分片 - 分成10个表
    config := sharding.Config{
        NumberOfShards:      10,  // users_0, users_1, ..., users_9
        ShardingKey:         "id",
        PrimaryKeyGenerator: sharding.PKSnowflake,
    }

    shardingInstance := sharding.Register(config, &User{})
    
    if err := shardingInstance.Initialize(db); err != nil {
        panic(err)
    }
    
    if err := db.Use(shardingInstance); err != nil {
        panic(err)
    }

    // 插入数据 - 自动路由到对应的分片表
    user := &User{
        Username: "zhangsan",
        Email:    "zhangsan@example.com",
    }
    db.Create(user)
    // 如果 ID 是 123456，会路由到 users_6 表（123456 % 10 = 6）

    // 查询数据 - 自动路由
    var users []User
    db.Where("id = ?", user.ID).Find(&users)
}
```

#### 自定义分片算法

```go
config := sharding.Config{
    NumberOfShards: 64,
    ShardingKey:    "user_id",
    
    // 自定义分片算法
    ShardingAlgorithm: func(value interface{}) (suffix string, err error) {
        var userID int64
        switch v := value.(type) {
        case int64:
            userID = v
        case string:
            userID, _ = strconv.ParseInt(v, 10, 64)
        }
        
        return fmt.Sprintf("_%02d", userID%64), nil
    },
    
    PrimaryKeyGenerator: sharding.PKSnowflake,
}
```

### 时间范围分片 ⭐

适用于按时间维度归档的场景，如订单表、日志表等。

#### 按月分表（订单系统）

```go
type Order struct {
    ID        int64     `gorm:"primaryKey"`
    UserID    int64     `gorm:"index"`
    Amount    float64
    Status    int
    CreatedAt time.Time `gorm:"index"`
}

// 配置按月分表
config := sharding.Config{
    ShardingType:        "time_range",
    TimeColumn:          "created_at",
    TimeRangeFormat:     "month", // orders_202601, orders_202602, ...
    PrimaryKeyGenerator: sharding.PKSnowflake,
}

shardingInstance := sharding.Register(config, &Order{})
db.Use(shardingInstance)

// 插入数据 - 自动路由到 orders_202601
order := &Order{
    UserID:    1001,
    Amount:    99.99,
    Status:    1,
    CreatedAt: time.Now(),
}
db.Create(order)

// 查询数据 - 自动路由到对应月份的表
var orders []Order
db.Where("created_at >= ? AND created_at < ?", 
    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
    time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
).Find(&orders)
```

#### 按日分表（日志系统）

```go
type DailyLog struct {
    ID        int64     `gorm:"primaryKey"`
    Level     string    `gorm:"size:20"`
    Message   string
    CreatedAt time.Time `gorm:"index"`
}

// 配置按日分表
config := sharding.Config{
    ShardingType:        "time_range",
    TimeColumn:          "created_at",
    TimeRangeFormat:     "day", // logs_20260101, logs_20260102, ...
    PrimaryKeyGenerator: sharding.PKSnowflake,
}

shardingInstance := sharding.Register(config, &DailyLog{})
db.Use(shardingInstance)

// 自动写入当天的日志表
log := &DailyLog{
    Level:     "ERROR",
    Message:   "Something went wrong",
    CreatedAt: time.Now(),
}
db.Create(log)
```

#### 支持的