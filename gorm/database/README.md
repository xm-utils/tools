# Database 数据库工具库

[![Go Version](https://img.shields.io/badge/go-1.24.2+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](#-许可证)
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
  - [上下文支持（链式 CRUD）](#上下文支持)
  - [表名前缀](#表名前缀)
  - [一主多从读写分离（DBResolver）](#一主多从读写分离dbresolver)
- [分库分表](#分库分表)
  - [取模分片](#取模分片)
  - [时间范围分片](#时间范围分片)
- [依赖](#依赖)
- [许可证](#许可证)

> ⚡ 想快速上手？阅读 [QUICKSTART.md](QUICKSTART.md)（5 分钟跑通初始化 + CRUD + 事务 + 分片）；分片细节另见 [sharding/QUICKSTART.md](sharding/QUICKSTART.md)。

## 📖 项目简介

本项目是对 GORM 的二次封装，旨在简化数据库在 Go 项目中的使用。提供了以下核心功能：

- **通用 CRUD**：基于泛型的增删改查操作，类型安全
- **事务管理**：简化的事务处理，支持嵌套事务
- **条件构建器**：链式调用构建复杂查询条件
- **分页查询**：内置分页和排序支持
- **批量操作**：高效的批量插入和更新
- **分库分表**：强大的分片插件，支持取模和时间范围两种策略
- **一主多从读写分离**：基于 DBResolver 支持主从读写分离
- **连接池管理**：自动优化连接池参数

## ✨ 主要特性

- ✅ **简洁的 API**：封装复杂的 GORM，提供简单易用的接口
- ✅ **泛型支持**：利用 Go 泛型特性，类型安全的数据库操作
- ✅ **条件构建器**：链式调用构建 WHERE 条件，支持 Eq、NotEq、In、NotIn、Like、LLike、RLike、NotLike、Between、Gt/Gte/Lt/Lte、IsNull/IsNotNull 等
- ✅ **事务管理**：简化的事务处理，自动回滚和提交
- ✅ **分页查询**：内置分页、排序、时间范围查询
- ✅ **批量操作**：高效的批量插入，自动分批处理
- ✅ **分库分表**：支持取模分片和时间范围分片
- ✅ **主键生成**：支持 Snowflake、PostgreSQL Sequence、MySQL Sequence
- ✅ **连接池优化**：自动设置合理的连接池参数
- ✅ **一主多从读写分离**：基于 DBResolver 实现主库写、多从库读，从库间随机负载均衡
- ✅ **单例连接**：`InitGorm` 基于 `sync.Once` 提供进程级单例 `*gorm.DB`，重复调用安全幂等

## 🚀 安装

```bash
go get github.com/xm-utils/tools/gorm/database
```

导入路径：

```go
import (
    "github.com/xm-utils/tools/gorm/database"           // 核心 CRUD / 事务 / 条件构建器
    "github.com/xm-utils/tools/gorm/database/sharding" // 分库分表插件
)
```

### 依赖要求

- Go 1.24.2+
- gorm.io/gorm v1.31.2+
- gorm.io/driver/mysql v1.6.0+
- gorm.io/plugin/dbresolver v1.6.2（一主多从读写分离）
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
    "github.com/xm-utils/tools/gorm/database"
)

func main() {
    // 创建配置（必填 user/host/port/database，Password 允许为空）
    // 连接字段已从顶层下沉到 db_master 节点（Master *MysqlNodeConfig）
    config := &database.MysqlConfig{
        Master: &database.MysqlNodeConfig{
            Host:     "127.0.0.1",
            Port:     "3306",
            User:     "root",
            Password: "password",
            Database: "mydb",
        },
        Debug: "true",

        // 连接池配置
        MaxIdleConns:    10,
        MaxOpenConns:    100,
        ConnMaxLifetime: 3600,
        ConnMaxIdleTime: 600,

        // 一主多从（可选）：见「一主多从读写分离（DBResolver）」章节
        // Slaves 为多个读库，每个节点同样需提供完整连接信息
        // Slaves: []database.MysqlNodeConfig{{Host: "10.0.1.1", Port: "3306", User: "root", Password: "password", Database: "mydb"}},
    }

    // 初始化数据库：内部基于 sync.Once，进程内只会真正初始化一次，重复调用安全
    if err := database.InitGorm(config); err != nil {
        log.Fatalf("Failed to init database: %v", err)
    }

    // 获取单例 *gorm.DB（读写由 DBResolver 自动路由，无需 alias 区分实例）
    db := database.GetDB()
    _ = db

    fmt.Println("Database initialized successfully")
}
```

> **必填校验**：`InitGorm` 首先检查 `config` 及 `config.Master` 非 nil，再校验主库节点 `User/Host/Port/Database` 非空（`Password` 允许为空）。缺失项会一次性列于返回的 error 中，避免生成畸形 DSN 后才连库失败。

### YAML 配置加载

> 完整字段说明与多场景模板见同目录的 [config.example.yaml](config.example.yaml)（含逐行注释、从库节点写法与 viper 加载代码）。

1. 创建配置文件 `database.yaml`（连接字段已下沉到 `db_master`，顶层不再有 `db_name/db_user/db_pwd/db_host/db_port/db_charset/db_location/db_alias/db_log_dir`）：

```yaml
db_master:                    # 主库(写库)，必填
  host: "127.0.0.1"
  port: "3306"
  user: "root"
  pwd: "password"
  database: "mydb"
  charset: "utf8mb4"          # 留空默认 utf8mb4
  location: "Asia/Shanghai"  # 留空默认 Local

db_debug: "true"
db_table_prefix: "app_"

# 连接池配置
db_max_idle_conns: 10
db_max_open_conns: 100
db_conn_max_lifetime: 3600
db_conn_max_idle_time: 600

# 一主多从读写分离（可选）：每个从库需填写完整连接信息
db_slaves:
  - { host: "10.0.1.1", port: "3306", user: "root", pwd: "password", database: "mydb" }
  - { host: "10.0.1.2", port: "3306", user: "root", pwd: "password", database: "mydb" }
```

2. 在代码中加载配置（需配合配置加载库如 viper）：

```go
package main

import (
    "log"
    "github.com/spf13/viper"
    "github.com/xm-utils/tools/gorm/database"
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

err := database.Insert[User](nil, user)
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

affected, err := database.InsertBatch[User](nil, 100, users)
if err != nil {
    log.Printf("Batch insert failed: %v", err)
} else {
    fmt.Printf("Batch inserted rows: %d\n", affected)
}
```

#### 查询记录

```go
// 根据 ID 查询单条（强制走主库，未命中返回 nil）
user := database.FindOne[User](1001)
if user != nil {
    fmt.Printf("User: %+v\n", *user)
}

// 按条件查询单条（以结构体非零字段作为 WHERE 条件，返回 *User；未命中返回 nil）
user = database.ReadOne(User{Username: "zhangsan"})
if user == nil {
    fmt.Println("User not found")
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
users, total, err := database.FindAll[User](param)
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
// 根据主键更新（仅更新非零字段）
user.Age = 26
err := database.Update[User](nil, user)

// 更新指定字段
err = database.Update[User](nil, user, "age", "status")

// 差异更新：比对 old 与 form，仅把 form 中非空且变化的字段写库（Select 只这些列）
// 不传 whereCols 按主键更新；传入 whereCols（Go 字段名）则在主键条件上追加这些字段的值作为额外 WHERE（AND）
// 无差异时直接返回 nil
err = database.UpdateModel[User](nil, old, form)
err = database.UpdateModel[User](tx, old, form, "Status")

// 根据条件批量更新（条件为 *Condition）
cond := database.Eq("username", "zhangsan")
updates := map[string]interface{}{
    "age":    27,
    "status": 0,
}
err = database.UpdateByCondition[User](nil, cond, updates)
```

#### 删除记录

```go
// 根据主键删除
err := database.Delete[User](nil, user)

// 根据条件删除（条件为 *Condition）
cond := database.Eq("status", 0)
err = database.DeleteByCondition[User](nil, cond)
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
    if err := database.Insert[User](tx, user); err != nil {
        return err
    }
    
    // 创建订单
    order := &Order{
        UserID: user.ID,
        Amount: 99.99,
    }
    if err := database.Insert[Order](tx, order); err != nil {
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
    if err := database.Insert[User](tx, user); err != nil {
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

// 比较运算：!= / > / >= / < / <=
cond.NotEq(true, "status", 0)
cond.Gt(true, "age", 18)
cond.Gte(true, "score", 60)
cond.Lt(true, "stock", 100)
cond.Lte(true, "price", 999)

// NOT IN
cond.NotIn(true, "dept", []interface{}{"hr", "admin"})

// IS NULL / IS NOT NULL（无值参数）
cond.IsNull(true, "deleted_at")
cond.IsNotNull(true, "email")

// 使用条件查询
users, err := database.FindList[User](cond)
```

> 单条件也可用包级快捷函数直接构造（无需 `NewCondition`）：`database.Eq("status", 1)`、`database.Gt("age", 18)`、`database.NotIn("dept", ids)`、`database.IsNull("deleted_at")` 等，返回一个独立的 `*Condition`（快捷函数之间不链式，链式请用 `cond.Xxx(true, ...)` 方法）。

**条件说明：**
- 第一个参数 `bool`：是否启用该条件（便于动态构建）
- 第二个参数：字段名
- 后续参数：值或范围

> **LIKE 转义**：`Like/LLike/RLike/NotLike` 会自动对用户输入值中的 `%`、`_`、`\` 进行转义（先 `\`→`\\`，再 `%`→`\%`、`_`→`\_`），使其按字面量匹配（如搜索 `100%` 不会误匹配任意含 "100" 的值）；两侧用于模糊匹配的通配符 `%` 由框架添加、不受转义影响。

> **空条件**：`FindList` / `Count` / `FindAll` 对 `nil` 或空 `*Condition`（`IsEmpty()` 为真，即无任何已启用条件）均不拼接 WHERE，等价于全表操作，不会生成畸形 SQL。

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

affected, err := database.InsertBatch[User](nil, 100, users)
```

#### 批量更新

```go
// 批量更新状态
cond := database.Eq("status", 1)
updates := map[string]interface{}{
    "status": 0,
}
err = database.UpdateByCondition[User](nil, cond, updates)
```

## 🚀 高级功能

### 泛型支持

所有查询和写入操作都支持泛型，提供类型安全：

```go
// 查询返回强类型
users, total, err := database.FindAll[User](param)
// users 类型为 []*User

user := database.FindOne[User](1001)
// user 类型为 *User

count, err := database.Count[User](cond)
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

summaries, err := database.Raw[UserSummary](
    "SELECT age, COUNT(*) as count FROM users GROUP BY age",
)
```

### 上下文支持

带 `context` 的操作统一走链式构建器 `Ctx[T](ctx)`：链式方法仅记录意图，终端操作（FindOne/FindAll/Insert/Update/Delete/Exec 等）才执行 SQL；`ctx` 超时或取消时进行中的查询立即中断并返回对应错误。

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// 读：条件 + 排序 + 分页
users, total, err := database.Ctx[User](ctx).
    Where(database.Eq("status", 1)).
    Order("-created_at", "id").
    Page(&PageParam{PageNum: 1, PageSize: 20}).
    FindAll()

// 读：强制走主库（写后立读避免主从延迟）
user, err := database.Ctx[User](ctx).Primary().FindOne(1001)

// 写：在事务中按条件批量更新
err = database.Ctx[User](ctx).Tx(tx).
    Where(database.Eq("status", 1)).
    UpdateByCondition(map[string]interface{}{"status": 0})
```

> 链式 API 与包级函数的关系：包级 `ReadOne/FindAll/Update` 等均为 `CtxBuilder` 的薄兼容层（携带 `context.Background()`，读类默认强制主库），两套签名一致，按需选用。

### 表名前缀

```go
// 设置全局表名前缀
database.SetPrefix("app_")

// 在模型中使用
func (User) TableName() string {
    return database.TableName("users") // 实际表名：app_users
}
```

### 一主多从读写分离（DBResolver）

`InitGorm` 基于 [`gorm.io/plugin/dbresolver`](https://gorm.io/docs/dbresolver.html) 实现**一主多从读写分离**，并采用 `sync.Once` 维护进程级单例 `*gorm.DB`，不再通过 `alias` 维护多个独立连接。

#### 核心行为

- **单例初始化**：`InitGorm(config)` 由 `sync.Once` 保证进程内仅初始化一次；重复调用直接返回首次的初始化结果（含错误），可安全放在任意启动位置。
- **一主多从**：主库使用 `config.Master`（`*MysqlNodeConfig`，作为唯一写库 Sources）；`Slaves` 配置多个从库（Replicas）。写操作走主库，读操作在多个从库间由 `RandomPolicy` 随机负载均衡。
- **节点自带连接信息**：主库与每个从库都是同一种节点配置，各自提供完整 `Host/Port/User/Password/Database`（当前实现不再自动继承主库凭证，从库需自行填写）；`Charset/Location` 留空时使用默认值 utf8mb4 / Local。
- **向后兼容**：若不配置 `Slaves`，则仅使用主库单连接，行为与旧版一致。
- **连接池**：`MaxIdleConns` 等参数会同时下发到主连接与 DBResolver 内每个从库连接池。

#### 配置结构

```go
// 节点配置（主库 Master 与从库 Slaves 元素共用同一结构）
// Host/Port/User/Password/Database 均需自行填写；Charset/Location 留空使用默认值
type MysqlNodeConfig struct {
    Host     string // 数据库 IP（域名）
    Port     string // 端口
    User     string // 用户名
    Password string // 密码
    Database string // 库名
    Charset  string // 字符集，留空默认 utf8mb4
    Location string // 时区，留空默认 Local
}

// MysqlConfig 关键字段
//   Master *MysqlNodeConfig   // 主库（写库），yaml: db_master
//   Slaves []MysqlNodeConfig  // 读节点（从）列表，yaml: db_slaves
```

#### 代码配置示例

```go
config := &database.MysqlConfig{
    // 主库（唯一写库）
    Master: &database.MysqlNodeConfig{
        Host:     "10.0.0.1",
        Port:     "3306",
        User:     "root",
        Password: "password",
        Database: "mydb",
    },

    // 多从（读库）：每个节点需自行填写完整连接信息
    Slaves: []database.MysqlNodeConfig{
        {Host: "10.0.1.1", Port: "3306", User: "root", Password: "password", Database: "mydb"},
        {Host: "10.0.1.2", Port: "3306", User: "root", Password: "password", Database: "mydb"},
    },

    MaxIdleConns:    10,
    MaxOpenConns:    100,
    ConnMaxLifetime: 3600,
    ConnMaxIdleTime: 600,
}

if err := database.InitGorm(config); err != nil {
    log.Fatal(err)
}

// 之后所有 CRUD/事务辅助函数都复用同一单例，读写自动路由
users, total, err := database.FindAll[User](param) // 读 → 从库
database.Insert[User](nil, user)                     // 写 → 主库
```

> 说明：若需强制单条查询走主库（如“写后立读”避免主从延迟），可使用 `db.Clauses(dbresolver.Write)`，或用链式 API `database.Ctx[User](ctx).Primary().FindOne(id)`。

#### 仅部分表读走从库（模型声明 + 初始化扫描）

默认**全部走主库**。若想让某些表读走从库，只需让对应模型实现 `ReplicaReader` 接口（`ReadFromReplica() bool`）并用 `RegisterModels` 注册；`InitGorm` 初始化时会**统一扫描**这些模型，把其对应表注册为“表级” DBResolver。表名由模型自动解析（遵循 `TableName()`/命名策略/全局前缀），**无需写进配置文件**。

```go
// 1. 模型声明“读走从库”
type Order struct {
    ID     int64 `gorm:"primaryKey"`
    UserID int64
}

func (Order) TableName() string { return "orders" }
func (Order) ReadFromReplica() bool { return true } // 声明：本表读走从库

// 2. 启动时注册模型（InitGorm 之前，通常在 init() 中）
func init() {
    database.RegisterModels(&Order{})
}

// 3. 正常初始化，无需传表名/模型
if err := database.InitGorm(config); err != nil {
    log.Fatal(err)
}
```

内部扫描逻辑（buildResolver）：

```go
// 遍历已注册模型，取 ReadFromReplica() 为 true 的表名（去重）
for _, m := range modelRegistry {
    if rr, ok := m.(ReplicaReader); ok && rr.ReadFromReplica() {
        stmt := &gorm.Statement{DB: instance}
        stmt.Parse(m)            // 解析真实表名
        datas = append(datas, stmt.Table)
    }
}
// datas 非空 => 仅这些表读走从库；datas 为空 => 不注册 resolver（全走主库）
dbresolver.Register(dbresolver.Config{Replicas: ..., Policy: dbresolver.RandomPolicy{}}, datas...)
```

路由规则：

| 表 | 读(Select/Query) | 写(Create/Update/Delete) |
| --- | --- | --- |
| `ReadFromReplica()==true` 的模型表 | 从库（多从随机） | 主库 |
| 其余所有表（含未声明的） | 主库 | 主库 |

**补充：**
- 前提：`config.Slaves` 非空。若无从库，即使模型声明了 `ReadFromReplica()` 也不会启用（`buildResolver` 直接返回 nil）。
- `ReadFromReplica()` 可根据环境变量/配置动态返回（如仅生产环境开从库），扫描在 `InitGorm` 时一次性完成。
- 强制单条查询走主库（如“写后立读”避免主从延迟）：对该表用 `db.Clauses(dbresolver.Write)`。

#### 显式路由读器：Primary / Replica

除链式 `Ctx[T](ctx).Primary()/.Replica()` 外，还提供包级读器 `database.Primary[T]()` / `database.Replica[T]()`（默认携带 `context.Background()`）：每次调用构造全新构建器，链式条件不会在多次调用间累积；未命中从库表时自动回落主库。

```go
// 强制走主库读（适合写后立读）
user := database.Primary[User]().FindOne(1001)              // 吞错返回 nil，未命中为 nil
users, total, err := database.Primary[User]().FindAll(param)

// 尝试走从库读（未注册从库的表回落主库）
count, err := database.Replica[User]().Count(database.Eq("status", 1))
list, err := database.Replica[User]().FindList(cond)
```

## 🗂️ 分库分表

本库提供了强大的分片插件，支持两种分片策略：**取模分片**和**时间范围分片**。

### 取模分片

适用于数据均匀分布的场景，如用户表、订单表等。

#### 配置示例

```go
package main

import (
    "github.com/xm-utils/tools/gorm/database/sharding"
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

#### 支持的时间类型

时间范围分片的路由值支持以下类型：

- **time.Time**：直接使用
- **string**：自动解析多种格式（`2026-01-15 10:30:00`、`2026-01-15T10:30:00Z`、`2026-01-15`）
- **int64**：Unix 时间戳（秒）

#### 分片注意事项

- **查询路由**：时间范围分片的 WHERE 支持 `=`、`>`、`>=`、`<`、`<=` 及 `BETWEEN`（取下界）定位分片；不含时间分片字段的查询会报 `ErrMissingShardingKey`（避免全分片扫描）。
- **AutoMigrate**：时间范围分片默认不预生成所有周期表，`AutoMigrate` 仅回退创建「当前周期」表；历史与后续周期表建议用 `GenerateTimeRangeTables` 批量生成表名后显式建表。
- **取模后缀宽度**：表后缀宽度按 `NumberOfShards` 十进制位数自动生成（4位数10000 → `_0000`..`_09999`），≥ 10000 也不会退化为空后缀。
- **Snowflake 主键**：`NumberOfShards` 配合 `PKSnowflake` 时不得超过 1024（合法节点 index 0..1023），超限时 `Initialize` 返回明确 error；雪花节点按需惰性创建，不再启动时预建 1024 个。

> 更多分片细节见 [sharding/README.md](sharding/README.md) 与 [sharding/QUICKSTART.md](sharding/QUICKSTART.md)。

## 📦 依赖

运行时直接依赖（见 `go.mod`，模块路径 `github.com/xm-utils/tools/gorm/database`）：

| 依赖 | 版本 | 用途 |
| --- | --- | --- |
| `gorm.io/gorm` | v1.31.2+ | ORM 核心 |
| `gorm.io/driver/mysql` | v1.6.0+ | MySQL 驱动 |
| `gorm.io/plugin/dbresolver` | v1.6.2 | 一主多从读写分离 |
| `github.com/xm-utils/tools/common` | v0.0.16 | 结构体差异比对（UpdateModel） |
| `github.com/bwmarrin/snowflake` | v0.3.0 | 分片雪花主键生成 |
| `github.com/longbridgeapp/sqlparser` | v0.3.2 | 分片 SQL 解析与路由 |
| `github.com/google/go-querystring` | v1.2.0 | DSN 参数编码 |
| `github.com/sirupsen/logrus` | v1.10.2 | 日志输出 |

仅测试依赖：`github.com/glebarez/sqlite`（单元测试以 SQLite 内存库模拟 MySQL，不进入生产链路）。

## 📄 许可证

MIT