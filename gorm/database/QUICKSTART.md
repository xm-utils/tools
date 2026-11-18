# Database - 快速开始指南

⏱️ 5 分钟跑通：初始化 → CRUD → 条件构建器 → 事务 → 链式 Context API → 读写分离 → 分库分表。

完整功能与参数说明见 [README.md](README.md)，分片细节见 [sharding/QUICKSTART.md](sharding/QUICKSTART.md)。

## 🚀 1. 安装与初始化

```bash
go get github.com/xm-utils/tools/gorm/database
```

```go
package main

import (
    "log"

    "github.com/xm-utils/tools/gorm/database"
)

type User struct {
    ID       int64  `gorm:"primaryKey;autoIncrement"`
    Username string `gorm:"size:50;not null"`
    Email    string `gorm:"size:100"`
    Age      int
    Status   int
}

func main() {
    // 连接字段集中在 db_master 节点（Master *MysqlNodeConfig），必填 user/host/port/database
    config := &database.MysqlConfig{
        Master: &database.MysqlNodeConfig{
            Host:     "127.0.0.1",
            Port:     "3306",
            User:     "root",
            Password: "password",
            Database: "mydb",
        },
        Debug: "true", // 调试日志，"true" 开启

        // 连接池（可选，留空使用默认值 10/100/3600s/600s）
        MaxIdleConns:    10,
        MaxOpenConns:    100,
        ConnMaxLifetime: 3600,
        ConnMaxIdleTime: 600,
    }

    // InitGorm 基于 sync.Once，进程内仅真正初始化一次，重复调用安全幂等
    if err := database.InitGorm(config); err != nil {
        log.Fatalf("init database failed: %v", err)
    }

    _ = database.GetDB() // 需要裸 *gorm.DB 时使用（未初始化返回 nil，请判空）
}
```

> **必填校验**：`InitGorm` 先检查 `config` 与 `config.Master` 非 nil，再校验主库节点 `User/Host/Port/Database` 非空（`Password` 允许为空），缺失会直接返回明确 error。
>
> 📄 需要用 YAML + viper 加载配置时，参照同目录的 [config.example.yaml](config.example.yaml)（含逐行注释与多场景模板）。

## ✏️ 2. CRUD 速查

以下包级函数均为泛型 API，`tx` 传 `nil` 表示使用进程级单例连接（在事务中则传 `tx`）。

```go
// 插入
user := &User{Username: "zhangsan", Email: "zhangsan@example.com", Age: 25}
err := database.Insert[User](nil, user)

// 批量插入（每批 100 条，返回累计影响行数）
affected, err := database.InsertBatch[User](nil, 100, users)

// 按 ID 查单条（强制走主库，未命中/出错返回 nil）
found := database.FindOne[User](1001)

// 按结构体非零字段查单条（返回 *User，未命中返回 nil）
found = database.ReadOne(User{Username: "zhangsan"})

// 查列表（不分页）
list, err := database.FindList[User](database.Eq("status", 1))

// 分页查询（返回列表 + 总数）
param := database.ListParam{
    Cond:  database.Eq("status", 1),
    Order: []string{"-created_at", "id"}, // "-" 前缀表示 DESC
    Page:  &myPageParam{PageNum: 1, PageSize: 20}, // 需实现 database.PageParam 接口
}
rows, total, err := database.FindAll[User](param)

// 更新（仅非零字段；指定列名时只更新这些列）
user.Age = 26
err = database.Update[User](nil, user, "age")

// 差异更新：比对 old 与 form，仅把非空且变化的字段写库；无差异直接返回 nil
err = database.UpdateModel[User](nil, old, form)
// 传 whereCols（Go 字段名）时：主键条件 AND 这些字段的值作为额外 WHERE
err = database.UpdateModel[User](tx, old, form, "Status")

// 条件批量更新（cond 为空时拒绝执行，避免全表更新）
err = database.UpdateByCondition[User](nil, database.Eq("status", 1),
    map[string]interface{}{"status": 0})

// 删除
err = database.Delete[User](nil, user)
err = database.DeleteByCondition[User](nil, database.Eq("status", 0))

// 统计
count, err := database.Count[User](database.Eq("age", 25))

// 原生 SQL
err = database.Exec("UPDATE users SET status = ? WHERE age > ?", 0, 18)
summaries, err := database.Raw[UserSummary]("SELECT age, COUNT(*) as count FROM users GROUP BY age")
```

## 🔍 3. 条件构建器

```go
// 链式：第一个参数 bool 控制该条件是否生效（便于动态拼接）
cond := database.NewCondition().
    Eq(true, "status", 1).
    Like(true, "username", "zhang").        // %zhang%，自动转义值中的 % _ \
    In(false, "age", []interface{}{20, 25}). // false：该条件被跳过
    Between(true, "created_at", start, end).
    IsNotNull(true, "email")

// 快捷函数：单条件一步构造（独立 *Condition，链式请用方法形式）
cond2 := database.Eq("status", 1)
cond3 := database.Gt("age", 18)
```

支持：`Eq / NotEq / Gt / Gte / Lt / Lte / In / NotIn / Like / LLike / RLike / NotLike / Between / IsNull / IsNotNull`。多个条件之间一律 **AND**。

> **LIKE 转义**：用户输入中的 `%`、`_`、`\` 会被自动转义为字面量匹配，框架添加的通配符 `%` 不受影响。

## 🔄 4. 事务

```go
// 方式一：Transaction 闭包（推荐，自动提交/回滚，panic 转 error）
err := database.Transaction(func(tx *gorm.DB) error {
    if err := database.Insert[User](tx, user); err != nil {
        return err
    }
    return database.Insert[Order](tx, order)
})

// 方式二：Tx 对象手动控制
tx := database.NewTx()
err = tx.Execute(func(tx *gorm.DB) error {
    return database.Insert[User](tx, user)
})
```

## ⚡ 5. 链式 Context API

`Ctx[T](ctx)` 是全功能构建器：链式方法只记录意图，终端操作才执行 SQL；`ctx` 超时/取消会中断进行中的查询。

```go
ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
defer cancel()

// 读：条件 + 排序 + 分页
users, total, err := database.Ctx[User](ctx).
    Where(database.Eq("status", 1)).
    Order("-created_at").
    Page(pageParam).
    FindAll()

// 读：强制主库（写后立读）/ 尝试从库
u, err := database.Ctx[User](ctx).Primary().FindOne(1001)

// 写：事务内按条件批量更新
err = database.Ctx[User](ctx).Tx(tx).
    Where(database.Eq("team", "x")).
    UpdateByCondition(map[string]interface{}{"age": 99})

// 删除：条件为空会被拒绝，避免全表删除
err = database.Ctx[User](ctx).Where(database.Eq("status", 0)).DeleteByCondition()
```

## 🗄️ 6. 一主多从读写分离

主库用 `config.Master`（节点配置），`Slaves` 配置多个从库（同一种节点结构）。**默认全部读写走主库**；只有实现 `ReplicaReader` 并注册的模型，其表读操作才走从库。

> ❗ 当前实现不再自动继承主库凭证：主库与每个从库都需各自填写完整 `Host/Port/User/Password/Database`（`Charset/Location` 留空则用默认 utf8mb4/Local）。

```go
config.Master = &database.MysqlNodeConfig{Host: "10.0.0.1", Port: "3306", User: "root", Password: "password", Database: "mydb"}
config.Slaves = []database.MysqlNodeConfig{
    {Host: "10.0.1.1", Port: "3306", User: "root", Password: "password", Database: "mydb"},
    {Host: "10.0.1.2", Port: "3306", User: "root", Password: "password", Database: "mydb"},
}

// 模型声明读走从库
func (Order) ReadFromReplica() bool { return true }

// InitGorm 之前注册（通常在 init()）
func init() { database.RegisterModels(&Order{}) }

database.InitGorm(config)

// 包级显式路由读器（也可用 Ctx[T](ctx).Primary()/.Replica()）
o, err := database.Replica[Order]().FindOne(1)   // 读 → 从库（随机）
u, err := database.Primary[User]().FindOne(1001) // 读 → 强制主库
```

## 🧩 7. 分库分表（sharding 子包）

### 取模分片

```go
import "github.com/xm-utils/tools/gorm/database/sharding"

config := sharding.Config{
    NumberOfShards:      10,                 // users_00 ... users_09（后缀宽度按位数自动生成）
    ShardingKey:         "id",
    PrimaryKeyGenerator: sharding.PKSnowflake, // 雪花主键，节点数不超过 1024
}

s := sharding.Register(config, &User{})
if err := s.Initialize(db); err != nil { /* 处理 */ }
if err := db.Use(s); err != nil { /* 处理 */ }

db.Create(&User{Username: "zhangsan"})       // 自动路由 + 自动生成 id
db.Where("id = ?", id).First(&user)          // 按分片键路由
lastSQL := s.LastQuery()                      // 查看最后一次改写后的 SQL
```

### 时间范围分片

```go
config := sharding.Config{
    ShardingType:        "time_range",
    TimeColumn:          "created_at",
    TimeRangeFormat:     "month", // year/month/week/day → orders_202601 ...
    PrimaryKeyGenerator: sharding.PKSnowflake,
}
s := sharding.Register(config, &Order{})
_ = s.Initialize(db)
_ = db.Use(s)
```

**注意事项**：
- 查询 WHERE 需包含分片键（`=`），时间分片另支持 `> >= < <= BETWEEN`（取下界）路由；缺失分片键报 `ErrMissingShardingKey`，不会全分片扫描。
- `AutoMigrate` 对时间分片只兜底创建「当前周期」表；历史/后续周期表用 `sharding.GenerateTimeRangeTables` 生成表名后显式建表。
- 分片键为负数 int 时取模路由已做非负归一，无需调用方处理。

## ❓ 8. 常见问题

**Q: `GetDB()` 返回 nil？**
A: 数据库未初始化。先调用 `InitGorm(config)` 并检查返回的 error；本库所有 API 遵循「panic 转 error」约定，不会因未初始化直接崩溃。

**Q: 重复调用 `InitGorm` 会重建连接吗？**
A: 不会。内部 `sync.Once` 保证只初始化一次，后续调用直接返回首次结果。

**Q: 事务里的函数 panic 了会怎样？**
A: `Transaction` / `NewTx` / `Execute` 均带 recover，panic 会被转为 error 返回（含堆栈），不会击穿进程。

**Q: `InsertBatch` 的 `batchSize` 传 0？**
A: `batchSize <= 0` 时整批一次写入（等价于 `len(dataList)`），不会死循环。

**Q: 分片表怎么建？**
A: 取模分片直接 `db.AutoMigrate(&User{})`，插件会按后缀逐表创建；时间分片见上文注意事项。

## 📚 更多文档

- [README.md](README.md) —— 完整功能说明
- [sharding/README.md](sharding/README.md) —— 分片插件详解
- [sharding/QUICKSTART.md](sharding/QUICKSTART.md) —— 时间范围分片速查
