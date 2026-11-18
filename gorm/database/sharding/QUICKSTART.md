# 时间范围分表 - 快速开始

## 5分钟上手指南

### 1. 安装依赖

```bash
# sharding 与 database 同属一个模块，直接拉取即可
go get github.com/xm-utils/tools/gorm/database
```

### 2. 最简单的示例

```go
package main

import (
    "time"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
    "github.com/xm-utils/tools/gorm/database/sharding"
)

// 定义订单模型
type Order struct {
    ID        int64     `gorm:"primaryKey"`
    UserID    int64
    Amount    float64
    CreatedAt time.Time `gorm:"index"`
}

func main() {
    // 连接数据库
    db, _ := gorm.Open(mysql.Open("root:password@tcp(localhost:3306)/test?charset=utf8mb4&parseTime=True"), 
        &gorm.Config{})

    // 配置按月分表
    config := sharding.Config{
        ShardingType:        "time_range",
        TimeColumn:          "created_at",
        TimeRangeFormat:     "month", // 自动创建 orders_202601, orders_202602...
        PrimaryKeyGenerator: sharding.PKSnowflake,
    }

    // 注册分表插件
    shardingInstance := sharding.Register(config, &Order{})
    shardingInstance.Initialize(db)
    db.Use(shardingInstance)

    // 插入数据 - 自动路由到当前月份的表
    order := &Order{
        UserID:    1001,
        Amount:    99.99,
        CreatedAt: time.Now(),
    }
    db.Create(order)

    // 查询数据 - 自动从对应月份的表查询
    var orders []Order
    db.Where("user_id = ?", 1001).Find(&orders)
    
    println("成功! 数据已插入到:", shardingInstance.LastQuery())
}
```

### 3. 运行测试验证

```bash
cd gorm/database/sharding
go test -v -run TestTimeRangeShardingAlgorithm
```

预期输出:
```
=== RUN   TestTimeRangeShardingAlgorithm
--- PASS: TestTimeRangeShardingAlgorithm
PASS
```

## 常用场景

### 场景1: 日志系统 (按日分表)

```go
type Log struct {
    ID        int64     `gorm:"primaryKey"`
    Level     string
    Message   string
    CreatedAt time.Time `gorm:"index"`
}

config := sharding.Config{
    ShardingType:        "time_range",
    TimeColumn:          "created_at",
    TimeRangeFormat:     "day", // logs_20260101, logs_20260102...
    PrimaryKeyGenerator: sharding.PKSnowflake,
}
```

### 场景2: 报表系统 (按年分表)

```go
type Report struct {
    ID        int64     `gorm:"primaryKey"`
    Year      int
    Data      string
    CreatedAt time.Time
}

config := sharding.Config{
    ShardingType:        "time_range",
    TimeColumn:          "created_at",
    TimeRangeFormat:     "year", // reports_2026, reports_2027...
    PrimaryKeyGenerator: sharding.PKSnowflake,
}
```

### 场景3: 统计数据 (按周分表)

```go
type Stats struct {
    ID        int64     `gorm:"primaryKey"`
    Metrics   string
    CreatedAt time.Time
}

config := sharding.Config{
    ShardingType:        "time_range",
    TimeColumn:          "created_at",
    TimeRangeFormat:     "week", // stats_2026W01, stats_2026W02...
    PrimaryKeyGenerator: sharding.PKSnowflake,
}
```

## 实用工具

### 批量预创建表

```go
// 预创建未来6个月的订单表
startTime := time.Now()
endTime := startTime.AddDate(0, 6, 0)

tables, _ := sharding.GenerateTimeRangeTables("orders", startTime, endTime, "month")

for _, table := range tables {
    db.Exec(`CREATE TABLE IF NOT EXISTS ` + table + ` (
        id BIGINT PRIMARY KEY,
        user_id BIGINT,
        amount DECIMAL(10,2),
        created_at DATETIME
    )`)
}
```

### 查看最后执行的SQL

```go
db.Create(&order)
println("SQL:", shardingInstance.LastQuery())
// 输出: INSERT INTO orders_202601 ...
```

## 常见问题

### Q1: 如何切换分片策略?

A: 只需修改配置中的 `ShardingType`:

```go
// 取模分片
config.ShardingType = "modulus"
config.NumberOfShards = 10

// 时间范围分片
config.ShardingType = "time_range"
config.TimeRangeFormat = "month"
```

### Q2: 支持哪些时间格式?

A: 支持以下输入:
- `time.Time` 对象
- 字符串: `"2026-01-15 10:30:00"`, `"2026-01-15"`, RFC3339等
- Unix时间戳: `int64` 类型

### Q3: 如何处理跨表查询?

A: 时间范围分片会自动处理,但建议在查询条件中包含时间字段以获得最佳性能:

```go
// ✅ 推荐 - 自动路由到特定表
db.Where("created_at >= ? AND created_at <= ?", start, end).Find(&orders)

// ⚠️ 不推荐 - 可能需要扫描所有表
db.Find(&orders)
```

### Q4: 需要手动创建表吗?

A: 取模分片表由 `AutoMigrate` 按 `NumberOfShards` 全部创建。时间范围分片不会预生成所有历史/未来周期表——`AutoMigrate` 仅回退创建「当前周期」表以保证首次启动可用,历史与后续周期表建议用 `GenerateTimeRangeTables` 批量生成表名后显式建表。

## 下一步

- 📖 查看 [README.md](README.md) 获取完整文档
- 💡 查看 [time_range_example.go](time_range_example.go) 获取更多示例
- 🧪 查看 [time_range_test.go](time_range_test.go) 了解测试用例

## 技术支持

如有问题,请查看:
1. [README.md](README.md) - 详细使用文档与注意事项
2. [CHECKLIST.md](CHECKLIST.md) - 功能与优化清单
3. 运行测试用例验证功能

祝使用愉快! 🎉
