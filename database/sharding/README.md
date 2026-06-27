# 分表功能说明

本模块提供了强大的分表功能,支持两种分片策略:
1. **取模分片** (Modulus Sharding) - 基于分片键的哈希取模
2. **时间范围分片** (Time Range Sharding) - 基于时间字段按周期分表

## 功能特性

### 取模分片 (Modulus Sharding)
- 支持基于整数或字符串字段进行分片
- 自动计算表后缀格式
- 适合数据均匀分布的场景

### 时间范围分片 (Time Range Sharding) ⭐ NEW
- **按年分表**: `orders_2026`, `orders_2027`, ...
- **按月分表**: `orders_202601`, `orders_202602`, ...
- **按周分表**: `orders_2026W01`, `orders_2026W02`, ...
- **按日分表**: `orders_20260101`, `orders_20260102`, ...
- 支持多种时间类型: `time.Time`, `string`, `int64` (Unix时间戳)
- 自动解析多种时间格式
- 动态创建新表,无需预定义所有表

## 使用示例

### 1. 按月分表 (订单系统)

```go
package main

import (
    "time"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
    "your-project/database/sharding"
)

type Order struct {
    ID        int64     `gorm:"primaryKey"`
    UserID    int64     `gorm:"index"`
    Amount    float64
    Status    int
    CreatedAt time.Time `gorm:"index"`
    UpdatedAt time.Time
}

func main() {
    db, err := gorm.Open(mysql.Open("dsn..."), &gorm.Config{})
    if err != nil {
        panic(err)
    }

    // 配置按月分表
    config := sharding.Config{
        ShardingType:        "time_range",
        TimeColumn:          "created_at",
        TimeRangeFormat:     "month", // _202601, _202602, ...
        PrimaryKeyGenerator: sharding.PKSnowflake,
    }

    shardingInstance := sharding.Register(config, &Order{})
    
    if err := shardingInstance.Initialize(db); err != nil {
        panic(err)
    }
    
    if err := db.Use(shardingInstance); err != nil {
        panic(err)
    }

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
    db.Where("created_at = ?", time.Now()).Find(&orders)
}
```

### 2. 按日分表 (日志系统)

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
    TimeRangeFormat:     "day", // _20260101, _20260102, ...
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

### 3. 按年分表 (报表系统)

```go
type YearlyReport struct {
    ID        int64     `gorm:"primaryKey"`
    Year      int
    Data      string
    CreatedAt time.Time `gorm:"index"`
}

config := sharding.Config{
    ShardingType:        "time_range",
    TimeColumn:          "created_at",
    TimeRangeFormat:     "year", // _2026, _2027, ...
    PrimaryKeyGenerator: sharding.PKSnowflake,
}
```

### 4. 按周分表 (统计数据)

```go
type WeeklyStats struct {
    ID        int64     `gorm:"primaryKey"`
    Week      int
    Metrics   string
    CreatedAt time.Time `gorm:"index"`
}

config := sharding.Config{
    ShardingType:        "time_range",
    TimeColumn:          "created_at",
    TimeRangeFormat:     "week", // _2026W01, _2026W02, ...
    PrimaryKeyGenerator: sharding.PKSnowflake,
}
```

### 5. 取模分表 (用户系统)

```go
type User struct {
    ID       int64  `gorm:"primaryKey"`
    Username string
    Email    string
}

// 配置取模分表 - 分成10个表
config := sharding.Config{
    NumberOfShards:      10, // users_0, users_1, ..., users_9
    ShardingKey:         "id",
    PrimaryKeyGenerator: sharding.PKSnowflake,
}

shardingInstance := sharding.Register(config, &User{})
db.Use(shardingInstance)
```

## 高级用法

### 生成指定时间范围内的所有表名

```go
import "your-project/database/sharding"

startTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
endTime := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)

// 生成2026年所有月份表
tables, err := sharding.GenerateTimeRangeTables("orders", startTime, endTime, "month")
// 结果: ["orders_202601", "orders_202602", ..., "orders_202612"]

// 批量创建表
for _, table := range tables {
    db.Exec("CREATE TABLE IF NOT EXISTS " + table + " (...)")
}
```

### 自定义分片算法

```go
config := sharding.Config{
    ShardingType:    "time_range",
    TimeColumn:      "created_at",
    TimeRangeFormat: "month",
    
    // 自定义分片算法
    ShardingAlgorithm: func(value interface{}) (suffix string, err error) {
        // 实现自己的逻辑
        return "_custom_suffix", nil
    },
}
```

### 双写模式 (迁移期间使用)

```go
config := sharding.Config{
    ShardingType:        "time_range",
    TimeColumn:          "created_at",
    TimeRangeFormat:     "month",
    DoubleWrite:         true, // 同时写入主表和分片表
    PrimaryKeyGenerator: sharding.PKSnowflake,
}
```

## 配置参数说明

| 参数 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| `ShardingType` | string | 分片类型: `"modulus"` 或 `"time_range"` | `"modulus"` |
| `ShardingKey` | string | 用于分片的字段名 | - |
| `NumberOfShards` | uint | 分片数量 (仅取模分片) | - |
| `TimeColumn` | string | 时间字段名 (仅时间范围分片) | `"created_at"` |
| `TimeRangeFormat` | string | 时间格式: `"year"`, `"month"`, `"week"`, `"day"` | `"month"` |
| `DoubleWrite` | bool | 是否双写到主表 | `false` |
| `PrimaryKeyGenerator` | int | 主键生成器: `PKSnowflake`, `PKPGSequence`, `PKMySQLSequence`, `PKCustom` | - |
| `ShardingAlgorithm` | func | 自定义分片算法函数 | 自动生成 |
| `ShardingSuffixs` | func | 生成所有表后缀的函数 | 自动生成 |

## 时间格式说明

| 格式 | 示例 | 说明 |
|------|------|------|
| `year` | `_2026` | 按年分表 |
| `month` | `_202601` | 按月分表 |
| `week` | `_2026W03` | 按周分表 (ISO周) |
| `day` | `_20260115` | 按日分表 |

## 支持的时间类型

时间范围分片支持以下时间类型:

1. **time.Time**: 直接使用
   ```go
   CreatedAt: time.Now()
   ```

2. **string**: 自动解析多种格式
   ```go
   "2026-01-15 10:30:00"
   "2026-01-15T10:30:00Z"
   "2026-01-15"
   ```

3. **int64**: Unix时间戳 (秒)
   ```go
   1736935800 // 2025-01-15 10:30:00 UTC
   ```

## 注意事项

1. **时间范围分片**: 
   - 表会根据需要动态创建,建议定期预创建未来时间的表
   - 使用 `GenerateTimeRangeTables` 工具函数批量生成表名

2. **取模分片**:
   - 需要在初始化时确定分片数量
   - 分片数量确定后不建议修改

3. **主键生成**:
   - 推荐使用 `PKSnowflake` (雪花算法)
   - 确保分布式环境下的唯一性

4. **查询优化**:
   - 尽量在查询中包含包含分片键或时间字段
   - 避免跨表全表扫描

## 性能建议

1. **索引优化**: 在分片键和时间字段上建立索引
2. **批量操作**: 使用批量插入提高性能
3. **定期归档**: 对历史数据进行归档处理
4. **监控告警**: 监控各分片表的数据量分布

## 测试

运行测试用例:

```bash
cd database/sharding
go test -v -run TestTimeRange
```

## 更多信息

- 查看 [time_range_example.go](time_range_example.go) 获取完整示例
- 查看 [time_range_test.go](time_range_test.go) 获取测试用例
- 查看 [sharding.go](sharding.go) 了解实现细节
