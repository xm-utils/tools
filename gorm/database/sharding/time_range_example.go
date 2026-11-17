package sharding

import (
	"fmt"
	"time"

	"gorm.io/gorm"
)

// TimeRangeShardingExample 演示如何使用时间范围分表功能
func TimeRangeShardingExample() {
	// 示例1: 按月分表（默认）
	monthlyConfig := Config{
		ShardingType:        "time_range",
		TimeColumn:          "created_at",
		TimeRangeFormat:     "month", // 格式: _202601, _202602, ...
		DoubleWrite:         false,
		PrimaryKeyGenerator: PKSnowflake,
	}

	// 注册分表配置
	sharding := Register(monthlyConfig, &Order{})

	// 示例2: 按日分表
	dailyConfig := Config{
		ShardingType:        "time_range",
		TimeColumn:          "created_at",
		TimeRangeFormat:     "day", // 格式: _20260101, _20260102, ...
		DoubleWrite:         false,
		PrimaryKeyGenerator: PKSnowflake,
	}

	shardingDaily := Register(dailyConfig, &DailyLog{})

	// 示例3: 按年分表
	yearlyConfig := Config{
		ShardingType:        "time_range",
		TimeColumn:          "created_at",
		TimeRangeFormat:     "year", // 格式: _2026, _2027, ...
		DoubleWrite:         false,
		PrimaryKeyGenerator: PKSnowflake,
	}

	shardingYearly := Register(yearlyConfig, &YearlyReport{})

	// 示例4: 按周分表
	weeklyConfig := Config{
		ShardingType:        "time_range",
		TimeColumn:          "created_at",
		TimeRangeFormat:     "week", // 格式: _2026W01, _2026W02, ...
		DoubleWrite:         false,
		PrimaryKeyGenerator: PKSnowflake,
	}

	shardingWeekly := Register(weeklyConfig, &WeeklyStats{})

	// 使用示例
	fmt.Println("Monthly sharding:", sharding)
	fmt.Println("Daily sharding:", shardingDaily)
	fmt.Println("Yearly sharding:", shardingYearly)
	fmt.Println("Weekly sharding:", shardingWeekly)
}

// Order 订单模型 - 按月分表
type Order struct {
	ID        int64 `gorm:"primaryKey"`
	UserID    int64 `gorm:"index"`
	Amount    float64
	Status    int
	CreatedAt time.Time `gorm:"index"`
	UpdatedAt time.Time
}

// DailyLog 日志模型 - 按日分表
type DailyLog struct {
	ID        int64  `gorm:"primaryKey"`
	Level     string `gorm:"size:20"`
	Message   string
	CreatedAt time.Time `gorm:"index"`
}

// YearlyReport 年报模型 - 按年分表
type YearlyReport struct {
	ID        int64 `gorm:"primaryKey"`
	Year      int
	Data      string
	CreatedAt time.Time `gorm:"index"`
}

// WeeklyStats 周统计模型 - 按周分表
type WeeklyStats struct {
	ID        int64 `gorm:"primaryKey"`
	Week      int
	Metrics   string
	CreatedAt time.Time `gorm:"index"`
}

// InitTimeRangeSharding 初始化时间范围分表
func InitTimeRangeSharding(db *gorm.DB, config Config, tables ...interface{}) error {
	sharding := Register(config, tables...)

	if err := sharding.Initialize(db); err != nil {
		return fmt.Errorf("failed to initialize sharding: %w", err)
	}

	// 注册插件
	if err := db.Use(sharding); err != nil {
		return fmt.Errorf("failed to register sharding plugin: %w", err)
	}

	return nil
}

// GetTimeRangeSuffix 根据时间获取表后缀
func GetTimeRangeSuffix(t time.Time, format string) (string, error) {
	switch format {
	case "year":
		return "_" + t.Format("2006"), nil
	case "month":
		return "_" + t.Format("200601"), nil
	case "week":
		_, week := t.ISOWeek()
		return fmt.Sprintf("_%dW%02d", t.Year(), week), nil
	case "day":
		return "_" + t.Format("20060102"), nil
	default:
		return "", fmt.Errorf("unsupported time range format: %s", format)
	}
}

// GenerateTimeRangeTables 生成指定时间范围内的所有表名
func GenerateTimeRangeTables(baseTableName string, startTime, endTime time.Time, format string) ([]string, error) {
	var tables []string

	current := startTime
	for !current.After(endTime) {
		suffix, err := GetTimeRangeSuffix(current, format)
		if err != nil {
			return nil, err
		}

		tableName := baseTableName + suffix
		tables = append(tables, tableName)

		// 根据格式递增时间
		switch format {
		case "year":
			current = current.AddDate(1, 0, 0)
		case "month":
			current = current.AddDate(0, 1, 0)
		case "week":
			current = current.AddDate(0, 0, 7)
		case "day":
			current = current.AddDate(0, 0, 1)
		}
	}

	return tables, nil
}
