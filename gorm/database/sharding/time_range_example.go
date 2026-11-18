package sharding

import (
	"fmt"
	"time"

	"gorm.io/gorm"
)

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
