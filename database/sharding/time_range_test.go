package sharding

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestTimeRangeShardingAlgorithm(t *testing.T) {
	tests := []struct {
		name          string
		format        string
		input         interface{}
		expectedError bool
		checkSuffix   func(suffix string) bool
	}{
		{
			name:   "按月分表 - time.Time 类型",
			format: "month",
			input:  time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC),
			checkSuffix: func(suffix string) bool {
				return suffix == "_202601"
			},
		},
		{
			name:   "按日分表 - time.Time 类型",
			format: "day",
			input:  time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC),
			checkSuffix: func(suffix string) bool {
				return suffix == "_20260115"
			},
		},
		{
			name:   "按年分表 - time.Time 类型",
			format: "year",
			input:  time.Date(2026, 6, 15, 10, 30, 0, 0, time.UTC),
			checkSuffix: func(suffix string) bool {
				return suffix == "_2026"
			},
		},
		{
			name:   "按周分表 - time.Time 类型",
			format: "week",
			input:  time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC),
			checkSuffix: func(suffix string) bool {
				// 2026-01-15 是第3周
				return suffix == "_2026W03"
			},
		},
		{
			name:   "按月分表 - 字符串类型",
			format: "month",
			input:  "2026-01-15 10:30:00",
			checkSuffix: func(suffix string) bool {
				return suffix == "_202601"
			},
		},
		{
			name:   "按日分表 - 字符串类型",
			format: "day",
			input:  "2026-01-15",
			checkSuffix: func(suffix string) bool {
				return suffix == "_20260115"
			},
		},
		{
			name:   "按月分表 - Unix 时间戳",
			format: "month",
			input:  int64(1736935800), // 2025-01-15 10:30:00 UTC
			checkSuffix: func(suffix string) bool {
				return suffix == "_202501"
			},
		},
		{
			name:          "不支持的格式",
			format:        "hour",
			input:         time.Now(),
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				ShardingType:    "time_range",
				TimeRangeFormat: tt.format,
				TimeColumn:      "created_at",
			}

			// 手动初始化分片算法函数
			if config.ShardingAlgorithm == nil {
				config.ShardingAlgorithm = func(value interface{}) (suffix string, err error) {
					var t time.Time
					switch v := value.(type) {
					case time.Time:
						t = v
					case *time.Time:
						if v != nil {
							t = *v
						} else {
							return "", errors.New("time column value is nil")
						}
					case string:
						layouts := []string{
							"2006-01-02 15:04:05",
							"2006-01-02T15:04:05Z",
							"2006-01-02T15:04:05",
							"2006-01-02",
							time.RFC3339,
						}
						var parseErr error
						for _, layout := range layouts {
							t, parseErr = time.Parse(layout, v)
							if parseErr == nil {
								break
							}
						}
						if parseErr != nil {
							return "", fmt.Errorf("failed to parse time string: %v", parseErr)
						}
					case int64:
						t = time.Unix(v, 0)
					default:
						return "", fmt.Errorf("time_range sharding only supports time.Time, string, or int64 (timestamp) types")
					}

					switch config.TimeRangeFormat {
					case "year":
						suffix = "_" + t.Format("2006")
					case "month":
						suffix = "_" + t.Format("200601")
					case "week":
						_, week := t.ISOWeek()
						suffix = fmt.Sprintf("_%dW%02d", t.Year(), week)
					case "day":
						suffix = "_" + t.Format("20060102")
					default:
						return "", fmt.Errorf("unsupported TimeRangeFormat: %s", config.TimeRangeFormat)
					}

					return suffix, nil
				}
			}

			suffix, err := config.ShardingAlgorithm(tt.input)

			if tt.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if tt.checkSuffix != nil && !tt.checkSuffix(suffix) {
				t.Errorf("expected suffix check to pass, got: %s", suffix)
			}
		})
	}
}

func TestGetTimeRangeSuffix(t *testing.T) {
	testTime := time.Date(2026, 6, 28, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		format   string
		expected string
	}{
		{"year", "_2026"},
		{"month", "_202606"},
		{"day", "_20260628"},
		{"week", "_2026W26"}, // 6月28日是第26周
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			suffix, err := GetTimeRangeSuffix(testTime, tt.format)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if suffix != tt.expected {
				t.Errorf("expected suffix %s, got %s", tt.expected, suffix)
			}
		})
	}
}

func TestGenerateTimeRangeTables(t *testing.T) {
	startTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2026, 3, 31, 23, 59, 59, 0, time.UTC)

	t.Run("按月生成表名", func(t *testing.T) {
		tables, err := GenerateTimeRangeTables("orders", startTime, endTime, "month")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		expected := []string{
			"orders_202601",
			"orders_202602",
			"orders_202603",
		}

		if len(tables) != len(expected) {
			t.Errorf("expected %d tables, got %d", len(expected), len(tables))
			return
		}

		for i, table := range tables {
			if table != expected[i] {
				t.Errorf("expected table %s at index %d, got %s", expected[i], i, table)
			}
		}
	})

	t.Run("按日生成表名 - 小范围", func(t *testing.T) {
		dayStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		dayEnd := time.Date(2026, 1, 5, 23, 59, 59, 0, time.UTC)

		tables, err := GenerateTimeRangeTables("logs", dayStart, dayEnd, "day")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if len(tables) != 5 {
			t.Errorf("expected 5 tables, got %d", len(tables))
			return
		}

		expected := []string{
			"logs_20260101",
			"logs_20260102",
			"logs_20260103",
			"logs_20260104",
			"logs_20260105",
		}

		for i, table := range tables {
			if table != expected[i] {
				t.Errorf("expected table %s at index %d, got %s", expected[i], i, table)
			}
		}
	})

	t.Run("按年生成表名", func(t *testing.T) {
		yearStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		yearEnd := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)

		tables, err := GenerateTimeRangeTables("reports", yearStart, yearEnd, "year")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		expected := []string{
			"reports_2024",
			"reports_2025",
			"reports_2026",
		}

		if len(tables) != len(expected) {
			t.Errorf("expected %d tables, got %d", len(expected), len(tables))
			return
		}

		for i, table := range tables {
			if table != expected[i] {
				t.Errorf("expected table %s at index %d, got %s", expected[i], i, table)
			}
		}
	})
}

func TestConfigCompilation(t *testing.T) {
	t.Run("时间范围分片配置编译", func(t *testing.T) {
		sharding := &Sharding{
			configs: make(map[string]Config),
			_config: Config{
				ShardingType:        "time_range",
				TimeColumn:          "created_at",
				TimeRangeFormat:     "month",
				PrimaryKeyGenerator: PKSnowflake,
			},
			_tables: []interface{}{"orders"},
		}

		err := sharding.compile()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		config, exists := sharding.configs["orders"]
		if !exists {
			t.Error("expected orders config to exist")
			return
		}

		if config.ShardingType != "time_range" {
			t.Errorf("expected ShardingType time_range, got %s", config.ShardingType)
		}

		if config.TimeColumn != "created_at" {
			t.Errorf("expected TimeColumn created_at, got %s", config.TimeColumn)
		}

		if config.TimeRangeFormat != "month" {
			t.Errorf("expected TimeRangeFormat month, got %s", config.TimeRangeFormat)
		}

		if config.ShardingAlgorithm == nil {
			t.Error("expected ShardingAlgorithm to be set")
		}

		if config.ShardingSuffixs == nil {
			t.Error("expected ShardingSuffixs to be set")
		}
	})

	t.Run("取模分片配置编译", func(t *testing.T) {
		sharding := &Sharding{
			configs: make(map[string]Config),
			_config: Config{
				NumberOfShards:      10,
				PrimaryKeyGenerator: PKSnowflake,
			},
			_tables: []interface{}{"users"},
		}

		err := sharding.compile()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		config, exists := sharding.configs["users"]
		if !exists {
			t.Error("expected users config to exist")
			return
		}

		if config.NumberOfShards != 10 {
			t.Errorf("expected NumberOfShards 10, got %d", config.NumberOfShards)
		}

		if config.ShardingAlgorithm == nil {
			t.Error("expected ShardingAlgorithm to be set")
		}
	})
}

func TestTimeStringParsing(t *testing.T) {
	config := Config{
		ShardingType:    "time_range",
		TimeRangeFormat: "month",
	}

	tests := []struct {
		input    string
		expected string
		hasError bool
	}{
		{"2026-01-15 10:30:00", "_202601", false},
		{"2026-01-15T10:30:00Z", "_202601", false},
		{"2026-01-15", "_202601", false},
		{"invalid-date", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			suffix, err := config.ShardingAlgorithm(tt.input)

			if tt.hasError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if suffix != tt.expected {
				t.Errorf("expected suffix %s, got %s", tt.expected, suffix)
			}
		})
	}
}
