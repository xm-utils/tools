package sharding

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/longbridgeapp/sqlparser"
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

// TestCompileSnowflakeShardsLimit 验证 P2-4：NumberOfShards 超限（>1024）且为 PKSnowflake 时，
// compile() 返回 error 而非 panic；边界值 1024 不应报错。
func TestCompileSnowflakeShardsLimit(t *testing.T) {
	build := func(shards uint) *Sharding {
		return &Sharding{
			configs: make(map[string]Config),
			_config: Config{
				NumberOfShards:      shards,
				PrimaryKeyGenerator: PKSnowflake,
			},
			_tables: []interface{}{"users"},
		}
	}

	t.Run("超限返回 error 而非 panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("compile 不应 panic，但恢复了: %v", r)
			}
		}()
		err := build(2048).compile()
		if err == nil {
			t.Fatal("NumberOfShards>1024 且 PKSnowflake 时 compile 应返回 error")
		}
		// P2-6：边界为 >1024（允许 1024），文案应为「not exceed 1024」且带上出错表名
		if !strings.Contains(err.Error(), "should not exceed 1024") {
			t.Errorf("错误文案应包含 'should not exceed 1024'，实际=%q", err.Error())
		}
		if !strings.Contains(err.Error(), `"users"`) {
			t.Errorf("错误文案应包含出错表名，实际=%q", err.Error())
		}
	})

	t.Run("边界 1024 不报错", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("compile 不应 panic，但恢复了: %v", r)
			}
		}()
		if err := build(1024).compile(); err != nil {
			t.Fatalf("NumberOfShards==1024 不应报错，实际=%v", err)
		}
	})
}

func TestTimeStringParsing(t *testing.T) {
	// 经由生产入口 Register + compile() 构造，拿到真实的 ShardingAlgorithm，
	// 避免直接调用裸 Config 上未初始化的 nil 算法函数。
	sharding := Register(Config{
		ShardingType:    "time_range",
		TimeRangeFormat: "month",
	}, "orders")
	if err := sharding.compile(); err != nil {
		t.Fatalf("compile 失败: %v", err)
	}
	config, exists := sharding.configs["orders"]
	if !exists {
		t.Fatal("expected orders config to exist")
	}
	if config.ShardingAlgorithm == nil {
		t.Fatal("expected ShardingAlgorithm to be set by compile")
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

// TestCompileModulusShardSuffixWidth 验证 P1-1：取模分片后缀宽度按 NumberOfShards 位数动态生成，
// 修复旧实现 NumberOfShards>=10000 时 tableFormat 为空、后缀恒为空串导致数据静默误落基表的缺陷。
func TestCompileModulusShardSuffixWidth(t *testing.T) {
	build := func(shards uint) Config {
		s := &Sharding{
			configs: make(map[string]Config),
			_config: Config{
				NumberOfShards:        shards,
				ShardingKey:           "uid",
				PrimaryKeyGenerator:   PKCustom,
				PrimaryKeyGeneratorFn: func(int64) int64 { return 0 },
			},
			_tables: []interface{}{"users"},
		}
		if err := s.compile(); err != nil {
			t.Fatalf("shards=%d compile 失败: %v", shards, err)
		}
		return s.configs["users"]
	}

	cases := []struct {
		shards     uint
		value      any
		wantSuffix string
	}{
		{10, 7, "_07"},          // 旧档 <100 -> 2 位
		{100, 7, "_007"},        // 旧档 <1000 -> 3 位
		{1000, 7, "_0007"},      // 旧档 <10000 -> 4 位
		{10000, 2345, "_02345"}, // 新覆盖：>=10000 -> 5 位（旧实现此处后缀为空）
		{100000, 1, "_000001"},  // 6 位
	}

	for _, c := range cases {
		cfg := build(c.shards)
		if cfg.ShardingAlgorithm == nil {
			t.Fatalf("shards=%d: ShardingAlgorithm 不应为空", c.shards)
		}
		suffix, err := cfg.ShardingAlgorithm(c.value)
		if err != nil {
			t.Fatalf("shards=%d value=%v: 意外错误 %v", c.shards, c.value, err)
		}
		if suffix == "" {
			t.Fatalf("shards=%d: 后缀不应为空（旧 bug 会静默误落基表）", c.shards)
		}
		if suffix != c.wantSuffix {
			t.Errorf("shards=%d value=%v: 期望后缀 %q, 实际 %q", c.shards, c.value, c.wantSuffix, suffix)
		}
	}
}

// TestTimeRangeNonInsertValueRangeOps 验证 P1-2：时间范围分片路由识别 >=、<=、>、<、BETWEEN，
// 取比较值（BETWEEN 取下界）正确路由到对应时间分片表。
func TestTimeRangeNonInsertValueRangeOps(t *testing.T) {
	s := Register(Config{
		ShardingType:    "time_range",
		ShardingKey:     "created_at",
		TimeRangeFormat: "month",
	}, "orders")
	if err := s.compile(); err != nil {
		t.Fatalf("compile 失败: %v", err)
	}

	cases := []struct {
		name  string
		query string
		args  []any
		want  string // 期望 stQuery 命中的分片表名
	}{
		{"gte", "SELECT * FROM orders WHERE created_at >= ?", []any{"2026-01-15 00:00:00"}, "orders_202601"},
		{"lte", "SELECT * FROM orders WHERE created_at <= ?", []any{"2026-02-20 00:00:00"}, "orders_202602"},
		{"gt", "SELECT * FROM orders WHERE created_at > ?", []any{"2026-03-01 00:00:00"}, "orders_202603"},
		{"lt", "SELECT * FROM orders WHERE created_at < ?", []any{"2026-04-30 00:00:00"}, "orders_202604"},
		{"between", "SELECT * FROM orders WHERE created_at BETWEEN ? AND ?", []any{"2026-05-01 00:00:00", "2026-07-31 00:00:00"}, "orders_202605"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, stQuery, tableName, err := s.resolve(c.query, c.args...)
			if err != nil {
				t.Fatalf("resolve 出错: %v", err)
			}
			if tableName != "orders" {
				t.Fatalf("期望 tableName=orders, 实际=%s", tableName)
			}
			if !strings.Contains(stQuery, c.want) {
				t.Errorf("期望 stQuery 含 %q, 实际=%q", c.want, stQuery)
			}
		})
	}
}

// TestResolvePrimaryKeyGeneratorPanicToError 验证 P2-4：主键生成回调 panic 时，
// resolve 应将其转为 error 返回，而不是让 panic 沿 ConnPool 执行链冒泡。
func TestResolvePrimaryKeyGeneratorPanicToError(t *testing.T) {
	s := Register(Config{
		NumberOfShards:        4,
		ShardingKey:           "uid",
		PrimaryKeyGenerator:   PKCustom,
		PrimaryKeyGeneratorFn: func(int64) int64 { panic("boom") },
	}, "users")
	if err := s.compile(); err != nil {
		t.Fatalf("compile 失败: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("resolve 不应让主键生成器 panic 冒泡，实际恢复了: %v", r)
		}
	}()

	_, _, _, err := s.resolve("INSERT INTO users (uid, name) VALUES (?, ?)", 1, "x")
	if err == nil {
		t.Fatal("主键生成器 panic 应被转为 error 返回")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error 应包含原始 panic 信息，实际=%q", err.Error())
	}
}

// TestShardingBindArgOutOfRangeNoPanic 验证 P2-5：insertValue / nonInsertValue 的 id 路径
// 对 BindExpr.Pos 越界做防护，异常 SQL 下返回明确 error 而非触发 index out of range panic。
func TestShardingBindArgOutOfRangeNoPanic(t *testing.T) {
	s := Register(Config{
		NumberOfShards:        4,
		ShardingKey:           "uid",
		PrimaryKeyGenerator:   PKCustom,
		PrimaryKeyGeneratorFn: func(int64) int64 { return 0 },
	}, "users")
	if err := s.compile(); err != nil {
		t.Fatalf("compile 失败: %v", err)
	}

	guard := func(name string, fn func() error) {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("%s: 越界绑定参数不应 panic，实际恢复: %v", name, r)
				}
			}()
			if err := fn(); err == nil {
				t.Fatal("期望返回越界 error，实际为 nil")
			} else if !strings.Contains(err.Error(), "out of range") {
				t.Errorf("error 应含 out of range，实际=%q", err.Error())
			}
		})
	}

	// insertValue：BindExpr.Pos 超出 args 长度
	guard("insertValue", func() error {
		names := []*sqlparser.Ident{{Name: "uid"}}
		exprs := []sqlparser.Expr{&sqlparser.BindExpr{Pos: 99}}
		_, _, err := s.insertValue("uid", names, exprs)
		return err
	})

	// nonInsertValue：id 等值路径的 BindExpr.Pos 越界
	guard("nonInsertValue_id", func() error {
		cond := &sqlparser.BinaryExpr{
			X:  &sqlparser.Ident{Name: "id"},
			Op: sqlparser.EQ,
			Y:  &sqlparser.BindExpr{Pos: 50},
		}
		cfg := s.configs["users"]
		_, _, _, err := s.nonInsertValue(cfg, cond)
		return err
	})
}
