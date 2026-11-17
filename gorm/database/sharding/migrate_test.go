package sharding

import (
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// migrateOrder 仅用于验证 time_range AutoMigrate 后缀回退，表名固定为 orders。
type migrateOrder struct {
	ID        int64 `gorm:"primaryKey"`
	UserID    int64
	CreatedAt time.Time
}

func (migrateOrder) TableName() string { return "orders" }

// TestTimeRangeAutoMigrateSuffixFallback 验证 P2-1：time_range 分片在未提供 ShardingSuffixs 时，
// splitShardingDsts 应回退生成「当前周期」分片表，而非报 suffixs is empty 使 AutoMigrate 失败。
func TestTimeRangeAutoMigrateSuffixFallback(t *testing.T) {
	gdb, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("打开内存 sqlite 失败: %v", err)
	}

	s := Register(Config{
		ShardingType:          "time_range",
		ShardingKey:           "created_at",
		TimeColumn:            "created_at",
		TimeRangeFormat:       "month",
		PrimaryKeyGenerator:   PKCustom,
		PrimaryKeyGeneratorFn: func(int64) int64 { return 0 },
	}, "orders")
	s.DB = gdb
	if err := s.compile(); err != nil {
		t.Fatalf("compile 失败: %v", err)
	}

	m := ShardingMigrator{Migrator: gdb.Migrator(), sharding: s, dialector: gdb.Dialector}
	shardingDsts, noSharding, err := m.splitShardingDsts(&migrateOrder{})
	if err != nil {
		t.Fatalf("time_range splitShardingDsts 不应再报 suffixs empty，实际=%v", err)
	}
	if len(noSharding) != 0 {
		t.Errorf("time_range 表不应落入 noSharding，实际=%d", len(noSharding))
	}
	if len(shardingDsts) != 1 {
		t.Fatalf("应生成当前周期 1 张分片表，实际=%d", len(shardingDsts))
	}

	suffix, err := GetTimeRangeSuffix(time.Now(), "month")
	if err != nil {
		t.Fatalf("GetTimeRangeSuffix 失败: %v", err)
	}
	want := "orders" + suffix
	if shardingDsts[0].table != want {
		t.Errorf("期望分片表 %q，实际=%q", want, shardingDsts[0].table)
	}
}
