package database

import (
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// testUser 是 CRUD 测试用的模型。
type testUser struct {
	ID   int64  `gorm:"primaryKey"`
	Name string `gorm:"column:name"`
	Age  int    `gorm:"column:age"`
	Team string `gorm:"column:team"`
}

func (testUser) TableName() string { return "test_users" }

// openTestDB 打开一个内存 SQLite 库并建表。
// 固定 MaxOpenConns=1，避免连接池多连接导致内存库实例不共享。
func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	gdb, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("打开内存 sqlite 失败: %v", err)
	}
	sqlDB, err := gdb.DB()
	if err != nil {
		t.Fatalf("获取 sql.DB 失败: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)

	if err := gdb.AutoMigrate(&testUser{}); err != nil {
		t.Fatalf("AutoMigrate 失败: %v", err)
	}
	return gdb
}

func seed(t *testing.T, gdb *gorm.DB, rows ...*testUser) {
	t.Helper()
	if err := gdb.Create(&rows).Error; err != nil {
		t.Fatalf("预置数据失败: %v", err)
	}
}

func countBy(t *testing.T, gdb *gorm.DB, team string) int64 {
	t.Helper()
	var c int64
	q := gdb.Model(&testUser{})
	if team != "" {
		q = q.Where("team = ?", team)
	}
	if err := q.Count(&c).Error; err != nil {
		t.Fatalf("统计失败: %v", err)
	}
	return c
}

// TestInsertBatchReturnsAffectedRows 验证 InsertBatch 返回累计影响行数。
func TestInsertBatchReturnsAffectedRows(t *testing.T) {
	gdb := openTestDB(t)

	rows := []*testUser{
		{Name: "u1", Age: 1, Team: "a"},
		{Name: "u2", Age: 2, Team: "a"},
		{Name: "u3", Age: 3, Team: "b"},
		{Name: "u4", Age: 4, Team: "b"},
		{Name: "u5", Age: 5, Team: "c"},
	}

	affected, err := InsertBatch(gdb, 2, rows)
	if err != nil {
		t.Fatalf("InsertBatch 出错: %v", err)
	}
	if affected != int64(len(rows)) {
		t.Fatalf("累计影响行数应为 %d，实际=%d", len(rows), affected)
	}
	if got := countBy(t, gdb, ""); got != int64(len(rows)) {
		t.Fatalf("库内应写入 %d 行，实际=%d", len(rows), got)
	}
}

// TestInsertBatchEmpty 验证空切片返回 (0, nil) 且不影响库。
func TestInsertBatchEmpty(t *testing.T) {
	gdb := openTestDB(t)
	affected, err := InsertBatch(gdb, 3, []*testUser{})
	if err != nil {
		t.Fatalf("空数据不应报错: %v", err)
	}
	if affected != 0 {
		t.Fatalf("空数据累计行数应为 0，实际=%d", affected)
	}
}

// TestUpdateByConditionUsesCondition 验证改用 *Condition 后按条件批量更新。
func TestUpdateByConditionUsesCondition(t *testing.T) {
	gdb := openTestDB(t)
	seed(t, gdb,
		&testUser{Name: "alice", Age: 10, Team: "x"},
		&testUser{Name: "bob", Age: 20, Team: "x"},
		&testUser{Name: "carol", Age: 30, Team: "y"},
	)

	// 仅把 team=x 的 age 批量改为 99
	if err := UpdateByCondition[testUser](gdb, Eq("team", "x"), map[string]interface{}{"age": 99}); err != nil {
		t.Fatalf("UpdateByCondition 出错: %v", err)
	}

	var alice, carol testUser
	if err := gdb.Where("name = ?", "alice").First(&alice).Error; err != nil {
		t.Fatalf("读取 alice 失败: %v", err)
	}
	if err := gdb.Where("name = ?", "carol").First(&carol).Error; err != nil {
		t.Fatalf("读取 carol 失败: %v", err)
	}

	if alice.Age != 99 {
		t.Fatalf("team=x 的 alice 应被更新为 99，实际=%d", alice.Age)
	}
	if carol.Age != 30 {
		t.Fatalf("team=y 的 carol 不应被改动，实际=%d", carol.Age)
	}
}

// TestUpdateByConditionEmptyUpdates 验证 updates 为空时报错。
func TestUpdateByConditionEmptyUpdates(t *testing.T) {
	gdb := openTestDB(t)
	if err := UpdateByCondition[testUser](gdb, Eq("team", "x"), map[string]interface{}{}); err == nil {
		t.Fatal("updates 为空应返回错误")
	}
}

// TestUpdateByConditionEmptyGuard 验证空条件/nil 条件被拒绝，避免全表更新。
func TestUpdateByConditionEmptyGuard(t *testing.T) {
	gdb := openTestDB(t)
	seed(t, gdb,
		&testUser{Name: "a", Age: 1, Team: "x"},
		&testUser{Name: "b", Age: 2, Team: "y"},
	)

	ups := map[string]interface{}{"age": 99}
	if err := UpdateByCondition[testUser](gdb, NewCondition(), ups); err == nil {
		t.Fatal("空 Condition 应返回错误，避免全表更新")
	}
	if err := UpdateByCondition[testUser](gdb, nil, ups); err == nil {
		t.Fatal("nil Condition 应返回错误")
	}

	// 守卫生效后，数据不应被全表改动
	var u testUser
	gdb.Where("name = ?", "a").First(&u)
	if u.Age != 1 {
		t.Fatalf("空条件不应更新任何行，a.Age 实际=%d", u.Age)
	}
}

// TestDeleteByConditionUsesCondition 验证改用 *Condition 后按条件删除。
func TestDeleteByConditionUsesCondition(t *testing.T) {
	gdb := openTestDB(t)
	seed(t, gdb,
		&testUser{Name: "a", Team: "x"},
		&testUser{Name: "b", Team: "x"},
		&testUser{Name: "c", Team: "y"},
	)

	if err := DeleteByCondition[testUser](gdb, Eq("team", "x")); err != nil {
		t.Fatalf("DeleteByCondition 出错: %v", err)
	}
	if got := countBy(t, gdb, "x"); got != 0 {
		t.Fatalf("team=x 应被删除，剩余=%d", got)
	}
	if got := countBy(t, gdb, "y"); got != 1 {
		t.Fatalf("team=y 不应被删除，剩余=%d", got)
	}
}

// TestDeleteByConditionEmptyGuard 验证空条件/nil 条件被拒绝。
func TestDeleteByConditionEmptyGuard(t *testing.T) {
	gdb := openTestDB(t)
	if err := DeleteByCondition[testUser](gdb, NewCondition()); err == nil {
		t.Fatal("空 Condition 应返回错误，避免全表删除")
	}
	if err := DeleteByCondition[testUser](gdb, nil); err == nil {
		t.Fatal("nil Condition 应返回错误")
	}
}

// TestUpdateModelByPrimaryKey 验证不传 whereCols 时仅按主键更新一行（保持原有语义）。
func TestUpdateModelByPrimaryKey(t *testing.T) {
	gdb := openTestDB(t)
	seed(t, gdb,
		&testUser{Name: "a1", Age: 1, Team: "x"},
		&testUser{Name: "a2", Age: 2, Team: "x"},
	)

	var old testUser
	if err := gdb.Where("name = ?", "a1").First(&old).Error; err != nil {
		t.Fatalf("读取 a1 失败: %v", err)
	}

	// 只有 Age 发生变化，fields=[Age]
	if err := UpdateModel(gdb, &old, testUser{Age: 88}); err != nil {
		t.Fatalf("UpdateModel 出错: %v", err)
	}

	var u1, u2 testUser
	gdb.Where("name = ?", "a1").First(&u1)
	gdb.Where("name = ?", "a2").First(&u2)
	if u1.Age != 88 {
		t.Fatalf("a1 应按主键更新为 88，实际=%d", u1.Age)
	}
	if u2.Age != 2 {
		t.Fatalf("a2 不应被改动，实际=%d", u2.Age)
	}
}

// TestUpdateModelByCondition 验证传入 whereCols 时保留主键条件：WHERE = 主键 AND form 中对应字段值，
// 因此只更新 old 那一行，不会批量更新同件的其他行。
func TestUpdateModelByCondition(t *testing.T) {
	gdb := openTestDB(t)
	seed(t, gdb,
		&testUser{Name: "a1", Age: 1, Team: "x"},
		&testUser{Name: "a2", Age: 2, Team: "x"},
		&testUser{Name: "b1", Age: 3, Team: "y"},
	)

	var old testUser
	if err := gdb.Where("name = ?", "a1").First(&old).Error; err != nil {
		t.Fatalf("读取 a1 失败: %v", err)
	}

	// whereCols=["Team"]，条件值 team=x 取自 form；WHERE = 主键(a1) AND team=x，只更新 a1
	if err := UpdateModel(gdb, &old, testUser{Age: 88, Team: "x"}, "Team"); err != nil {
		t.Fatalf("UpdateModel 条件更新出错: %v", err)
	}

	var u1 testUser
	if err := gdb.Where("name = ?", "a1").First(&u1).Error; err != nil {
		t.Fatalf("读取 a1 失败: %v", err)
	}
	if u1.Age != 88 {
		t.Fatalf("主键命中的 a1 应更新为 88，实际=%d", u1.Age)
	}

	// a2 同为 team=x，但主键不同，保留主键条件后不应被更新
	var u2 testUser
	if err := gdb.Where("name = ?", "a2").First(&u2).Error; err != nil {
		t.Fatalf("读取 a2 失败: %v", err)
	}
	if u2.Age != 2 {
		t.Fatalf("a2 主键不匹配不应被更新，实际=%d", u2.Age)
	}

	var y testUser
	if err := gdb.Where("name = ?", "b1").First(&y).Error; err != nil {
		t.Fatalf("读取 b1 失败: %v", err)
	}
	if y.Age != 3 {
		t.Fatalf("team=y 的 b1 不应被改动，实际=%d", y.Age)
	}
}

// TestFindListAndCountEmptyConditionGuard 验证 P2-2：FindList/Count 传入非 nil 的空 Condition 时
// 应短路不拼 WHERE，与传 nil 行为一致（返回全量），且非空条件仍正常生效。
func TestFindListAndCountEmptyConditionGuard(t *testing.T) {
	gdb := openTestDB(t)
	restore := db
	db = gdb
	t.Cleanup(func() { db = restore })

	seed(t, gdb,
		&testUser{Name: "a", Age: 1, Team: "x"},
		&testUser{Name: "b", Age: 2, Team: "x"},
		&testUser{Name: "c", Age: 3, Team: "y"},
	)

	// nil 条件：全部 3 行
	listAll, err := FindList[testUser](nil)
	if err != nil {
		t.Fatalf("nil 条件 FindList 出错: %v", err)
	}
	if len(listAll) != 3 {
		t.Fatalf("nil 条件应返回全部 3 行，实际=%d", len(listAll))
	}

	// 空 Condition（非 nil）：应与 nil 行为一致，不加 WHERE、不报错
	listEmpty, err := FindList[testUser](NewCondition())
	if err != nil {
		t.Fatalf("空条件 FindList 应被守卫短路、不报错，实际: %v", err)
	}
	if len(listEmpty) != 3 {
		t.Fatalf("空 Condition 应返回全部 3 行，实际=%d", len(listEmpty))
	}

	cNil, err := Count[testUser](nil)
	if err != nil {
		t.Fatalf("nil Count 出错: %v", err)
	}
	cEmpty, err := Count[testUser](NewCondition())
	if err != nil {
		t.Fatalf("空条件 Count 应被守卫短路、不报错，实际: %v", err)
	}
	if cNil != 3 || cEmpty != 3 {
		t.Fatalf("Count 期望 nil 与空条件均为 3，实际 nil=%d empty=%d", cNil, cEmpty)
	}

	// 非空条件仍生效
	cX, err := Count[testUser](Eq("team", "x"))
	if err != nil {
		t.Fatalf("非空条件 Count 出错: %v", err)
	}
	if cX != 2 {
		t.Fatalf("team=x 应为 2，实际=%d", cX)
	}
}
