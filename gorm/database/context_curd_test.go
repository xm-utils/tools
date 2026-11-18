package database

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

// ctxTestPage 是 PageParam 的测试实现。
type ctxTestPage struct {
	pageNum  int32
	pageSize int32
}

func (p ctxTestPage) IsValid() bool { return p.pageSize > 0 }
func (p ctxTestPage) GetLimit() (limit, offset int32) {
	return p.pageSize, (p.pageNum - 1) * p.pageSize
}

func seedCtxUsers(t *testing.T, gdb *gorm.DB) {
	t.Helper()
	seed(t, gdb,
		&testUser{Name: "u1", Age: 1, Team: "a"},
		&testUser{Name: "u2", Age: 2, Team: "a"},
		&testUser{Name: "u3", Age: 3, Team: "b"},
	)
}

// TestCtxBuilderHappyPath 覆盖链式构建器的主要读写路径：Insert/FindOne/ReadOne/FindList/Count/FindAll/Update/Delete。
func TestCtxBuilderHappyPath(t *testing.T) {
	gdb := openTestDB(t)
	seedCtxUsers(t, gdb)
	ctx := context.Background()

	// Insert
	if err := Ctx[testUser](ctx).Tx(gdb).Insert(&testUser{Name: "i1", Age: 10, Team: "z"}); err != nil {
		t.Fatalf("Insert 出错: %v", err)
	}

	// FindOne（Primary 路由提示在无 DBResolver 时应为无操作且不报错）
	var saved testUser
	if err := gdb.Where("name = ?", "i1").First(&saved).Error; err != nil {
		t.Fatalf("读取 i1 失败: %v", err)
	}
	got, err := Ctx[testUser](ctx).Tx(gdb).Primary().FindOne(saved.ID)
	if err != nil {
		t.Fatalf("FindOne 出错: %v", err)
	}
	if got == nil || got.Name != "i1" {
		t.Fatalf("FindOne 结果不符: %+v", got)
	}

	// ReadOne：按结构体非零字段查询；未命中返回 (nil, nil)
	one, err := Ctx[testUser](ctx).Tx(gdb).ReadOne(testUser{Name: "u1"})
	if err != nil {
		t.Fatalf("ReadOne 出错: %v", err)
	}
	if one == nil || one.Age != 1 {
		t.Fatalf("ReadOne 结果不符: %+v", one)
	}
	miss, err := Ctx[testUser](ctx).Tx(gdb).ReadOne(testUser{Name: "not-exist"})
	if err != nil || miss != nil {
		t.Fatalf("ReadOne 未命中应返回 (nil, nil)，实际: %+v, %v", miss, err)
	}

	// FindList + Where
	list, err := Ctx[testUser](ctx).Tx(gdb).Where(Eq("team", "a")).FindList()
	if err != nil {
		t.Fatalf("FindList 出错: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("team=a 应有 2 条，实际=%d", len(list))
	}

	// Count
	cnt, err := Ctx[testUser](ctx).Tx(gdb).Where(Eq("team", "a")).Count()
	if err != nil {
		t.Fatalf("Count 出错: %v", err)
	}
	if cnt != 2 {
		t.Fatalf("team=a 计数应为 2，实际=%d", cnt)
	}

	// FindAll：Order("-age") + Page 第 1 页取 2 条
	page1, total, err := Ctx[testUser](ctx).Tx(gdb).
		Where(Eq("team", "a")).Order("-age").Page(ctxTestPage{pageNum: 1, pageSize: 2}).FindAll()
	if err != nil {
		t.Fatalf("FindAll 出错: %v", err)
	}
	if total != 2 {
		t.Fatalf("FindAll total 应为 2，实际=%d", total)
	}
	if len(page1) != 2 || page1[0].Name != "u2" || page1[1].Name != "u1" {
		t.Fatalf("FindAll 排序/分页结果不符: %+v", page1)
	}

	// Update：按主键只更新非零字段
	target := &testUser{ID: saved.ID, Age: 20}
	if err := Ctx[testUser](ctx).Tx(gdb).Update(target, "age"); err != nil {
		t.Fatalf("Update 出错: %v", err)
	}
	gdb.First(&saved, saved.ID)
	if saved.Age != 20 || saved.Name != "i1" {
		t.Fatalf("Update 后字段不符: %+v", saved)
	}

	// Delete：按主键
	if err := Ctx[testUser](ctx).Tx(gdb).Delete(&testUser{ID: saved.ID}); err != nil {
		t.Fatalf("Delete 出错: %v", err)
	}
	if got := countBy(t, gdb, "z"); got != 0 {
		t.Fatalf("i1 应被删除，剩余=%d", got)
	}
}

// TestCtxBuilderCanceled 验证 ctx 已取消时读操作直接返回 context.Canceled。
func TestCtxBuilderCanceled(t *testing.T) {
	gdb := openTestDB(t)
	seedCtxUsers(t, gdb)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := Ctx[testUser](ctx).Tx(gdb).FindList(); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消后 FindList 应返回 context.Canceled，实际=%v", err)
	}
	if _, err := Ctx[testUser](ctx).Tx(gdb).FindOne(1); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消后 FindOne 应返回 context.Canceled，实际=%v", err)
	}
	if err := Ctx[testUser](ctx).Tx(gdb).Insert(&testUser{Name: "x"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消后 Insert 应返回 context.Canceled，实际=%v", err)
	}
}

// TestCtxBuilderDeadlineExceeded 验证超时 ctx 到期后操作返回 DeadlineExceeded。
func TestCtxBuilderDeadlineExceeded(t *testing.T) {
	gdb := openTestDB(t)
	seedCtxUsers(t, gdb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	time.Sleep(30 * time.Millisecond)

	if _, err := Ctx[testUser](ctx).Tx(gdb).Count(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("超时后 Count 应返回 context.DeadlineExceeded，实际=%v", err)
	}
}

// TestCtxBuilderInsertBatchCancel 验证批量插入在 ctx 取消后中断且不写入数据。
func TestCtxBuilderInsertBatchCancel(t *testing.T) {
	gdb := openTestDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	rows := []*testUser{{Name: "b1"}, {Name: "b2"}}
	affected, err := Ctx[testUser](ctx).Tx(gdb).InsertBatch(1, rows)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("取消后 InsertBatch 应返回 context.Canceled，实际=%v", err)
	}
	if affected != 0 || countBy(t, gdb, "") != 0 {
		t.Fatalf("取消后不应写入任何行，affected=%d", affected)
	}
}

// TestCtxBuilderUpdateByCondition 验证条件批量更新的链式用法与空条件守卫。
func TestCtxBuilderUpdateByCondition(t *testing.T) {
	gdb := openTestDB(t)
	seedCtxUsers(t, gdb)
	ctx := context.Background()

	// 空条件应被拒绝，避免全表更新
	if err := Ctx[testUser](ctx).Tx(gdb).UpdateByCondition(map[string]interface{}{"age": 99}); err == nil {
		t.Fatal("无 Where 条件时 UpdateByCondition 应返回错误")
	}

	if err := Ctx[testUser](ctx).Tx(gdb).Where(Eq("team", "a")).
		UpdateByCondition(map[string]interface{}{"age": 99}); err != nil {
		t.Fatalf("链式 UpdateByCondition 出错: %v", err)
	}

	var u3 testUser
	if err := gdb.Where("name = ?", "u3").First(&u3).Error; err != nil {
		t.Fatalf("读取 u3 失败: %v", err)
	}
	if u3.Age != 3 {
		t.Fatalf("team=b 的 u3 不应被改动，实际=%d", u3.Age)
	}
	cnt, err := Ctx[testUser](ctx).Tx(gdb).Where(Eq("age", 99)).Count()
	if err != nil || cnt != 2 {
		t.Fatalf("team=a 的 2 条应被更新为 99，cnt=%d err=%v", cnt, err)
	}
}

// TestCtxBuilderDeleteByCondition 验证条件删除的链式用法与空条件守卫。
func TestCtxBuilderDeleteByCondition(t *testing.T) {
	gdb := openTestDB(t)
	seedCtxUsers(t, gdb)
	ctx := context.Background()

	if err := Ctx[testUser](ctx).Tx(gdb).DeleteByCondition(); err == nil {
		t.Fatal("无 Where 条件时 DeleteByCondition 应返回错误")
	}

	if err := Ctx[testUser](ctx).Tx(gdb).Where(Eq("team", "a")).DeleteByCondition(); err != nil {
		t.Fatalf("链式 DeleteByCondition 出错: %v", err)
	}
	if got := countBy(t, gdb, "a"); got != 0 {
		t.Fatalf("team=a 应被删除，剩余=%d", got)
	}
	if got := countBy(t, gdb, "b"); got != 1 {
		t.Fatalf("team=b 不应被删除，剩余=%d", got)
	}
}

// TestCtxBuilderUpdateModel 验证链式 UpdateModel 与包级函数语义一致：
// 无差异返回 nil、nil old 返回 ErrRecordNotFound、whereCols 保留主键条件。
func TestCtxBuilderUpdateModel(t *testing.T) {
	gdb := openTestDB(t)
	seedCtxUsers(t, gdb)
	ctx := context.Background()

	// nil old
	if err := Ctx[testUser](ctx).Tx(gdb).UpdateModel(nil, testUser{Name: "x"}); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("nil old 应返回 ErrRecordNotFound，实际=%v", err)
	}

	var old testUser
	if err := gdb.Where("name = ?", "u1").First(&old).Error; err != nil {
		t.Fatalf("读取 u1 失败: %v", err)
	}

	// 无差异：直接返回 nil
	if err := Ctx[testUser](ctx).Tx(gdb).UpdateModel(&old, old); err != nil {
		t.Fatalf("无差异应返回 nil，实际=%v", err)
	}

	// 按主键更新差异字段
	form := testUser{ID: old.ID, Name: "u1-new", Age: old.Age, Team: old.Team}
	if err := Ctx[testUser](ctx).Tx(gdb).UpdateModel(&old, form); err != nil {
		t.Fatalf("UpdateModel 出错: %v", err)
	}
	var after testUser
	if err := gdb.First(&after, old.ID).Error; err != nil {
		t.Fatalf("读取更新结果失败: %v", err)
	}
	if after.Name != "u1-new" {
		t.Fatalf("UpdateModel 未生效: %+v", after)
	}

	// whereCols：主键条件 AND team 字段，条件不匹配时不应误伤其他行
	var u2 testUser
	if err := gdb.Where("name = ?", "u2").First(&u2).Error; err != nil {
		t.Fatalf("读取 u2 失败: %v", err)
	}
	form2 := testUser{ID: u2.ID, Name: "u2-new", Age: u2.Age, Team: "not-match"}
	if err := Ctx[testUser](ctx).Tx(gdb).UpdateModel(&u2, form2, "Team"); err != nil {
		t.Fatalf("带 whereCols 的 UpdateModel 出错: %v", err)
	}
	var u2after testUser
	gdb.First(&u2after, u2.ID)
	if u2after.Name == "u2-new" {
		t.Fatal("whereCols 值与库内不一致时不应更新成功")
	}
}

// TestCtxBuilderRawAndExec 验证原生 SQL 的 ctx 包裹执行。
func TestCtxBuilderRawAndExec(t *testing.T) {
	gdb := openTestDB(t)
	seedCtxUsers(t, gdb)
	ctx := context.Background()

	affected, err := Ctx[testUser](ctx).Tx(gdb).Exec("UPDATE test_users SET age = age + 1 WHERE team = ?", "b")
	if err != nil {
		t.Fatalf("Exec 出错: %v", err)
	}
	if affected != 1 {
		t.Fatalf("Exec 影响行数应为 1，实际=%d", affected)
	}

	list, err := Ctx[testUser](ctx).Tx(gdb).Raw("SELECT * FROM test_users WHERE team = ?", "b")
	if err != nil {
		t.Fatalf("Raw 出错: %v", err)
	}
	if len(list) != 1 || list[0].Age != 4 {
		t.Fatalf("Raw 结果不符: %+v", list)
	}

	// 取消后原生 SQL 同样受 ctx 控制
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Ctx[testUser](canceled).Tx(gdb).Raw("SELECT 1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消后 Raw 应返回 context.Canceled，实际=%v", err)
	}
}

// TestCtxBuilderNoDB 验证未初始化且未指定 Tx 时返回 error 而非 panic。
func TestCtxBuilderNoDB(t *testing.T) {
	if GetDB() != nil {
		t.Skip("单例已被其他测试初始化，跳过")
	}
	_, err := Ctx[testUser](context.Background()).FindList()
	if err == nil || err.Error() != "database connection not found" {
		t.Fatalf("未初始化应返回 connection not found，实际=%v", err)
	}
}

// ctxTestEvent 是时间区间查询测试用的模型（testUser 无时间列）。
type ctxTestEvent struct {
	ID   int64     `gorm:"primaryKey"`
	Name string    `gorm:"column:name"`
	At   time.Time `gorm:"column:at"`
}

func (ctxTestEvent) TableName() string { return "ctx_test_events" }

// ctxTestTime 是 TimeParam 的测试实现。
type ctxTestTime struct {
	column     string
	start, end time.Time
}

func (p ctxTestTime) IsValid() bool                   { return !p.start.IsZero() && !p.end.IsZero() }
func (p ctxTestTime) GetColumn() string               { return p.column }
func (p ctxTestTime) GetTime() (time.Time, time.Time) { return p.start, p.end }

// TestCtxBuilderQueryAndTime 验证 Query/Time 链式条件与 ApplyListParam 透传。
func TestCtxBuilderQueryAndTime(t *testing.T) {
	gdb := openTestDB(t)
	if err := gdb.AutoMigrate(&ctxTestEvent{}); err != nil {
		t.Fatalf("AutoMigrate 失败: %v", err)
	}
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	events := []*ctxTestEvent{
		{Name: "e1", At: base},
		{Name: "e2", At: base.Add(24 * time.Hour)},
		{Name: "e3", At: base.Add(48 * time.Hour)},
	}
	if err := gdb.Create(&events).Error; err != nil {
		t.Fatalf("预置数据失败: %v", err)
	}
	ctx := context.Background()

	// Query：map 条件与链式 Where 叠加
	list, err := Ctx[ctxTestEvent](ctx).Tx(gdb).
		Query(map[string]interface{}{"name LIKE ?": "e%"}).
		Where(Gte("at", base.Add(24*time.Hour))).
		Order("at").FindList()
	if err != nil {
		t.Fatalf("Query 条件查询出错: %v", err)
	}
	if len(list) != 2 || list[0].Name != "e2" || list[1].Name != "e3" {
		t.Fatalf("Query+Where 结果不符: %+v", list)
	}

	// Time：半开区间 [start, end)，end 边界不含
	e12, err := Ctx[ctxTestEvent](ctx).Tx(gdb).
		Time(ctxTestTime{column: "at", start: base, end: base.Add(48 * time.Hour)}).
		Order("at").FindList()
	if err != nil {
		t.Fatalf("Time 条件查询出错: %v", err)
	}
	if len(e12) != 2 || e12[0].Name != "e1" || e12[1].Name != "e2" {
		t.Fatalf("Time 半开区间应命中 e1/e2，实际=%+v", e12)
	}

	// ApplyListParam：Cond + Query + Time + Order 一次性透传，total 只统计命中行
	param := ListParam{
		Cond:  Eq("name", "e3"),
		Query: map[string]interface{}{"name != ?": "e1"},
		Time:  ctxTestTime{column: "at", start: base.Add(47 * time.Hour), end: base.Add(72 * time.Hour)},
		Order: []string{"-at"},
	}
	users, total, err := Ctx[ctxTestEvent](ctx).Tx(gdb).ApplyListParam(param).FindAll()
	if err != nil {
		t.Fatalf("ApplyListParam FindAll 出错: %v", err)
	}
	if total != 1 || len(users) != 1 || users[0].Name != "e3" {
		t.Fatalf("ApplyListParam 应只命中 e3，total=%d list=%+v", total, users)
	}
}
