package sharding

import (
	"context"
	"database/sql"
	"testing"

	"gorm.io/gorm"
)

// fakeConnPool 实现 gorm.ConnPool，仅用于 switchConn 测试，不执行真实 SQL。
type fakeConnPool struct {
	name string
}

func (f *fakeConnPool) PrepareContext(_ context.Context, _ string) (*sql.Stmt, error) {
	return nil, nil
}
func (f *fakeConnPool) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, nil
}
func (f *fakeConnPool) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, nil
}
func (f *fakeConnPool) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	return nil
}

// newStmtDB 构造一个仅用于承载 Statement.ConnPool/Vars 的最小 *gorm.DB，无需真实连接。
func newStmtDB(pool gorm.ConnPool) *gorm.DB {
	return &gorm.DB{Statement: &gorm.Statement{ConnPool: pool}}
}

// TestSwitchConnWrapsRawPool 验证底层为原始连接池时会被包装为绑定当前实例的 *ConnPool。
func TestSwitchConnWrapsRawPool(t *testing.T) {
	s := &Sharding{}
	raw := &fakeConnPool{name: "raw"}
	gdb := newStmtDB(raw)

	s.switchConn(gdb)

	wrapped, ok := gdb.Statement.ConnPool.(*ConnPool)
	if !ok {
		t.Fatalf("底层为原始连接池时应包装为 *ConnPool，实际=%T", gdb.Statement.ConnPool)
	}
	if wrapped.sharding != s {
		t.Fatal("包装的 ConnPool 应绑定当前 sharding 实例")
	}
	if wrapped.ConnPool != gorm.ConnPool(raw) {
		t.Fatal("包装应保持原始底层连接池")
	}
}

// TestSwitchConnIdempotentNoRebuild 验证底层连接未变化时复用同一包装、不重复包装。
func TestSwitchConnIdempotentNoRebuild(t *testing.T) {
	s := &Sharding{}
	gdb := newStmtDB(&fakeConnPool{name: "raw"})

	s.switchConn(gdb)
	first := gdb.Statement.ConnPool

	// 底层未变化（已是本插件包装），再次调用不应重建
	s.switchConn(gdb)
	if gdb.Statement.ConnPool != first {
		t.Fatal("底层连接未变化时不应重建 ConnPool（应复用同一实例）")
	}

	// 不应出现 ConnPool 嵌套包装（会导致 resolve 被执行两次）
	if inner, ok := first.(*ConnPool); ok {
		if _, double := inner.ConnPool.(*ConnPool); double {
			t.Fatal("不应出现 ConnPool 嵌套包装")
		}
	}
}

// TestSwitchConnRebuildWhenUnderlyingChanged 验证底层连接变化时基于新底层重建包装。
func TestSwitchConnRebuildWhenUnderlyingChanged(t *testing.T) {
	s := &Sharding{}
	gdb := newStmtDB(&fakeConnPool{name: "before"})
	s.switchConn(gdb)

	// 模拟底层变化：语句重新拿到一个未包装的底层池（如进入事务）
	newRaw := &fakeConnPool{name: "after"}
	gdb.Statement.ConnPool = newRaw
	s.switchConn(gdb)

	wrapped, ok := gdb.Statement.ConnPool.(*ConnPool)
	if !ok {
		t.Fatal("底层变化后应重新包装")
	}
	if wrapped.ConnPool != gorm.ConnPool(newRaw) {
		t.Fatal("重建后的 ConnPool 应基于新的底层连接池")
	}
}

// TestSwitchConnHonorsIgnoreKey 验证设置了忽略分片标记时保持原始连接池不包装。
func TestSwitchConnHonorsIgnoreKey(t *testing.T) {
	s := &Sharding{}
	raw := &fakeConnPool{name: "raw"}
	gdb := newStmtDB(raw)
	gdb.Set(ShardingIgnoreStoreKey, true)

	s.switchConn(gdb)

	if _, ok := gdb.Statement.ConnPool.(*ConnPool); ok {
		t.Fatal("设置 ShardingIgnoreStoreKey 时不应包装连接池")
	}
	if gdb.Statement.ConnPool != gorm.ConnPool(raw) {
		t.Fatal("忽略分片时应保持原始连接池不变")
	}
}

// TestSwitchConnNilPool 验证底层为 nil 时不 panic 且不赋值。
func TestSwitchConnNilPool(t *testing.T) {
	s := &Sharding{}
	gdb := newStmtDB(nil)

	s.switchConn(gdb)

	if gdb.Statement.ConnPool != nil {
		t.Fatal("底层为 nil 时不应赋值包装")
	}
}
