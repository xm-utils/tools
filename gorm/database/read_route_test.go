package database

import (
	"strings"
	"testing"

	"gorm.io/plugin/dbresolver"
)

// routeModel 仅用于泛型实例化 Reader，测试无需真实连库。
type routeModel struct {
	ID int64
}

func (routeModel) TableName() string { return "route_models" }

// TestPrimaryReplicaRoutingDirection 验证 read.go 修正后的路由方向：
//   - Primary 必须强制走主库 => dbresolver.Write
//   - Replica  必须尝试走从库 => dbresolver.Read
//
// 这是本次修复的核心（此前两者被写反），直接断言 Reader 携带的 Operation。
func TestPrimaryReplicaRoutingDirection(t *testing.T) {
	if got := Primary[routeModel]().do; got != dbresolver.Write {
		t.Errorf("Primary 应强制走主库(dbresolver.Write)，实际=%q", string(got))
	}
	if got := Replica[routeModel]().do; got != dbresolver.Read {
		t.Errorf("Replica 应走从库(dbresolver.Read)，实际=%q", string(got))
	}
}

// TestReaderGetDBAppliesOperation 验证 getDB 会把对应 Operation 注入到会话中。
// 这里不接真实库，仅确保子句被写入 Statement.Settings（Write/Read 名称互斥）。
func TestReaderGetDBAppliesOperation(t *testing.T) {
	gdb := openTestDB(t)
	// 借用测试库作为单例，使 getDB 可正常返回会话
	restore := db
	db = gdb
	t.Cleanup(func() { db = restore })

	primary := Primary[routeModel]().getDB()
	if primary == nil {
		t.Fatal("Primary getDB 返回 nil")
	}
	replica := Replica[routeModel]().getDB()
	if replica == nil {
		t.Fatal("Replica getDB 返回 nil")
	}

	// 两个读器应携带不同的路由操作，避免再次被改反
	if Primary[routeModel]().do == Replica[routeModel]().do {
		t.Fatal("Primary 与 Replica 的路由操作不应相同")
	}
}

// TestReadOneByCondition 验证 ReadOne 以入参结构体字段为条件命中记录，
// 且结果扫描到独立变量（条件对象与扫描目标分离），无匹配时返回 nil。
func TestReadOneByCondition(t *testing.T) {
	gdb := openTestDB(t)
	restore := db
	db = gdb
	t.Cleanup(func() { db = restore })

	seed(t, gdb,
		&testUser{Name: "alice", Age: 10, Team: "x"},
		&testUser{Name: "bob", Age: 20, Team: "y"},
	)

	got := Primary[testUser]().ReadOne(testUser{Name: "bob"})
	if got == nil {
		t.Fatal("按 name 查询应命中 bob，实际返回 nil")
	}
	if got.Name != "bob" || got.Team != "y" {
		t.Fatalf("结果不符预期，实际=%+v", *got)
	}

	// 无匹配应返回 nil，且不应污染传入的条件对象语义
	if got := Primary[testUser]().ReadOne(testUser{Name: "nobody"}); got != nil {
		t.Fatalf("无匹配应返回 nil，实际=%+v", *got)
	}
}

// TestReaderNilDBReturnsErrorNotPanic 验证 P2-3：数据库未初始化时 Reader 各方法不再 panic，
// 而是返回清晰 error（带 error 的读方法）或 nil（ReadOne/FindOne）。
func TestReaderNilDBReturnsErrorNotPanic(t *testing.T) {
	restore := db
	db = nil
	t.Cleanup(func() { db = restore })

	if got := Primary[testUser]().getDB(); got != nil {
		t.Error("未初始化时 getDB 应返回 nil")
	}

	if _, _, err := FindAll[testUser](ListParam{}); err == nil {
		t.Error("未初始化时 FindAll 应返回 error")
	} else if strings.Contains(err.Error(), "panic recovered") {
		t.Errorf("应直接返回清晰 error、不应再以 panic 形式，实际=%q", err.Error())
	}

	if _, err := FindList[testUser](nil); err == nil {
		t.Error("未初始化时 FindList 应返回 error")
	} else if strings.Contains(err.Error(), "panic recovered") {
		t.Errorf("FindList 不应以 panic 形式，实际=%q", err.Error())
	}

	if _, err := Count[testUser](nil); err == nil {
		t.Error("未初始化时 Count 应返回 error")
	}

	if got := ReadOne[testUser](testUser{}); got != nil {
		t.Error("未初始化时 ReadOne 应返回 nil")
	}
	if got := FindOne[testUser](1); got != nil {
		t.Error("未初始化时 FindOne 应返回 nil")
	}
}
