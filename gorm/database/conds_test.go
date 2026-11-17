package database

import (
	"reflect"
	"testing"
)

// TestConditionLikeValues 验证修正后的 LIKE 匹配值：不再包含前导空格与双/三百分号。
func TestConditionLikeValues(t *testing.T) {
	cases := []struct {
		name string
		cond *Condition
		sql  string
		val  string
	}{
		{"Like", Like("name", "abc"), "name like ?", "%abc%"},
		{"NotLike", NotLike("name", "abc"), "name not like ?", "%abc%"},
		{"LLike", LLike("name", "abc"), "name like ?", "%abc"},
		{"RLike", RLike("name", "abc"), "name like ?", "abc%"},
	}
	for _, tc := range cases {
		q, v := tc.cond.GormWhere()
		if q != tc.sql {
			t.Errorf("%s: sql 期望 %q，实际 %q", tc.name, tc.sql, q)
		}
		if len(v) != 1 || v[0] != tc.val {
			t.Errorf("%s: 参数期望 [%q]，实际 %v", tc.name, tc.val, v)
		}
	}
}

// TestConditionLikeNonStringArg 验证非字符串 arg 也能正确转成模式串。
func TestConditionLikeNonStringArg(t *testing.T) {
	q, v := Like("code", 123).GormWhere()
	if q != "code like ?" || v[0] != "%123%" {
		t.Errorf("非字符串 arg：期望 [%%123%%]，实际 sql=%q args=%v", q, v)
	}
}

// TestConditionCombineAndArgs 验证多条件以 AND 拼接、参数按序展开。
func TestConditionCombineAndArgs(t *testing.T) {
	c := NewCondition().
		Eq(true, "team", "x").
		Between(true, "age", 10, 20).
		Eq(false, "ignored", 1) // b=false 应被跳过

	q, v := c.GormWhere()
	wantSQL := "team = ? AND age BETWEEN ? AND ?"
	if q != wantSQL {
		t.Fatalf("sql 期望 %q，实际 %q", wantSQL, q)
	}
	wantArgs := []any{"x", 10, 20}
	if !reflect.DeepEqual(v, wantArgs) {
		t.Fatalf("args 期望 %v，实际 %v", wantArgs, v)
	}
}

// TestConditionIn 验证 In 将整个切片作为单个参数交给 GORM 展开。
func TestConditionIn(t *testing.T) {
	ids := []int{1, 2, 3}
	q, v := In("id", ids).GormWhere()
	if q != "id in ?" {
		t.Fatalf("sql 期望 %q，实际 %q", "id in ?", q)
	}
	if len(v) != 1 || !reflect.DeepEqual(v[0], ids) {
		t.Fatalf("args 期望 [[1 2 3]]，实际 %v", v)
	}
}

// TestConditionEmpty 验证空条件与 nil 接收者的安全行为。
func TestConditionEmpty(t *testing.T) {
	if !NewCondition().IsEmpty() {
		t.Error("新建条件应为空")
	}
	var nilCond *Condition
	if !nilCond.IsEmpty() {
		t.Error("nil 条件的 IsEmpty 应返回 true")
	}
	q, v := nilCond.GormWhere()
	if q != "" || v != nil {
		t.Errorf("nil 条件 GormWhere 期望 (\"\", nil)，实际 (%q, %v)", q, v)
	}
}

// TestConditionCompareOps 验证比较类运算符生成的 sql 与参数。
func TestConditionCompareOps(t *testing.T) {
	cases := []struct {
		cond *Condition
		sql  string
		val  any
	}{
		{NotEq("age", 18), "age != ?", 18},
		{Gt("age", 18), "age > ?", 18},
		{Gte("age", 18), "age >= ?", 18},
		{Lt("age", 18), "age < ?", 18},
		{Lte("age", 18), "age <= ?", 18},
	}
	for _, tc := range cases {
		q, v := tc.cond.GormWhere()
		if q != tc.sql {
			t.Errorf("sql 期望 %q，实际 %q", tc.sql, q)
		}
		if len(v) != 1 || v[0] != tc.val {
			t.Errorf("args 期望 [%v]，实际 %v", tc.val, v)
		}
	}
}

// TestConditionNotIn 验证 NotIn 与 In 对称：整个切片作为单参数，op 为 not in；非切片被跳过。
func TestConditionNotIn(t *testing.T) {
	ids := []int{1, 2, 3}
	q, v := NotIn("id", ids).GormWhere()
	if q != "id not in ?" {
		t.Fatalf("sql 期望 %q，实际 %q", "id not in ?", q)
	}
	if len(v) != 1 || !reflect.DeepEqual(v[0], ids) {
		t.Fatalf("args 期望 [[1 2 3]]，实际 %v", v)
	}
	if !NotIn("id", 1).IsEmpty() {
		t.Error("NotIn 传入非切片应被跳过，保持空条件")
	}
}

// TestConditionNull 验证 IsNull/IsNotNull 无参数，且 b=false 时跳过。
func TestConditionNull(t *testing.T) {
	q, v := IsNull("deleted_at").GormWhere()
	if q != "deleted_at is null" || len(v) != 0 {
		t.Errorf("IsNull：期望 (\"deleted_at is null\", [])，实际 (%q, %v)", q, v)
	}
	q, v = IsNotNull("name").GormWhere()
	if q != "name is not null" || len(v) != 0 {
		t.Errorf("IsNotNull：期望 (\"name is not null\", [])，实际 (%q, %v)", q, v)
	}
	if c := NewCondition(); !c.IsNull(false, "x").IsNotNull(false, "y").IsEmpty() {
		t.Error("b=false 的 IsNull/IsNotNull 应被跳过")
	}
}

// TestConditionFluentCombine 验证链式组合多个新条件按加入顺序 AND 拼接。
func TestConditionFluentCombine(t *testing.T) {
	q, v := NewCondition().
		Gte(true, "age", 18).
		Lt(true, "age", 60).
		NotIn(true, "status", []string{"a", "b"}).
		IsNotNull(true, "name").
		GormWhere()
	wantSQL := "age >= ? AND age < ? AND status not in ? AND name is not null"
	if q != wantSQL {
		t.Fatalf("sql 期望 %q，实际 %q", wantSQL, q)
	}
	if len(v) != 3 || v[0] != 18 || v[1] != 60 || !reflect.DeepEqual(v[2], []string{"a", "b"}) {
		t.Fatalf("args 不符预期：%v", v)
	}
}

// TestConditionLikeEscapeSpecialChars 验证 P1-3：LIKE 值中的 % _ \ 被转义为字面量，
// 两侧通配符不转义，避免用户输入中的通配符误参与模式匹配。
func TestConditionLikeEscapeSpecialChars(t *testing.T) {
	cases := []struct {
		name string
		cond *Condition
		val  string
	}{
		// 全模糊：%abc% 值中 % 被转义，两侧自动通配符保留
		{"Like-percent", Like("name", "a%b"), "%a\\%b%"},
		// 下划线：_ 被转义
		{"Like-underscore", Like("code", "A_B"), "%A\\_B%"},
		// 反斜杠：\ 先被转义为 \\
		{"Like-backslash", Like("path", `a\b`), "%a\\\\b%"},
		// 左模糊：仅前侧加 %，值内 % 仍转义
		{"LLike", LLike("name", "100%"), "%100\\%"},
		// 右模糊：仅后侧加 %
		{"RLike", RLike("name", "a_b"), "a\\_b%"},
		// NotLike 同样转义
		{"NotLike", NotLike("name", "x%y_z"), "%x\\%y\\_z%"},
		// 普通字符串不受影响
		{"Like-plain", Like("name", "abc"), "%abc%"},
	}
	for _, tc := range cases {
		_, v := tc.cond.GormWhere()
		if len(v) != 1 || v[0] != tc.val {
			t.Errorf("%s: 参数期望 [%q]，实际 %v", tc.name, tc.val, v)
		}
	}
}

// TestEscapeLike 直接验证 escapeLike 辅助函数的逐字符转义行为。
func TestEscapeLike(t *testing.T) {
	cases := []struct{ in, want string }{
		{"abc", "abc"},
		{"a%b", `a\%b`},
		{"a_b", `a\_b`},
		{`a\b`, `a\\b`},
		{`100%`, `100\%`},
		{`%_%`, `\%\_\%`},
		{"中文_测试%", `中文\_测试\%`}, // 多字节 UTF-8 不受逐字节转义影响
		{"", ""},
	}
	for _, tc := range cases {
		if got := escapeLike(tc.in); got != tc.want {
			t.Errorf("escapeLike(%q) 期望 %q，实际 %q", tc.in, tc.want, got)
		}
	}
}
