package database

import (
	"fmt"
	"reflect"
	"strings"
)

type condValue struct {
	sql  string
	args []any // 统一使用切片存储参数，支持单个或多个占位符
}

// Condition struct.
// work for WHERE conditions.
type Condition struct {
	params []condValue
}

// NewCondition return new condition struct
func NewCondition() *Condition {
	return &Condition{}
}
func Eq(col string, arg any) *Condition {
	return NewCondition().Eq(true, col, arg)
}
func In(col string, arg any) *Condition {
	return NewCondition().In(true, col, arg)
}
func Like(col string, arg any) *Condition {
	return NewCondition().Like(true, col, arg)
}
func NotLike(col string, arg any) *Condition {
	return NewCondition().NotLike(true, col, arg)
}
func LLike(col string, arg any) *Condition {
	return NewCondition().LLike(true, col, arg)
}
func RLike(col string, arg any) *Condition {
	return NewCondition().RLike(true, col, arg)
}
func Between(col string, start, end any) *Condition {
	return NewCondition().Between(true, col, start, end)
}

// NotEq col != ?
func NotEq(col string, arg any) *Condition { return NewCondition().NotEq(true, col, arg) }

// Gt col > ?
func Gt(col string, arg any) *Condition { return NewCondition().Gt(true, col, arg) }

// Gte col >= ?
func Gte(col string, arg any) *Condition { return NewCondition().Gte(true, col, arg) }

// Lt col < ?
func Lt(col string, arg any) *Condition { return NewCondition().Lt(true, col, arg) }

// Lte col <= ?
func Lte(col string, arg any) *Condition { return NewCondition().Lte(true, col, arg) }

// NotIn col NOT IN (?)
func NotIn(col string, arg any) *Condition { return NewCondition().NotIn(true, col, arg) }

// IsNull col IS NULL
func IsNull(col string) *Condition { return NewCondition().IsNull(true, col) }

// IsNotNull col IS NOT NULL
func IsNotNull(col string) *Condition { return NewCondition().IsNotNull(true, col) }

func (c *Condition) Eq(b bool, col string, arg any) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " = ?",
		args: []any{arg},
	})
	return c
}
func (c *Condition) In(b bool, col string, args any) *Condition {
	return c.inList(b, "in", col, args)
}

// Like 全模糊匹配：col LIKE %value%
func (c *Condition) Like(b bool, col string, arg any) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " like ?",
		args: []any{likeValue(arg, true, true)},
	})
	return c
}

// NotLike 全模糊不匹配：col NOT LIKE %value%
func (c *Condition) NotLike(b bool, col string, arg any) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " not like ?",
		args: []any{likeValue(arg, true, true)},
	})
	return c
}

// LLike 左模糊匹配：col LIKE %value （以 value 结尾）
func (c *Condition) LLike(b bool, col string, arg any) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " like ?",
		args: []any{likeValue(arg, true, false)},
	})
	return c
}

// RLike 右模糊匹配：col LIKE value% （以 value 开头）
func (c *Condition) RLike(b bool, col string, arg any) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " like ?",
		args: []any{likeValue(arg, false, true)},
	})
	return c
}
func (c *Condition) Between(b bool, col string, start, end any) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  fmt.Sprintf("%s BETWEEN ? AND ?", col),
		args: []any{start, end}, // 两个参数对应两个占位符
	})
	return c
}

// NotIn col NOT IN (?)：与 In 一致，整个切片作为一个参数交给 GORM 展开。
func (c *Condition) NotIn(b bool, col string, args any) *Condition {
	return c.inList(b, "not in", col, args)
}

// NotEq col != ?
func (c *Condition) NotEq(b bool, col string, arg any) *Condition {
	return c.compare(b, "!=", col, arg)
}

// Gt col > ?
func (c *Condition) Gt(b bool, col string, arg any) *Condition {
	return c.compare(b, ">", col, arg)
}

// Gte col >= ?
func (c *Condition) Gte(b bool, col string, arg any) *Condition {
	return c.compare(b, ">=", col, arg)
}

// Lt col < ?
func (c *Condition) Lt(b bool, col string, arg any) *Condition {
	return c.compare(b, "<", col, arg)
}

// Lte col <= ?
func (c *Condition) Lte(b bool, col string, arg any) *Condition {
	return c.compare(b, "<=", col, arg)
}

// IsNull col IS NULL（无参数）
func (c *Condition) IsNull(b bool, col string) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{sql: col + " is null"})
	return c
}

// IsNotNull col IS NOT NULL（无参数）
func (c *Condition) IsNotNull(b bool, col string) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{sql: col + " is not null"})
	return c
}

// compare 追加 `col <op> ?` 单值比较条件；b 为 false 时跳过。
func (c *Condition) compare(b bool, op, col string, arg any) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " " + op + " ?",
		args: []any{arg},
	})
	return c
}

// inList 追加 `col <op> ?` 列表条件，把整个切片/数组作为单个参数交给 GORM 展开；
// b 为 false 或 args 非切片/数组时跳过。
func (c *Condition) inList(b bool, op, col string, args any) *Condition {
	if !b {
		return c
	}
	v := reflect.ValueOf(args)
	if kind := v.Kind(); kind != reflect.Slice && kind != reflect.Array {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " " + op + " ?",
		args: []any{args},
	})
	return c
}

// likeValue 构造 LIKE 的匹配值：将 arg 转为字符串并按需在前/后添加 % 通配符。
// 之前的实现把 " %%"/" %%%" 作为 Sprintf 的参数传入，%% 不会被转义，导致生成
// 带前导空格与双/三百分号的错误值；这里统一用字符串拼接修正。
// 同时对值中的 LIKE 特殊字符（\ % _）做转义，保证用户输入按字面量匹配（避免把
// 用户输入的 % 或 _ 误当通配符），MySQL 默认转义符为反斜杠。
func likeValue(arg any, prefix, suffix bool) string {
	var b strings.Builder
	if prefix {
		b.WriteByte('%')
	}
	b.WriteString(escapeLike(fmt.Sprintf("%v", arg)))
	if suffix {
		b.WriteByte('%')
	}
	return b.String()
}

// escapeLike 转义 LIKE 模式中的特殊字符：先转义反斜杠本身，再转义 % 与 _ 通配符，
// 使其按字面量匹配。逐字节处理，多字节 UTF-8 字节均 >=0x80 不会与 \ % _ 冲突，故安全。
func escapeLike(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			b.WriteString(`\\`)
		case '%':
			b.WriteString(`\%`)
		case '_':
			b.WriteString(`\_`)
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// IsEmpty check the condition arguments are empty or not.
func (c *Condition) IsEmpty() bool {
	return c == nil || len(c.params) == 0
}

func (c *Condition) GormWhere() (string, []any) {
	if c == nil {
		return "", nil
	}

	sqls := make([]string, 0, len(c.params))
	vals := make([]any, 0, len(c.params))

	for _, param := range c.params {
		sqls = append(sqls, param.sql)
		// 将每个条件的参数展开添加到总参数列表
		vals = append(vals, param.args...)
	}
	return strings.Join(sqls, " AND "), vals
}
