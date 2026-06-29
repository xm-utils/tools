package database

import (
	"fmt"
	"reflect"
	"strings"
)

type condValue struct {
	cond *Condition
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
	c := &Condition{}
	return c
}

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
	if !b {
		return c
	}

	v := reflect.ValueOf(args)
	kind := v.Kind()
	if kind != reflect.Slice && kind != reflect.Array {
		return c
	}

	c.params = append(c.params, condValue{
		sql:  col + " in ?",
		args: []any{args}, // 整个切片作为一个参数
	})
	return c
}

func (c *Condition) Like(b bool, col string, arg any) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " like ?",
		args: []any{fmt.Sprintf("%s%s%s", " %%", arg, " %%%")},
	})
	return c
}

func (c *Condition) NotLike(b bool, col string, arg any) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " not like ?",
		args: []any{fmt.Sprintf("%s%s%s", " %%", arg, " %%%")},
	})
	return c
}

func (c *Condition) LLike(b bool, col string, arg any) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " like ?",
		args: []any{fmt.Sprintf("%s%s", " %%", arg)},
	})
	return c
}
func (c *Condition) RLike(b bool, col string, arg any) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " like ?",
		args: []any{fmt.Sprintf("%s%s", arg, " %%%")},
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

// IsEmpty check the condition arguments are empty or not.
func (c *Condition) IsEmpty() bool {
	return len(c.params) == 0
}

func (c *Condition) GormWhere() (string, []any) {
	sqls := make([]string, 0)
	vals := make([]any, 0)

	for _, param := range c.params {
		sqls = append(sqls, param.sql)
		// 将每个条件的参数展开添加到总参数列表
		vals = append(vals, param.args...)
	}
	return strings.Join(sqls, " AND "), vals
}
