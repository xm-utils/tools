package database

import (
	"fmt"
	"strings"
)

type condValue struct {
	cond *Condition
	sql  string
	args interface{}
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

func (c *Condition) Eq(b bool, col string, args interface{}) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " = ?",
		args: args,
	})
	return c
}

func (c *Condition) In(b bool, col string, args ...interface{}) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " in ?",
		args: args,
	})
	return c
}

func (c *Condition) Like(b bool, col string, args interface{}) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " like ?",
		args: fmt.Sprintf("%s%s%s", " %%", args, " %%"),
	})
	return c
}

func (c *Condition) NotLike(b bool, col string, args interface{}) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql:  col + " not like ?",
		args: fmt.Sprintf("%s%s%s", " %%", args, " %%"),
	})
	return c
}

func (c *Condition) LLike(b bool, col string, args interface{}) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql: col + " like ?",

		args: fmt.Sprintf("%s%s", " %%", args),
	})
	return c
}
func (c *Condition) RLike(b bool, col string, args interface{}) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql: col + " like ?",

		args: fmt.Sprintf("%s%s", args, " %%"),
	})
	return c
}

func (c *Condition) Between(b bool, col, start, end string) *Condition {
	if !b {
		return c
	}
	c.params = append(c.params, condValue{
		sql: fmt.Sprintf("%s BETWEEN %s AND %s", col, start, end),
	})
	return c
}

// IsEmpty check the condition arguments are empty or not.
func (c *Condition) IsEmpty() bool {
	return len(c.params) == 0
}

func (c *Condition) GormWhere() (string, []interface{}) {
	sqls := make([]string, 0)

	vals := make([]interface{}, 0)
	for _, param := range c.params {
		sqls = append(sqls, param.sql)
		vals = append(vals, param.args)
	}
	return strings.Join(sqls, " AND "), vals

}
