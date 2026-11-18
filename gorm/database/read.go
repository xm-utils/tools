package database

import (
	"context"

	"gorm.io/plugin/dbresolver"
)

// Primary 返回强制走主库(源)的读器：内部使用 dbresolver.Write，
// 即使表已注册从库，也会命中主库连接（适合“写后立读”避免主从延迟）。
// 实际执行委托 CtxBuilder（context.Background()，等价于不携带 ctx）。
func Primary[T any]() *Reader[T] {
	return &Reader[T]{
		do: dbresolver.Write,
	}
}

// Replica 返回尝试走从库(副本)的读器：内部使用 dbresolver.Read，
// 命中已注册从库的表时走从库，否则回落到主库连接。
// 实际执行委托 CtxBuilder（context.Background()，等价于不携带 ctx）。
func Replica[T any]() *Reader[T] {
	return &Reader[T]{
		do: dbresolver.Read,
	}
}

type Reader[T any] struct {
	do dbresolver.Operation
}

// reader 每次调用构造全新的构建器，仅携带读写路由提示；
// 避免同一 Reader 实例被复用时链式条件在多次调用间累积。
func (w *Reader[T]) reader() *CtxBuilder[T] {
	b := Ctx[T](context.Background())
	b.do, b.hasDo = w.do, true
	return b
}

// ReadOne 按入参 t 的结构体字段作为条件查询单条记录（兼容层：吞错返回 nil）。
func (w *Reader[T]) ReadOne(t T, cols ...string) (result *T) {
	result, _ = w.reader().ReadOne(t, cols...)
	return
}

// FindOne 根据 ID 查询单条记录（兼容层：吞错返回 nil）。
func (w *Reader[T]) FindOne(id int64) (result *T) {
	result, _ = w.reader().FindOne(id)
	return
}

// FindAll 分页查询列表：ListParam 的 Cond/Query/Time/Order/Page 经 ApplyListParam 透传。
func (w *Reader[T]) FindAll(param ListParam) (list []*T, total int64, err error) {
	return w.reader().ApplyListParam(param).FindAll()
}

// FindList 查询列表（不分页，可选分页参数）。
func (w *Reader[T]) FindList(cond *Condition, page ...PageParam) (list []*T, err error) {
	b := w.reader().Where(cond)
	if len(page) > 0 {
		b = b.Page(page[0])
	}
	return b.FindList()
}

// Count 统计数量（不受排序影响）。
func (w *Reader[T]) Count(cond *Condition) (count int64, err error) {
	return w.reader().Where(cond).Count()
}
