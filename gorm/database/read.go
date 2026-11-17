package database

import (
	"errors"
	"fmt"
	"runtime/debug"
	"strings"

	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"
)

// Primary 返回强制走主库(源)的读器：内部使用 dbresolver.Write，
// 即使表已注册从库，也会命中主库连接（适合“写后立读”避免主从延迟）。
func Primary[T any]() *Reader[T] {
	return &Reader[T]{
		do: dbresolver.Write,
	}
}

// Replica 返回尝试走从库(副本)的读器：内部使用 dbresolver.Read，
// 命中已注册从库的表时走从库，否则回落到主库连接。
func Replica[T any]() *Reader[T] {
	return &Reader[T]{
		do: dbresolver.Read,
	}
}

type Reader[T any] struct {
	do dbresolver.Operation
}

// getDB 返回携带当前路由操作的会话；未初始化时返回 nil（不 panic），由调用方判空并返回 error/nil，
// 与包级函数「未初始化返回 error」的风格保持一致。
func (w *Reader[T]) getDB() *gorm.DB {
	db := GetDB()
	if db == nil {
		return nil
	}
	return db.Clauses(w.do)
}

// ReadOne 按入参 t 的结构体字段作为条件查询单条记录。
// t 仅用于构造 WHERE 条件，结果扫描到独立变量，避免条件对象被查询结果覆盖。
func (w *Reader[T]) ReadOne(t T, cols ...string) (result *T) {

	defer func() {
		if r := recover(); r != nil {
			result = nil
		}
	}()

	db := w.getDB()
	if db == nil {
		return nil
	}

	if len(cols) > 0 {
		cs := []interface{}{}
		for _, col := range cols {
			cs = append(cs, col)
		}
		db = db.Where(t, cs)
	} else {
		db = db.Where(t)
	}
	var out T
	if err := db.First(&out).Error; err != nil {
		return nil
	}
	return &out
}
func (w *Reader[T]) FindOne(id int64) (result *T) {
	defer func() {
		if r := recover(); r != nil {
			result = nil
		}
	}()

	db := w.getDB()
	if db == nil {
		return nil
	}

	var model T
	if err := db.First(&model, id).Error; err != nil {
		return nil
	}
	return &model
}

// FindAll 分页查询列表
func (w *Reader[T]) FindAll(param ListParam) (list []*T, total int64, err error) {
	defer recoverToErr(&err)
	db := w.getDB()
	if db == nil {
		return nil, 0, errors.New("database connection not found")
	}
	var model T
	query := db.Model(&model)

	// 应用自定义查询条件
	if param.Cond != nil && !param.Cond.IsEmpty() {
		q, v := param.Cond.GormWhere()
		query = query.Where(q, v...)
	}

	if param.Query != nil {
		for key, val := range param.Query {
			query = query.Where(key, val)
		}
	}

	// 应用时间范围查询
	if param.Time != nil && param.Time.IsValid() {
		column := param.Time.GetColumn()
		start, end := param.Time.GetTime()
		query = query.Where(fmt.Sprintf("%s >= ?", column), start).
			Where(fmt.Sprintf("%s < ?", column), end)
	}

	// 获取总数

	if err = query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if total == 0 {
		return make([]*T, 0), 0, nil
	}

	// 应用排序
	if len(param.Order) > 0 {
		orderList := make([]string, len(param.Order))
		for i, order := range param.Order {
			if strings.HasPrefix(order, "-") {
				orderList[i] = order[1:] + " DESC"
			} else {
				orderList[i] = order
			}
		}
		query = query.Order(strings.Join(orderList, ","))
	}

	// 应用分页
	if param.Page != nil && param.Page.IsValid() {
		limit, offset := param.Page.GetLimit()
		query = query.Limit(int(limit)).Offset(int(offset))
	}

	// 查询数据
	err = query.Find(&list).Error
	if err != nil {
		return nil, 0, err
	}

	return list, total, nil
}

// FindList 查询列表（不分页）
func (w *Reader[T]) FindList(cond *Condition, page ...PageParam) (list []*T, err error) {
	defer recoverToErr(&err)
	db := w.getDB()
	if db == nil {
		return nil, errors.New("database connection not found")
	}

	var model T
	query := db.Model(&model)

	if cond != nil && !cond.IsEmpty() {
		q, v := cond.GormWhere()
		query = query.Where(q, v...)
	}

	// 应用分页
	if len(page) > 0 {
		p := page[0]
		if p.IsValid() {
			limit, offset := p.GetLimit()
			query = query.Limit(int(limit)).Offset(int(offset))
		}
	}

	err = query.Find(&list).Error
	return
}

// Count 统计数量
func (w *Reader[T]) Count(cond *Condition) (count int64, err error) {
	defer recoverToErr(&err)
	db := w.getDB()
	if db == nil {
		return 0, errors.New("database connection not found")
	}
	var model T
	query := db.Model(&model)

	if cond != nil && !cond.IsEmpty() {
		q, v := cond.GormWhere()
		query = query.Where(q, v...)
	}

	err = query.Count(&count).Error
	return
}

// recoverToErr 捕获 beego orm 调用中可能出现的 panic，并将其转换为 error 返回，
// 避免因底层 panic 直接导致程序崩溃。
func recoverToErr(err *error) {
	if r := recover(); r != nil {
		var e error
		switch v := r.(type) {
		case error:
			e = v
		default:
			e = fmt.Errorf("%v", v)
		}
		if err != nil {
			*err = fmt.Errorf("panic recovered: %w\nstack: %s", e, debug.Stack())
		}
	}
}
