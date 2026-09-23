package database

import (
	"fmt"
	"runtime/debug"

	"github.com/beego/beego/v2/client/orm"
	"github.com/beego/beego/v2/client/orm/clauses/order_clause"
	"github.com/xm-utils/tools/common"
)

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

type ListParam struct {
	Param      *orm.Condition
	Page       *common.PageParam
	Time       *common.TimeParam
	TimeColumn string
	Order      []*order_clause.Order
}

func ReadOne[T any](t T, cols ...string) (result *T) {
	defer func() {
		if r := recover(); r != nil {
			result = nil
		}
	}()
	if err := orm.NewOrm().Read(&t, cols...); err != nil {
		return nil
	}
	return &t
}

// FindOne 根据ID查询单条记录
func FindOne[T any](id int64) (result *T) {
	defer func() {
		if r := recover(); r != nil {
			result = nil
		}
	}()
	var model T
	err := orm.NewOrm().QueryTable(&model).Filter("id", id).One(&model)
	if err != nil {
		return nil
	}
	return &model
}

func FindAll[T any](form ListParam) (list []*T, total int64, err error) {
	defer recoverToErr(&err)
	query := orm.NewOrm().QueryTable(new(T))
	if form.Param != nil && !form.Param.IsEmpty() {
		query = query.SetCond(form.Param)
	}
	timeParam := form.Time
	if timeParam != nil && timeParam.IsValid() {
		column := form.TimeColumn
		start, end := timeParam.GetTime()
		// 结束时间只传日期(如 2006-01-02)时，解析结果为当天 00:00:00，
		// 需补全为当天结束时间 23:59:59，避免 __lte 闭区间漏掉当天整天的数据
		if len(timeParam.EndTime) <= 10 {
			end = common.EndByTime(end)
		}
		query = query.Filter(fmt.Sprintf("%s__gte", column), start).
			Filter(fmt.Sprintf("%s__lte", column), end)
	}

	total, err = query.Count()
	if err != nil {
		return
	}
	if total == 0 {
		return
	}
	if len(form.Order) > 0 {
		query = query.OrderClauses(form.Order...)
	}
	if form.Page != nil && form.Page.IsValid() {
		limit, offset := form.Page.GetLimit()
		query = query.Limit(limit, offset)
	}

	list = make([]*T, 0)
	_, err = query.All(&list)

	return
}

func FindList[T any](cond *orm.Condition) (list []*T, err error) {
	defer recoverToErr(&err)
	query := orm.NewOrm().QueryTable(new(T)).SetCond(cond)
	list = make([]*T, 0)
	_, err = query.All(&list)
	return
}
func Count[T any](cond *orm.Condition) (count int64, err error) {
	defer recoverToErr(&err)
	query := orm.NewOrm().QueryTable(new(T)).SetCond(cond)
	count, err = query.Count()
	return
}

func Update[T any](o orm.TxOrmer, form T, columns ...string) (err error) {
	defer recoverToErr(&err)
	if o == nil {
		_, err = orm.NewOrm().Update(form, columns...)
	} else {
		_, err = o.Update(form, columns...)
	}
	return err
}

func UpdateModel[T any](o orm.TxOrmer, old *T, form T) (err error) {
	defer recoverToErr(&err)
	if old == nil {
		return orm.ErrNoRows
	}
	var fields []string
	old, fields = common.CompareAndAssign(old, form)
	if len(fields) == 0 {
		return nil
	}
	return Update(o, old, fields...)
}

func UpdateByCondition[T any](o orm.TxOrmer, cond *orm.Condition, param orm.Params) (err error) {
	defer recoverToErr(&err)
	if len(param) <= 0 {
		return orm.ErrArgs
	}
	var query orm.QuerySeter
	if o == nil {
		query = orm.NewOrm().QueryTable(new(T))
	} else {
		query = o.QueryTable(new(T))
	}

	if cond != nil && !cond.IsEmpty() {
		query = query.SetCond(cond)
	}
	_, err = query.Update(param)
	return
}

func Delete[T any](o orm.TxOrmer, form T, cols ...string) (err error) {
	defer recoverToErr(&err)
	if o == nil {
		_, err = orm.NewOrm().Delete(form, cols...)
	} else {
		_, err = o.Delete(form, cols...)
	}
	return
}

func DeleteByCondition[T any](o orm.TxOrmer, cond *orm.Condition) (err error) {
	defer recoverToErr(&err)
	if cond.IsEmpty() {
		return orm.ErrArgs
	}

	var query orm.QuerySeter
	if o == nil {
		query = orm.NewOrm().QueryTable(new(T))
	} else {
		query = o.QueryTable(new(T))
	}

	_, err = query.SetCond(cond).Delete()
	return
}

func Insert[T any](o orm.TxOrmer, form T) (err error) {
	defer recoverToErr(&err)
	if o == nil {
		_, err = orm.NewOrm().Insert(form)
	} else {
		_, err = o.Insert(form)
	}
	return
}

func InsertBatch(o orm.TxOrmer, bulk int, m interface{}) (i int64, err error) {
	defer recoverToErr(&err)
	if o == nil {
		i, err = orm.NewOrm().InsertMulti(bulk, m)
	} else {
		i, err = o.InsertMulti(bulk, m)
	}
	return
}
