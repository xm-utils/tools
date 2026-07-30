package database_orm

import (
	"fmt"

	"github.com/beego/beego/v2/client/orm"
)

func ReadOne[T any](t T, cols ...string) *T {
	if err := orm.NewOrm().Read(&t, cols...); err != nil {
		return nil
	}
	return &t
}

// FindOne 根据ID查询单条记录
func FindOne[T any](id int64) *T {
	var model T
	err := orm.NewOrm().QueryTable(&model).Filter("id", id).One(&model)
	if err != nil {
		return nil
	}
	return &model
}

func FindAll[T any](form ListParam) (list []*T, total int64, err error) {
	query := orm.NewOrm().QueryTable(new(T))
	if form.Param != nil && !form.Param.IsEmpty() {
		query = query.SetCond(form.Param)
	}
	timeParam := form.Time
	if timeParam != nil && timeParam.IsValid() {
		column := timeParam.Column
		start, end := timeParam.GetTime()
		query = query.Filter(fmt.Sprintf("%s__gte", column), start).Filter(fmt.Sprintf("%s__lt", column), end)
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
	query := orm.NewOrm().QueryTable(new(T)).SetCond(cond)
	list = make([]*T, 0)
	_, err = query.All(&list)
	return
}
func Count[T any](cond *orm.Condition) (int64, error) {
	query := orm.NewOrm().QueryTable(new(T)).SetCond(cond)
	return query.Count()
}

func Update[T any](o orm.TxOrmer, form T, columns ...string) (err error) {
	if o == nil {
		_, err = orm.NewOrm().Update(form, columns...)
	} else {
		_, err = o.Update(form, columns...)
	}
	return err
}

func UpdateModel[T any](o orm.TxOrmer, old *T, form T) error {
	if old == nil {
		return orm.ErrNoRows
	}
	var fields []string
	old, fields = compareAndAssign(old, form)
	if len(fields) == 0 {
		return nil
	}
	return Update(o, old, fields...)
}

func UpdateByCondition[T any](o orm.TxOrmer, cond *orm.Condition, param orm.Params) (err error) {
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
	if o == nil {
		_, err = orm.NewOrm().Delete(form, cols...)
	} else {
		_, err = o.Delete(form, cols...)
	}
	return
}

func DeleteByCondition[T any](o orm.TxOrmer, cond *orm.Condition) (err error) {
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
	if o == nil {
		_, err = orm.NewOrm().Insert(form)
	} else {
		_, err = o.Insert(form)
	}
	return
}

func InsertBatch(o orm.TxOrmer, bulk int, m interface{}) (i int64, err error) {
	if o == nil {
		i, err = orm.NewOrm().InsertMulti(bulk, m)
	} else {
		i, err = o.InsertMulti(bulk, m)
	}
	return
}
