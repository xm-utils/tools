package database

import (
	"reflect"
	"time"

	"github.com/beego/beego/v2/client/orm"
	"github.com/beego/beego/v2/client/orm/clauses/order_clause"
)

const (
	DefaultPage     int = 1
	DefaultPageSize int = 20
	MaxPageSize     int = 500

	TimeFormat = "2006-01-02 15:04:05"
	DateFormat = "2006-01-02"
)

// PageParam 用于查询的类
type PageParam struct {
	Page     int
	PageSize int
}

func (bqp *PageParam) IsValid() bool {
	return bqp.Page > 0 && bqp.PageSize > 0
}

func (bqp *PageParam) Offset() int {
	offset := 0
	if bqp.Page > 1 {
		offset = (bqp.Page - 1) * bqp.PageSize
	}
	return offset
}

func (bqp *PageParam) GetLimit() (limit, offset int) {
	if bqp.Page < DefaultPage {
		bqp.Page = DefaultPage
	}
	if bqp.PageSize <= 0 {
		bqp.PageSize = DefaultPageSize
	}
	if bqp.PageSize > MaxPageSize {
		bqp.PageSize = MaxPageSize
	}

	limit = bqp.PageSize
	offset = (bqp.Page - 1) * bqp.PageSize
	return
}

// TimeParam 时间区间参数
type TimeParam struct {
	Column    string `json:"column" form:"column"`
	StartTime string `json:"startTime" form:"startTime" binding:"required"` //开始时间
	EndTime   string `json:"endTime" form:"endTime" binding:"required"`     //结束时间
	st        time.Time
	et        time.Time
}

func (req *TimeParam) IsValid() bool {
	if req == nil {
		return false
	}
	if req.StartTime == "" || req.EndTime == "" {
		return false
	}

	st, err := parseLocalTime(req.StartTime)
	if err != nil {
		return false
	}
	req.st = st
	et, err := parseLocalTime(req.EndTime)
	if err != nil {
		return false
	}
	req.et = et

	return true
}

func (req *TimeParam) GetTime() (start, end time.Time) {
	if !req.IsValid() {
		return
	}

	return req.st, req.et
}

func (req *TimeParam) DiffDays() int {
	t1, t2 := req.GetTime()
	return int(t2.Sub(t1).Hours() / 24)
}

type ListParam struct {
	Param *orm.Condition
	Page  *PageParam
	Time  *TimeParam
	Order []*order_clause.Order
}

func parseLocalTime(value string) (time.Time, error) {
	layout := DateFormat
	if len(value) > 10 {
		layout = TimeFormat
	}
	return time.Parse(layout, value)
}

// CompareAndAssign 比较两个相同类型的结构体实例，将b中非空且与a不同的字段值赋给a
// 使用泛型确保a和b的类型一致
// 参数：
//   - a: 目标结构体指针
//   - b: 源结构体实例
//
// 返回值：
//   - 修改后的a结构体指针
//   - 有差异的字段名称列表
func compareAndAssign[T any](target *T, src T) (*T, []string) {
	// 获取反射值
	aValue := reflect.ValueOf(target)
	bValue := reflect.ValueOf(src)

	// 确保a是指针类型且不是nil
	if aValue.Kind() != reflect.Ptr || aValue.IsNil() {
		return target, []string{}
	}

	// 获取实际的元素
	aElem := aValue.Elem()
	bElem := bValue

	// 确保是结构体类型
	if aElem.Kind() != reflect.Struct {
		return target, []string{}
	}

	// 存储有差异的字段名称
	diffFields := make([]string, 0)

	// 遍历结构体的所有字段
	for i := 0; i < bElem.NumField(); i++ {
		bField := bElem.Field(i)
		aField := aElem.Field(i)

		// 获取字段名
		fieldName := bElem.Type().Field(i).Name

		// 检查b中的字段是否为空值
		if isEmptyValue(bField) {
			continue
		}

		// 检查字段是否可设置
		if !aField.CanSet() {
			continue
		}

		// 比较字段值是否不同
		if !reflect.DeepEqual(aField.Interface(), bField.Interface()) {
			// 将b中的值赋给a
			aField.Set(bField)
			// 添加到差异字段列表
			diffFields = append(diffFields, fieldName)
		}
	}

	return target, diffFields
}

// isEmptyValue 检查值是否为空
// 注意：对于布尔值，false不被认为是空值
func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	case reflect.Struct:
		// 处理时间类型
		if t, ok := v.Interface().(time.Time); ok {
			return t.IsZero()
		}
		// 注意：布尔值false不被认为是空值，因此不在此列
	}
	return false
}
