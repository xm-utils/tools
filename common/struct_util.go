package common

import (
	"reflect"
	"strings"
	"time"
)

// CompareAndAssign 比较两个相同类型的结构体实例，将b中非空且与a不同的字段值赋给a
// 使用泛型确保a和b的类型一致
// 参数：
//   - a: 目标结构体指针
//   - b: 源结构体实例
//
// 返回值：
//   - 修改后的a结构体指针
//   - 有差异的字段名称列表
func CompareAndAssign[T any](target *T, src T) (*T, []string) {
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

	// 递归比较并赋值
	compareAndAssignRecursive(aElem, bElem, &diffFields)

	return target, diffFields
}

// compareAndAssignRecursive 递归比较两个结构体字段，将src中非空且不同的字段赋给target
// prefix 用于拼接嵌套字段名（如 "Address.City"）
func compareAndAssignRecursive(aElem, bElem reflect.Value, diffFields *[]string) {
	for i := 0; i < bElem.NumField(); i++ {
		bField := bElem.Field(i)
		aField := aElem.Field(i)

		// 获取字段名
		fieldName := bElem.Type().Field(i).Name
		fullFieldName := fieldName

		// 检查b中的字段是否为空值
		if isEmptyValue(bField) {
			continue
		}

		// 检查字段是否可设置
		if !aField.CanSet() {
			continue
		}

		// 如果字段是嵌套结构体（非 time.Time 等特殊类型），递归处理
		if bField.Kind() == reflect.Struct && !isSpecialStruct(bField) {
			compareAndAssignRecursive(aField, bField, diffFields)
			continue
		}

		// 如果字段是指向结构体的指针（非 nil），解引用后递归处理
		if bField.Kind() == reflect.Ptr && !bField.IsNil() && bField.Elem().Kind() == reflect.Struct && !isSpecialStruct(bField.Elem()) {
			// 如果 target 侧为 nil，需要先分配
			if aField.IsNil() {
				aField.Set(reflect.New(bField.Type().Elem()))
			}
			compareAndAssignRecursive(aField.Elem(), bField.Elem(), diffFields)
			continue
		}

		// 比较字段值是否不同
		if !reflect.DeepEqual(aField.Interface(), bField.Interface()) {
			// 将b中的值赋给a
			aField.Set(bField)
			// 添加到差异字段列表
			*diffFields = append(*diffFields, fullFieldName)
		}
	}
}

// isSpecialStruct 判断结构体是否为特殊类型（如 time.Time），不应递归展开
func isSpecialStruct(v reflect.Value) bool {
	if _, ok := v.Interface().(time.Time); ok {
		return true
	}
	return false
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
		// 对于普通嵌套结构体，所有字段都为零值时视为空
		for i := 0; i < v.NumField(); i++ {
			if !isEmptyValue(v.Field(i)) {
				return false
			}
		}
		return true

	}
	return false
}

// GetColumnTags 获取结构体中所有字段的column标签值
// 参数：
//   - s: 结构体实例或结构体指针
//
// 返回值：
//   - map[string]string: key为字段名，value为column标签值，如果没有column标签则返回字段名
func GetColumnTags(s interface{}) map[string]string {
	result := make(map[string]string)

	// 获取反射类型
	t := reflect.TypeOf(s)

	// 如果是指针，获取指向的元素
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// 确保是结构体类型
	if t.Kind() != reflect.Struct {
		return result
	}

	// 遍历结构体的所有字段
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldName := field.Name

		// 获取orm标签
		ormTag := field.Tag.Get("orm")
		if ormTag == "-" {
			continue
		}
		// 解析column值
		column := parseColumnFromTag(ormTag)
		if column != "" {
			result[fieldName] = column
		} else {
			// 如果没有column标签，则使用字段名作为默认值
			result[fieldName] = fieldName
		}
	}

	return result
}

// GetColumnTag 获取结构体中指定字段的column标签值
// 参数：
//   - s: 结构体实例或结构体指针
//   - fieldName: 字段名
//
// 返回值：
//   - string: column标签值，如果不存在则返回空字符串
//
// 兼容旧版本的单字段查询
func GetColumnTag(s interface{}, fieldName string) string {
	// 为了保持向后兼容性，继续支持单字段查询
	// 获取反射类型
	t := reflect.TypeOf(s)

	// 如果是指针，获取指向的元素
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// 确保是结构体类型
	if t.Kind() != reflect.Struct {
		return ""
	}

	// 获取指定字段
	field, ok := t.FieldByName(fieldName)
	if !ok {
		return ""
	}

	// 获取orm标签
	ormTag := field.Tag.Get("orm")
	if ormTag == "-" {
		return ""
	}

	// 解析并返回column值
	return parseColumnFromTag(ormTag)
}

// GetColumnTagsByName 根据指定的字段名列表获取column标签值
// 参数：
//   - s: 结构体实例或结构体指针
//   - fieldNames: 字段名列表，支持传入多个字段名
//
// 返回值：
//   - []string: column标签值列表，如果没有column标签则返回字段名
func GetColumnTagsByName(s interface{}, fieldNames ...string) []string {
	result := make([]string, 0)
	// 如果没有传入字段名，则返回空map
	if len(fieldNames) == 0 {
		return result
	}

	// 获取反射类型
	t := reflect.TypeOf(s)

	// 如果是指针，获取指向的元素
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// 确保是结构体类型
	if t.Kind() != reflect.Struct {
		return result
	}

	// 遍历指定的字段名
	for _, fieldName := range fieldNames {
		// 获取指定字段
		field, ok := t.FieldByName(fieldName)
		if !ok {
			continue
		}

		// 获取orm标签
		ormTag := field.Tag.Get("orm")
		if ormTag == "-" {
			continue
		}
		// 解析column值
		column := parseColumnFromTag(ormTag)
		if column != "" {
			result = append(result, column)
		} else {
			// 如果没有column标签，则使用字段名作为默认值
			result = append(result, fieldName)
		}
	}

	return result
}

// parseColumnFromTag 从orm标签中解析column值
// 示例: orm:"column(order_no)" -> "order_no"
func parseColumnFromTag(ormTag string) string {

	// 查找column()部分
	start := strings.Index(ormTag, "column(")
	if start == -1 {
		return ""
	}

	// 从column(之后开始查找
	start += 7 // len("column(") = 7
	end := strings.Index(ormTag[start:], ")")
	if end == -1 {
		return ""
	}

	// 提取column值
	column := ormTag[start : start+end]
	return strings.TrimSpace(column)
}
