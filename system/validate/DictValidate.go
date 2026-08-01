package validate

import (
	common2 "github.com/xm-utils/tools/common"
)

type DictListForm struct {
	*common2.PageParam
	Name string `form:"name" json:"name" comment:"名称" `
	Code string `form:"code" json:"code" comment:"编码" `
}

func DictListFormError() map[string]string {
	formError := make(map[string]string)
	formError["Page.required"] = "分页参数必须填写"
	formError["PageSize.required"] = "分页参数必须填写"
	return formError
}

type DictAddForm struct {
	DictName string `json:"dictName" form:"dictName" binding:"required"`
	DictType string `json:"dictType" form:"dictType"  binding:"required"`
	Status   int8   `json:"status" form:"status"`
	Remark   string `json:"remark" form:"remark"`
}

func DictAddFormError() map[string]string {
	formError := make(map[string]string)
	formError["DictName.required"] = "字典名称参数缺失"
	formError["DictType.required"] = "字典类型参数缺失"
	return formError
}

type DictEditForm struct {
	Id int64 `json:"id" form:"id" binding:"required"`
	DictAddForm
}

func DictEditFormError() map[string]string {
	formError := DictAddFormError()
	formError["Id.required"] = "Id缺失"
	return formError
}

type DictDetailListForm struct {
	*common2.PageParam
	Code     string `form:"code" json:"code" comment:"类型编码" `
	ParentId int64  `form:"parentId" json:"parentId" comment:"父级ID" `
}

func DictDetailListFormError() map[string]string {
	formError := make(map[string]string)
	formError["Page.required"] = "分页参数必须填写"
	formError["PageSize.required"] = "分页参数必须填写"
	return formError
}

type DictDetailAddForm struct {
	DictType  string `json:"dictType" form:"dictType"  binding:"required"`
	DictLabel string `json:"dictLabel" form:"dictLabel" binding:"required" `
	DictValue string `json:"dictValue" form:"dictValue" binding:"required"`
	DictSort  int    `json:"dictSort" form:"dictSort" binding:"required"`
	CssClass  string `json:"cssClass" form:"cssClass"`
	ListClass string `json:"listClass" form:"listClass"`
	Status    int8   `json:"status" form:"status"`
	Remark    string `json:"remark" form:"remark"`
}

func DictDetailAddFormError() map[string]string {
	formError := make(map[string]string)
	formError["DictType.required"] = "字典类型参数缺失"
	formError["DictLabel.required"] = "数据标签参数缺失"
	formError["DictValue.required"] = "数据键值参数缺失"
	formError["DictSort.required"] = "显示排序参数缺失"
	return formError
}

type DictDetailEditForm struct {
	Id int64 `json:"id" form:"id" binding:"required"`
	DictDetailAddForm
}

func DictDetailEditFormError() map[string]string {
	formError := DictDetailAddFormError()
	formError["Id.required"] = "Id缺失"
	return formError
}
