package validate

import (
	common "github.com/xm-utils/tools/common"
)

type RoleListParam struct {
	*common.PageParam
	RoleName string `json:"roleName,omitempty" form:"roleName"`
	Status   int8   `json:"status,omitempty" form:"status"`
}

type RoleUserListParam struct {
	*common.PageParam
	RoleId   int64  `json:"roleId" form:"roleId"`
	UserName string `json:"userName" form:"userName"`
	Phone    string `json:"phoneNumber" form:"phoneNumber"`
}

type RoleAddParam struct {
	RoleName          string  `json:"roleName" binding:"required"`
	RoleKey           string  `json:"roleKey" binding:"required"`
	RoleSort          int     `json:"roleSort" binding:"required"`
	DataScope         int     `json:"dataScope"`
	MenuCheckStrictly int     `json:"menuCheckStrictly"`
	DeptCheckStrictly int     `json:"deptCheckStrictly"`
	Status            int     `json:"status"`
	MenuIds           []int64 `json:"menuIds"`
	Remark            string  `json:"remark"`
}

func RoleAddParamError() map[string]string {
	formError := make(map[string]string)
	formError["RoleName.required"] = "角色名称必须填写"
	formError["RoleKey.required"] = "权限字符必须填写"
	formError["RoleSort.required"] = "角色顺序必须填写"
	return formError
}

type RoleEditParam struct {
	RoleAddParam
	RoleId int64 `json:"roleId" form:"roleId" binding:"required"`
}

func RoleEditParamError() map[string]string {
	formError := make(map[string]string)
	formError["RoleId.required"] = "角色ID必须填写"
	formError["RoleName.required"] = "角色名称必须填写"
	formError["RoleKey.required"] = "权限字符必须填写"
	formError["RoleSort.required"] = "角色顺序必须填写"
	return formError
}

type RoleChangeStatusParam struct {
	RoleId int64 `json:"roleId" form:"roleId" binding:"required"`
	Status int   `json:"status" binding:"required"`
}

func RoleChangeStatusParamError() map[string]string {
	formError := make(map[string]string)
	formError["RoleId.required"] = "角色ID必须填写"
	formError["Status.required"] = "角色状态必须填写"
	return formError
}

type RoleCancelParam struct {
	RoleId int64   `json:"roleId,omitempty" form:"roleId" binding:"required"`
	UserId []int64 `json:"userId,omitempty" form:"userId" binding:"required"`
}

func RoleCancelParamError() map[string]string {
	formError := make(map[string]string)
	formError["RoleId.required"] = "角色ID必须填写"
	formError["UserId.required"] = "用户ID必须填写"
	return formError
}
