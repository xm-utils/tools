package validate

type AddMenuForm struct {
	MenuName   string `json:"menuName,omitempty" form:"menuName" binding:"required" comment:"菜单名称"`
	MenuNameEn string `json:"menuNameEn,omitempty" form:"menuNameEn" comment:"菜单名称"`
	ParentId   int64  `json:"parentId,omitempty" form:"parentId" comment:"父菜单ID"`
	OrderNum   int    `json:"orderNum,omitempty" form:"orderNum" comment:"显示顺序"`
	Path       string `json:"path,omitempty" form:"path" comment:"路由地址"`
	Component  string `json:"component,omitempty" form:"component" comment:"组件路径"`
	Query      string `json:"query,omitempty" form:"query" comment:"路由参数"`
	RouteName  string `json:"routeName,omitempty" form:"routeName" comment:"路由名称"`
	IsFrame    int8   `json:"isFrame,omitempty" form:"isFrame" binding:"min=1,max=2" comment:"是否为外链（1是 2否）"`
	IsCache    int8   `json:"isCache,omitempty" form:"isCache" binding:"min=1,max=2" comment:"是否缓存（0缓存 1不缓存）"`
	MenuType   string `json:"menuType,omitempty" form:"menuType" binding:"required" comment:"类型（M目录 C菜单 F按钮）"`
	Visible    int8   `json:"visible,omitempty" form:"visible" binding:"min=1,max=2" comment:"显示状态（0显示 1隐藏）"`
	Status     int8   `json:"status,omitempty" form:"status" binding:"required,min=1,max=2" comment:"菜单状态（0正常 1停用）"`
	Perms      string `json:"perms,omitempty" form:"perms" comment:"权限字符串"`
	Icon       string `json:"icon,omitempty" form:"icon" comment:"菜单图标"`
}

func AddMenuFormError() map[string]string {
	formError := make(map[string]string)
	formError["AppId.required"] = "应用ID必须填写"
	formError["MenuName.required"] = "标题必须填写"
	formError["MenuType.required"] = "菜单类型必须填写"
	formError["IsFrame.min"] = "是否外链参数有误"
	formError["IsFrame.max"] = "是否外链参数有误"
	formError["Visible.min"] = "显示状态参数有误"
	formError["Visible.max"] = "显示状态参数有误"
	formError["Status.required"] = "状态参数有误"
	formError["Status.min"] = "状态参数有误"
	formError["Status.max"] = "状态参数有误"
	return formError
}

type EditMenuForm struct {
	MenuId int64 `json:"menuId" form:"menuId" binding:"required"`
	AddMenuForm
}

func EditMenuFormError() map[string]string {
	formError := make(map[string]string)
	formError["MenuId.required"] = "菜单Id缺失"
	formError["MenuName.required"] = "标题必须填写"
	formError["MenuType.required"] = "菜单类型必须填写"
	formError["IsFrame.min"] = "是否外链参数有误"
	formError["IsFrame.max"] = "是否外链参数有误"
	formError["Visible.min"] = "显示状态参数有误"
	formError["Visible.max"] = "显示状态参数有误"
	formError["Status.required"] = "状态参数有误"
	formError["Status.min"] = "状态参数有误"
	formError["Status.max"] = "状态参数有误"
	return formError
}
