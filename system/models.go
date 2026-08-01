package system

import (
	"time"

	"github.com/beego/beego/v2/client/orm"
	"github.com/xm-utils/tools/common"
)

const (
	MenuTypeDir    = "M" //菜单类型（目录）
	MenuTypeMenu   = "C" //菜单类型（菜单）
	MenuTypeButton = "F" //菜单类型（按钮）
)
const (
	MenuLayout     = "Layout"     //Layout组件标识
	MenuParentView = "ParentView" //ParentView组件标识
	MenuInnerLink  = "InnerLink"  //InnerLink组件标识
)

const (
	MenuTableName       = "menu"
	UserTableName       = "user"
	DictTableName       = "dict_type"
	DictDetailTableName = "dict_data"
	RoleTableName       = "role"
	RoleUserTableName   = "user_role"
	RoleMenuTableName   = "role_menu"
)

type BaseModel struct {
	CreateBy   string    `json:"createBy,omitempty" comment:"创建者"`
	CreateTime time.Time `json:"createTime" orm:"auto_now_add;type(datetime)" comment:"创建时间"`
	UpdateBy   string    `json:"updateBy,omitempty" comment:"更新者"`
	UpdateTime time.Time `json:"updateTime" orm:"auto_now;type(datetime)" comment:"更新时间"`
	Remark     string    `json:"remark,omitempty" comment:"备注"`
}

type DictType struct {
	BaseModel
	Id       int64  `orm:"pk;auto" json:"dictId,omitempty" comment:"字典主键"`
	DictName string `json:"dictName,omitempty" comment:"字典名称" `
	DictType string `json:"dictType,omitempty" comment:"字典类型" `
	Status   int8   `json:"status,omitempty" comment:"状态（0正常 1停用）"`
}

func (m *DictType) TableName() string {
	return DictTableName
}

type DictData struct {
	BaseModel
	Id        int64  `orm:"pk;auto" json:"id,omitempty" comment:"字典编码"`
	DictSort  int    `json:"dictSort,omitempty" comment:"字典排序" `
	DictLabel string `json:"dictLabel,omitempty" comment:"字典标签" `
	DictValue string `json:"dictValue,omitempty" comment:"字典键值" `
	DictType  string `json:"dictType,omitempty" comment:"字典类型" `
	CssClass  string `json:"cssClass,omitempty" comment:"样式属性（其他样式扩展）" `
	ListClass string `json:"listClass,omitempty" comment:"表格回显样式" `
	IsDefault string `json:"isDefault,omitempty" comment:"是否默认（Y是 N否）" `
	Status    int8   `json:"status,omitempty"`
}

func (m *DictData) TableName() string {
	return DictDetailTableName
}

type SysMenu struct {
	BaseModel
	Id         int64  `orm:"pk;auto" json:"menuId"  comment:"菜单ID"`
	MenuName   string `json:"menuName" comment:"菜单名称"`
	MenuNameEn string `json:"menuNameEn" comment:"菜单名称"`
	ParentId   int64  `json:"parentId" comment:"父菜单ID"`
	OrderNum   int    `json:"orderNum" comment:"显示顺序"`
	Path       string `json:"path" comment:"路由地址"`
	Component  string `json:"component" comment:"组件路径"`
	Query      string `json:"query" comment:"路由参数"`
	RouteName  string `json:"routeName" comment:"路由名称"`
	IsFrame    int8   `json:"isFrame" comment:"是否为外链（1是 2否）"`
	IsCache    int8   `json:"isCache" comment:"是否缓存（0缓存 1不缓存）"`
	MenuType   string `json:"menuType" comment:"类型（M目录 C菜单 F按钮）"`
	Visible    int8   `json:"visible" comment:"显示状态（0显示 1隐藏）"`
	Status     int8   `json:"status" comment:"菜单状态（0正常 1停用）"`
	Perms      string `json:"perms" comment:"权限字符串"`
	Icon       string `json:"icon" comment:"菜单图标"`
}

func (model *SysMenu) TableName() string {
	return MenuTableName
}
func (model *SysMenu) GetRouteName() string {
	// 非外链并且是一级目录（类型为目录）
	if model.isMenuFrame() {
		return ""
	}

	routerName := model.RouteName
	if len(routerName) < 1 {
		routerName = model.Path
	}
	return routerName
}

func (model *SysMenu) isMenuFrame() bool {
	return model.ParentId == 0 && MenuTypeMenu == model.MenuType && model.IsFrame == common.StatusDisable
}
func (model *SysMenu) isInnerLink() bool {
	return model.IsFrame == common.StatusDisable && common.IsHttp(model.Path)
}

func (model *SysMenu) isParentView() bool {
	return model.ParentId != 0 && MenuTypeDir == model.MenuType

}

func (model *SysMenu) GetRouterPath() string {
	// 内链打开外网方式
	if model.ParentId != 0 && model.isInnerLink() {
		return model.Path
	}
	// 非外链并且是一级目录（类型为目录）
	if 0 == model.ParentId && MenuTypeDir == model.MenuType && model.IsFrame == common.StatusDisable {
		return "/" + model.Path
	} else if model.isMenuFrame() { // 非外链并且是一级目录（类型为菜单）
		return "/"
	}
	return model.Path
}

func (model *SysMenu) GetComponent() string {
	component := MenuLayout
	if len(model.Component) > 0 && !model.isMenuFrame() {
		component = model.Component
	} else if len(model.Component) <= 0 && model.ParentId != 0 && model.isInnerLink() {
		component = MenuInnerLink
	} else if len(model.Component) <= 0 && model.isParentView() {
		component = MenuParentView
	}
	return component
}

func (model *SysMenu) FormatSelectTree() *common.SelectTree {
	return &common.SelectTree{
		Id:       model.Id,
		Label:    model.MenuName,
		Children: make([]*common.SelectTree, 0),
	}
}

func RegisterModel() {
	orm.RegisterModelWithPrefix("sys_",
		new(SysUser),
		new(SysRole),
		new(SysMenu),
		new(DictType),
		new(DictData),
		new(SysRoleMenu),
		new(SysUserRole))
}
