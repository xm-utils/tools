package system

import (
	"sort"
	"strings"

	"github.com/beego/beego/v2/client/orm"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/beego/database"
	"github.com/xm-utils/tools/common"
	"github.com/xm-utils/tools/system/validate"
)

type MenuListForm struct {
	Status   uint8   `json:"status" form:"status" comment:"状态"`
	MenuName string  `json:"menuName" form:"menuName" comment:"菜单名称"`
	MenuType string  `json:"menuType" form:"menuType" comment:"类型（M目录 C菜单 F按钮）"`
	RoleId   int64   `json:"roleId" form:"roleId" comment:"<UNK>"`
	UserId   int64   `json:"userId" form:"userId" comment:"<UNK>"`
	MenuId   []int64 `json:"menuId" form:"menuId" comment:"<UNK>"`
}

type MenuTreeModel struct {
	Id         int64            `json:"id"`
	Name       string           `json:"name,omitempty"`
	NameEn     string           `json:"name_en,omitempty"`
	Path       string           `json:"path,omitempty"`
	Hidden     bool             `json:"hidden,omitempty"`
	Redirect   string           `json:"redirect,omitempty"`
	Component  string           `json:"component,omitempty"`
	Query      string           `json:"query,omitempty"`
	AlwaysShow bool             `json:"alwaysShow,omitempty"`
	Meta       *MenuMeta        `json:"meta,omitempty"`
	Children   []*MenuTreeModel `json:"children,omitempty"`
}
type MenuMeta struct {
	Title   string `json:"title,omitempty"`
	Icon    string `json:"icon,omitempty"`
	NoCache bool   `json:"noCache,omitempty"`
	Link    string `json:"link,omitempty"`
}
type MenuTreeResp struct {
	Id       int64           `json:"id"`
	Label    string          `json:"label"`
	LabelCn  string          `json:"label_cn"`
	IconCls  string          `json:"iconCls"`
	ScriptId string          `json:"scriptid"`
	Children []*MenuTreeResp `json:"children"`
}

// BuildMenuTree 构建菜单树
func BuildMenuTree(menuList []*SysMenu, rootId int64) []*MenuTreeModel {
	var menuSort = make([]int, 0)
	var menuMapList = make(map[int]*SysMenu, 0)
	for _, menu := range menuList {
		if menu.MenuType == MenuTypeButton {
			continue
		}
		key := menu.OrderNum*100 + int(menu.ParentId*10+menu.Id) // (1000-menu.OrderNum)*10000 + menu.MenuId
		menuSort = append(menuSort, key)
		menuMapList[key] = menu
	}
	var menuListAfterSort = make([]*SysMenu, 0)
	sort.Ints(menuSort)
	for _, key := range menuSort {
		menuListAfterSort = append(menuListAfterSort, menuMapList[key])
	}

	var refer = make(map[int64]*MenuTreeModel)
	for _, value := range menuList {
		refer[value.Id] = formatMenu(value)
	}
	var tree = make([]*MenuTreeModel, 0)
	for _, value := range menuListAfterSort {
		parentId := value.ParentId
		if parentId == rootId {
			tree = append(tree, refer[value.Id])
		} else {
			if parent, ok := refer[parentId]; ok {
				parent = refer[parentId]
				parent.Children = append(parent.Children, refer[value.Id])
			}
		}
	}
	return tree
}

func BuildSelectTree(menuList []*SysMenu, rootId int64) []*common.SelectTree {
	var result = make([]*common.SelectTree, 0)

	var menuSort = make([]int, 0)
	var menuMapList = make(map[int]*SysMenu)
	for _, menu := range menuList {
		key := menu.OrderNum*100 + int(menu.ParentId*10+menu.Id) // (1000-menu.OrderNum)*10000 + menu.MenuId
		menuSort = append(menuSort, key)
		menuMapList[key] = menu
	}
	var menuListAfterSort = make([]*SysMenu, 0)
	sort.Ints(menuSort)
	for _, key := range menuSort {
		menuListAfterSort = append(menuListAfterSort, menuMapList[key])
	}

	var refer = make(map[int64]*common.SelectTree)
	for _, value := range menuList {
		refer[value.Id] = value.FormatSelectTree()
	}

	for _, value := range menuListAfterSort {
		parentId := value.ParentId
		if parentId == rootId {
			result = append(result, refer[value.Id])
		} else {
			if parent, ok := refer[parentId]; ok {
				parent = refer[parentId]
				parent.Children = append(parent.Children, refer[value.Id])
			}
		}
	}

	return result
}

func formatMenu(menu *SysMenu) *MenuTreeModel {
	meta := &MenuMeta{
		Title:   menu.MenuName,
		Icon:    menu.Icon,
		NoCache: menu.IsCache == common.StatusEnable,
	}
	if common.IsHttp(menu.Path) {
		meta.Link = menu.Path
	}

	return &MenuTreeModel{
		Id:         menu.Id,
		Name:       menu.MenuName,
		NameEn:     menu.MenuNameEn,
		Path:       menu.GetRouterPath(),
		Hidden:     menu.Visible == common.StatusDisable,
		Redirect:   "",
		Component:  menu.GetComponent(),
		Query:      menu.Query,
		AlwaysShow: false,
		Meta:       meta,
	}
}

type Menu struct {
	log *logrus.Entry
}

func NewMenu() *Menu {
	return &Menu{
		log: logrus.WithField("module", "MenuController"),
	}
}

func (ctrl *Menu) List(c *gin.Context) {
	var form MenuListForm
	if err := shouldBind(c, &form, map[string]string{}); err != nil {
		common.GinError(c, common.CommonParamError, err.Error())
		return
	}

	adminUserId := c.GetInt64(AdminUserIdKey)
	form.UserId = adminUserId

	menuList := SysMenuList(&form)

	common.GinSuccess(c, menuList)
}

func (ctrl *Menu) Add(c *gin.Context) {
	//绑定参数
	var form validate.AddMenuForm
	var formError = validate.AddMenuFormError()
	if errData := shouldBind(c, &form, formError); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	oldMenu := database.ReadOne(SysMenu{ParentId: form.ParentId, MenuName: form.MenuName}, "parent_id", "menu_name")

	if oldMenu != nil {
		common.GinError(c, AdminMenuAddFailure, "菜单名称已存在")
		return
	}

	var saveData = &SysMenu{
		MenuName:   form.MenuName,
		MenuNameEn: form.MenuNameEn,
		ParentId:   form.ParentId,
		OrderNum:   form.OrderNum,
		Path:       form.Path,
		Component:  form.Component,
		Query:      form.Query,
		RouteName:  form.RouteName,
		IsFrame:    form.IsFrame,
		IsCache:    form.IsCache,
		MenuType:   form.MenuType,
		Visible:    form.Visible,
		Status:     form.Status,
		Perms:      form.Perms,
		Icon:       form.Icon,
	}

	saveData.CreateBy = c.GetString(AdminUsernameKey)

	err := database.Insert(nil, saveData)
	if err != nil {
		common.GinError(c, AdminMenuAddFailure, err.Error())
		return
	}
	common.GinSuccess(c, saveData)

}

func (ctrl *Menu) Edit(c *gin.Context) {
	var form validate.EditMenuForm
	var formError = validate.EditMenuFormError()
	if errData := shouldBind(c, &form, formError); errData != nil {
		common.GinError(c, errData.Code(), errData.Error())
		return
	}

	one := database.FindOne[SysMenu](form.MenuId)
	if one == nil {
		common.GinError(c, common.CommonDataNotExist, "菜单不存在")
		return
	}

	var saveData = SysMenu{
		Id:         form.MenuId,
		MenuName:   form.MenuName,
		MenuNameEn: form.MenuNameEn,
		ParentId:   form.ParentId,
		OrderNum:   form.OrderNum,
		Path:       form.Path,
		Component:  form.Component,
		Query:      form.Query,
		RouteName:  form.RouteName,
		IsFrame:    form.IsFrame,
		IsCache:    form.IsCache,
		MenuType:   form.MenuType,
		Visible:    form.Visible,
		Status:     form.Status,
		Perms:      form.Perms,
		Icon:       form.Icon,
	}

	saveData.UpdateBy = c.GetString(AdminUsernameKey)

	err := database.UpdateModel(nil, one, saveData)
	if err != nil {
		common.GinError(c, AdminMenuEditFailure, err.Error())
		return
	}
	common.GinSuccess(c, "")

}

func (ctrl *Menu) Del(c *gin.Context) {
	param := common.IdParam{}
	if err := shouldBindUri(c, &param, common.IdParamError()); err != nil {
		common.GinError(c, common.CommonParamError, err.Error())
		return
	}

	one := database.FindOne[SysMenu](param.Id)
	if one == nil {
		common.GinSuccess(c, "")
		return
	}

	count, _ := database.Count[SysMenu](orm.NewCondition().And("parent_id", param.Id))
	if count > 0 {
		common.GinError(c, AdminMenuDeleteFailure, "存在子菜单,不允许删除")
		return
	}

	roleMenus, _ := database.Count[SysRoleMenu](orm.NewCondition().And("menu_id", param.Id))
	if roleMenus > 0 {
		common.GinError(c, AdminMenuDeleteFailure, "菜单已分配,不允许删除")
		return
	}

	if err := database.Delete(nil, one); err != nil {
		common.GinError(c, AdminMenuDeleteFailure, err.Error())
		return
	}
	common.GinSuccess(c, "")

}

func (ctrl *Menu) Info(c *gin.Context) {

	param := common.IdParam{}
	if err := shouldBindUri(c, &param, common.IdParamError()); err != nil {
		common.GinError(c, common.CommonParamError, err.Error())
		return
	}

	menu := database.FindOne[SysMenu](param.Id)
	common.GinSuccess(c, menu)
	return
}

func (ctrl *Menu) TreeSelect(c *gin.Context) {
	adminUserId := c.Value(AdminUserIdKey).(int64)
	menuList := SysMenuList(&MenuListForm{
		UserId: adminUserId,
	})
	menuTree := BuildSelectTree(menuList, 0)

	common.GinSuccess(c, menuTree)
}

func (ctrl *Menu) RoleMenuTreeSelect(c *gin.Context) {
	param := common.IdParam{}
	if err := shouldBindUri(c, &param, common.IdParamError()); err != nil {
		common.GinError(c, common.CommonParamError, err.Error())
		return
	}

	cond1 := orm.NewCondition().And("role_id", param.Id)
	roleMenus, _ := database.FindList[SysRoleMenu](cond1)
	menuIdList := make([]int64, 0)
	for _, roleMenu := range roleMenus {
		menuIdList = append(menuIdList, roleMenu.MenuId)
	}

	adminUserId := c.GetInt64(AdminUserIdKey)
	menuList := SysMenuList(&MenuListForm{
		UserId: adminUserId,
		Status: common.StatusEnable,
	})
	menuTree := BuildMenuTree(menuList, 0)

	common.GinSuccess(c, map[string]interface{}{
		"checkedKeys": menuIdList,
		"menus":       menuTree,
	})
}

func SysMenuList(form *MenuListForm) []*SysMenu {
	cond := orm.NewCondition()
	if form.Status > 0 {
		cond = cond.And("status", form.Status)
	}

	if len(form.MenuName) > 0 {
		cond = cond.And("menu_name__contains", form.MenuName)
	}
	if form.MenuType != "" {
		split := strings.Split(form.MenuType, ",")
		if len(split) > 1 {
			cond = cond.And("menu_type__in", split)
		} else {
			cond = cond.And("menu_type", split[0])
		}
	}

	roleIdList := make([]int64, 0)
	if form.UserId > 0 && form.UserId != SuperAdminUserId {
		cond1 := orm.NewCondition().And("user_id", form.UserId)
		userRoles, err := database.FindList[SysUserRole](cond1)
		if err != nil || len(userRoles) < 1 {
			return []*SysMenu{}
		}

		for _, role := range userRoles {
			roleIdList = append(roleIdList, role.RoleId)
		}
	}
	if form.RoleId > 0 && form.RoleId != SuperAdminUserId {
		roleIdList = []int64{form.RoleId}
	}

	if len(roleIdList) > 0 {
		cond1 := orm.NewCondition().And("role_id__in", roleIdList)
		roleMenus, err := database.FindList[SysRoleMenu](cond1)
		if err != nil || len(roleMenus) < 1 {
			return []*SysMenu{}
		}
		menuIdList := make([]int64, 0)
		for _, roleMenu := range roleMenus {
			menuIdList = append(menuIdList, roleMenu.MenuId)
		}
		if len(menuIdList) < 1 {
			return []*SysMenu{}
		}
		cond = cond.And("id__in", menuIdList)
	}

	if len(form.MenuId) > 0 {
		cond = cond.And("id__in", form.MenuId)
	}

	data, err := database.FindList[SysMenu](cond)
	if err != nil {
		logrus.Errorf("list menu error: %v", err)
		return []*SysMenu{}
	}

	return data
}

func AccessByUserId(userId int64) map[string]int {

	if userId == SuperAdminUserId {
		return map[string]int{
			"*:*:*": common.StatusEnable,
		}
	}

	menuList := SysMenuList(&MenuListForm{
		UserId: userId,
	})

	result := make(map[string]int)
	for _, menu := range menuList {
		if len(menu.Perms) < 1 {
			continue
		}
		result[menu.Perms] = common.StatusEnable
	}
	return result
}
