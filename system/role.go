package system

import (
	"github.com/beego/beego/v2/client/orm"
	"github.com/gin-gonic/gin"
	"github.com/xm-utils/tools/beego/database"
	"github.com/xm-utils/tools/common"
	"github.com/xm-utils/tools/system/validate"
)

type SysRole struct {
	BaseModel
	Id                int64  `orm:"pk;auto" json:"id" comment:""`
	RoleName          string `json:"roleName,omitempty" comment:"角色名称"`
	RoleKey           string `json:"roleKey,omitempty" comment:"角色权限"`
	RoleSort          int    `json:"roleSort,omitempty" comment:"角色排序"`
	DataScope         int    `json:"dataScope,omitempty" comment:"数据范围"`
	MenuCheckStrictly int    `json:"menuCheckStrictly,omitempty" comment:"菜单树选择项是否关联显示（ 0：父子不互相关联显示 1：父子互相关联显示）"`
	DeptCheckStrictly int    `json:"deptCheckStrictly,omitempty" comment:"部门树选择项是否关联显示 0：父子不互相关联显示 1：父子互相关联显示"`
	Status            int    `json:"status,omitempty" comment:"角色状态 0正常 1停用"`
	DelFlag           int    `json:"delFlag,omitempty" comment:"删除标志 0代表存在 2代表删除"`
	Flag              bool   `orm:"-" json:"flag,omitempty" comment:"用户是否存在此角色标识 默认不存在"`
}

func (m *SysRole) TableName() string {
	return RoleTableName
}

type SysRoleMenu struct {
	Id     int64 `json:"id"  comment:"主键"`
	MenuId int64 ` json:"menuId"  comment:"菜单ID"`
	RoleId int64 ` json:"roleId"  comment:"角色ID"`
}

func (model *SysRoleMenu) TableName() string {
	return RoleMenuTableName
}

type SysUserRole struct {
	Id     int64 `json:"id"  comment:"主键"`
	UserId int64 `json:"userId,omitempty" comment:""`
	RoleId int64 `json:"roleId,omitempty"  comment:"角色ID"`
}

func (m *SysUserRole) TableName() string {
	return RoleUserTableName
}

type SysRoleController struct {
}

func (ctrl *SysRoleController) PageList(c *gin.Context) {

	var form validate.RoleListParam
	_ = shouldBind(c, &form, map[string]string{})

	cond := orm.NewCondition()

	if form.Status > 0 {
		cond = cond.And("status", form.Status)
	}
	if len(form.RoleName) > 0 {
		cond = cond.And("role_name__contains", form.RoleName)
	}
	cond = cond.And("del_flag", common.StatusEnable)
	list, total, err := database.FindAll[SysRole](database.ListParam{
		Param: cond,
		Page:  form.PageParam,
	})
	if err != nil {
		common.GinError(c, common.CommonSystemError, err.Error())
		return
	}
	res := common.PageResponse{
		TotalCount: total,
		List:       list,
	}
	common.GinSuccess(c, res)
}

func (ctrl *SysRoleController) SelectList(c *gin.Context) {

	cond := orm.NewCondition().And("status", common.StatusEnable)
	list, err := database.FindList[SysRole](cond)
	if err != nil {
		common.GinError(c, common.CommonSystemError, err.Error())
		return
	}

	res := make([]common.SelectOption[int64], len(list))
	for i, role := range list {
		res[i] = common.SelectOption[int64]{
			Type:     role.Id,
			TypeName: role.RoleName,
		}
	}

	common.GinSuccess(c, res)
}

func (ctrl *SysRoleController) Add(c *gin.Context) {
	var form validate.RoleAddParam
	var formError = validate.RoleAddParamError()

	if errData := shouldBind(c, &form, formError); errData != nil {
		common.GinError(c, errData.Code(), errData.Error())
		return
	}

	role := &SysRole{
		BaseModel: BaseModel{
			Remark: form.Remark,
		},
		RoleName:          form.RoleName,
		RoleKey:           form.RoleKey,
		RoleSort:          form.RoleSort,
		DataScope:         form.DataScope,
		MenuCheckStrictly: form.MenuCheckStrictly,
		DeptCheckStrictly: form.DeptCheckStrictly,
		Status:            common.StatusEnable,
		DelFlag:           common.StatusEnable,
	}
	role.CreateBy = c.GetString(AdminUsernameKey)

	err := database.NewOrmTx().Execute(func(o orm.TxOrmer) error {
		roleId, err := o.Insert(role)
		if err != nil {
			return err
		}
		if form.MenuIds != nil && len(form.MenuIds) > 0 {
			roleMenu := make([]*SysRoleMenu, len(form.MenuIds))
			for i, menuId := range form.MenuIds {
				roleMenu[i] = &SysRoleMenu{
					RoleId: roleId,
					MenuId: menuId,
				}
			}
			_, err := o.InsertMulti(len(roleMenu), roleMenu)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		common.GinError(c, AdminGroupAddFailure, err.Error())
		return
	}
	common.GinSuccess(c, "")
}

func (ctrl *SysRoleController) Edit(c *gin.Context) {
	var form validate.RoleEditParam
	if errData := shouldBind(c, &form, validate.RoleEditParamError()); errData != nil {
		common.GinError(c, errData.Code(), errData.Error())
		return
	}

	old := database.FindOne[SysRole](form.RoleId)
	if old == nil {
		common.GinError(c, common.CommonDataNotExist, "角色不存在")
		return
	}

	role := SysRole{
		BaseModel: BaseModel{
			Remark: form.Remark,
		},
		Id:                form.RoleId,
		RoleName:          form.RoleName,
		RoleKey:           form.RoleKey,
		RoleSort:          form.RoleSort,
		DataScope:         form.DataScope,
		MenuCheckStrictly: form.MenuCheckStrictly,
		DeptCheckStrictly: form.DeptCheckStrictly,
		Status:            form.Status,
	}
	role.UpdateBy = c.GetString(AdminUsernameKey)

	err := database.NewOrmTx().Execute(func(o orm.TxOrmer) error {
		if err := database.UpdateModel(o, old, role); err != nil {
			return err
		}

		if form.MenuIds != nil && len(form.MenuIds) > 0 {
			if _, err := o.Delete(&SysRoleMenu{RoleId: form.RoleId}, "role_id"); err != nil {
				return err
			}

			roleMenu := make([]*SysRoleMenu, len(form.MenuIds))
			for i, menuId := range form.MenuIds {
				roleMenu[i] = &SysRoleMenu{
					RoleId: role.Id,
					MenuId: menuId,
				}
			}
			if _, err := o.InsertMulti(len(roleMenu), roleMenu); err != nil {
				return err
			}
		}
		return nil
	})

	if err == nil {
		common.GinSuccess(c, "")
		return
	}

	common.GinError(c, AdminGroupEditFailure, err.Error())

}

func (ctrl *SysRoleController) Info(c *gin.Context) {
	var param common.IdParam
	if err := shouldBindUri(c, &param, common.IdParamError()); err != nil {
		common.GinError(c, common.CommonParamError, err.Error())
		return
	}

	common.GinSuccess(c, database.FindOne[SysRole](param.Id))
}

func (ctrl *SysRoleController) Del(c *gin.Context) {
	var param common.IdParam
	if err := shouldBindUri(c, &param, common.IdParamError()); err != nil {
		common.GinError(c, common.CommonParamError, err.Error())
		return
	}

	err := database.NewOrmTx().Execute(func(o orm.TxOrmer) error {
		if err := database.DeleteByCondition[SysRole](o, orm.NewCondition().And("id__in", param.Id)); err != nil {
			return err
		}
		if err := database.DeleteByCondition[SysRoleMenu](o, orm.NewCondition().And("role_id__in", param.Id)); err != nil {
			return err
		}
		if err := database.DeleteByCondition[SysUserRole](o, orm.NewCondition().And("role_id__in", param.Id)); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		common.GinError(c, AdminGroupDeleteFailure, err.Error())
		return
	}
	common.GinSuccess(c, "")

}

func (ctrl *SysRoleController) ChangeStatus(c *gin.Context) {

	var form validate.RoleChangeStatusParam
	var formError = validate.RoleChangeStatusParamError()

	if errData := shouldBind(c, &form, formError); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	role := &SysRole{
		Id:     form.RoleId,
		Status: form.Status,
	}
	role.UpdateBy = c.GetString(AdminUsernameKey)

	err := database.Update(nil, role, "status", "update_by")
	if err == nil {
		common.GinSuccess(c, "")
		return
	}

	common.GinError(c, AdminGroupUpdateFailure, err.Error())
}

func (ctrl *SysRoleController) AllocatedList(c *gin.Context) {

	var form validate.RoleUserListParam
	var formError = validate.RoleChangeStatusParamError()

	if errData := shouldBind(c, &form, formError); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}
	if form.RoleId == 0 {
		common.GinError(c, common.CommonParamError, "")
		return
	}

	list, total, err := UserPageList(&UserListParam{
		PageParam:   form.PageParam,
		RoleId:      form.RoleId,
		UserName:    form.UserName,
		PhoneNumber: form.Phone,
		Status:      common.StatusEnable,
	})
	if err != nil {
		common.GinError(c, common.CommonSystemError, err.Error())
		return
	}
	res := common.PageResponse{
		TotalCount: total,
		List:       list,
	}
	common.GinSuccess(c, res)
}

func (ctrl *SysRoleController) UnAllocatedList(c *gin.Context) {
	var form validate.RoleUserListParam
	var formError = validate.RoleChangeStatusParamError()

	if errData := shouldBind(c, &form, formError); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}
	if form.RoleId == 0 {
		common.GinError(c, common.CommonParamError, "")
		return
	}
	cond := orm.NewCondition()
	userRoles, err := database.FindList[SysUserRole](orm.NewCondition().And("role_id", form.RoleId))
	if err == nil && len(userRoles) > 0 {
		userIdList := make([]int64, len(userRoles))
		for i, role := range userRoles {
			userIdList[i] = role.UserId
		}
		cond = cond.AndNot("id__in", userIdList)
	}

	if len(form.UserName) > 0 {
		cond = cond.And("user_name", form.UserName)
	}
	if len(form.Phone) > 0 {
		cond = cond.And("phone_number", form.Phone)
	}

	cond = cond.And("status", common.StatusEnable)

	list, total, err := database.FindAll[SysUser](database.ListParam{
		Param: cond,
		Page:  form.PageParam,
	})

	if err != nil {
		common.GinError(c, common.CommonSystemError, err.Error())
		return
	}

	res := common.PageResponse{
		TotalCount: total,
		List:       list,
	}
	common.GinSuccess(c, res)
}

func (ctrl *SysRoleController) RemoveUser(c *gin.Context) {
	var form validate.RoleCancelParam
	var formError = validate.RoleCancelParamError()

	if errData := shouldBind(c, &form, formError); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	cond := orm.NewCondition().And("role_id", form.RoleId).And("user_id__in", form.UserId)
	err := database.DeleteByCondition[SysUserRole](nil, cond)

	if err != nil {
		common.GinError(c, AdminGroupUpdateFailure, err.Error())
		return
	}
	common.GinSuccess(c, "")
}

func (ctrl *SysRoleController) SelectAll(c *gin.Context) {
	var form validate.RoleCancelParam
	var formError = validate.RoleCancelParamError()
	if errData := shouldBind(c, &form, formError); errData != nil {
		common.GinError(c, errData.Code(), errData.Error())
		return
	}

	list := make([]*SysUserRole, len(form.UserId))
	for i, id := range form.UserId {
		list[i] = &SysUserRole{
			UserId: id,
			RoleId: form.RoleId,
		}
	}
	_, err := database.InsertBatch(nil, len(list), list)

	if err != nil {
		common.GinError(c, AdminGroupUpdateFailure, err.Error())
		return
	}
	common.GinSuccess(c, "")
}
