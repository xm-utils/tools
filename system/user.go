package system

import (
	"slices"
	"time"

	"github.com/beego/beego/v2/client/orm"
	"github.com/beego/beego/v2/client/orm/clauses/order_clause"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/beego/database"
	"github.com/xm-utils/tools/common"
	"github.com/xm-utils/tools/system/validate"
)

type SysUser struct {
	BaseModel
	Id          int64     `orm:"pk;auto" json:"id" comment:""`
	UserName    string    `json:"userName,omitempty" comment:"用户账号"`
	NickName    string    `json:"nickName,omitempty" comment:"用户昵称"`
	Email       string    `json:"email,omitempty" comment:"用户邮箱"`
	PhoneNumber string    `json:"phoneNumber,omitempty" comment:"手机号码"`
	Sex         int8      `json:"sex,omitempty" comment:"用户性别"`
	Avatar      string    `json:"avatar,omitempty" comment:"用户头像"`
	Password    string    `json:"password,omitempty" comment:"密码"`
	Status      int8      `json:"status,omitempty" comment:" 帐号状态（1正常 2停用）"`
	DelFlag     int       `json:"delFlag,omitempty" comment:"删除标志（1代表存在 3代表删除）"`
	LoginIp     string    `json:"loginIp,omitempty" comment:"最后登录IP"`
	LoginDate   time.Time `json:"loginDate" comment:"最后登录时间"`
}

func (m *SysUser) TableName() string {
	return UserTableName
}

type UserListParam struct {
	*common.PageParam
	*common.TimeParam
	RoleId      int64   `json:"roleId,omitempty" form:"roleId"`
	UserName    string  `json:"userName,omitempty" form:"userName"`
	NickName    string  `json:"nickName" form:"nickName"`
	PhoneNumber string  `json:"phoneNumber,omitempty" form:"phoneNumber"`
	Status      int8    `json:"status" form:"status"`
	UserIdArr   []int64 `json:"userIdArr,omitempty" form:"userIdArr"`
}

func UserPageList(param *UserListParam) ([]*SysUser, int64, error) {
	cond := orm.NewCondition()
	if param.UserIdArr != nil && len(param.UserIdArr) > 0 {
		cond = cond.And("id__in", param.UserIdArr)
	}
	if len(param.UserName) > 0 {
		cond = cond.And("user_name", param.UserName)
	}
	if len(param.PhoneNumber) > 0 {
		cond = cond.And("phone_number", param.PhoneNumber)
	}
	if param.NickName != "" {
		cond = cond.And("nick_name", param.NickName)
	}
	if param.Status > 0 {
		cond = cond.And("status", param.Status)
	}
	if param.TimeParam != nil && param.TimeParam.IsValid() {
		param.TimeParam.Column = "create_time"
	}

	if param.RoleId > 0 {
		roleUsers, _ := database.FindList[SysUserRole](orm.NewCondition().And("role_id", param.RoleId))
		if len(roleUsers) > 0 {
			userIdList := make([]int64, len(roleUsers))
			for i, userRole := range roleUsers {
				userIdList[i] = userRole.UserId
			}
			cond = cond.And("id__in", userIdList)
		} else {
			return []*SysUser{}, 0, nil
		}
	}

	return database.FindAll[SysUser](database.ListParam{
		Param: cond,
		Page:  param.PageParam,
		Time:  param.TimeParam,
		Order: order_clause.ParseOrder("-id"),
	})
}

type User struct {
	log *logrus.Entry
}

func NewUser() *User {
	return &User{
		log: logrus.WithField("module", "UserController"),
	}
}

func (ctrl *User) UserInfo(c *gin.Context) {

	var form common.IdParam
	if errData := shouldBindUri(c, &form, common.IdParamError()); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	// 用户是否存在检测
	userInfo := database.FindOne[SysUser](form.Id)
	if userInfo == nil {
		common.GinError(c, AdminUserNoExist, "")
		return
	}
	userInfo.Password = ""
	//查询用户所属于分组
	common.GinSuccess(c, userInfo)
}

func (ctrl *User) List(c *gin.Context) {
	var form UserListParam
	//_ = shouldBind(c, &form, map[string]string{})
	if errData := shouldBind(c, &form, validate.ListAdminGroupFormError()); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	list, total, err := UserPageList(&form)
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

func (ctrl *User) Add(c *gin.Context) {
	var form validate.AddUserParam
	var formError = validate.AddUserParamError()
	if errData := shouldBind(c, &form, formError); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	oldUser := database.ReadOne(SysUser{UserName: form.UserName}, "user_name")
	if oldUser != nil {
		common.GinError(c, AdminUsernameIsExist)
		return
	}

	userModel := &SysUser{
		BaseModel: BaseModel{
			Remark:   form.Remark,
			CreateBy: c.GetString(AdminUsernameKey),
		},
		UserName:    form.UserName,
		NickName:    form.NickName,
		Email:       form.Email,
		PhoneNumber: form.Phone,
		Sex:         form.Sex,
		Password:    form.Password,
		Status:      form.Status,
		DelFlag:     common.StatusEnable,
	}

	err := database.NewOrmTx().Execute(func(o orm.TxOrmer) error {
		insert, err := o.Insert(userModel)
		if err != nil {
			return err
		}
		if form.RoleIds != nil && len(form.RoleIds) > 0 {
			userRoles := make([]*SysUserRole, len(form.RoleIds))
			for i, roleId := range form.RoleIds {
				userRoles[i] = &SysUserRole{
					RoleId: roleId,
					UserId: insert,
				}
			}
			_, err := database.InsertBatch(nil, len(userRoles), userRoles)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		common.GinError(c, AdminUserAddFailure, err.Error())
		return
	}
	common.GinSuccess(c, "")

}

func (ctrl *User) Edit(c *gin.Context) {

	var form validate.EditUserParam
	var formError = validate.EditUserParamError()
	if errData := shouldBind(c, &form, formError); errData != nil {
		common.GinError(c, errData.Code(), errData.Error())
		return
	}

	one := database.FindOne[SysUser](form.UserId)
	if one == nil {
		common.GinError(c, AdminUserNoExist, "")
		return
	}

	userModel := SysUser{
		BaseModel: BaseModel{
			Remark:   form.Remark,
			UpdateBy: c.GetString(AdminUsernameKey),
		},
		Id:          form.UserId,
		NickName:    form.NickName,
		Email:       form.Email,
		PhoneNumber: form.Phone,
		Sex:         form.Sex,
	}

	err := database.UpdateModel(nil, one, userModel)
	if err != nil {
		common.GinError(c, AdminUserAddFailure, err.Error())
		return
	}
	common.GinSuccess(c, "")

}

func (ctrl *User) Del(c *gin.Context) {

	var form common.IdParam
	if errData := shouldBindUri(c, &form, common.IdParamError()); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	one := database.FindOne[SysUser](form.Id)
	if one == nil {
		common.GinError(c, AdminUserNoExist, "")
		return
	}

	err := database.NewOrmTx().Execute(func(o orm.TxOrmer) error {
		if _, err := o.Delete(one); err != nil {
			return err
		}

		if err := database.DeleteByCondition[SysUserRole](o, orm.NewCondition().And("user_id", form.Id)); err != nil {
			return err
		}
		return nil
	})

	if err == nil {
		common.GinSuccess(c, "")
		return
	}
	common.GinError(c, AdminUserDeleteFailure, err.Error())

}

func (ctrl *User) Status(c *gin.Context) {

	var form validate.EditAdminUserStatusForm
	var formError = validate.EditAdminUserStatusFormError()
	if errData := shouldBind(c, &form, formError); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	sysUser := database.FindOne[SysUser](form.UserId)
	if sysUser == nil {
		common.GinError(c, AdminUserNoExist, "")
		return
	}

	if sysUser.Status == form.Status {
		common.GinSuccess(c, "")
		return
	}

	sysUser.Status = form.Status
	sysUser.UpdateBy = c.GetString(AdminUsernameKey)

	err := database.Update(nil, sysUser, "status", "update_by")
	if err != nil {
		common.GinError(c, AdminUserEditStatusFailure, err.Error())
		return
	}

	common.GinSuccess(c, "")
}

func (ctrl *User) EditPassword(c *gin.Context) {

}

func (ctrl *User) ResetPassword(c *gin.Context) {

	var form validate.ResetPasswordParam
	var formError = validate.ResetPasswordParamError()
	if errData := shouldBind(c, &form, formError); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	user := database.FindOne[SysUser](form.UserId)
	if user == nil {
		common.GinError(c, AdminUserNoExist, "")
		return
	}
	user.UpdateBy = c.GetString(AdminUsernameKey)
	pwd, err := common.HashPassword(form.Password)
	if err != nil {
		common.GinError(c, AdminUserAddFailure, err.Error())
		return
	}

	user.Password = pwd
	err = database.Update(nil, user, "password", "update_by")
	if err == nil {
		common.GinSuccess(c, "")
		return
	}

	common.GinError(c, AdminUserAddFailure, err.Error())
}

func (ctrl *User) AddAuthRole(c *gin.Context) {

	var form validate.SetGroupParam
	var formError = validate.SetGroupParamError()
	if errData := shouldBind(c, &form, formError); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	sysUser := database.FindOne[SysUser](form.UserId)
	if sysUser == nil {
		common.GinError(c, AdminUserNoExist, "")
		return
	}

	err := database.NewOrmTx().Execute(func(o orm.TxOrmer) error {
		delCond := orm.NewCondition().And("user_id", form.UserId)
		if err := database.DeleteByCondition[SysUserRole](o, delCond); err != nil {
			return err
		}

		roles := make([]*SysUserRole, len(form.RoleId))
		for i, id := range form.RoleId {
			role := &SysUserRole{UserId: form.UserId, RoleId: id}
			roles[i] = role
		}
		if _, err := database.InsertBatch(o, len(roles), roles); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		common.GinError(c, AdminUserSetGroupError, err.Error())
		return
	}

	common.GinSuccess(c, "")
}

func (ctrl *User) AuthRole(c *gin.Context) {
	var form common.IdParam
	if errData := shouldBindUri(c, &form, common.IdParamError()); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	// 用户是否存在检测
	userInfo := database.FindOne[SysUser](form.Id)
	if userInfo == nil {
		common.GinError(c, AdminUserNoExist, "")
		return
	}

	list, _ := database.FindList[SysUserRole](orm.NewCondition().And("user_id", form.Id))
	roleIdList := make([]int64, 0)
	for _, role := range list {
		roleIdList = append(roleIdList, role.RoleId)
	}

	roleList, _ := database.FindList[SysRole](orm.NewCondition().And("status", common.StatusEnable))
	if len(roleIdList) > 0 && len(roleList) > 0 {
		for _, role := range roleList {
			if slices.Contains(roleIdList, role.Id) {
				role.Flag = true
			}
		}
	}

	common.GinSuccess(c, struct {
		User  *SysUser   `json:"user"`
		Roles []*SysRole `json:"roles"`
	}{
		User:  userInfo,
		Roles: roleList,
	})
}
