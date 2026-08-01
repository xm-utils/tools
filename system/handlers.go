package system

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/beego/beego/v2/client/orm"
	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/beego/database"
	"github.com/xm-utils/tools/common"
	"github.com/xm-utils/tools/redis"
	captcha2 "github.com/xm-utils/tools/system/captcha"
	"github.com/xm-utils/tools/system/validate"
)

type AdminUser struct {
	Token       string       `json:"token"`
	UserId      int64        `json:"user_id,omitempty"`     //用户ID
	UserName    string       `json:"user_name,omitempty"`   //姓名
	UserPhone   string       `json:"user_phone,omitempty"`  //手机号
	UserEmail   string       `json:"user_email,omitempty"`  //邮箱
	NickName    string       `json:"nick_name,omitempty"`   //昵称
	Avatar      string       `json:"avatar,omitempty"`      //头像
	Sex         int32        `json:"sex,omitempty"`         //性别
	Roles       []*AdminRole `json:"roles,omitempty"`       //角色ID列表
	Permissions []string     `json:"permissions,omitempty"` //权限字符列表
}

type AdminRole struct {
	RoleId   int64  `json:"role_id,omitempty"`   //角色ID
	RoleName string `json:"role_name,omitempty"` //角色名称
}

type UserInfoResp struct {
	Permissions []string   `json:"permissions,omitempty"`
	Roles       []string   `json:"roles,omitempty"`
	User        *AdminUser `json:"user,omitempty"`
}

func SignIn(c *gin.Context) {
	//绑定参数
	var signInform validate.SignInForm
	var formError = validate.SignInFormError()

	if errData := shouldBind(c, &signInform, formError); errData != nil {
		common.GinError(c, errData.Code(), errData.Error())
		return
	}

	if !captcha2.VerifyCode(signInform.Uuid, signInform.CheckCode) {
		logrus.WithFields(logrus.Fields{
			"module":    "login",
			"captchaId": signInform.Uuid,
			"code":      signInform.CheckCode,
		}).Infof("captcha code check error")
		common.GinError(c, AdminUserCheckCodeError)
		return
	}

	ip := common.GetClientIP(c)
	adminUser, err := LoginHandler(signInform.Username, signInform.Password, signInform.Uuid, ip)
	if err != nil {
		common.GinError(c, err.Code(), err.Error())
		return
	}

	c.Set(AdminUsernameKey, adminUser.UserName)
	c.Set(AdminUserIdKey, adminUser.UserId)

	common.GinSuccess(c, adminUser)
	return
}

func SignOut(c *gin.Context) {

	token := c.GetHeader(common.GinHeaderTokenKey)

	_ = redis.Delete(context.Background(), token)
	_ = redis.HDel(context.Background(), AdminUserAccessCacheKey, token)

	common.GinSuccess(c, "")

}

func GetUserAccess(c *gin.Context) {

	adminUserId := c.GetInt64(AdminUserIdKey)

	adminUserModel := database.FindOne[SysUser](adminUserId)
	if adminUserModel == nil {
		common.GinError(c, AdminUserNoExist, "")
		return
	}

	accessMap := make(map[string]int)

	common.GinSuccess(c, accessMap)
}
func FindUserPermissions(c *gin.Context) {

	adminUserId := c.GetInt64(AdminUserIdKey)

	adminUserModel := database.FindOne[SysUser](adminUserId)
	if adminUserModel == nil {
		common.GinError(c, AdminUserNoExist, "")
		return
	}
	access := AccessByUserId(adminUserId)
	var perms = make([]string, 0)
	for k, _ := range access {
		perms = append(perms, k)
	}

	common.GinSuccess(c, perms)
}

func LoginUserInfo(c *gin.Context) {
	result := &UserInfoResp{}

	adminUserId := c.GetInt64(AdminUserIdKey)

	loginInfo, err := redis.Get[*AdminUser](c.Request.Context(), fmt.Sprintf("login:user:%d", adminUserId))
	if err != nil {
		c.String(http.StatusUnauthorized, "身份信息已失效，请重新登录")
		return
	}
	result.User = loginInfo
	roles := loginInfo.Roles

	if len(roles) > 0 {
		roleIdList := make([]int64, len(roles))
		for i, role := range roles {
			roleIdList[i] = role.RoleId
		}
		cond := orm.NewCondition().And("id__in", roleIdList)
		roleList, _ := database.FindList[SysRole](cond)
		if len(roleList) > 0 {
			roleKeys := make([]string, len(roleList))
			for i, role := range roleList {
				roleKeys[i] = role.RoleKey
			}
			result.Roles = roleKeys
		}
	}

	var perms = make([]string, 0)
	access := AccessByUserId(adminUserId)
	for k, _ := range access {
		perms = append(perms, k)
	}
	result.Permissions = perms

	//查询用户所属于分组
	common.GinSuccess(c, result)
}

func MenuList(c *gin.Context) {
	adminUserId := c.GetInt64(AdminUserIdKey)
	adminUserTreeList := make([]*MenuTreeModel, 0)

	menuList := SysMenuList(&MenuListForm{
		Status: common.StatusEnable,
		UserId: adminUserId,
	})

	if len(menuList) < 1 {
		common.GinSuccess(c, adminUserTreeList)
		return
	}

	common.GinSuccess(c, BuildMenuTree(menuList, 0))
}

func Captcha(c *gin.Context) {
	var captchaCode []byte
	var captchaIdString string
	var err error
	captchaIdString, captchaCode, err = captcha2.NewCaptchaCode(4)
	if nil != err {
		common.GinError(c, common.CommonCaptchaCreateError, err.Error())
		return
	}

	logrus.WithFields(logrus.Fields{
		"module":      "general/getCaptcha",
		"captchaId":   captchaIdString,
		"captchaCode": captchaCode,
	}).Infof("验证码生成")

	var content bytes.Buffer
	captcha2.NewImage(captchaIdString, captchaCode, 180, 80).WriteTo(&content)
	encodedString := base64.StdEncoding.EncodeToString(content.Bytes())

	common.GinSuccess(c, map[string]interface{}{
		"captchaEnabled": true,
		"uuid":           captchaIdString,
		"img":            encodedString,
	})
}

func shouldBind(ctx *gin.Context, form interface{}, formError map[string]string) common.CustomError {

	if err := ctx.ShouldBind(form); err != nil {
		return bindError(err, formError)
	}

	return nil
}
func shouldBindUri(ctx *gin.Context, form interface{}, formError map[string]string) common.CustomError {
	if err := ctx.ShouldBindUri(form); err != nil {
		return bindError(err, formError)
	}

	return nil
}

func bindError(err error, formError map[string]string) common.CustomError {
	var errInfo validator.ValidationErrors
	var errMessage common.CustomError
	errors.As(err, &errInfo)
	for _, info := range errInfo {
		errTag := info.Field() + "." + info.Tag()
		logrus.Error(formError[errTag])
		errMessage = common.NewError(common.CommonParamError, formError[errTag])
		break
	}
	return errMessage
}

func LoginHandler(userName, password, uuid, ip string) (*AdminUser, common.CustomError) {

	//查询用户，错误返回
	adminUserModel := database.ReadOne(SysUser{UserName: userName}, "user_name")
	if adminUserModel == nil {
		return nil, common.NewError(AdminUserNoExist)
	}

	//验证密码
	if err := common.CheckPassword(password, adminUserModel.Password); err != nil {
		return nil, common.NewError(AdminUserPasswordError)
	}

	//验证状态
	if adminUserModel.Status != common.StatusEnable && adminUserModel.Id != SuperAdminUserId {
		return nil, common.NewError(AdminUserNoAllowLogin)
	}

	cond1 := orm.NewCondition().And("user_id", adminUserModel.Id)
	userRoles, _ := database.FindList[SysUserRole](cond1)
	roleIdList := make([]int64, len(userRoles))
	for i, role := range userRoles {
		roleIdList[i] = role.RoleId
	}

	if len(roleIdList) == 0 {
		return nil, common.NewError(AdminUserNoAllowLogin, "未分配权限，限制登陆")

	}

	token, err := common.GenerateAccessToken(adminUserModel.Id, adminUserModel.UserName, 2, "", ip)
	if err != nil {
		return nil, common.NewError(AdminUserNoAllowLogin)
	}

	//获取用户拥有的权限
	accessMap := AccessByUserId(adminUserModel.Id)
	redis.HSet(context.Background(), AdminUserAccessCacheKey, token, accessMap)

	adminLoginInfo := &AdminUser{
		Token:     token,
		UserId:    adminUserModel.Id,
		UserName:  adminUserModel.UserName,
		NickName:  adminUserModel.NickName,
		Avatar:    adminUserModel.Avatar,
		UserPhone: adminUserModel.PhoneNumber,
		UserEmail: adminUserModel.Email,
		Sex:       int32(adminUserModel.Sex),
		Roles:     make([]*AdminRole, 0),
	}

	roles, _ := database.FindList[SysRole](orm.NewCondition().And("id__in", roleIdList))
	if len(roles) > 0 {
		for _, role := range roles {
			adminLoginInfo.Roles = append(adminLoginInfo.Roles, &AdminRole{
				RoleId:   role.Id,
				RoleName: role.RoleName,
			})
		}
	}

	if err1 := redis.Set(context.Background(), fmt.Sprintf("login:user:%d", adminUserModel.Id), adminLoginInfo, AdminUserLoginCacheExpiration); err1 != nil {
		return nil, common.NewError(common.CommonSystemError, "设置缓存失败")
	}

	//修改用户登陆信息
	adminUserModel.LoginIp = ip
	adminUserModel.LoginDate = time.Now()

	if updateErr := database.Update(nil, adminUserModel, "login_ip", "login_date"); updateErr != nil {
		logrus.Errorf("修改用户登陆信息出错%v", updateErr)
	}

	return adminLoginInfo, nil

}
