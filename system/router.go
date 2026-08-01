package system

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xm-utils/tools/common"
	"github.com/xm-utils/tools/redis"
)

func Init(e *gin.Engine) {
	system := e.Group("/system")
	initNoLoginRoute(system)
	InitRouter(system.Group("/v1", JWTAuth(), AccessAuth()))
}

func initNoLoginRoute(noLogin *gin.RouterGroup) {

	noLogin.POST("/signIn", SignIn)
	noLogin.GET("/general/getCaptcha", Captcha)
}

func InitRouter(g *gin.RouterGroup) {
	initUserRoute(g)
	initMenuRoute(g)
	initRoleRoute(g)
	initDictRoute(g)
}

func initUserRoute(v1 *gin.RouterGroup) {
	// 管理员模块

	//loginUser := controller.NewLoginUser()
	//v1.POST("/user/signOut", loginUser.SignOut)
	//v1.GET("/user/userInfo", loginUser.Info)
	//v1.GET("/user/access", loginUser.GetUserAccess)
	//v1.GET("/user/findUserPermissions", loginUser.FindUserPermissions)
	//v1.GET("/user/menuList", loginUser.MenuList)
	v1.POST("/user/signOut", SignOut)
	v1.GET("/user/userInfo", LoginUserInfo)
	v1.GET("/user/access", GetUserAccess)
	v1.GET("/user/findUserPermissions", FindUserPermissions)
	v1.GET("/user/menuList", MenuList)

	adminUser := NewUser()
	v1.GET("/user/list", adminUser.List)
	v1.POST("/user/add", adminUser.Add)
	v1.POST("/user/update", adminUser.Edit)
	v1.POST("/user/resetPwd", adminUser.ResetPassword)
	v1.POST("/user/changeStatus", adminUser.Status)
	v1.GET("/user/:id", adminUser.UserInfo)
	v1.DELETE("/user/:id", adminUser.Del)
	v1.GET("/user/authRole/:id", adminUser.AuthRole)
	v1.POST("/user/authRole", adminUser.AddAuthRole)
}
func initMenuRoute(v1 *gin.RouterGroup) {
	// 菜单模块
	adminMenu := NewMenu()
	v1.POST("/menu/add", adminMenu.Add)
	v1.POST("/menu/edit", adminMenu.Edit)
	v1.GET("/menu/info/:id", adminMenu.Info)
	v1.DELETE("/menu/delete/:id", adminMenu.Del)
	v1.GET("/menu/list", adminMenu.List)
	v1.GET("/menu/treeSelect", adminMenu.TreeSelect)
	v1.GET("/menu/roleMenuTreeselect/:id", adminMenu.RoleMenuTreeSelect)

}
func initRoleRoute(v1 *gin.RouterGroup) {
	//角色
	role := new(SysRoleController)
	v1.GET("/role/list", role.PageList)
	v1.GET("/role/selectList", role.SelectList)
	v1.POST("/role/add", role.Add)
	v1.POST("/role/edit", role.Edit)
	v1.POST("/role/status", role.ChangeStatus)
	v1.GET("/role/info/:id", role.Info)
	v1.DELETE("/role/delete/:id", role.Del)
	v1.GET("/role/authUser/allocatedList", role.AllocatedList)
	v1.GET("/role/authUser/unallocatedList", role.UnAllocatedList)
	v1.POST("/role/authUser/cancel", role.RemoveUser)
	v1.POST("/role/authUser", role.SelectAll)
}
func initDictRoute(v1 *gin.RouterGroup) {
	//数据字典
	dict := NewDict()
	v1.GET("/dict/type/list", dict.GetList)
	v1.GET("/dict/type/select", dict.SelectList)
	v1.GET("/dict/type/:id", dict.Info)
	v1.POST("/dict/type/add", dict.Add)
	v1.POST("/dict/type/update", dict.Edit)
	v1.DELETE("/dict/type/delete/:id", dict.Del)
	v1.DELETE("/dict/type/refreshCache", dict.RefreshCache)

	dictDetail := NewDictDetail()
	v1.GET("/dict/data/list", dictDetail.GetList)
	v1.GET("/dict/data/:id", dictDetail.Info)
	v1.GET("/dict/data/findByType", dictDetail.FindByType)
	v1.POST("/dict/data/add", dictDetail.Add)
	v1.POST("/dict/data/update", dictDetail.Edit)
	v1.DELETE("/dict/data/delete/:id", dictDetail.Del)
}

func JWTAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := common.GetTokenFormGinContext(c)
		if token == "" {
			c.String(http.StatusUnauthorized, "身份信息已失效，请重新登录")
			c.Abort()
			return
		}

		claims, err2 := common.ParseAccessToken(token)
		if err2 != nil {
			c.String(http.StatusUnauthorized, "身份信息已失效，请重新登录")
			c.Abort()
			return
		}

		adminUserLoginInfo, err := redis.Get[*AdminUser](c.Request.Context(), fmt.Sprintf("login:user:%d", claims.Uid))
		if err != nil {
			c.String(http.StatusUnauthorized, "身份信息已失效，请重新登录")
			c.Abort()
			return
		}

		if adminUserLoginInfo == nil {
			c.String(http.StatusUnauthorized, "身份信息已失效，请重新登录")
			c.Abort()
			return
		}

		_ = redis.ExpireIn(c.Request.Context(), token, AdminUserLoginCacheExpiration*time.Second)

		c.Set(AdminUsernameKey, adminUserLoginInfo.UserName)
		c.Set(AdminUserIdKey, adminUserLoginInfo.UserId)

		c.Next()
	}
}

func AccessAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		//如果是公司内部超级账号就跳过
		adminUsername := c.GetString(AdminUsernameKey)
		if adminUsername == SuperAdminUsername {
			c.Next()
			return
		}

		path := c.Request.URL.Path

		adminUserId := c.GetInt64(AdminUserIdKey)

		actionCode := path[len(Version)+2:]

		isNeedCheckAccess := false
		var actionDescription string

		if menu, err := redis.HGet[SysMenu](c.Request.Context(), AllAccessCodeCacheKey, actionCode); err == nil {
			actionDescription = menu.Remark
			isNeedCheckAccess = true
		}

		adminUserAccessCacheKey := AdminUserAccessCacheKey + adminUsername
		isExistUserAccessCache := redis.IsExist(c.Request.Context(), adminUserAccessCacheKey)
		if !isExistUserAccessCache {
			//获取用户拥有的权限
			accessMap := AccessByUserId(adminUserId)
			for routerKey, access := range accessMap {
				redis.HSet(c.Request.Context(), adminUserAccessCacheKey, routerKey, access)
			}
		}

		//行为描述，用户记录日志

		if accessCheck, err := redis.HGet[int](c.Request.Context(), adminUserAccessCacheKey, ActionIsNeedLog); nil == err {
			code := AdminUserAccessLimit
			if isNeedCheckAccess && accessCheck == common.StatusDisable {
				c.JSON(http.StatusOK, gin.H{"code": code, "message": ErrorMessage[code]})
				c.Abort()
				return
			}
		}

		c.Set(ActionIsNeedLog, common.StatusDisable)
		c.Set(ActionRouterKey, actionCode)
		c.Set(ActionDescriptionKey, actionDescription)
		c.Next()
	}
}
