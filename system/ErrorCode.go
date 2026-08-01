package system

import "github.com/xm-utils/tools/common"

func init() {
	for i, s := range ErrorMessage {
		common.ErrorMessage[i] = s
	}
}

// 错误码以20开头 模块id（第三位）+ 三位错误码
const SuccessCode = 200

// 管理员模块 201***
const (
	AdminUserCheckCodeError              = 201001 + iota //验证码错误
	AdminUsernameIsExist                                 //用户名已被使用
	AdminUserAddFailure                                  //注册失败
	AdminUserDeleteFailure                               //删除失败
	AdminUserKickOut                                     //账号被踢下线提示
	AdminUserLoginByOther                                //账号已被其他人登陆
	AdminUserNoExist                                     //账号不存在
	AdminUserPasswordError                               //密码错误
	AdminUserSetGroupError                               //设置分组失败
	AdminUserEditPasswordFailure                         //修改密码失败
	AdminUserEditStatusFailure                           //修改状态失败
	AdminUserGetMenuError                                //获取菜单错误
	AdminUserNoAllowLogin                                //管理员限制登陆
	AdminUserCreateTwoFactorFailure                      //创建二次验证失败
	AdminUserSaveTwoFactorFailure                        //保存二次验证失败
	AdminUserSwitchTwoFactorFailure                      //开关二次验证失败
	AdminUserTwoAuthFailure                              //二次验证错误
	AdminUserSetAccountPermissionFailure                 //没有此权限,请先设置该账号为不可见
	AdminUserForceSignOutFailure                         //强制下线
	AdminUserAccessLimit                                 //权限限制
	AdminUserAccessNoDesc                                //权限未添加说明
)

// 管理员菜单 2011**
const (
	AdminMenuAddFailure    = 201101 + iota //添加菜单失败
	AdminMenuEditFailure                   //编辑菜单失败
	AdminMenuDeleteFailure                 //删除菜单失败
	AdminMenuNoExist                       //菜单不存在
	AdminMenuInfoError                     //获取菜单详情失败
)

// 数据字典 2012**
const (
	AdminDictTypeAddFailure      = 201201 + iota //添加数据字典类型失败
	AdminDictTypeEditFailure                     //编辑数据字典类型失败
	AdminDictTypeDeleteFailure                   //删除数据字典类型失败
	AdminDictTypeExist                           //字典类型已存在
	AdminDictRefreshCacheFailure                 //删除数据字典类型失败
	AdminDictTypeNoExist                         //数据字典类型不存在
	AdminDictTypeInfoError                       //获取数据字典类型详情失败
	AdminDictDataAddFailure                      //添加字典数据失败
	AdminDictDataEditFailure                     //编辑字典数据失败
	AdminDictDataDeleteFailure                   //删除字典数据失败
	AdminDictDataNoExist                         //字典数据不存在
	AdminDictDataInfoError                       //获取字典数据详情失败
	AdminDictDataValueExist                      //字典数据值已存在

)

// 角色 2013**
const (
	AdminGroupAddFailure    = 201301 + iota //添加分组失败
	AdminGroupEditFailure                   //修改分组失败
	AdminGroupUpdateFailure                 //更新分组失败
	AdminGroupDetailFailure                 //获取详情失败
	AdminGroupDeleteFailure                 //获取详情失败
)

var ErrorMessage = map[int]string{
	SuccessCode: "操作成功",

	//管理员模块
	AdminUserCheckCodeError:              "验证码错误",
	AdminUsernameIsExist:                 "用户名已被使用",
	AdminUserAddFailure:                  "添加管理员失败",
	AdminUserDeleteFailure:               "管理员删除失败",
	AdminUserKickOut:                     "账号被踢下线提示",
	AdminUserLoginByOther:                "账号已被其他人登陆",
	AdminUserNoExist:                     "账号不存在",
	AdminUserPasswordError:               "密码错误",
	AdminUserSetGroupError:               "设置分组失败",
	AdminUserEditPasswordFailure:         "修改密码失败",
	AdminUserEditStatusFailure:           "修改状态失败",
	AdminUserGetMenuError:                "获取管理员菜单失败",
	AdminUserNoAllowLogin:                "该账号已被限制登陆",
	AdminUserCreateTwoFactorFailure:      "创建二次验证失败",
	AdminUserSaveTwoFactorFailure:        "保存二次验证失败",
	AdminUserSwitchTwoFactorFailure:      "开关二次验证失败",
	AdminUserTwoAuthFailure:              "二次验证错误",
	AdminUserSetAccountPermissionFailure: "没有此权限,请先设置该账号为不可见",
	AdminUserForceSignOutFailure:         "强制下线失败",
	AdminUserAccessLimit:                 "您没有权限操作，请联系admin管理员",
	AdminUserAccessNoDesc:                "权限未添加描述，请联系客服",

	//管理员菜单模块
	AdminMenuAddFailure:    "添加菜单失败",
	AdminMenuEditFailure:   "编辑菜单失败",
	AdminMenuDeleteFailure: "删除菜单失败",
	AdminMenuNoExist:       "菜单不存在",
	AdminMenuInfoError:     "获取菜单详情失败",

	// 角色
	AdminGroupAddFailure:    "添加分组失败",
	AdminGroupEditFailure:   "编辑分组失败",
	AdminGroupUpdateFailure: "更新分组状态失败",
	AdminGroupDetailFailure: "分组详情获取失败",
	AdminGroupDeleteFailure: "删除分组失败",

	//数据字典
	AdminDictTypeAddFailure:      "添加数据字典类型失败",
	AdminDictTypeEditFailure:     "编辑数据字典类型失败",
	AdminDictTypeDeleteFailure:   "删除数据字典类型失败",
	AdminDictRefreshCacheFailure: "数据字典刷新缓存失败",
	AdminDictTypeNoExist:         "数据字典类型不存在",
	AdminDictTypeInfoError:       "获取数据字典类型详情失败",
	AdminDictDataAddFailure:      "添加字典数据失败",
	AdminDictDataEditFailure:     "编辑字典数据失败",
	AdminDictDataDeleteFailure:   "删除字典数据失败",
	AdminDictDataNoExist:         "字典数据不存在",
	AdminDictDataInfoError:       "获取字典数据详情失败",
	AdminDictDataValueExist:      "字典数据值已存在",
}
