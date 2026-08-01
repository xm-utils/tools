package system

const (
	SuperAdminUserId   = 1
	SuperAdminUsername = "system"
	HttpPath           = "web"
	NoRowFoundError    = "<QuerySeter> no row found"
)

const (
	AdminUserIdKey    = "adminUserId"
	AdminUsernameKey  = "adminUsername"
	UrgeUserHashCache = "hash:urge:user"
	Version           = "v1"
)

// 管理员登陆信息
const (
	AdminUserLoginCacheKey          = "hash:admin_user_login_info"
	AdminUserLoginCacheExpiration   = 3600 * 24
	AdminUserAccountPermissionCache = "admin_account_permission"
	AdminUserRoleKey                = "hash:admin_user_role_key"
	AdminUserAccessCacheKey         = "hash:admin_user_access_key:"
	AllAccessCodeCacheKey           = "hash:all_access_code_cache_key:"
)

const (
	SYS_DICT_CACHE_KEY        = "hash:sys_dict_cache"
	SYS_DICT_CACHE_EXPIRATION = 3600 * 24
	SYS_DICT_DETAIL_CACHE_KEY = "hash:sys_dict_detail_cache"
)
