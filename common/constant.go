package common

const (
	GinHeaderTokenKey        = "Authorization"
	GinHeaderRefreshTokenKey = "Refresh-Token"
)

const (
	StatusDelete  = 3
	StatusDisable = 2
	StatusEnable  = 1
)

var Status = map[int]string{
	StatusDelete:  "删除",
	StatusDisable: "禁用",
	StatusEnable:  "启用",
}
