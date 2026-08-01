package validate

type AddUserParam struct {
	UserName string  `json:"userName" form:"userName" binding:"required,alphanum"`
	NickName string  `json:"nickName" form:"nickName" binding:"required"`
	Password string  `json:"password" form:"password" binding:"required,alphanum"`
	Phone    string  `json:"phoneNumber" form:"phoneNumber"`
	Email    string  `json:"email" form:"email"`
	Sex      int8    `json:"sex" form:"sex"`
	Status   int8    `json:"status" form:"status"`
	Remark   string  `json:"remark" form:"remark"`
	RoleIds  []int64 `json:"roleIds" form:"roleIds"`
}

func AddUserParamError() map[string]string {
	formError := make(map[string]string)
	formError["Username.alphanum"] = "用户名只能包含字母、数字"
	formError["Username.required"] = "用户名必须填写"
	formError["Password.alphanum"] = "密码只能包含字母、数字"
	formError["Password.required"] = "密码必须填写"
	formError["NickName.required"] = "昵称必须填写"
	return formError
}

type SignInForm struct {
	Username  string `json:"username" form:"username" binding:"required,alphanum"`
	Password  string `json:"password" form:"password" binding:"required"`
	CheckCode string `json:"code" form:"code" binding:"required"`
	Uuid      string `json:"uuid" form:"uuid" binding:"required"`
}

func SignInFormError() map[string]string {
	formError := make(map[string]string)
	formError["Username.alphanum"] = "用户名只能包含字母、数字"
	formError["Username.required"] = "用户名必须填写"
	formError["Password.required"] = "密码必须填写"
	formError["CheckCode.required"] = "验证码必须填写"
	return formError
}

type EditUserParam struct {
	UserId   int64  `json:"userId" form:"userId" binding:"required"`
	NickName string `json:"nickName" form:"nickName"`
	Phone    string `json:"phoneNumber" form:"phoneNumber"`
	Email    string `json:"email" form:"email"`
	Sex      int8   `json:"sex" form:"sex"`
	Remark   string `json:"remark" form:"remark"`
}

func EditUserParamError() map[string]string {
	formError := make(map[string]string)
	formError["UserId.required"] = "管理员Id参数缺失"
	return formError
}

type ResetPasswordParam struct {
	UserId   int64  `json:"userId" form:"userId" binding:"required"`
	Password string `json:"password" form:"password" binding:"required"`
}

func ResetPasswordParamError() map[string]string {
	formError := make(map[string]string)
	formError["UserId.required"] = "管理员Id参数缺失"
	formError["Password.required"] = "密码必须填写"
	return formError
}

type EditPasswordForm struct {
	OriginalPassword string `json:"originalPassword" form:"originalPassword"`
	Password         string `json:"password" form:"password" binding:"required,eqfield=ConfirmPassword"`
	ConfirmPassword  string `json:"confirmPassword" form:"confirmPassword" binding:"required"`
}

func EditPasswordFormError() map[string]string {
	formError := make(map[string]string)
	formError["Password.required"] = "密码必须填写"
	formError["Password.eqfield"] = "密码必须跟确认密码一致"
	formError["ConfirmPassword.required"] = "确认密码必须填写"
	return formError
}

type EditAdminUserStatusForm struct {
	UserId int64 `json:"userId" form:"userId" binding:"required"`
	Status int8  `json:"status" form:"status" binding:"required,min=-1,max=2"`
}

func EditAdminUserStatusFormError() map[string]string {
	formError := make(map[string]string)
	formError["UserId.required"] = "管理员Id参数缺失"
	formError["Status.required"] = "状态参数缺失"
	formError["Status.min"] = "状态参数错误"
	formError["Status.max"] = "状态参数错误"
	return formError
}

type SetGroupParam struct {
	UserId int64   `json:"userId" form:"userId" binding:"required"`
	RoleId []int64 `json:"roleId" form:"roleId" binding:"required"`
}

func SetGroupParamError() map[string]string {
	formError := make(map[string]string)
	formError["UserId.required"] = "管理员Id参数缺失"
	formError["RoleId.required"] = "分组Id错误"
	return formError
}
