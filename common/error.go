package common

type CustomError interface {
	error
	Code() int
}

type Errors struct {
	code int
	msg  string
}

func (err *Errors) Error() string {
	return err.msg
}

// Code 获取错误码
func (err *Errors) Code() int {
	return err.code
}

func NewError(code int, msg ...string) CustomError {
	message := ""
	if len(msg) > 0 {
		message = msg[0]
	} else {
		message = ErrorMessage[code]
	}
	return &Errors{code, message}
}

const SuccessCode = 200

// 公共错误 200***
const (
	CommonParamError         = 200001 + iota //请求参数不合法
	CommonPermissionDenied                   //账号没有访问接口的权限
	CommonSystemError                        //系统错误
	CommonTokenTimeOut                       //登录超时
	CommonCaptchaCreateError                 //验证码生成失败
	CommonTokenCreateError                   //token生成失败
	CommonDataNotExist                       //数据不存在
	CommonDbError
	CommonDbInsertError
	CommonDbUpdateError
)

var ErrorMessage = map[int]string{
	SuccessCode: "操作成功",

	//公共错误
	CommonParamError:         "请求参数不合法",
	CommonPermissionDenied:   "账号没有访问接口的权限",
	CommonSystemError:        "系统错误",
	CommonTokenTimeOut:       "登录超时,请重新登录",
	CommonCaptchaCreateError: "验证码生成失败",
	CommonTokenCreateError:   "token生成失败",
	CommonDataNotExist:       "数据不存在",
	CommonDbError:            "系统错误",
	CommonDbInsertError:      "数据保存失败",
	CommonDbUpdateError:      "数据更新失败",
}
