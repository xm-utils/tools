package common

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"github.com/sirupsen/logrus"
)

type Select[T comparable] []SelectOption[T]

func (t Select[T]) ToMap() map[T]string {
	m := make(map[T]string)
	for _, v := range t {
		m[v.Type] = v.TypeName
	}
	return m
}

type SelectOption[T comparable] struct {
	Type     T      `json:"type"`
	TypeName string `json:"typeName"`
}

type SelectTree struct {
	Id       int64         `json:"id"`
	Label    string        `json:"label"`
	Children []*SelectTree `json:"children"`
}

// Response 统一响应结构
type Response struct {
	Code    int32       `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type PageResponse struct {
	TotalCount int64       `json:"totalCount"`
	List       interface{} `json:"list"`
}

// GinSuccess 成功响应
func GinSuccess(c *gin.Context, data interface{}, message ...string) {
	msg := ""
	if len(message) > 0 {
		msg = message[0]
	} else {
		msg = ErrorMessage[SuccessCode]
	}
	c.JSON(http.StatusOK, Response{
		Code:    SuccessCode,
		Message: msg,
		Data:    data,
	})
}

// GinError 错误响应
func GinError(c *gin.Context, code int, message ...string) {
	msg := ""
	if len(message) > 0 {
		msg = message[0]
	} else {
		msg = ErrorMessage[code]
	}
	c.JSON(http.StatusOK, Response{
		Code:    int32(code),
		Message: msg,
	})
}

func GinShouldBind(ctx *gin.Context, form interface{}, formError map[string]string) CustomError {
	if err := ctx.ShouldBind(form); err != nil {
		return bindError(err, formError)
	}
	return nil
}
func GinShouldBindUri(ctx *gin.Context, form interface{}, formError map[string]string) CustomError {
	if err := ctx.ShouldBindUri(form); err != nil {
		return bindError(err, formError)
	}
	return nil
}
func bindError(err error, formError map[string]string) CustomError {

	var errInfo validator.ValidationErrors
	errors.As(err, &errInfo)
	errMsg := err.Error()
	for _, info := range errInfo {
		errTag := info.Field() + "." + info.Tag()
		logrus.Error(formError[errTag])
		errMsg = formError[errTag]
		break
	}
	return NewError(CommonParamError, errMsg)
}
