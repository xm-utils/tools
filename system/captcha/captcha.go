package captcha

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/redis"
)

type CaptchaCacheInfo struct {
	Id   string
	Code []byte
	Time int64
}

const (
	CaptchaCacheKey   = ":hash:hall:captcha_key"
	CaptchaExpiration = 5 * 60
)

func getCacheKey(id string) string {
	return fmt.Sprintf("captcha_key:%s", id)
}
func NewCaptchaCode(length int) (string, []byte, error) {
	captchaId := randomId()
	captchaCode := RandomDigits(length)

	err := redis.Set(context.Background(), getCacheKey(captchaId), captchaCode, CaptchaExpiration)
	if nil != err {
		return captchaId, captchaCode, errors.New("save captcha code to redis failure")
	}
	return captchaId, captchaCode, nil
}

func ReLoadCaptchaCode(captchaId string, length int) ([]byte, error) {
	captchaCode := RandomDigits(length)
	info := &CaptchaCacheInfo{
		Id:   captchaId,
		Code: captchaCode,
		Time: time.Now().Unix(),
	}
	err := redis.HSet(context.Background(), CaptchaCacheKey, captchaId, info)
	if nil != err {
		return captchaCode, errors.New("save captcha code to redis failure")
	}
	return captchaCode, nil
}

func VerifyCode(captchaId string, checkCode string) bool {
	defer redis.Delete(context.Background(), getCacheKey(captchaId))

	captchaCode, err := redis.Get[[]byte](context.Background(), getCacheKey(captchaId))

	if err != nil || len(captchaCode) == 0 {
		logrus.WithFields(logrus.Fields{
			"module": "VerifyCode",
			"error":  err.Error(),
		}).Warnf("captcha code check error")
		return false
	}

	ns := make([]byte, len(checkCode))
	for i := range ns {
		d := checkCode[i]
		switch {
		case '0' <= d && d <= '9':
			ns[i] = d - '0'
		case d == ' ' || d == ',':
			// ignore
		default:
			return false
		}
	}
	return bytes.Equal(ns, captchaCode)
}
