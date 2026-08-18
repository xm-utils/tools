package common

import (
	"strconv"

	"github.com/shopspring/decimal"
)

// AbsInt 对int类型取绝对值
func AbsInt(num int64) int64 {
	if num < 0 {
		return -num
	}
	return num
}

// Cent2Yuan 人民币分转为元
func Cent2Yuan(fen uint64) float64 {
	return decimal.NewFromUint64(fen).Div(decimal.NewFromInt(100)).InexactFloat64()
}
func Cent2YuanStr(fen uint64) string {
	return decimal.NewFromUint64(fen).Div(decimal.NewFromInt(100)).StringFixed(2)
}

func YuanStr2Cent(str string) uint64 {
	yuan, _ := decimal.NewFromString(str)
	return Yuan2Cent(yuan.InexactFloat64())
}

// Yuan2Cent 人民币元转为分
func Yuan2Cent(yuan float64) uint64 {
	return uint64(decimal.NewFromFloat(yuan).Mul(decimal.NewFromInt(100)).IntPart())
}

// RateToClient 比率还原后返回客户端
func RateToClient(num uint64) float64 {
	return decimal.NewFromUint64(num).Div(decimal.NewFromInt(10000)).InexactFloat64()
}
func Rate2ClientStr(num uint64) string {
	return decimal.NewFromUint64(num).Div(decimal.NewFromInt(10000)).StringFixed(2)
}

func Rate2DB(yuan float64) uint64 {
	return uint64(decimal.NewFromFloat(yuan).Mul(decimal.NewFromInt(10000)).IntPart())
}

func RateStr2DB(str string) uint64 {
	yuan, _ := decimal.NewFromString(str)
	return uint64(yuan.Mul(decimal.NewFromInt(10000)).IntPart())
}

func StringToFloat64(str string) float64 {
	yuan, _ := decimal.NewFromString(str)
	return yuan.InexactFloat64()
}

func StringToInt32(s string) int32 {
	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int32(i)
}

func StringToInt64(s string) int64 {
	d, err2 := decimal.NewFromString(s)
	if err2 != nil {
		return 0
	}
	return d.IntPart()
}

func StringToUInt(s string) uint {
	i, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return uint(i)
}

func StringToUInt64(s string) uint64 {
	i, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return i
}

func StringToInt(s string) int {
	d, err2 := decimal.NewFromString(s)
	if err2 != nil {
		return 0
	}
	return int(d.IntPart())
}

func Float64ToString(value float64) string {
	return decimal.NewFromFloat(value).StringFixed(2)
}
