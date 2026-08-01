package common

import (
	"fmt"
	"math"
	"strconv"
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
	yuan := float64(fen) / 100.0
	return yuan
}
func Cent2YuanStr(fen uint64) string {
	yuan := float64(fen) / 100.0
	return fmt.Sprintf("%.2f", yuan)
}

func YuanStr2Cent(str string) uint64 {
	yuan := StringToFloat64(str)
	return Yuan2Cent(yuan)
}

// Yuan2Cent 人民币元转为分
func Yuan2Cent(yuan float64) uint64 {
	return uint64(yuan * 100)
	//de := decimal.NewFromFloat(yuan)
	//return de.Mul(decimal.NewFromFloat(100)).IntPart()
}

// RateToClient 比率还原后返回客户端
func RateToClient(num uint64) float64 {
	rate := float64(num) / float64(10000)
	return rate
}
func Rate2ClientStr(num uint64) string {
	rate := float64(num) / float64(10000)
	return fmt.Sprintf("%.2f", rate)
}

func Rate2DB(yuan float64) uint64 {
	return uint64(yuan * 10000)
	//de := decimal.NewFromFloat(yuan)
	//return de.Mul(decimal.NewFromFloat(10000)).IntPart()
}

func RateStr2DB(str string) uint64 {
	yuan := StringToFloat64(str)
	return uint64(yuan * 10000)
	//de := decimal.NewFromFloat(yuan)
	//return de.Mul(decimal.NewFromFloat(10000)).IntPart()
}

func StringToFloat64(str string) float64 {
	value, err := strconv.ParseFloat(str, 32)
	if err != nil {
		return 0
	}
	return value
}

func StringToInt32(s string) int32 {
	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int32(i)
}

func StringToInt64(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return i
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
	num, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return num
}

func Float64ToString(value float64) string {

	return strconv.FormatFloat(value, 'f', 2, 64)
}

func TuiGuangAmountConversion(str string) string {
	floatValue, err := strconv.ParseFloat(str, 32)
	if err != nil {
		return ""
	}
	return strconv.Itoa(int(floatValue * 100))
}

// 浮点数向下取整
func Float32Floor(float float32) float32 {
	return float32(math.Floor(float64(float)))
}
