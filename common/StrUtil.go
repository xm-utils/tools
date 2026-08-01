// 字符串工具
// Neo

package common

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type String string

// /转换为字符串列表
func (object String) ToStrList() (list []string) {
	s := strings.TrimSpace(string(object))
	if 0 >= len(s) {
		return
	}

	s = strings.Trim(strings.Trim(s, "["), "]")
	if 0 >= len(s) {
		return
	}

	list = strings.Split(s, ",")
	return
}

// /转换为int64字符串
func (object String) ToIntList() (list []int, err error) {
	var strArr []string
	strArr = object.ToStrList()
	if 0 == len(strArr) {
		return
	}

	list = make([]int, len(strArr))
	for i := 0; i < len(strArr); i++ {
		var v int
		v, err = strconv.Atoi(strings.TrimSpace(strArr[i]))
		//v, err = strconv.ParseInt(strings.TrimSpace(strArr[i]), 10, 64)
		if nil != err {
			return
		}
		list[i] = v
	}

	return
}

// /转换为int64字符串
func (object String) ToInt64List() (list []int64, err error) {
	var strArr []string
	strArr = object.ToStrList()
	if 0 == len(strArr) {
		return
	}

	list = make([]int64, len(strArr))
	for i := 0; i < len(strArr); i++ {
		var v int64
		v, err = strconv.ParseInt(strings.TrimSpace(strArr[i]), 10, 64)
		if nil != err {
			return
		}
		list[i] = v
	}

	return
}

// /转换为数字
func (object String) ToInt64(errorValue int64) (v int64, err error) {
	return strconv.ParseInt(string(object), 10, 64)
}

// /转换为数字
func (object String) ToInt64Default(errorValue int64) int64 {
	if 0 >= len(object) {
		return errorValue
	}

	v, err := object.ToInt64(errorValue)
	if nil != err {
		return errorValue
	}

	return v
}

func IntArray2String(arr []int) []string {
	if len(arr) < 1 {
		return []string{}
	}
	result := make([]string, 0)
	for _, i2 := range arr {
		result = append(result, fmt.Sprintf("%d", i2))
	}
	return result
}

// /标准化结构体成员名
func NormalizeSTFieldName(fileName string) string {
	result := make([]byte, 2*len(fileName)-1)
	for i, j := 0, 0; i < len(fileName); i++ {
		if 'A' <= fileName[i] && 'Z' >= fileName[i] {
			if 0 != i {
				result[j] = '_'
				j++
			}
			result[j] = fileName[i] + 'a' - 'A'
			j++
			continue
		}
		result[j] = fileName[i]
		j++
	}

	return strings.TrimFunc(string(result), func(r rune) bool {
		return rune(0) == r
	})
}

// /获取结构体成员名
func GetSTFieldName(object interface{}) []string {
	names := make([]string, 0)
	v := reflect.TypeOf(object).Elem()
	for i := 0; i < v.NumField(); i++ {
		ormTag, ok := v.Field(i).Tag.Lookup(`orm`)
		if ok && "-" == ormTag {
			continue
		}
		names = append(names, v.Field(i).Name)
	}
	return names
}

// /获取结构体表转化成员名
func GetSTNormalizeFieldName(object interface{}) []string {
	names := GetSTFieldName(object)
	for i := 0; i < len(names); i++ {
		names[i] = NormalizeSTFieldName(names[i])
	}
	return names
}

func CardHid(card string) string {
	if len(card) < 3 {
		return card
	}
	if len(card) < 9 {
		return card[:1] + "******" + card[len(card)-1:]
	}
	return card[:4] + "******" + card[len(card)-5:]
}

// 生成密码和盐
func GeneratePwd(password string) (pwd, salt string) {
	salt = RandomString(16)
	pwd = MD5(password + salt)
	return
}

func VerifyPwd(password, salt, target string) bool {
	pwd := MD5(target + salt)

	return password == pwd
}

// 用户名打码
func AddUserNameMosaic(userName string) string {
	if len(userName) < 3 {
		return userName
	} else if len(userName) <= 7 {
		return userName[:3] + "****" + userName[3:]
	} else {
		return userName[:3] + "****" + userName[len(userName)-4:]
	}
}

// 个人资料第4-7位加*
func AddUserProfileMosaic(str string) string {
	if len(str) < 3 {
		return str
	} else if len(str) <= 7 {
		return str[:3] + "****" + str[3:]
	} else {
		return str[:3] + "****" + str[7:]
	}
}
func JoinSlice(s []int) string {
	var ss = make([]string, len(s))
	for i := range ss {
		ss[i] = fmt.Sprintf("%d", s[i])
	}
	return strings.Join(ss, ",")
}

// ex: 123,555 -> [123,555]
func ParseStrToArrayInt(target string, sep string) []int {
	target = strings.TrimSpace(target)
	if target == "" {
		return nil
	}

	ids := strings.Split(target, sep)
	result := make([]int, len(ids))
	for index, id := range ids {
		id, err := strconv.Atoi(strings.TrimSpace(id))

		if err != nil {
			return nil
		}

		result[index] = id
	}
	return result
}

// ex: "棋牌","捕魚" -> ["棋牌","捕魚"]
func ParseStrToArrayStr(target string, sep string) []string {
	target = strings.TrimSpace(target)
	if target == "" {
		return nil
	}
	return strings.Split(target, sep)
}

// RandomString 在数字、大写字母、小写字母范围内生成num位的随机字符串
func RandomString(length int) string {
	// 48 ~ 57 数字
	// 65 ~ 90 A ~ Z
	// 97 ~ 122 a ~ z
	// 一共62个字符，在0~61进行随机，小于10时，在数字范围随机，
	// 小于36在大写范围内随机，其他在小写范围随机
	rand.Seed(time.Now().UnixNano())
	result := make([]string, 0, length)
	for i := 0; i < length; i++ {
		t := rand.Intn(62)
		if t < 10 {
			result = append(result, strconv.Itoa(rand.Intn(10)))
		} else if t < 36 {
			result = append(result, strconv.Itoa(rand.Intn(26)+65))
		} else {
			result = append(result, strconv.Itoa(rand.Intn(26)+97))
		}
	}
	return strings.Join(result, "")
}

func CreateOrderNo(length int) (string, error) {
	if length < 16 || length > 32 {
		return "", errors.New("min length is 16,max length is 32")
	}
	randString := "99999999999999999999"
	preString := "00000000000000000000"

	timeString := time.Now().Format("060102150405")
	length = length - len(timeString)

	randString = randString[0:length]

	rand.Seed(time.Now().UnixNano())
	intNumber, _ := strconv.Atoi(randString)
	randNum := strconv.Itoa(rand.Intn(intNumber))
	randString = preString[0:length-len(randNum)] + randNum
	return timeString + randString, nil
}
