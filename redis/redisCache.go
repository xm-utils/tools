package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	client *redis.Client
	//ctx    context.Context
	Key string
)

const Nil = redis.Nil

type Config struct {
	Prefix   string `yaml:"prefix" json:"prefix" comment:"KEY前缀"`
	Host     string `yaml:"host" json:"host" comment:"主机名"`
	Password string `yaml:"password" json:"password" comment:"密码"`
	DbNum    int    `yaml:"dbNum" json:"dbNum" comment:"数据库"`
}

func InitRedisCache(config *Config) error {
	//ctx = context.Background()
	Key = config.Prefix

	cli, err := startAndGC(config.Host, config.Password, config.DbNum)
	if err != nil {
		return errors.New(fmt.Sprintf("can't connect redis service %v", err))
	}

	client = cli
	return nil
}

func associate(originKey interface{}) string {
	return fmt.Sprintf("%s:%s", Key, originKey)
}

// start gc routine based on config string settings.
func startAndGC(host, passWord string, dbNum int) (*redis.Client, error) {

	cli := redis.NewClient(&redis.Options{
		Addr:     host,
		Password: passWord,
		DB:       dbNum,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	cmd := cli.Ping(ctx)
	if cmd.Err() != nil {
		return nil, errors.New(fmt.Sprintf("redis connect errors: %v \n", cmd.Err()))
	}

	return cli, nil
}

// IsExist check if cached value exists or not.
func IsExist(ctx context.Context, key string) bool {
	val := client.Exists(ctx, associate(key)).Val()
	return val != 0
}

// Delete delete cached value by key.
func Delete(ctx context.Context, key string) error {
	return client.Del(ctx, associate(key)).Err()
}

// Subscribe 订阅主题
func Subscribe(ctx context.Context, channel ...string) *redis.PubSub {
	return client.Subscribe(ctx, channel...)
}

// PSubscribe 订阅主题
func PSubscribe(ctx context.Context, channel ...string) *redis.PubSub {
	return client.PSubscribe(ctx, channel...)
}

// Publish 发布主题消息
func Publish(ctx context.Context, channel string, msg interface{}) error {
	msgByte, err := encode(msg)
	if err != nil {
		return err
	}
	return client.Publish(ctx, channel, msgByte).Err()
}

func ReceiveMessage(ctx context.Context, pubSub *redis.PubSub) (*redis.Message, error) {
	return pubSub.ReceiveMessage(ctx)
}

// ClearAll clear all cache.
func ClearAll(ctx context.Context) error {
	return client.FlushAll(ctx).Err()
}

func ExpireAt(ctx context.Context, key string, t time.Time) error {
	return client.ExpireAt(ctx, associate(key), t).Err()
}
func ExpireIn(ctx context.Context, key string, d time.Duration) error {
	return client.Expire(ctx, associate(key), d).Err()
}

func Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	// 对 keys 统一添加前缀，与其他函数行为一致
	prefixedKeys := make([]string, len(keys))
	for i, k := range keys {
		prefixedKeys[i] = associate(k)
	}

	cmd := client.Eval(ctx, script, prefixedKeys, args...)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	return cmd.Val(), cmd.Err()
}
func encode(data interface{}) (string, error) {
	// 使用反射来处理各种类型
	val := reflect.ValueOf(data)

	// 如果是指针，获取其指向的值
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return "", errors.New("data is nil")
		}
		val = val.Elem()
	}

	switch val.Kind() {
	case reflect.String:
		return val.String(), nil
	case reflect.Bool:
		if val.Bool() {
			return "true", nil
		}
		return "false", nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("%d", val.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprintf("%d", val.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%g", val.Float()), nil
	case reflect.Slice:
		// 检查是否为 []byte 类型
		if val.Type().Elem().Kind() == reflect.Uint8 {
			return string(val.Bytes()), nil
		}
		// 其他切片类型使用 JSON 序列化
		fallthrough
	default:
		// 其他复杂类型使用 JSON 序列化
		bytes, err := json.Marshal(data)
		if err != nil {
			return "", err
		}
		return string(bytes), nil
	}
}
func decode(data string, v any) error {
	if data == "" {
		return errors.New("data is nil")
	}

	switch v.(type) {
	case string:
		v = data
		return nil
	case []byte:
		v = []byte(data)
		return nil
	default:
		return json.Unmarshal([]byte(data), v)
	}
}

func decodeVal[T any](data string) (T, error) {
	if data == "" {
		var zero T
		return zero, errors.New("data is nil")
	}

	var res = new(T)

	// 获取目标类型的反射类型
	targetType := reflect.TypeOf(res).Elem()

	// 处理基本类型
	switch targetType.Kind() {
	case reflect.String:
		str := data
		return any(str).(T), nil
	case reflect.Int:
		intVal, err := strconv.Atoi(data)
		if err != nil {
			var zero T
			return zero, err
		}
		return any(intVal).(T), nil
	case reflect.Int64:
		//int64Val := utils.StringToInt64(data)
		int64Val, err := strconv.ParseInt(data, 10, 64)
		if err != nil {
			var zero T
			return zero, err
		}
		return any(int64Val).(T), nil
	case reflect.Float64:
		//floatVal := utils.StringToFloat64(data)
		floatVal, err := strconv.ParseFloat(data, 64)
		if err != nil {
			var zero T
			return zero, err
		}
		return any(floatVal).(T), nil
	case reflect.Bool:
		boolVal := data == "true" || data == "1" || data == "True" || data == "TRUE"
		return any(boolVal).(T), nil
	case reflect.Slice:
		// 检查是否为 []byte 类型
		if targetType.Elem().Kind() == reflect.Uint8 { // []byte 实际上是 []uint8
			byteSlice := []byte(data)
			return any(byteSlice).(T), nil
		}
		// 对于其他切片类型，使用 JSON 反序列化
		fallthrough
	default:
		// 对于复杂类型，使用 JSON 反序列化
		err := json.Unmarshal([]byte(data), res)
		if err != nil {
			var zero T
			return zero, err
		}
		return *res, nil
	}
}

func decodeArr[T any](data []string) ([]T, error) {
	res := make([]T, len(data))
	for i, v := range data {
		val, err := decodeVal[T](v)
		if err != nil {
			return nil, err
		}
		res[i] = val
	}
	return res, nil
}
