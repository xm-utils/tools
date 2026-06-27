package redis

import (
	"context"
	"time"
)

// BLPop 1	BLPOP key1 [key2... ] timeout 移出并获取列表的第一个元素， 如果列表没有元素会阻塞列表直到等待超时或发现可弹出元素为止。
func BLPop[T any](ctx context.Context, timeout int, keys ...string) (k string, v T, err error) {
	kArr := make([]string, len(keys))
	for i, val := range keys {
		kArr[i] = associate(val)
	}
	cmd := client.BLPop(ctx, time.Duration(timeout)*time.Second, kArr...)
	if cmd.Err() != nil {
		err = cmd.Err()
		return
	}
	k = cmd.Val()[0]
	v, err = decodeVal[T](cmd.Val()[1])
	return
}

// BRPop 2	BRPOP key1 [key2 ] timeout 移出并获取列表的最后一个元素， 如果列表没有元素会阻塞列表直到等待超时或发现可弹出元素为止。
func BRPop[T any](ctx context.Context, timeout int, keys ...string) (k string, v T, err error) {
	kArr := make([]string, len(keys))
	for i, val := range keys {
		kArr[i] = associate(val)
	}
	cmd := client.BRPop(ctx, time.Duration(timeout)*time.Second, kArr...)
	if cmd.Err() != nil {
		err = cmd.Err()
		return
	}
	k = cmd.Val()[0]
	v, err = decodeVal[T](cmd.Val()[1])
	return
}

// BRPopLPush 3	BRPOPLPUSH source destination timeout 从列表中弹出一个值，将弹出的元素插入到另外一个列表中并返回它； 如果列表没有元素会阻塞列表直到等待超时或发现可弹出元素为止。
func BRPopLPush(ctx context.Context, source, destination string, timeout int) (string, error) {
	cmd := client.BRPopLPush(ctx, associate(source), associate(destination), time.Duration(timeout)*time.Second)
	return cmd.Val(), cmd.Err()
}

// LIndex 4	LINDEX key index 通过索引获取列表中的元素
func LIndex[T any](ctx context.Context, key string, index int64) (t T, err error) {
	cmd := client.LIndex(ctx, associate(key), index)
	if cmd.Err() != nil {
		err = cmd.Err()
		return
	}
	return decodeVal[T](cmd.Val())
}

// LInsert 5	LINSERT key BEFORE|AFTER pivot value 在列表的元素前或者后插入元素
func LInsert(ctx context.Context, key string, before string, pivot, val interface{}) error {
	bytes, err := encode(val)
	if err != nil {
		return err
	}
	return client.LInsert(ctx, associate(key), before, pivot, bytes).Err()
}

// LLen 6	LLEN key 获取列表长度
func LLen(ctx context.Context, key string) (int64, error) {
	cmd := client.LLen(ctx, associate(key))
	return cmd.Val(), cmd.Err()
}

// LPop 7	LPOP key 移出并获取列表的第一个元素
func LPop[T any](ctx context.Context, key string) (t T, err error) {
	cmd := client.LPop(ctx, associate(key))
	if cmd.Err() != nil {
		err = cmd.Err()
		return
	}
	return decodeVal[T](cmd.Val())
}

// LPush 8	LPUSH key value1 [value2] 将一个或多个值插入到列表头部
func LPush(ctx context.Context, key string, vals ...interface{}) error {
	arr := make([]interface{}, len(vals))
	for i, v := range vals {
		val, err := encode(v)
		if err != nil {
			return err
		}
		arr[i] = val
	}
	return client.LPush(ctx, associate(key), arr...).Err()
}

// LPushX 9	LPUSHX key value 将一个值插入到已存在的列表头部
func LPushX(ctx context.Context, key string, vals ...interface{}) error {
	arr := make([]interface{}, len(vals))
	for i, v := range vals {
		val, err := encode(v)
		if err != nil {
			return err
		}
		arr[i] = val
	}
	return client.LPushX(ctx, associate(key), arr...).Err()
}

// LRange 10	LRANGE key start stop 获取列表指定范围内的元素
func LRange[T any](ctx context.Context, key string, start, stop int64) (res []T, err error) {
	cmd := client.LRange(ctx, associate(key), start, stop)
	if cmd.Err() != nil {
		err = cmd.Err()
		return
	}
	return decodeArr[T](cmd.Val())
}

// LRem 11	LREM key count value 移除列表元素
func LRem(ctx context.Context, key string, index int64, val interface{}) (int64, error) {
	bytes, err := encode(val)
	if err != nil {
		return 0, err
	}
	cmd := client.LRem(ctx, associate(key), index, bytes)
	return cmd.Val(), cmd.Err()
}

// LSet 12	LSET key index value 通过索引设置列表元素的值
func LSet(ctx context.Context, key string, index int64, val interface{}) error {
	bytes, err := encode(val)
	if err != nil {
		return err
	}
	return client.LSet(ctx, associate(key), index, bytes).Err()
}

// Ltrim 13	LTRIM key start stop 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
func Ltrim(ctx context.Context, key string, start, stop int64) error {
	return client.LTrim(ctx, associate(key), start, stop).Err()
}

// RPop 14	RPOP key 移除列表的最后一个元素，返回值为移除的元素。
func RPop[T any](ctx context.Context, key string) (t T, err error) {
	cmd := client.RPop(ctx, associate(key))
	if cmd.Err() != nil {
		err = cmd.Err()
		return
	}
	return decodeVal[T](cmd.Val())
}

// RPopLPush 15	RPOPLPUSH source destination 移除列表的最后一个元素，并将该元素添加到另一个列表并返回
func RPopLPush[T any](ctx context.Context, source, destination string) (t T, err error) {
	cmd := client.RPopLPush(ctx, associate(source), associate(destination))
	if cmd.Err() != nil {
		err = cmd.Err()
		return
	}
	return decodeVal[T](cmd.Val())
}

// RPush 16	RPUSH key value1 [value2] 在列表中添加一个或多个值到列表尾部
func RPush(ctx context.Context, key string, vals ...any) error {
	arr := make([]interface{}, len(vals))
	for i, v := range vals {
		val, err := encode(v)
		if err != nil {
			return err
		}
		arr[i] = val
	}

	return client.RPush(ctx, associate(key), arr...).Err()
}

// RPushX 17 RPUSHX key value 为已存在的列表添加值
func RPushX(ctx context.Context, key string, vals ...interface{}) error {
	arr := make([]interface{}, len(vals))
	for i, v := range vals {
		val, err := encode(v)
		if err != nil {
			return err
		}
		arr[i] = val
	}
	return client.RPushX(ctx, associate(key), arr...).Err()
}
