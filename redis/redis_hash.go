package redis

import "context"

// HDel key field1 [field2] 删除一个或多个哈希表字段
func HDel(ctx context.Context, key string, fields ...string) error {
	return client.HDel(ctx, associate(key), fields...).Err()
}

// HExists HEXISTS key field 查看哈希表 key 中，指定的字段是否存在。
func HExists(ctx context.Context, key, field string) (bool, error) {
	exists := client.HExists(ctx, associate(key), field)
	return exists.Val(), exists.Err()
}

// HGet HGET key field 获取存储在哈希表中指定字段的值。
func HGet[T any](ctx context.Context, key string, field string) (val T, err error) {
	cmd := client.HGet(ctx, associate(key), field)
	if cmd.Err() != nil {
		err = cmd.Err()
		return
	}

	return decodeVal[T](cmd.Val())
}

// HGetAll HGETALL key 获取在哈希表中指定 key 的所有字段和值
func HGetAll[T any](ctx context.Context, key string) (error, map[string]T) {
	cmd := client.HGetAll(ctx, associate(key))
	if cmd.Err() != nil {
		return cmd.Err(), nil
	}

	result := make(map[string]T)
	for k, v := range cmd.Val() {
		val, err := decodeVal[T](v)
		if err != nil {
			continue
		}
		result[k] = val
	}

	return nil, result
}

// HIncrBy HINCRBY key field increment 为哈希表 key 中的指定字段的整数值加上增量 increment 。
func HIncrBy(ctx context.Context, key string, field string, incr int64) (int64, error) {
	cmd := client.HIncrBy(ctx, associate(key), field, incr)
	return cmd.Val(), cmd.Err()
}

// HIncrByFloat HINCRBYFLOAT key field increment 为哈希表 key 中的指定字段的浮点数值加上增量 increment 。
func HIncrByFloat(ctx context.Context, key string, field string, incr float64) (float64, error) {
	cmd := client.HIncrByFloat(ctx, associate(key), field, incr)
	return cmd.Val(), cmd.Err()
}

// HKeys 7	HKEYS key 获取哈希表中的所有字段
func HKeys(ctx context.Context, key string) ([]string, error) {
	cmd := client.HKeys(ctx, associate(key))
	return cmd.Val(), cmd.Err()
}

// HLen 8	HLEN key 获取哈希表中字段的数量
func HLen(ctx context.Context, key string) int64 {
	hLen := client.HLen(ctx, associate(key))
	if hLen.Err() != nil {
		return 0
	}
	return hLen.Val()
}

// HMGet 9	HMGET key field1 [field2] 获取所有给定字段的值
func HMGet[T any](ctx context.Context, key string, fields ...string) []T {
	cmd := client.HMGet(ctx, associate(key), fields...)
	if cmd.Err() != nil {
		return []T{}
	}
	result := make([]T, len(cmd.Val()))
	for i, v := range cmd.Val() {
		val, err := decodeVal[T](v.(string))
		if err != nil {
			continue
		}
		result[i] = val
	}
	return result
}

// HMSet 10	HMSET key field1 value1 [field2 value2 ] 同时将多个 field-value (域-值)对设置到哈希表 key 中。
func HMSet(ctx context.Context, key string, fields map[string]interface{}) error {
	args := make([]interface{}, 0)
	for k, v := range fields {
		val, err := encode(v)
		if err != nil {
			return err
		}
		args = append(args, k, val)
	}
	return client.HMSet(ctx, associate(key), args).Err()

}

// HSet 11	HSET key field value 将哈希表 key 中的字段 field 的值设为 value 。
func HSet(ctx context.Context, key string, field string, val interface{}) error {
	valByte, err := encode(val)
	if err != nil {
		return err
	}
	return client.HSet(ctx, associate(key), field, valByte).Err()
}

// HSetnx 12	HSETNX key field value 只有在字段 field 不存在时，设置哈希表字段的值。
func HSetnx(ctx context.Context, key string, field string, val interface{}) (bool, error) {
	valByte, err := encode(val)
	if err != nil {
		return false, err
	}
	return client.HSetNX(ctx, associate(key), field, valByte).Result()
}

// HVals 13	HVALS key 获取哈希表中所有值。
func HVals[T any](ctx context.Context, key string) ([]T, error) {
	cmd := client.HVals(ctx, associate(key))
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	return decodeArr[T](cmd.Val())
}
