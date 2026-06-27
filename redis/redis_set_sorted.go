package redis

import (
	"context"
	"strconv"

	"github.com/go-redis/redis/v8"
)

// ZAdd ZADD 向有序集合添加一个或多个成员，或者更新已存在成员的分数
func ZAdd(ctx context.Context, key string, pairs map[interface{}]float64) error {
	var args []*redis.Z
	for k, v := range pairs {
		val, err := encode(k)
		if err != nil {
			return err
		}
		args = append(args, &redis.Z{
			Score:  v,
			Member: val,
		})
	}

	return client.ZAdd(ctx, associate(key), args...).Err()
}

func ZAddByScore(ctx context.Context, key string, member interface{}, score float64) error {

	val, err := encode(member)
	if err != nil {
		return err
	}

	args := &redis.Z{
		Score:  score,
		Member: val,
	}

	return client.ZAdd(ctx, associate(key), args).Err()
}

// ZCard ZCARD 获取有序集合的成员数
func ZCard(ctx context.Context, key string) (int64, error) {
	cmd := client.ZCard(ctx, associate(key))
	return cmd.Val(), cmd.Err()
}

// ZCount ZCOUNT 计算在有序集合中指定区间分数的成员数
func ZCount(ctx context.Context, key, min, max string) (int64, error) {
	cmd := client.ZCount(ctx, associate(key), min, max)
	return cmd.Val(), cmd.Err()
}

// ZScore ZSCORE
func ZScore(ctx context.Context, key, member interface{}) (float64, error) {
	val, err := encode(member)
	if err != nil {
		return 0, err
	}
	cmd := client.ZScore(ctx, associate(key), val)
	return cmd.Val(), cmd.Err()
}

// ZIncrBy ZINCRBY 有序集合中对指定成员的分数加上增量 increment
func ZIncrBy(ctx context.Context, key, member interface{}, increment float64) (float64, error) {
	val, err := encode(member)
	if err != nil {
		return 0, err
	}
	cmd := client.ZIncrBy(ctx, associate(key), increment, val)
	return cmd.Val(), cmd.Err()
}

// ZInterStore ZINTERSTORE 计算给定的一个或多个有序集的交集并将结果集存储在新的有序集合 destination 中
func ZInterStore(ctx context.Context, destination string, keys ...string) (int64, error) {
	arr := make([]string, len(keys))
	for i, v := range keys {
		arr[i] = associate(v)
	}
	cmd := client.ZInterStore(ctx, associate(destination), &redis.ZStore{
		Keys: arr,
	})
	return cmd.Val(), cmd.Err()
}

// ZLexCount ZLEXCOUNT 在有序集合中计算指定字典区间内成员数量
func ZLexCount(ctx context.Context, key, min, max string) (int64, error) {
	cmd := client.ZLexCount(ctx, associate(key), min, max)
	return cmd.Val(), cmd.Err()
}

// ZRange ZRANGE 通过索引区间返回有序集合指定区间内的成员
func ZRange[T any](ctx context.Context, key string, start, stop int64) (res []T, err error) {
	cmd := client.ZRange(ctx, associate(key), start, stop)
	if cmd.Err() != nil {
		err = cmd.Err()
		return
	}
	return decodeArr[T](cmd.Val())
}

// ZRangeWithScores ZRANGE 通过分数返回有序集合指定区间内的成员
func ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]redis.Z, error) {
	cmd := client.ZRangeWithScores(ctx, associate(key), start, stop)
	return cmd.Val(), cmd.Err()
}

// ZRangeByLex ZRANGEBYLEX key min max 通过字典区间返回有序集合的成员
func ZRangeByLex(ctx context.Context, key, min, max string) ([]string, error) {
	cmd := client.ZRangeByLex(ctx, associate(key), &redis.ZRangeBy{
		Min: min,
		Max: max,
	})
	return cmd.Val(), cmd.Err()

}

// ZRangeByScore ZRANGEBYSCORE 通过分数返回有序集合指定区间内的成员
func ZRangeByScore[T any](ctx context.Context, key, min, max string) (res []T, err error) {
	cmd := client.ZRangeByScore(ctx, associate(key), &redis.ZRangeBy{
		Min: min,
		Max: max,
	})
	if cmd.Err() != nil {
		err = cmd.Err()
		return
	}
	return decodeArr[T](cmd.Val())
}

// ZRangeByScoreWithScores ZRANGEBYSCORE [WITHSCORES] 通过分数返回有序集合指定区间内的成员
func ZRangeByScoreWithScores(ctx context.Context, key, min, max string) (res []redis.Z, err error) {
	cmd := client.ZRangeByScoreWithScores(ctx, associate(key), &redis.ZRangeBy{
		Min: min,
		Max: max,
	})
	return cmd.Val(), cmd.Err()
}

// ZRank ZRANK key member 返回有序集合中指定成员的索引
func ZRank(ctx context.Context, key, member interface{}) (int64, error) {
	val, err := encode(member)
	if err != nil {
		return 0, err
	}
	cmd := client.ZRank(ctx, associate(key), val)
	return cmd.Val(), cmd.Err()
}

// ZRevRank ZREVRANK key member 返回有序集合中指定成员的排名，有序集成员按分数值递减(从大到小)排序
func ZRevRank(ctx context.Context, key, member interface{}) (int64, error) {
	val, err := encode(member)
	if err != nil {
		return 0, err
	}
	cmd := client.ZRevRank(ctx, associate(key), val)
	return cmd.Val(), cmd.Err()
}

// ZRevRange ZREVRANGE 返回有序集中指定区间内的成员，通过索引，分数从高到低
func ZRevRange[T any](ctx context.Context, key string, start, stop int64) ([]T, error) {
	cmd := client.ZRevRange(ctx, associate(key), start, stop)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return decodeArr[T](cmd.Val())
}

// ZRevRangeByScore ZREVRANGEBYSCORE key max min [WITHSCORES] 返回有序集中指定分数区间内的成员，分数从高到低排序
func ZRevRangeByScore[T any](ctx context.Context, key, max, min string) ([]T, error) {
	cmd := client.ZRevRangeByScore(ctx, associate(key), &redis.ZRangeBy{
		Min: max,
		Max: min,
	})
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return decodeArr[T](cmd.Val())
}

// ZRevRangeByScoreWithScores ZREVRANGEBYSCORE [WITHSCORES] 返回有序集中指定分数区间内的成员，分数从高到低排序
func ZRevRangeByScoreWithScores(ctx context.Context, key, max, min string) (res []redis.Z, err error) {
	cmd := client.ZRevRangeByScoreWithScores(ctx, associate(key), &redis.ZRangeBy{
		Min: max,
		Max: min,
	})
	if cmd.Err() != nil {
		err = cmd.Err()
		return
	}

	return cmd.Val(), cmd.Err()
}

// ZRem ZREM 移除有序集合中的一个或多个成员
func ZRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	values := make([]interface{}, len(members))
	for i, v := range members {
		str, err := encode(v)
		if err != nil {
			return 0, err
		}
		values[i] = str
	}
	cmd := client.ZRem(ctx, associate(key), values...)
	return cmd.Val(), cmd.Err()
}

// ZRemRangeByLex ZREMRANGEBYLEX key min max 移除有序集合中给定的字典区间的所有成员
func ZRemRangeByLex(ctx context.Context, key, min, max string) (int64, error) {
	cmd := client.ZRemRangeByLex(ctx, associate(key), min, max)
	return cmd.Val(), cmd.Err()
}

// ZRemRangeByRank ZREMRANGEBYRANK key start stop 移除有序集合中给定的排名区间的所有成员
func ZRemRangeByRank(ctx context.Context, key string, start, stop int64) (int64, error) {
	cmd := client.ZRemRangeByRank(ctx, associate(key), start, stop)
	return cmd.Val(), cmd.Err()
}

// ZRemRangeByScore ZREMRANGEBYSCORE key min max 移除有序集合中给定的分数区间的所有成员
func ZRemRangeByScore(ctx context.Context, key string, min, max int64) (int64, error) {
	cmd := client.ZRemRangeByScore(ctx, associate(key), strconv.FormatInt(min, 10), strconv.FormatInt(max, 10))
	return cmd.Val(), cmd.Err()
}

// ZUnionStore ZUNIONSTORE destination numkeys key [key ...] 计算给定的一个或多个有序集的并集，并存储在新的 key 中
func ZUnionStore(ctx context.Context, destination string, keys ...string) (int64, error) {

	arr := make([]string, len(keys))
	for i, v := range keys {
		arr[i] = associate(v)
	}

	cmd := client.ZUnionStore(ctx, associate(destination), &redis.ZStore{
		Keys: arr,
	})
	return cmd.Val(), cmd.Err()
}
