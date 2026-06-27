package redis

import "context"

// SAdd 1	SADD key member1 [member2] 向集合添加一个或多个成员
func SAdd(ctx context.Context, key string, members ...interface{}) (int64, error) {
	arr := make([]interface{}, len(members))
	for i, v := range members {
		val, err := encode(v)
		if err != nil {
			return 0, err
		}
		arr[i] = val
	}

	cmd := client.SAdd(ctx, associate(key), arr...)
	return cmd.Val(), cmd.Err()
}

// SCard 2	SCARD key 获取集合的成员数
func SCard(ctx context.Context, key string) int64 {
	return client.SCard(ctx, associate(key)).Val()
}

// SDiff 3	SDIFF key1 [key2] 返回第一个集合与其他集合之间的差异。
func SDiff[T any](ctx context.Context, keys ...string) (res []T, err error) {
	cmd := client.SDiff(ctx, keys...)
	if cmd.Err() != nil {
		return nil, err
	}
	return decodeArr[T](cmd.Val())
}

// SDiffStore 4	SDIFFSTORE destination key1 [key2] 返回给定所有集合的差集并存储在 destination 中
func SDiffStore(ctx context.Context, destination string, keys ...string) (int64, error) {
	cmd := client.SDiffStore(ctx, associate(destination), keys...)
	return cmd.Val(), cmd.Err()
}

// SInter 5	SINTER key1 [key2] 返回给定所有集合的交集
func SInter[T any](ctx context.Context, keys ...string) (res []T, err error) {
	cmd := client.SInter(ctx, keys...)
	if cmd.Err() != nil {
		return nil, err
	}
	return decodeArr[T](cmd.Val())
}

// SInterStore 6	SINTERSTORE destination key1 [key2] 返回给定所有集合的交集并存储在 destination 中
func SInterStore(ctx context.Context, destination string, keys ...string) (int64, error) {
	cmd := client.SInterStore(ctx, associate(destination), keys...)
	return cmd.Val(), cmd.Err()
}

// SIsMember 7	SISMEMBER key member 判断 member 元素是否是集合 key 的成员
func SIsMember(ctx context.Context, key string, member interface{}) (bool, error) {
	val, err := encode(member)
	if err != nil {
		return false, err
	}
	cmd := client.SIsMember(ctx, associate(key), val)
	return cmd.Val(), cmd.Err()
}

// SMembers 8	SMEMBERS key 返回集合中的所有成员
func SMembers[T any](ctx context.Context, key string) (res []T, err error) {
	cmd := client.SMembers(ctx, associate(key))
	if cmd.Err() != nil {
		return nil, err
	}
	return decodeArr[T](cmd.Val())
}

// SMove 9	SMOVE source destination member 将 member 元素从 source 集合移动到 destination 集合
func SMove(ctx context.Context, source, destination string, member interface{}) (bool, error) {
	val, err := encode(member)
	if err != nil {
		return false, err
	}

	cmd := client.SMove(ctx, associate(source), associate(destination), val)
	return cmd.Val(), cmd.Err()
}

// SPop 10	SPOP key 移除并返回集合中的一个随机元素
func SPop[T any](ctx context.Context, key string) (t T, err error) {
	cmd := client.SPop(ctx, associate(key))
	if cmd.Err() != nil {
		err = cmd.Err()
		return
	}

	return decodeVal[T](cmd.Val())
}

// SRandMember 11	SRANDMEMBER key [count] 返回集合中一个或多个随机数
func SRandMember[T any](ctx context.Context, key string, count int) (res []T, err error) {
	cmd := client.SRandMemberN(ctx, associate(key), int64(count))
	if cmd.Err() != nil {
		return nil, err
	}
	return decodeArr[T](cmd.Val())

}

// SRem 12	SREM key member1 [member2] 移除集合中一个或多个成员
func SRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	arr := make([]interface{}, len(members))
	for i, v := range members {
		val, err := encode(v)
		if err != nil {
			return 0, err
		}
		arr[i] = val
	}

	cmd := client.SRem(ctx, associate(key), arr...)
	return cmd.Val(), cmd.Err()
}

// SUnion 13	SUNION key1 [key2] 返回所有给定集合的并集
func SUnion[T any](ctx context.Context, keys ...string) (res []T, err error) {
	cmd := client.SUnion(ctx, keys...)
	if cmd.Err() != nil {
		return nil, err
	}
	return decodeArr[T](cmd.Val())
}

// SUnionStore 14	SUNIONSTORE destination key1 [key2] 所有给定集合的并集存储在 destination 集合中
func SUnionStore(ctx context.Context, destination string, keys ...string) (int64, error) {
	cmd := client.SUnionStore(ctx, associate(destination), keys...)
	return cmd.Val(), cmd.Err()
}
