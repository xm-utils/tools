package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisDeduplicationStore 基于Redis的去重存储（适用于分布式环境）
type RedisDeduplicationStore struct {
	ctx    context.Context
	client *redis.Client
}

// NewRedisDeduplicationStore 创建Redis去重存储
func NewRedisDeduplicationStore(client *redis.Client) *RedisDeduplicationStore {
	return &RedisDeduplicationStore{
		ctx:    context.Background(),
		client: client,
	}
}

// IsDuplicate 检查消息是否已处理
func (s *RedisDeduplicationStore) IsDuplicate(key string) bool {
	cmd := s.client.Exists(s.ctx, key)
	return cmd.Val() != 0
}

// MarkProcessed 标记消息已处理（使用SETNX + EXPIRE保证原子性）
func (s *RedisDeduplicationStore) MarkProcessed(key string, ttl int64) error {
	// 使用 SETNX 确保只有第一个请求能设置成功
	cmd := s.client.SetNX(s.ctx, key, "1", time.Duration(ttl)*time.Second)
	//set, err := redis.SetnxExpire(s.ctx, key, "1", ttl)
	if cmd.Err() != nil {
		return fmt.Errorf("redis setnx failed: %w", cmd.Err())
	}

	if !cmd.Val() {
		// 如果key已存在，刷新过期时间
		if err := s.client.Expire(s.ctx, key, time.Duration(ttl)*time.Second).Err(); err != nil {
			return fmt.Errorf("redis expire failed: %w", err)
		}
	}

	return nil
}

// Close 关闭存储
func (s *RedisDeduplicationStore) Close() error {
	// Redis客户端通常由外部管理，这里不做关闭操作
	return s.client.Close()
}
