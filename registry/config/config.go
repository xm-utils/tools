// Package config 提供配置中心的统一抽象，与 registry 服务抽象配套。
//
// 业务代码只依赖 ConfigSource 接口；consul KV / nacos 配置由
// registry/consul、registry/nacos 子包提供适配器实现。
package config

import (
	"context"
	"errors"
)

// 通用错误
var (
	// ErrKeyEmpty 配置键为空
	ErrKeyEmpty = errors.New("config: key cannot be empty")
	// ErrKeyNotFound 配置键不存在
	ErrKeyNotFound = errors.New("config: key not found")
)

// Change 配置变更事件。
type Change struct {
	Key   string // 配置键（consul: KV 路径；nacos: dataId）
	Value []byte // 变更后的内容；配置被删除时长度为 0
}

// ConfigSource 配置源能力：读写 + 变更监听。
//
// 实现要求：
//   - Get 在键不存在时返回 ErrKeyNotFound（可用 errors.Is 判定）；
//   - Watch 在 ctx 取消后必须关闭返回的 channel 并停止后台监听；
//   - 同一 key 重复 Watch 的行为由实现决定（建议幂等或返回错误）。
type ConfigSource interface {
	// Get 读取配置内容。
	Get(ctx context.Context, key string) ([]byte, error)
	// Put 写入配置内容（存在则覆盖）。
	Put(ctx context.Context, key string, value []byte) error
	// Delete 删除配置。
	Delete(ctx context.Context, key string) error
	// Watch 监听某个 key 的内容变化，首次调用不推送当前值，仅推送后续变更。
	Watch(ctx context.Context, key string) (<-chan Change, error)
	// Name 返回后端类型标识："consul" / "nacos"。
	Name() string
	// Close 释放底层资源（幂等），并终止所有 Watch。
	Close() error
}
