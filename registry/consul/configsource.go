package consul

import (
	"context"
	"fmt"
	"sync"
	"time"

	xmconsul "github.com/xm-utils/tools/consul"
	"github.com/xm-utils/tools/registry/config"
)

// 编译期接口断言
var _ config.ConfigSource = (*ConfigAdapter)(nil)

// watchBlockTimeout Consul WatchKV 单次阻塞查询时长。
// ctx 取消后最迟经过该时间关闭 Watch channel。
const watchBlockTimeout = 10 * time.Second

// consulKVClient 适配器依赖的 Consul KV 能力集，*xmconsul.Client 天然实现；
// 抽象为接口便于单测桩替换。
type consulKVClient interface {
	GetKV(key string) (*xmconsul.KVPair, error)
	PutKV(key string, value []byte) error
	DeleteKV(key string) error
	WatchKV(key string, lastIndex uint64, timeout time.Duration) (*xmconsul.KVPair, uint64, error)
}

// ConfigAdapter 基于 Consul KV 的配置源适配器，实现 config.ConfigSource。
type ConfigAdapter struct {
	client consulKVClient

	mu      sync.Mutex
	watches map[string]int // key -> 活跃 watcher 数（观测与调试用）
	closeCh chan struct{}  // Close 时关闭，联动终止所有 Watch
	closed  bool
}

// NewConfigAdapter 根据配置创建 Consul KV 配置源适配器。
func NewConfigAdapter(cfg *xmconsul.Config) (*ConfigAdapter, error) {
	client, err := xmconsul.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("registry/consul: %w", err)
	}
	return newConfigAdapter(client), nil
}

// NewConfigAdapterFromClient 复用已有 consul.Client 创建配置源适配器。
func NewConfigAdapterFromClient(client *xmconsul.Client) *ConfigAdapter {
	return newConfigAdapter(client)
}

func newConfigAdapter(client *xmconsul.Client) *ConfigAdapter {
	return &ConfigAdapter{
		client:  client,
		watches: make(map[string]int),
		closeCh: make(chan struct{}),
	}
}

// Name 返回后端标识。
func (c *ConfigAdapter) Name() string { return "consul" }

// Get 读取 KV。键不存在时返回 config.ErrKeyNotFound。
func (c *ConfigAdapter) Get(ctx context.Context, key string) ([]byte, error) {
	if key == "" {
		return nil, config.ErrKeyEmpty
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	pair, err := c.client.GetKV(key)
	if err != nil {
		return nil, err
	}
	if pair == nil {
		return nil, fmt.Errorf("%w: %s", config.ErrKeyNotFound, key)
	}
	return pair.Value, nil
}

// Put 写入 KV。
func (c *ConfigAdapter) Put(ctx context.Context, key string, value []byte) error {
	if key == "" {
		return config.ErrKeyEmpty
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return c.client.PutKV(key, value)
}

// Delete 删除 KV。
func (c *ConfigAdapter) Delete(ctx context.Context, key string) error {
	if key == "" {
		return config.ErrKeyEmpty
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return c.client.DeleteKV(key)
}

// Watch 监听 KV 变化。基于 Consul blocking query（WatchKV）实现，
// 首轮先取当前 ModifyIndex 作为基线（不推送现有值），后续仅推送变更/删除。
// ctx 取消或 Close 后 channel 关闭。
func (c *ConfigAdapter) Watch(ctx context.Context, key string) (<-chan config.Change, error) {
	if key == "" {
		return nil, config.ErrKeyEmpty
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, fmt.Errorf("registry/consul: config adapter already closed")
	}
	c.watches[key]++
	c.mu.Unlock()

	watchCtx, cancel := context.WithCancel(ctx)
	// 监控 Close：适配器关闭时终止本次 Watch
	go func() {
		defer cancel()
		select {
		case <-c.closeCh:
		case <-watchCtx.Done():
		}
	}()

	ch := make(chan config.Change, 1)

	go func() {
		defer func() {
			cancel()
			c.mu.Lock()
			if c.watches[key] > 0 {
				c.watches[key]--
			}
			c.mu.Unlock()
			close(ch)
		}()

		// 建立基线：记录当前索引，避免启动即推送旧值。
		// 基线查询失败时必须重试直到成功：若带着 idx=0 进入主循环，
		// WaitIndex=0 的查询会立即返回当前值并被误推一次。
		var idx uint64
		for {
			if watchCtx.Err() != nil {
				return
			}
			pair, curIdx, err := c.client.WatchKV(key, 0, 100*time.Millisecond)
			if err == nil {
				if pair != nil {
					idx = pair.ModifyIndex
				} else {
					idx = curIdx
				}
				break
			}
			select {
			case <-time.After(time.Second):
			case <-watchCtx.Done():
				return
			}
		}

		for {
			if watchCtx.Err() != nil {
				return
			}
			pair, newIdx, err := c.client.WatchKV(key, idx, watchBlockTimeout)
			if err != nil {
				if watchCtx.Err() != nil {
					return
				}
				// 瞬时错误：短暂退避后重试，避免错误风暴
				select {
				case <-time.After(time.Second):
				case <-watchCtx.Done():
					return
				}
				continue
			}
			if newIdx != idx {
				idx = newIdx
				var value []byte
				if pair != nil {
					value = pair.Value
				}
				select {
				case ch <- config.Change{Key: key, Value: value}:
				case <-watchCtx.Done():
					return
				}
			}
		}
	}()
	return ch, nil
}

// Close 终止所有 Watch 并释放资源（幂等）。
func (c *ConfigAdapter) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	close(c.closeCh)
	return nil
}
