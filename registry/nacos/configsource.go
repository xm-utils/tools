package nacos

import (
	"context"
	"fmt"
	"sync"

	xmnacos "github.com/xm-utils/tools/nacos"
	"github.com/xm-utils/tools/registry/config"
)

// 编译期接口断言
var _ config.ConfigSource = (*ConfigAdapter)(nil)

// nacosConfigClient 适配器依赖的 Nacos 配置客户端能力集，
// *xmnacos.ConfigClientWrapper 天然实现；抽象为接口便于单测桩替换。
type nacosConfigClient interface {
	GetConfig(dataId string) (string, error)
	PublishConfig(dataId, content string) (bool, error)
	DeleteConfig(dataId string) (bool, error)
	ListenConfig(dataId string, listener func(namespace, group, dataId, data string)) error
	CancelListenConfig(dataId string) error
}

// ConfigAdapter 基于 Nacos 配置中心的配置源适配器，实现 config.ConfigSource。
// key 即 Nacos dataId，分组固定为客户端配置中的 GroupName。
type ConfigAdapter struct {
	client nacosConfigClient

	mu      sync.Mutex
	watches map[string]*watcher // key -> 活跃监听
	seq     int64               // watcher 令牌发号器
	closed  bool
}

// watcher 单次 Watch 的登记信息。
// id 用于 Close/覆盖时判定 SDK 监听注销是否由自己负责；
// mu+stopped 保证回调发送与 channel 关闭互斥，杜绝 send on closed channel。
type watcher struct {
	id      int64
	cancel  context.CancelFunc
	mu      sync.Mutex
	stopped bool
}

// deliver 向 ch 投递变更；已停止则静默丢弃。
// 缓冲满时丢弃旧值保留最新（配置监听只关心终态内容）。
// 仅由 SDK 回调 goroutine 调用，与清理 goroutine 的 close 通过 w.mu 互斥。
func (w *watcher) deliver(ch chan config.Change, change config.Change) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return
	}
	select {
	case ch <- change:
	default:
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- change:
		default:
		}
	}
}

// NewConfigAdapter 根据配置创建 Nacos 配置源适配器。要求 cfg.Enabled 为 true。
func NewConfigAdapter(cfg *xmnacos.Config) (*ConfigAdapter, error) {
	client, err := xmnacos.NewConfigClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("registry/nacos: %w", err)
	}
	return &ConfigAdapter{client: client, watches: make(map[string]*watcher)}, nil
}

// NewConfigAdapterFromClient 复用已有配置客户端创建适配器。
func NewConfigAdapterFromClient(client *xmnacos.ConfigClientWrapper) *ConfigAdapter {
	return &ConfigAdapter{client: client, watches: make(map[string]*watcher)}
}

// Name 返回后端标识。
func (c *ConfigAdapter) Name() string { return "nacos" }

// Get 读取配置。Nacos 对不存在的 dataId 返回空内容，统一映射为 config.ErrKeyNotFound。
func (c *ConfigAdapter) Get(ctx context.Context, key string) ([]byte, error) {
	if key == "" {
		return nil, config.ErrKeyEmpty
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	content, err := c.client.GetConfig(key)
	if err != nil {
		return nil, err
	}
	if content == "" {
		return nil, fmt.Errorf("%w: %s", config.ErrKeyNotFound, key)
	}
	return []byte(content), nil
}

// Put 发布配置。
func (c *ConfigAdapter) Put(ctx context.Context, key string, value []byte) error {
	if key == "" {
		return config.ErrKeyEmpty
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	success, err := c.client.PublishConfig(key, string(value))
	if err != nil {
		return err
	}
	if !success {
		return fmt.Errorf("registry/nacos: publish config %q rejected by server", key)
	}
	return nil
}

// Delete 删除配置。
func (c *ConfigAdapter) Delete(ctx context.Context, key string) error {
	if key == "" {
		return config.ErrKeyEmpty
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	success, err := c.client.DeleteConfig(key)
	if err != nil {
		return err
	}
	if !success {
		return fmt.Errorf("registry/nacos: delete config %q rejected by server", key)
	}
	return nil
}

// Watch 监听 dataId 变化，基于 Nacos ListenConfig 回调推送。
// ctx 取消、重复 Watch 同一 key（后者覆盖前者）或 Close 后旧 channel 关闭。
// SDK 监听以 dataId 为全局唯一注册项，因此注销必须与注册在同一临界区内串行化，
// 避免被覆盖的旧 watcher 误注销新监听。
func (c *ConfigAdapter) Watch(ctx context.Context, key string) (<-chan config.Change, error) {
	if key == "" {
		return nil, config.ErrKeyEmpty
	}

	watchCtx, cancel := context.WithCancel(ctx)
	ch := make(chan config.Change, 1)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("registry/nacos: config adapter already closed")
	}
	// 同一 key 重复 Watch：终止旧监听（覆盖语义）。
	// 旧清理 goroutine 会在 c.mu 上阻塞到本函数完成新注册，从而保证其注销判定时
	// 看到的已是新 owner，不会误删新监听。
	if old, ok := c.watches[key]; ok {
		old.cancel()
	}
	c.seq++
	w := &watcher{id: c.seq, cancel: cancel}
	c.watches[key] = w

	err := c.client.ListenConfig(key, func(namespace, group, dataId, data string) {
		w.deliver(ch, config.Change{Key: dataId, Value: []byte(data)})
	})
	if err != nil {
		// 注册失败：回收登记并注销（SDK 侧对未注册 key 的注销是安全的 no-op）
		delete(c.watches, key)
		_ = c.client.CancelListenConfig(key)
		c.mu.Unlock()
		cancel()
		w.mu.Lock()
		if !w.stopped {
			w.stopped = true
			close(ch)
		}
		w.mu.Unlock()
		return nil, err
	}
	c.mu.Unlock()

	// ctx 取消或适配器 Close 时：关闭 channel，并在仍为 owner 时注销 SDK 监听
	go func() {
		<-watchCtx.Done()
		w.mu.Lock()
		if !w.stopped {
			w.stopped = true
			close(ch)
		}
		w.mu.Unlock()

		c.mu.Lock()
		if cur, ok := c.watches[key]; ok && cur.id == w.id {
			delete(c.watches, key)
			// 监听已终止，注销失败不影响调用方
			_ = c.client.CancelListenConfig(key)
		}
		c.mu.Unlock()
	}()
	return ch, nil
}

// Close 注销所有 SDK 监听并释放资源（幂等）。
// 不整体清空 watches map，由各 watcher 的清理 goroutine 自行注销并回收登记，
// 保证 Close 路径上的 CancelListenConfig 与 owner 判定不竞态。
func (c *ConfigAdapter) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	watchers := make([]*watcher, 0, len(c.watches))
	for _, w := range c.watches {
		watchers = append(watchers, w)
	}
	c.mu.Unlock()

	for _, w := range watchers {
		w.cancel()
	}
	return nil
}
