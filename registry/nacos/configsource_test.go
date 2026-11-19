package nacos

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xm-utils/tools/registry/config"
)

// fakeConfigClient nacosConfigClient 测试桩：记录注册/注销，可手动触发变更回调。
type fakeConfigClient struct {
	mu         sync.Mutex
	listeners  map[string]func(namespace, group, dataId, data string)
	cancelled  []string
	failListen bool
}

func newFakeConfigClient() *fakeConfigClient {
	return &fakeConfigClient{listeners: make(map[string]func(string, string, string, string))}
}

func (f *fakeConfigClient) GetConfig(dataId string) (string, error) { return "v", nil }

func (f *fakeConfigClient) PublishConfig(dataId, content string) (bool, error) { return true, nil }

func (f *fakeConfigClient) DeleteConfig(dataId string) (bool, error) { return true, nil }

func (f *fakeConfigClient) ListenConfig(dataId string, listener func(namespace, group, dataId, data string)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failListen {
		return errors.New("listen failed")
	}
	f.listeners[dataId] = listener
	return nil
}

func (f *fakeConfigClient) CancelListenConfig(dataId string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.listeners, dataId)
	f.cancelled = append(f.cancelled, dataId)
	return nil
}

// trigger 模拟 Nacos 服务端推送变更（运行在 SDK 内部 goroutine 的回调路径）。
func (f *fakeConfigClient) trigger(dataId, data string) {
	f.mu.Lock()
	listener := f.listeners[dataId]
	f.mu.Unlock()
	if listener != nil {
		listener("ns", "grp", dataId, data)
	}
}

func (f *fakeConfigClient) cancelCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.cancelled)
}

func newTestConfigAdapter(f *fakeConfigClient) *ConfigAdapter {
	return &ConfigAdapter{client: f, watches: make(map[string]*watcher)}
}

func TestConfigWatchPushOnChange(t *testing.T) {
	f := newFakeConfigClient()
	c := newTestConfigAdapter(f)

	ch, err := c.Watch(context.Background(), "app.yaml")
	require.NoError(t, err)

	f.trigger("app.yaml", "v1")
	select {
	case change := <-ch:
		assert.Equal(t, "app.yaml", change.Key)
		assert.Equal(t, []byte("v1"), change.Value)
	case <-time.After(time.Second):
		t.Fatal("未收到变更推送")
	}
}

// TestConfigWatchOverwriteKeepsNewListener 覆盖语义回归：旧 watcher 被唤醒后
// 不得注销新 watcher 在 SDK 侧的全局监听（M1）。
func TestConfigWatchOverwriteKeepsNewListener(t *testing.T) {
	f := newFakeConfigClient()
	c := newTestConfigAdapter(f)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ch1, err := c.Watch(ctx1, "app.yaml")
	require.NoError(t, err)

	ch2, err := c.Watch(context.Background(), "app.yaml")
	require.NoError(t, err)

	// 旧 channel 应被关闭
	assertClosed(t, ch1)

	// 新监听必须仍然有效：触发推送，ch2 应收到
	require.Eventually(t, func() bool {
		f.trigger("app.yaml", "v2")
		select {
		case change := <-ch2:
			return string(change.Value) == "v2"
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond, "覆盖后新监听被旧 watcher 误注销")

	// 覆盖路径不应发生任何 SDK 注销
	assert.Equal(t, 0, f.cancelCount())
}

func TestConfigWatchCtxCancelClosesAndCancels(t *testing.T) {
	f := newFakeConfigClient()
	c := newTestConfigAdapter(f)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := c.Watch(ctx, "app.yaml")
	require.NoError(t, err)

	cancel()
	assertClosed(t, ch)
	require.Eventually(t, func() bool { return f.cancelCount() == 1 }, time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.watches) == 0
	}, time.Second, 10*time.Millisecond)
}

func TestConfigCloseCancelsAllOwnedWatches(t *testing.T) {
	f := newFakeConfigClient()
	c := newTestConfigAdapter(f)

	chA, err := c.Watch(context.Background(), "a.yaml")
	require.NoError(t, err)
	chB, err := c.Watch(context.Background(), "b.yaml")
	require.NoError(t, err)

	require.NoError(t, c.Close())
	assertClosed(t, chA)
	assertClosed(t, chB)
	require.Eventually(t, func() bool { return f.cancelCount() == 2 }, time.Second, 10*time.Millisecond)
	assert.NoError(t, c.Close()) // 幂等
}

// TestConfigWatchConcurrentTriggerAndCancel 回归 M2：SDK 回调发送与清理 goroutine
// 的 close 并发时不得 send on closed channel（需配合 -race 运行）。
func TestConfigWatchConcurrentTriggerAndCancel(t *testing.T) {
	f := newFakeConfigClient()
	c := newTestConfigAdapter(f)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := c.Watch(ctx, "app.yaml")
	require.NoError(t, err)

	// 不消费：缓冲置满后触发取消，再从多个 goroutine 高频回调
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				f.trigger("app.yaml", "x")
			}
		}()
	}
	cancel()
	wg.Wait()

	// 排空直至关闭；若存在 send-on-closed 竞态，进程 panic 会使测试失败
	for range ch { //nolint:revive // 排空 channel 直至关闭
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	assert.Empty(t, c.watches)
}

func TestConfigWatchListenFailureCleansUp(t *testing.T) {
	f := newFakeConfigClient()
	f.failListen = true
	c := newTestConfigAdapter(f)

	ch, err := c.Watch(context.Background(), "app.yaml")
	require.Error(t, err)
	assert.Nil(t, ch)

	c.mu.Lock()
	defer c.mu.Unlock()
	assert.Empty(t, c.watches)
}

func assertClosed(t *testing.T, ch <-chan config.Change) {
	t.Helper()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "channel 应已关闭")
	case <-time.After(time.Second):
		t.Fatal("channel 超时未关闭")
	}
}
