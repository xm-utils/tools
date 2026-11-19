package consul

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	xmconsul "github.com/xm-utils/tools/consul"
)

// kvResult 脚本化的一次 WatchKV 应答。
type kvResult struct {
	pair *xmconsul.KVPair
	idx  uint64
	err  error
}

// fakeKV consulKVClient 测试桩：WatchKV 按脚本顺序应答；脚本耗尽后返回错误，
// 使适配器进入可被 ctx 取消的退避重试路径，避免阻塞测试收尾。
type fakeKV struct {
	mu     sync.Mutex
	script []kvResult
}

func newFakeKV(script ...kvResult) *fakeKV {
	return &fakeKV{script: script}
}

func (f *fakeKV) WatchKV(key string, lastIndex uint64, timeout time.Duration) (*xmconsul.KVPair, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.script) > 0 {
		r := f.script[0]
		f.script = f.script[1:]
		return r.pair, r.idx, r.err
	}
	return nil, 0, io.EOF
}

func (f *fakeKV) GetKV(key string) (*xmconsul.KVPair, error) { return nil, nil }
func (f *fakeKV) PutKV(key string, value []byte) error       { return nil }
func (f *fakeKV) DeleteKV(key string) error                  { return nil }

func newTestConfigAdapter(f *fakeKV) *ConfigAdapter {
	return &ConfigAdapter{
		client:  f,
		watches: make(map[string]int),
		closeCh: make(chan struct{}),
	}
}

func kvPair(value string, idx uint64) *xmconsul.KVPair {
	return &xmconsul.KVPair{Key: "k", Value: []byte(value), ModifyIndex: idx}
}

// TestConfigWatchBaselineErrorNoSpuriousPush 回归 N1：基线查询失败后必须重试建立基线，
// 首条真实推送只能是基线之后的变更，而非当前旧值。
func TestConfigWatchBaselineErrorNoSpuriousPush(t *testing.T) {
	// 脚本：基线失败 → 基线成功(V@3) → 主循环返回变更(V2@4)
	f := newFakeKV(
		kvResult{err: io.EOF},
		kvResult{pair: kvPair("V", 3)},
		kvResult{pair: kvPair("V2", 4)},
	)
	c := newTestConfigAdapter(f)

	ch, err := c.Watch(t.Context(), "app/config")
	require.NoError(t, err)

	// 基线失败重试退避 1s，之后应只收到 V2
	select {
	case change := <-ch:
		assert.Equal(t, []byte("V2"), change.Value, "基线失败后不应误推当前旧值")
	case <-time.After(3 * time.Second):
		t.Fatal("未收到变更推送")
	}
}

func TestConfigWatchPushOnChange(t *testing.T) {
	f := newFakeKV(
		kvResult{pair: kvPair("V", 3)},
		kvResult{pair: kvPair("V2", 4)},
	)
	c := newTestConfigAdapter(f)

	ch, err := c.Watch(t.Context(), "app/config")
	require.NoError(t, err)

	select {
	case change := <-ch:
		assert.Equal(t, "app/config", change.Key)
		assert.Equal(t, []byte("V2"), change.Value)
	case <-time.After(2 * time.Second):
		t.Fatal("未收到变更推送")
	}
}

func TestConfigWatchKeyAbsentBaseline(t *testing.T) {
	// 键不存在：基线取 curIdx；随后键被创建应推送
	f := newFakeKV(
		kvResult{pair: nil, idx: 7},
		kvResult{pair: kvPair("V8", 8)},
	)
	c := newTestConfigAdapter(f)

	ch, err := c.Watch(t.Context(), "app/config")
	require.NoError(t, err)

	select {
	case change := <-ch:
		assert.Equal(t, []byte("V8"), change.Value)
	case <-time.After(2 * time.Second):
		t.Fatal("未收到新建键推送")
	}
}

func TestConfigWatchCtxCancelClosesChannel(t *testing.T) {
	f := newFakeKV(kvResult{pair: kvPair("V", 3)})
	c := newTestConfigAdapter(f)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := c.Watch(ctx, "app/config")
	require.NoError(t, err)

	cancel()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "ctx 取消后 channel 应关闭")
	case <-time.After(watchBlockTimeout + time.Second):
		t.Fatal("channel 超时未关闭")
	}
}

func TestConfigCloseTerminatesWatchAndIdempotent(t *testing.T) {
	f := newFakeKV(kvResult{pair: kvPair("V", 3)})
	c := newTestConfigAdapter(f)

	ch, err := c.Watch(context.Background(), "app/config")
	require.NoError(t, err)

	require.NoError(t, c.Close())
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "Close 后 channel 应关闭")
	case <-time.After(watchBlockTimeout + time.Second):
		t.Fatal("channel 超时未关闭")
	}
	assert.NoError(t, c.Close()) // 幂等

	_, err = c.Watch(context.Background(), "app/config")
	assert.Error(t, err, "Close 后 Watch 应报错")
}
