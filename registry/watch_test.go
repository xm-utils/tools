package registry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDiscovery 可编程的 Discovery 桩，用于 Watch 测试。
type fakeDiscovery struct {
	mu        sync.Mutex
	instances []*ServiceInstance
	err       error
	calls     int
}

func (f *fakeDiscovery) Services(ctx context.Context, req DiscoverRequest) ([]*ServiceInstance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	out := make([]*ServiceInstance, 0, len(f.instances))
	for _, inst := range f.instances {
		copied := *inst
		out = append(out, &copied)
	}
	return out, nil
}

func (f *fakeDiscovery) AllServices(ctx context.Context) ([]string, error) { return nil, nil }

func (f *fakeDiscovery) set(instances []*ServiceInstance) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.instances = instances
}

func (f *fakeDiscovery) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeDiscovery) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func newInst(id string, healthy bool) *ServiceInstance {
	return &ServiceInstance{ID: id, Name: "svc", Host: "10.0.0.1", Port: 8080, Healthy: healthy}
}

func TestWatchValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := Watch(ctx, nil, "svc", WatchConfig{}, nil)
	assert.ErrorIs(t, err, ErrUnsupported)

	d := &fakeDiscovery{}
	_, err = Watch(ctx, d, "", WatchConfig{}, nil)
	assert.ErrorIs(t, err, ErrServiceEmpty)
}

func TestWatchPushesInitialSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := &fakeDiscovery{instances: []*ServiceInstance{newInst("a", true)}}
	ch, err := Watch(ctx, d, "svc", WatchConfig{Interval: 10 * time.Millisecond}, nil)
	require.NoError(t, err)

	select {
	case got := <-ch:
		require.Len(t, got, 1)
		assert.Equal(t, "a", got[0].ID)
	case <-time.After(time.Second):
		t.Fatal("initial snapshot not received")
	}
}

func TestWatchPushesOnlyOnChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := &fakeDiscovery{instances: []*ServiceInstance{newInst("a", true)}}
	ch, err := Watch(ctx, d, "svc", WatchConfig{Interval: 10 * time.Millisecond}, nil)
	require.NoError(t, err)

	// 消费初始快照
	<-ch

	// 相同内容轮询多次，不应再推送
	time.Sleep(80 * time.Millisecond)
	require.Greater(t, d.callCount(), 2, "轮询应持续进行")
	select {
	case snap := <-ch:
		t.Fatalf("无变化不应推送, got %v", snap)
	default:
	}

	// 变更实例健康状态，应推送新快照
	d.set([]*ServiceInstance{newInst("a", false)})
	select {
	case got := <-ch:
		require.Len(t, got, 1)
		assert.False(t, got[0].Healthy)
	case <-time.After(time.Second):
		t.Fatal("change not pushed")
	}

	// 新增实例，应再次推送
	d.set([]*ServiceInstance{newInst("a", false), newInst("b", true)})
	select {
	case got := <-ch:
		require.Len(t, got, 2)
	case <-time.After(time.Second):
		t.Fatal("addition not pushed")
	}
}

func TestWatchOrderInsensitive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ab := []*ServiceInstance{newInst("a", true), newInst("b", true)}
	ba := []*ServiceInstance{newInst("b", true), newInst("a", true)}

	d := &fakeDiscovery{instances: ab}
	ch, err := Watch(ctx, d, "svc", WatchConfig{Interval: 10 * time.Millisecond}, nil)
	require.NoError(t, err)
	<-ch // 初始快照

	// 仅顺序变化不算变更
	d.set(ba)
	time.Sleep(80 * time.Millisecond)
	select {
	case snap := <-ch:
		t.Fatalf("顺序变化不应推送, got %v", snap)
	default:
	}
}

func TestWatchErrorCallbackAndContinue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := &fakeDiscovery{instances: []*ServiceInstance{newInst("a", true)}}
	errCh := make(chan error, 4)
	ch, err := Watch(ctx, d, "svc", WatchConfig{Interval: 10 * time.Millisecond}, func(e error) {
		errCh <- e
	})
	require.NoError(t, err)
	<-ch // 初始快照

	boom := errors.New("boom")
	d.setErr(boom)
	select {
	case e := <-errCh:
		assert.ErrorIs(t, e, boom)
	case <-time.After(time.Second):
		t.Fatal("onError not invoked")
	}

	// 错误恢复后轮询继续，内容变化仍能推送
	d.setErr(nil)
	d.set([]*ServiceInstance{newInst("a", true), newInst("c", true)})
	select {
	case got := <-ch:
		assert.Len(t, got, 2)
	case <-time.After(time.Second):
		t.Fatal("watch did not resume after error")
	}
}

func TestWatchClosedOnCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	d := &fakeDiscovery{instances: []*ServiceInstance{newInst("a", true)}}
	ch, err := Watch(ctx, d, "svc", WatchConfig{Interval: 10 * time.Millisecond}, nil)
	require.NoError(t, err)

	cancel()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // channel 已关闭，符合预期
			}
			// 丢弃关闭前缓冲的快照，继续等待关闭信号
		case <-deadline:
			t.Fatal("channel not closed after cancel")
		}
	}
}

func TestSignatureStable(t *testing.T) {
	a := []*ServiceInstance{{ID: "1", Host: "h", Port: 1, Healthy: true}}
	b := []*ServiceInstance{{ID: "1", Host: "h", Port: 1, Healthy: true}, nil}
	assert.Equal(t, signature(a), signature(b), "nil 元素应被忽略")

	c := []*ServiceInstance{{ID: "2", Host: "h", Port: 2}}
	assert.NotEqual(t, signature(a), signature(c))
}

func TestServiceInstanceAddress(t *testing.T) {
	inst := ServiceInstance{Host: "127.0.0.1", Port: 8500}
	assert.Equal(t, "127.0.0.1:8500", inst.Address())
}
