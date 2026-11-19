package nacos

import (
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/stretchr/testify/assert"
	"github.com/xm-utils/tools/registry"
)

func TestMapInstance(t *testing.T) {
	src := &model.Instance{
		InstanceId: "order#10.0.0.1#8080#DEFAULT",
		Ip:         "10.0.0.1",
		Port:       8080,
		Weight:     2.5,
		Healthy:    true,
		Enable:     true,
		Metadata:   map[string]string{"tags": "v1, canary", "zone": "az1"},
	}

	inst := mapInstance(src, "order")
	assert.Equal(t, src.InstanceId, inst.ID)
	assert.Equal(t, "order", inst.Name)
	assert.Equal(t, "10.0.0.1:8080", inst.Address())
	assert.Equal(t, []string{"v1", "canary"}, inst.Tags, "Meta[tags] 应拆分为 Tags")
	assert.Equal(t, 2.5, inst.Weight)
	assert.True(t, inst.Healthy)
}

func TestMapInstanceDisabledIsUnhealthy(t *testing.T) {
	src := &model.Instance{Ip: "10.0.0.1", Port: 8080, Healthy: true, Enable: false}
	inst := mapInstance(src, "order")
	assert.False(t, inst.Healthy, "实例被禁用应视为不健康")
}

func TestMapInstanceNoTags(t *testing.T) {
	src := &model.Instance{Ip: "10.0.0.1", Port: 80, Metadata: map[string]string{"zone": "az1"}}
	inst := mapInstance(src, "order")
	assert.Nil(t, inst.Tags)
}

func TestToNacosInfo(t *testing.T) {
	inst := registry.ServiceInstance{
		Name:   "order",
		Host:   "10.0.0.1",
		Port:   8080,
		Weight: 3,
		Tags:   []string{"v1", "canary"},
		Meta:   map[string]string{"zone": "az1"},
	}

	info := toNacosInfo(inst)
	assert.Equal(t, "order", info.Name)
	assert.Equal(t, "10.0.0.1", info.Host)
	assert.Equal(t, uint64(8080), info.Port)
	assert.Equal(t, 3.0, info.Weight)
	assert.Equal(t, "az1", info.Metadata["zone"])
	assert.Equal(t, "v1,canary", info.Metadata["tags"], "Tags 应并入 Metadata[tags]")
}

func TestToNacosInfoTagsOverrideSameKey(t *testing.T) {
	inst := registry.ServiceInstance{
		Name: "order", Host: "h", Port: 1,
		Tags: []string{"v2"},
		Meta: map[string]string{"tags": "v1"},
	}
	info := toNacosInfo(inst)
	assert.Equal(t, "v2", info.Metadata["tags"], "Tags 应覆盖 Meta 中同名键")
}

func TestMatchTags(t *testing.T) {
	assert.True(t, matchTags([]string{"a", "b"}, nil))
	assert.True(t, matchTags([]string{"a", "b"}, []string{"a"}))
	assert.True(t, matchTags([]string{"a", "b"}, []string{"b", "a"}))
	assert.False(t, matchTags([]string{"a"}, []string{"a", "b"}))
	assert.False(t, matchTags(nil, []string{"a"}))
}

func TestSplitTags(t *testing.T) {
	assert.Nil(t, splitTags(""))
	assert.Equal(t, []string{"a", "b"}, splitTags("a,b"))
	assert.Equal(t, []string{"a", "b"}, splitTags(" a , b "), "应去除空白元素")
	assert.Equal(t, []string{"a"}, splitTags("a,,b,")[0:1], "空元素应被剔除")
}

func TestValidateInstance(t *testing.T) {
	valid := registry.ServiceInstance{Name: "order", Host: "10.0.0.1", Port: 8080}
	assert.NoError(t, validateInstance(&valid))

	assert.ErrorIs(t, validateInstance(nil), registry.ErrInstanceEmpty)
	assert.ErrorIs(t, validateInstance(&registry.ServiceInstance{Host: "h", Port: 1}), registry.ErrServiceEmpty)
	assert.Error(t, validateInstance(&registry.ServiceInstance{Name: "order", Port: 1}))
	assert.Error(t, validateInstance(&registry.ServiceInstance{Name: "order", Host: "h"}))
}

func TestAdapterName(t *testing.T) {
	// 无需真实连接即可验证元信息
	a := NewFromNamingClient(nil)
	assert.Equal(t, "nacos", a.Name())
	assert.NoError(t, a.Close())

	ca := NewConfigAdapterFromClient(nil)
	assert.Equal(t, "nacos", ca.Name())
}

func TestConfigAdapterClosed(t *testing.T) {
	ca := NewConfigAdapterFromClient(nil)
	assert.NoError(t, ca.Close())
	assert.NoError(t, ca.Close(), "Close 应幂等")

	_, err := ca.Watch(t.Context(), "any-key")
	assert.Error(t, err, "关闭后 Watch 应报错")
}
