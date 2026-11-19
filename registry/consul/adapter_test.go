package consul

import (
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	xmconsul "github.com/xm-utils/tools/consul"
	"github.com/xm-utils/tools/registry"
)

func newEntry(id, name, addr string, port int, checkStatus string) *xmconsul.ServiceDiscovery {
	return &xmconsul.ServiceDiscovery{
		Service: &api.ServiceEntry{
			Node: &api.Node{Address: "10.0.0.9"},
			Service: &api.AgentService{
				ID:      id,
				Service: name,
				Address: addr,
				Port:    port,
				Tags:    []string{"v1"},
				Meta:    map[string]string{"zone": "az1"},
			},
			Checks: api.HealthChecks{
				&api.HealthCheck{Status: checkStatus},
			},
		},
	}
}

func TestMapServiceEntry(t *testing.T) {
	entry := newEntry("order-1", "order", "10.0.0.1", 8080, "passing")
	inst := mapServiceEntry(entry)

	assert.Equal(t, "order-1", inst.ID)
	assert.Equal(t, "order", inst.Name)
	assert.Equal(t, "10.0.0.1:8080", inst.Address())
	assert.Equal(t, []string{"v1"}, inst.Tags)
	assert.Equal(t, map[string]string{"zone": "az1"}, inst.Meta)
	assert.True(t, inst.Healthy)
}

func TestMapServiceEntryHostFallbackToNode(t *testing.T) {
	entry := newEntry("order-1", "order", "", 8080, "passing")
	inst := mapServiceEntry(entry)
	assert.Equal(t, "10.0.0.9", inst.Host, "Service.Address 为空时应回退节点地址")
}

func TestMapServiceEntryUnhealthy(t *testing.T) {
	entry := newEntry("order-1", "order", "10.0.0.1", 8080, "critical")
	inst := mapServiceEntry(entry)
	assert.False(t, inst.Healthy)
}

func TestMapServiceEntryNil(t *testing.T) {
	assert.Nil(t, mapServiceEntry(nil))
	assert.Nil(t, mapServiceEntry(&xmconsul.ServiceDiscovery{}))
}

func TestIsHealthy(t *testing.T) {
	assert.True(t, isHealthy(nil), "无检查视为健康")
	assert.True(t, isHealthy(api.HealthChecks{
		&api.HealthCheck{Status: "passing"},
		&api.HealthCheck{Status: "passing"},
	}))
	assert.False(t, isHealthy(api.HealthChecks{
		&api.HealthCheck{Status: "passing"},
		&api.HealthCheck{Status: "warning"},
	}))
}

func TestConvertChecks(t *testing.T) {
	inst := registry.ServiceInstance{Name: "order"}
	specs := []registry.HealthCheckSpec{
		{HTTP: "http://10.0.0.1:8080/health", Interval: 10 * time.Second, Timeout: 2 * time.Second},
		{TCP: "10.0.0.1:8080", Interval: time.Minute},
		{TTL: 30 * time.Second},
	}

	checks := convertChecks(inst, specs)
	assert.Len(t, checks, 3)

	assert.Equal(t, "order-check-0", checks[0].CheckID)
	assert.Equal(t, "http://10.0.0.1:8080/health", checks[0].HTTP)
	assert.Equal(t, "10s", checks[0].Interval)
	assert.Equal(t, "2s", checks[0].Timeout)

	assert.Equal(t, "10.0.0.1:8080", checks[1].TCP)
	assert.Equal(t, "1m0s", checks[1].Interval)
	assert.Empty(t, checks[1].Timeout)

	assert.Equal(t, "30s", checks[2].TTL)
}

func TestDefaultServiceID(t *testing.T) {
	assert.Equal(t, "order-10.0.0.1-8080", defaultServiceID("order", "10.0.0.1", 8080))
}

func TestValidateInstance(t *testing.T) {
	valid := registry.ServiceInstance{Name: "order", Host: "10.0.0.1", Port: 8080}
	assert.NoError(t, validateInstance(&valid))

	assert.ErrorIs(t, validateInstance(nil), registry.ErrInstanceEmpty)

	noName := registry.ServiceInstance{Host: "10.0.0.1", Port: 80}
	assert.ErrorIs(t, validateInstance(&noName), registry.ErrServiceEmpty)

	noHost := registry.ServiceInstance{Name: "order", Port: 80}
	assert.Error(t, validateInstance(&noHost))

	badPort := registry.ServiceInstance{Name: "order", Host: "10.0.0.1", Port: 0}
	assert.Error(t, validateInstance(&badPort))

	overPort := registry.ServiceInstance{Name: "order", Host: "10.0.0.1", Port: 70000}
	assert.Error(t, validateInstance(&overPort))
}

func TestAdapterName(t *testing.T) {
	// 无需真实连接即可验证元信息
	a := NewFromClient(nil)
	assert.Equal(t, "consul", a.Name())
	assert.NoError(t, a.Close())
}
