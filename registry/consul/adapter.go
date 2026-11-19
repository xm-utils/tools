// Package consul 提供基于 github.com/xm-utils/tools/consul 的注册中心适配器。
package consul

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/hashicorp/consul/api"
	xmconsul "github.com/xm-utils/tools/consul"
	"github.com/xm-utils/tools/registry"
)

// 编译期接口断言
var _ registry.Backend = (*Adapter)(nil)

// Adapter Consul 注册中心适配器，实现 registry.Backend。
type Adapter struct {
	client *xmconsul.Client
}

// New 根据配置创建适配器（内部会测试与 Consul 的连通性）。
func New(cfg *xmconsul.Config) (*Adapter, error) {
	client, err := xmconsul.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("registry/consul: %w", err)
	}
	return &Adapter{client: client}, nil
}

// NewFromClient 复用已有 consul.Client 创建适配器（便于与现有代码共享连接）。
func NewFromClient(client *xmconsul.Client) *Adapter {
	return &Adapter{client: client}
}

// Name 返回后端标识。
func (a *Adapter) Name() string { return "consul" }

// Close Consul SDK 无需显式释放，恒返回 nil（幂等）。
func (a *Adapter) Close() error { return nil }

// Register 注册服务实例。
// Instance.ID 为空时自动生成 "name-host-port" 作为 ServiceID。
// Checks 转换为 Consul AgentCheck（HTTP/TCP/TTL 三种类型按非空字段取用）。
func (a *Adapter) Register(ctx context.Context, req *registry.RegisterRequest) error {
	if req == nil {
		return registry.ErrInstanceEmpty
	}
	if err := validateInstance(&req.Instance); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	inst := req.Instance
	serviceID := inst.ID
	if serviceID == "" {
		serviceID = defaultServiceID(inst.Name, inst.Host, inst.Port)
	}

	reg := &xmconsul.ServiceRegistration{
		ServiceName: inst.Name,
		ServiceID:   serviceID,
		Address:     inst.Host,
		Port:        inst.Port,
		Tags:        inst.Tags,
		Meta:        inst.Meta,
		Checks:      convertChecks(inst, req.Checks),
	}
	return a.client.RegisterService(reg)
}

// Deregister 按实例 ID 注销；ID 为空时回退尝试 "name-host-port" 默认 ID。
func (a *Adapter) Deregister(ctx context.Context, inst registry.ServiceInstance) error {
	if inst.Name == "" {
		return registry.ErrServiceEmpty
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	serviceID := inst.ID
	if serviceID == "" {
		serviceID = defaultServiceID(inst.Name, inst.Host, inst.Port)
	}
	return a.client.DeregisterService(serviceID)
}

// Services 查询服务实例。带 Tags 条件时走 Consul 原生 tag 过滤。
func (a *Adapter) Services(ctx context.Context, req registry.DiscoverRequest) ([]*registry.ServiceInstance, error) {
	if req.Name == "" {
		return nil, registry.ErrServiceEmpty
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var (
		entries []*xmconsul.ServiceDiscovery
		err     error
	)
	if len(req.Tags) > 0 {
		entries, err = a.client.DiscoverServicesWithTags(req.Name, req.Tags, req.HealthyOnly)
	} else {
		entries, err = a.client.DiscoverServices(req.Name, req.HealthyOnly)
	}
	if err != nil {
		return nil, err
	}

	instances := make([]*registry.ServiceInstance, 0, len(entries))
	for _, entry := range entries {
		inst := mapServiceEntry(entry)
		if inst == nil {
			continue
		}
		instances = append(instances, inst)
	}
	return instances, nil
}

// AllServices 返回 Consul 中所有已注册的服务名（已排序）。
func (a *Adapter) AllServices(ctx context.Context) ([]string, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	services, err := a.client.GetAllServices()
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// mapServiceEntry 将 Consul 服务发现结果映射为统一模型。
// 健康判定：所有 check 状态为 passing 视为健康；无 check 视为健康。
func mapServiceEntry(entry *xmconsul.ServiceDiscovery) *registry.ServiceInstance {
	if entry == nil || entry.Service == nil {
		return nil
	}
	svc := entry.Service.Service
	if svc == nil {
		return nil
	}

	inst := &registry.ServiceInstance{
		ID:      svc.ID,
		Name:    svc.Service,
		Host:    svc.Address,
		Port:    svc.Port,
		Tags:    svc.Tags,
		Meta:    svc.Meta,
		Healthy: isHealthy(unionChecks(entry)),
	}
	// Consul 注册时 Address 可为空（回退节点地址），查询结果同步回退
	if inst.Host == "" && entry.Service.Node != nil {
		inst.Host = entry.Service.Node.Address
	}
	return inst
}

// unionChecks 优先取 ServiceDiscovery.Checks，为空时回退 ServiceEntry 内的原始检查列表。
func unionChecks(entry *xmconsul.ServiceDiscovery) api.HealthChecks {
	if len(entry.Checks) > 0 {
		return entry.Checks
	}
	if entry.Service != nil {
		return entry.Service.Checks
	}
	return nil
}

// isHealthy 检查 HealthChecks 是否全部 passing。
func isHealthy(checks api.HealthChecks) bool {
	for _, check := range checks {
		if check.Status != "passing" {
			return false
		}
	}
	return true
}

// convertChecks 将抽象健康检查转换为 Consul AgentCheck。
// specs 为空时不注册任何健康检查（Consul 对无检查的服务视为始终通过）。
func convertChecks(inst registry.ServiceInstance, specs []registry.HealthCheckSpec) []*xmconsul.AgentCheck {
	checks := make([]*xmconsul.AgentCheck, 0, len(specs))
	for i, spec := range specs {
		check := &xmconsul.AgentCheck{
			CheckID: fmt.Sprintf("%s-check-%d", inst.Name, i),
			Name:    fmt.Sprintf("health check %d", i),
			HTTP:    spec.HTTP,
			TCP:     spec.TCP,
		}
		if spec.Interval > 0 {
			check.Interval = spec.Interval.String()
		}
		if spec.Timeout > 0 {
			check.Timeout = spec.Timeout.String()
		}
		if spec.TTL > 0 {
			check.TTL = spec.TTL.String()
		}
		checks = append(checks, check)
	}
	return checks
}

func defaultServiceID(name, host string, port int) string {
	return strings.Join([]string{name, host, fmt.Sprintf("%d", port)}, "-")
}

func validateInstance(inst *registry.ServiceInstance) error {
	if inst == nil {
		return registry.ErrInstanceEmpty
	}
	if inst.Name == "" {
		return registry.ErrServiceEmpty
	}
	if inst.Host == "" {
		return fmt.Errorf("registry: instance host cannot be empty")
	}
	if inst.Port <= 0 || inst.Port > 65535 {
		return fmt.Errorf("registry: instance port must be in (0, 65535], got %d", inst.Port)
	}
	return nil
}
