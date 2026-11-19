// Package nacos 提供基于 github.com/xm-utils/tools/nacos 的注册中心适配器。
package nacos

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	xmnacos "github.com/xm-utils/tools/nacos"
	"github.com/xm-utils/tools/registry"
)

// metaTagsKey Nacos 无原生 tag 概念，Tags 以逗号拼接后存入该 Meta 键。
const metaTagsKey = "tags"

// 编译期接口断言
var _ registry.Backend = (*Adapter)(nil)

// Adapter Nacos 注册中心适配器，实现 registry.Backend。
type Adapter struct {
	naming *xmnacos.NamingClientWrapper
}

// New 根据配置创建适配器。要求 cfg.Enabled 为 true。
func New(cfg *xmnacos.Config) (*Adapter, error) {
	naming, err := xmnacos.NewNamingClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("registry/nacos: %w", err)
	}
	return &Adapter{naming: naming}, nil
}

// NewFromNamingClient 复用已有命名客户端创建适配器。
func NewFromNamingClient(naming *xmnacos.NamingClientWrapper) *Adapter {
	return &Adapter{naming: naming}
}

// Name 返回后端标识。
func (a *Adapter) Name() string { return "nacos" }

// Close Nacos SDK 由内部 goroutine 维持心跳，无显式释放接口，恒返回 nil（幂等）。
func (a *Adapter) Close() error { return nil }

// Register 注册服务实例（临时实例，SDK 自动维持心跳）。
// Checks 声明会被忽略——Nacos 心跳机制替代主动健康检查。
// Tags 会并入 Metadata["tags"]（逗号分隔）。
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
	info := toNacosInfo(inst)
	if info.Weight <= 0 {
		info.Weight = 1
	}
	return a.naming.RegisterInstance(info)
}

// Deregister 按 Host:Port 注销实例。
// 底层 wrapper 的 DeregisterInstance 不传 Ip/Port（服务端要求必填），
// 这里通过 GetClient() 直连 SDK 传入完整参数。
func (a *Adapter) Deregister(ctx context.Context, inst registry.ServiceInstance) error {
	if inst.Name == "" {
		return registry.ErrServiceEmpty
	}
	if inst.Host == "" || inst.Port <= 0 {
		return fmt.Errorf("registry/nacos: host and port are required to deregister")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	success, err := a.naming.GetClient().DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          inst.Host,
		Port:        uint64(inst.Port),
		ServiceName: inst.Name,
		GroupName:   a.naming.GetGroupName(),
		Cluster:     a.naming.GetClusterName(),
		Ephemeral:   true,
	})
	if err != nil {
		return fmt.Errorf("registry/nacos: deregister failed: %w", err)
	}
	if !success {
		return fmt.Errorf("registry/nacos: deregister rejected by server")
	}
	return nil
}

// Services 查询服务实例列表。Tags 条件由适配器基于 Metadata["tags"] 在结果内过滤。
func (a *Adapter) Services(ctx context.Context, req registry.DiscoverRequest) ([]*registry.ServiceInstance, error) {
	if req.Name == "" {
		return nil, registry.ErrServiceEmpty
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	instances, err := a.naming.SelectInstances(req.Name, req.HealthyOnly)
	if err != nil {
		return nil, err
	}

	result := make([]*registry.ServiceInstance, 0, len(instances))
	for i := range instances {
		inst := mapInstance(&instances[i], req.Name)
		if !matchTags(inst.Tags, req.Tags) {
			continue
		}
		result = append(result, inst)
	}
	return result, nil
}

// AllServices 分页拉取 Nacos 中所有已注册的服务名（已排序）。
func (a *Adapter) AllServices(ctx context.Context) ([]string, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	const pageSize = uint32(200)
	var names []string
	for pageNo := uint32(1); ; pageNo++ {
		list, err := a.naming.GetAllServices(pageNo, pageSize)
		if err != nil {
			return nil, err
		}
		names = append(names, list.Doms...)
		if uint32(len(list.Doms)) < pageSize || int64(len(names)) >= list.Count {
			break
		}
	}
	sort.Strings(names)
	return names, nil
}

// toNacosInfo 将统一模型转换为 wrapper 的实例信息（Tags 并入 Metadata）。
func toNacosInfo(inst registry.ServiceInstance) xmnacos.ServiceInstanceInfo {
	metadata := make(map[string]string, len(inst.Meta)+1)
	for k, v := range inst.Meta {
		metadata[k] = v
	}
	if len(inst.Tags) > 0 {
		metadata[metaTagsKey] = strings.Join(inst.Tags, ",")
	}

	return xmnacos.ServiceInstanceInfo{
		Name:     inst.Name,
		Host:     inst.Host,
		Port:     uint64(inst.Port),
		Weight:   inst.Weight,
		Metadata: metadata,
	}
}

// mapInstance 将 Nacos model.Instance 映射为统一模型。
func mapInstance(src *model.Instance, serviceName string) *registry.ServiceInstance {
	tags := splitTags(src.Metadata[metaTagsKey])

	return &registry.ServiceInstance{
		ID:      src.InstanceId,
		Name:    serviceName,
		Host:    src.Ip,
		Port:    int(src.Port),
		Tags:    tags,
		Meta:    src.Metadata,
		Healthy: src.Healthy && src.Enable,
		Weight:  src.Weight,
	}
}

// matchTags 判断实例标签是否覆盖全部过滤标签。
func matchTags(instTags, wantTags []string) bool {
	if len(wantTags) == 0 {
		return true
	}
	set := make(map[string]struct{}, len(instTags))
	for _, t := range instTags {
		set[t] = struct{}{}
	}
	for _, want := range wantTags {
		if _, ok := set[want]; !ok {
			return false
		}
	}
	return true
}

func splitTags(joined string) []string {
	if joined == "" {
		return nil
	}
	parts := strings.Split(joined, ",")
	tags := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			tags = append(tags, p)
		}
	}
	return tags
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
