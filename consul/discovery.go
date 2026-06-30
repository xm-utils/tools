package consul

import (
	"fmt"

	"github.com/hashicorp/consul/api"
	"github.com/sirupsen/logrus"
)

// ServiceDiscovery 服务发现结果
type ServiceDiscovery struct {
	Service *api.ServiceEntry
	Checks  []*api.HealthCheck
}

// DiscoverServices 根据服务名称发现服务
// serviceName: 服务名称
// passingOnly: 是否只返回健康状态为 passing 的服务
func (c *Client) DiscoverServices(serviceName string, passingOnly bool) ([]*ServiceDiscovery, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("service name cannot be empty")
	}

	entries, meta, err := c.consulClient.Health().Service(serviceName, "", passingOnly, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to discover services: %w", err)
	}

	logrus.Debugf("Query Meta: %+v", meta)

	var discoveries []*ServiceDiscovery
	for _, entry := range entries {
		discoveries = append(discoveries, &ServiceDiscovery{
			Service: entry,
			Checks:  entry.Checks,
		})
	}

	return discoveries, nil
}

// DiscoverServicesWithTags 根据服务名称和标签发现服务
// serviceName: 服务名称
// tags: 标签列表，服务必须包含所有指定的标签
// passingOnly: 是否只返回健康状态为 passing 的服务
func (c *Client) DiscoverServicesWithTags(serviceName string, tags []string, passingOnly bool) ([]*ServiceDiscovery, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("service name cannot be empty")
	}

	tagFilter := ""
	if len(tags) > 0 {
		// Consul 的 tag 过滤使用逗号分隔
		for i, tag := range tags {
			if i > 0 {
				tagFilter += ","
			}
			tagFilter += tag
		}
	}

	entries, meta, err := c.consulClient.Health().Service(serviceName, tagFilter, passingOnly, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to discover services with tags: %w", err)
	}

	logrus.Debugf("Query Meta: %+v", meta)

	var discoveries []*ServiceDiscovery
	for _, entry := range entries {
		discoveries = append(discoveries, &ServiceDiscovery{
			Service: entry,
			Checks:  entry.Checks,
		})
	}

	return discoveries, nil
}

// GetHealthyServices 获取所有健康的服务实例
func (c *Client) GetHealthyServices(serviceName string) ([]*ServiceDiscovery, error) {
	return c.DiscoverServices(serviceName, true)
}

// GetServiceAddresses 获取服务的地址列表
// 返回格式: ["ip:port", "ip:port", ...]
func (c *Client) GetServiceAddresses(serviceName string, passingOnly bool) ([]string, error) {
	services, err := c.DiscoverServices(serviceName, passingOnly)
	if err != nil {
		return nil, err
	}

	var addresses []string
	for _, svc := range services {
		address := fmt.Sprintf("%s:%d", svc.Service.Service.Address, svc.Service.Service.Port)
		addresses = append(addresses, address)
	}

	return addresses, nil
}

// GetServiceByID 根据服务 ID 获取服务信息
func (c *Client) GetServiceByID(serviceID string) (*api.CatalogService, error) {
	if serviceID == "" {
		return nil, fmt.Errorf("service ID cannot be empty")
	}

	services, _, err := c.consulClient.Catalog().Service(serviceID, "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get service by ID: %w", err)
	}

	if len(services) == 0 {
		return nil, fmt.Errorf("service %s not found", serviceID)
	}

	return services[0], nil
}

// GetAllServices 获取所有已注册的服务名称列表
func (c *Client) GetAllServices() (map[string][]string, error) {
	services, _, err := c.consulClient.Catalog().Services(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get all services: %w", err)
	}

	return services, nil
}

// WatchServices 监听服务变化（阻塞查询）
// lastIndex: 上次查询的索引，首次调用传 0
// timeout: 超时时间
// 返回: 服务列表、新的索引、错误
func (c *Client) WatchServices(serviceName string, passingOnly bool, lastIndex uint64, timeout string) ([]*ServiceDiscovery, uint64, error) {
	if serviceName == "" {
		return nil, 0, fmt.Errorf("service name cannot be empty")
	}

	opts := &api.QueryOptions{
		WaitIndex: lastIndex,
		WaitTime:  0, // 使用默认超时
	}

	if timeout != "" {
		// 可以解析 timeout 字符串设置 WaitTime
		// 这里简化处理，使用默认值
	}

	entries, meta, err := c.consulClient.Health().Service(serviceName, "", passingOnly, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to watch services: %w", err)
	}

	var discoveries []*ServiceDiscovery
	for _, entry := range entries {
		discoveries = append(discoveries, &ServiceDiscovery{
			Service: entry,
			Checks:  entry.Checks,
		})
	}

	return discoveries, meta.LastIndex, nil
}
