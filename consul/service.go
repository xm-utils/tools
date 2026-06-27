package consul

import (
	"fmt"

	"github.com/hashicorp/consul/api"
)

// ServiceRegistration 服务注册信息
type ServiceRegistration struct {
	ServiceName string            // 服务名称
	ServiceID   string            // 服务 ID（唯一标识）
	Address     string            // 服务地址
	Port        int               // 服务端口
	Tags        []string          // 服务标签
	Meta        map[string]string // 服务元数据
	Checks      []*AgentCheck     // 健康检查配置
}

// AgentCheck 健康检查配置
type AgentCheck struct {
	CheckID                        string // 检查 ID
	Name                           string // 检查名称
	HTTP                           string // HTTP 检查地址
	Interval                       string // 检查间隔，例如: "10s"
	Timeout                        string // 超时时间，例如: "5s"
	TCP                            string // TCP 检查地址
	TTL                            string // TTL 检查时间
	Notes                          string // 备注
	DeregisterCriticalServiceAfter string // 严重故障后注销服务的时间
}

// RegisterService 注册服务
func (c *Client) RegisterService(reg *ServiceRegistration) error {
	if reg == nil {
		return fmt.Errorf("service registration cannot be nil")
	}
	if reg.ServiceName == "" {
		return fmt.Errorf("service name cannot be empty")
	}
	if reg.Address == "" {
		return fmt.Errorf("service address cannot be empty")
	}
	if reg.Port <= 0 {
		return fmt.Errorf("service port must be greater than 0")
	}
	// 如果没有提供 ServiceID，使用 ServiceName 作为 ID
	serviceID := reg.ServiceID
	if serviceID == "" {
		serviceID = reg.ServiceName
	}
	// 构建服务注册配置
	registration := &api.AgentServiceRegistration{
		ID:      serviceID,
		Name:    reg.ServiceName,
		Address: reg.Address,
		Port:    reg.Port,
		Tags:    reg.Tags,
		Meta:    reg.Meta,
	}

	// 添加健康检查
	if len(reg.Checks) > 0 {
		for _, check := range reg.Checks {
			agentCheck := &api.AgentServiceCheck{
				CheckID:                        check.CheckID,
				Name:                           check.Name,
				HTTP:                           check.HTTP,
				Interval:                       check.Interval,
				Timeout:                        check.Timeout,
				TCP:                            check.TCP,
				TTL:                            check.TTL,
				Notes:                          check.Notes,
				DeregisterCriticalServiceAfter: check.DeregisterCriticalServiceAfter,
			}
			registration.Checks = append(registration.Checks, agentCheck)
		}
	}

	// 注册服务
	err := c.consulClient.Agent().ServiceRegister(registration)
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}

	return nil
}

// DeregisterService 注销服务
func (c *Client) DeregisterService(serviceID string) error {
	if serviceID == "" {
		return fmt.Errorf("service ID cannot be empty")
	}

	err := c.consulClient.Agent().ServiceDeregister(serviceID)
	if err != nil {
		return fmt.Errorf("failed to deregister service: %w", err)
	}

	return nil
}

// GetLocalServices 获取本地代理上注册的所有服务
func (c *Client) GetLocalServices() (map[string]*api.AgentService, error) {
	services, err := c.consulClient.Agent().Services()
	if err != nil {
		return nil, fmt.Errorf("failed to get local services: %w", err)
	}

	return services, nil
}

// GetLocalService 获取本地代理上的指定服务
func (c *Client) GetLocalService(serviceID string) (*api.AgentService, error) {
	services, err := c.GetLocalServices()
	if err != nil {
		return nil, err
	}

	service, ok := services[serviceID]
	if !ok {
		return nil, fmt.Errorf("service %s not found", serviceID)
	}

	return service, nil
}
