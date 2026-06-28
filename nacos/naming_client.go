package nacos

import (
	"errors"
	"fmt"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// NamingClientWrapper Nacos服务发现客户端包装器（闭包模式）
type NamingClientWrapper struct {
	client      naming_client.INamingClient
	groupName   string
	clusterName string
}

// NewNamingClient 创建服务发现客户端
func NewNamingClient(config *Config) (*NamingClientWrapper, error) {
	if config == nil {
		return nil, errors.New("nacos config is nil")
	}
	if !config.Enabled {
		return nil, errors.New("nacos is not enabled")
	}

	serverConfigs, clientConfig := config.getConfig()

	// 创建命名客户端
	client, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  clientConfig,
			ServerConfigs: serverConfigs,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("create naming client failed: %w", err)
	}

	wrapper := &NamingClientWrapper{
		client:      client,
		groupName:   config.ClientConfig.GroupName,
		clusterName: config.ClientConfig.ClusterName,
	}

	return wrapper, nil
}

// ServiceInstanceInfo 服务实例信息
type ServiceInstanceInfo struct {
	Name     string
	Host     string
	Port     uint64
	Weight   float64
	Metadata map[string]string
}

// RegisterInstance 注册服务实例
func (n *NamingClientWrapper) RegisterInstance(info ServiceInstanceInfo) error {
	if n.client == nil {
		return errors.New("naming client not initialized")
	}

	if info.Host == "" {
		localIP, err := GetLocalIP()
		if err != nil {
			return errors.New("the service host cannot be empty")
		}
		info.Host = localIP
	}

	success, err := n.client.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          info.Host,
		Port:        info.Port,
		Weight:      info.Weight,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true, // 临时实例，服务断开后会自动删除（推荐）
		Metadata:    info.Metadata,
		ServiceName: info.Name,
		GroupName:   n.groupName,
		ClusterName: n.clusterName,
	})
	if !success || err != nil {
		return fmt.Errorf("service registration failed. error: %v", err)
	}
	return nil
}

// DeregisterInstance 注销服务实例
func (n *NamingClientWrapper) DeregisterInstance(name string) (bool, error) {
	if n.client == nil {
		return false, errors.New("naming client not initialized")
	}

	success, err := n.client.DeregisterInstance(vo.DeregisterInstanceParam{
		ServiceName: name,
		GroupName:   n.groupName,
		Cluster:     n.clusterName,
		Ephemeral:   true,
	})
	if err != nil {
		return false, fmt.Errorf("deregister instance failed: %w", err)
	}
	return success, nil
}

// SelectOneHealthyInstance 选择一个健康的实例（负载均衡）
func (n *NamingClientWrapper) SelectOneHealthyInstance(serviceName string) (*model.Instance, error) {
	if n.client == nil {
		return nil, errors.New("naming client not initialized")
	}

	instance, err := n.client.SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam{
		ServiceName: serviceName,
		GroupName:   n.groupName,
	})
	if err != nil {
		return nil, fmt.Errorf("select healthy instance failed: %w", err)
	}
	return instance, nil
}

// SelectInstances 查询服务实例列表
func (n *NamingClientWrapper) SelectInstances(serviceName string, healthy bool) ([]model.Instance, error) {
	if n.client == nil {
		return nil, errors.New("naming client not initialized")
	}

	instances, err := n.client.SelectInstances(vo.SelectInstancesParam{
		ServiceName: serviceName,
		GroupName:   n.groupName,
		HealthyOnly: healthy,
	})
	if err != nil {
		return nil, fmt.Errorf("select instances failed: %w", err)
	}
	return instances, nil
}

// GetAllServices 获取所有服务列表
func (n *NamingClientWrapper) GetAllServices(pageNo, pageSize uint32) (*model.ServiceList, error) {
	if n.client == nil {
		return nil, errors.New("naming client not initialized")
	}

	serviceList, err := n.client.GetAllServicesInfo(vo.GetAllServiceInfoParam{
		PageNo:    pageNo,
		PageSize:  pageSize,
		GroupName: n.groupName,
	})
	if err != nil {
		return nil, fmt.Errorf("get all services failed: %w", err)
	}
	return &serviceList, nil
}

// GetServiceDetail 获取服务详细信息
func (n *NamingClientWrapper) GetServiceDetail(serviceName string, clusters []string) (*model.Service, error) {
	if n.client == nil {
		return nil, errors.New("naming client not initialized")
	}

	service, err := n.client.GetService(vo.GetServiceParam{
		ServiceName: serviceName,
		GroupName:   n.groupName,
		Clusters:    clusters,
	})
	if err != nil {
		return nil, fmt.Errorf("get service detail failed: %w", err)
	}
	return &service, nil
}

// SubscribeParam 订阅参数
type SubscribeParam struct {
	ServiceName       string                                     // 服务名
	SubscribeCallback func(services []model.Instance, err error) // 回调函数
}

// Subscribe 订阅服务变化
func (n *NamingClientWrapper) Subscribe(param SubscribeParam) error {
	if n.client == nil {
		return errors.New("naming client not initialized")
	}

	subscribeParam := &vo.SubscribeParam{
		ServiceName:       param.ServiceName,
		GroupName:         n.groupName,
		Clusters:          []string{n.clusterName},
		SubscribeCallback: param.SubscribeCallback,
	}

	err := n.client.Subscribe(subscribeParam)
	if err != nil {
		return fmt.Errorf("subscribe service failed: %w", err)
	}
	return nil
}

// Unsubscribe 取消订阅服务
func (n *NamingClientWrapper) Unsubscribe(param SubscribeParam) error {
	if n.client == nil {
		return errors.New("naming client not initialized")
	}

	unsubscribeParam := &vo.SubscribeParam{
		ServiceName:       param.ServiceName,
		GroupName:         n.groupName,
		Clusters:          []string{n.clusterName},
		SubscribeCallback: param.SubscribeCallback,
	}

	err := n.client.Unsubscribe(unsubscribeParam)
	if err != nil {
		return fmt.Errorf("unsubscribe service failed: %w", err)
	}
	return nil
}

// BatchRegisterInstances 批量注册实例
func (n *NamingClientWrapper) BatchRegisterInstances(instances []ServiceInstanceInfo) error {
	for _, info := range instances {
		err := n.RegisterInstance(info)
		if err != nil {
			return fmt.Errorf("batch register instance [%s:%d] failed: %w", info.Host, info.Port, err)
		}
	}
	return nil
}

// BatchDeregisterInstances 批量注销实例
func (n *NamingClientWrapper) BatchDeregisterInstances(instances []ServiceInstanceInfo) error {
	for _, info := range instances {
		_, err := n.DeregisterInstance(info.Name)
		if err != nil {
			return fmt.Errorf("batch deregister instance [%s:%d] failed: %w", info.Host, info.Port, err)
		}
	}
	return nil
}

// GetClient 获取底层命名客户端（用于高级用法）
func (n *NamingClientWrapper) GetClient() naming_client.INamingClient {
	return n.client
}

// GetGroupName 获取组名
func (n *NamingClientWrapper) GetGroupName() string {
	return n.groupName
}

// GetClusterName 获取集群名称
func (n *NamingClientWrapper) GetClusterName() string {
	return n.clusterName
}
