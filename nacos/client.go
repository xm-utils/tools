package nacos

import (
	"errors"
	"fmt"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

var (
	configClient config_client.IConfigClient
	namingClient naming_client.INamingClient
	groupName    string
	clusterName  string
)

func InitClient(config *Config) error {
	if config == nil {
		return errors.New("nacos config is nil")
	}
	if config.Enabled == false {
		return nil
	}

	serverConfigs, clientConfig := config.getConfig()

	var err error
	// 创建配置客户端
	configClient, err = clients.NewConfigClient(
		vo.NacosClientParam{
			ClientConfig:  clientConfig,
			ServerConfigs: serverConfigs,
		},
	)
	if err != nil {
		return err
	}

	namingClient, err = clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  clientConfig,
			ServerConfigs: serverConfigs,
		},
	)
	if err != nil {
		return err
	}
	return nil
}

func GetConfig(dataId string) (string, error) {
	return configClient.GetConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  groupName,
	})
}

func ListenConfig(dataId string, listener func(namespace, group, dataId, data string)) error {
	return configClient.ListenConfig(vo.ConfigParam{
		DataId:   dataId,
		Group:    groupName,
		OnChange: listener,
	})
}

// PublishConfig 发布配置
func PublishConfig(dataId, content string) (bool, error) {
	success, err := configClient.PublishConfig(vo.ConfigParam{
		DataId:  dataId,
		Group:   groupName,
		Content: content,
	})
	if err != nil {
		return false, fmt.Errorf("publish config failed: %w", err)
	}
	return success, nil
}

// DeleteConfig 删除配置
func DeleteConfig(dataId string) (bool, error) {
	success, err := configClient.DeleteConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  groupName,
	})
	if err != nil {
		return false, fmt.Errorf("delete config failed: %w", err)
	}
	return success, nil
}

// SearchConfig 搜索配置
func SearchConfig(search string, pageNo, pageSize uint32) (*model.ConfigPage, error) {
	configPage, err := configClient.SearchConfig(vo.SearchConfigParam{
		Search:   search,
		PageNo:   int(pageNo),
		PageSize: int(pageSize),
	})
	if err != nil {
		return nil, fmt.Errorf("search config failed: %w", err)
	}
	return configPage, nil
}

// CancelListenConfig 取消监听配置
func CancelListenConfig(dataId string) error {
	err := configClient.CancelListenConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  groupName,
	})
	if err != nil {
		return fmt.Errorf("cancel listen config failed: %w", err)
	}
	return nil
}

type ServiceInfo struct {
	Name     string
	Host     string
	Port     uint64
	Weight   float64
	Metadata map[string]string
}

func RegisterInstance(info ServiceInfo) error {
	if info.Host == "" {
		localIP, err := GetLocalIP()
		if err != nil {
			return errors.New("the service host cannot be empty")
		}
		info.Host = localIP
	}

	success, err := namingClient.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          info.Host,
		Port:        info.Port,
		Weight:      info.Weight,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true, // 临时实例，服务断开后会自动删除（推荐）
		Metadata:    info.Metadata,
		ServiceName: info.Name,
		GroupName:   groupName,
		ClusterName: clusterName,
	})
	if !success || err != nil {
		return errors.New(fmt.Sprintf("Service registration failed. error: %v", err))
	}
	return nil
}

func DeregisterInstance(name string) (bool, error) {
	return namingClient.DeregisterInstance(vo.DeregisterInstanceParam{
		ServiceName: name,
		GroupName:   groupName,
		Cluster:     clusterName,
		Ephemeral:   true,
	})
}

// SelectOneHealthyInstance 选择一个健康的实例（负载均衡）
func SelectOneHealthyInstance(serviceName string) (*model.Instance, error) {
	instance, err := namingClient.SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam{
		ServiceName: serviceName,
		GroupName:   groupName,
	})
	if err != nil {
		return nil, fmt.Errorf("select healthy instance failed: %w", err)
	}
	return instance, nil
}

// SelectInstances 查询服务实例列表
func SelectInstances(serviceName string, healthy bool) ([]model.Instance, error) {
	instances, err := namingClient.SelectInstances(vo.SelectInstancesParam{
		ServiceName: serviceName,
		GroupName:   groupName,
		HealthyOnly: healthy,
	})
	if err != nil {
		return nil, fmt.Errorf("select instances failed: %w", err)
	}
	return instances, nil
}

// GetAllServices 获取所有服务列表
func GetAllServices(pageNo, pageSize uint32) (*model.ServiceList, error) {
	serviceList, err := namingClient.GetAllServicesInfo(vo.GetAllServiceInfoParam{
		PageNo:    pageNo,
		PageSize:  pageSize,
		GroupName: groupName,
	})
	if err != nil {
		return nil, fmt.Errorf("get all services failed: %w", err)
	}
	return &serviceList, nil
}

// GetServiceDetail 获取服务详细信息
func GetServiceDetail(serviceName string, clusters []string) (*model.Service, error) {
	service, err := namingClient.GetService(vo.GetServiceParam{
		ServiceName: serviceName,
		GroupName:   groupName,
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
func Subscribe(param SubscribeParam) error {
	subscribeParam := &vo.SubscribeParam{
		ServiceName:       param.ServiceName,
		GroupName:         groupName,
		Clusters:          []string{clusterName},
		SubscribeCallback: param.SubscribeCallback,
	}

	err := namingClient.Subscribe(subscribeParam)
	if err != nil {
		return fmt.Errorf("subscribe service failed: %w", err)
	}
	return nil
}

// Unsubscribe 取消订阅服务
func Unsubscribe(param SubscribeParam) error {
	unsubscribeParam := &vo.SubscribeParam{
		ServiceName:       param.ServiceName,
		GroupName:         groupName,
		Clusters:          []string{clusterName},
		SubscribeCallback: param.SubscribeCallback,
	}

	err := namingClient.Unsubscribe(unsubscribeParam)
	if err != nil {
		return fmt.Errorf("unsubscribe service failed: %w", err)
	}
	return nil
}

// BatchRegisterInstances 批量注册实例
func BatchRegisterInstances(instances []ServiceInfo) error {
	for _, info := range instances {
		err := RegisterInstance(info)
		if err != nil {
			return fmt.Errorf("batch register instance [%s:%d] failed: %w", info.Host, info.Port, err)
		}
	}
	return nil
}

// BatchDeregisterInstances 批量注销实例
func BatchDeregisterInstances(instances []ServiceInfo) error {
	for _, info := range instances {
		_, err := DeregisterInstance(info.Name)
		if err != nil {
			return fmt.Errorf("batch deregister instance [%s:%d] failed: %w", info.Host, info.Port, err)
		}
	}
	return nil
}

// GetNamingClient 获取命名客户端（用于高级用法）
func GetNamingClient() naming_client.INamingClient {
	return namingClient
}

// GetConfigClient 获取配置客户端（用于高级用法）
func GetConfigClient() config_client.IConfigClient {
	return configClient
}
