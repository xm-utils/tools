package etcd

import (
	"context"
	"encoding/json"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// DiscoverServices 发现服务
func DiscoverServices(serviceName string) ([]ServiceInfo, error) {
	if client == nil {
		return nil, fmt.Errorf("etcd client not initialized")
	}

	// 构建查询前缀
	prefix := fmt.Sprintf("/services/%s/", serviceName)

	// 查询所有匹配的服务
	resp, err := client.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("discover services failed: %w", err)
	}

	var services []ServiceInfo
	for _, kv := range resp.Kvs {
		var service ServiceInfo
		if err := json.Unmarshal(kv.Value, &service); err != nil {
			continue // 跳过解析失败的服务
		}
		services = append(services, service)
	}

	return services, nil
}

// WatchService 监听服务变化
func WatchService(serviceName string, callback func([]ServiceInfo)) error {
	if client == nil {
		return fmt.Errorf("etcd client not initialized")
	}

	// 构建监听前缀
	prefix := fmt.Sprintf("/services/%s/", serviceName)

	// 创建watcher
	watchChan := client.Watch(context.Background(), prefix, clientv3.WithPrefix())

	// 启动监听协程
	go func() {
		for watchResp := range watchChan {
			if watchResp.Err() != nil {
				continue
			}

			// 获取当前所有服务实例
			services, err := DiscoverServices(serviceName)
			if err != nil {
				continue
			}

			// 调用回调函数
			callback(services)
		}
	}()

	return nil
}

// GetServiceInstances 获取服务实例列表（简化版本）
func GetServiceInstances(serviceName string) ([]string, error) {
	services, err := DiscoverServices(serviceName)
	if err != nil {
		return nil, err
	}

	var instances []string
	for _, service := range services {
		instance := fmt.Sprintf("%s:%d", service.Host, service.Port)
		instances = append(instances, instance)
	}

	return instances, nil
}

// SelectServiceInstance 选择一个服务实例（简单轮询）
func SelectServiceInstance(serviceName string) (*ServiceInfo, error) {
	services, err := DiscoverServices(serviceName)
	if err != nil {
		return nil, err
	}

	if len(services) == 0 {
		return nil, fmt.Errorf("no available instances for service: %s", serviceName)
	}

	// 简单返回第一个可用实例
	return &services[0], nil
}

// FilterServicesByMetadata 根据元数据过滤服务
func FilterServicesByMetadata(serviceName string, metadata map[string]string) ([]ServiceInfo, error) {
	services, err := DiscoverServices(serviceName)
	if err != nil {
		return nil, err
	}

	var filtered []ServiceInfo
	for _, service := range services {
		if matchMetadata(service.Metadata, metadata) {
			filtered = append(filtered, service)
		}
	}

	return filtered, nil
}

// matchMetadata 检查服务元数据是否匹配过滤条件
func matchMetadata(serviceMetadata, filterMetadata map[string]string) bool {
	for key, value := range filterMetadata {
		if serviceValue, exists := serviceMetadata[key]; !exists || serviceValue != value {
			return false
		}
	}
	return true
}
