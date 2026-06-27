package nacos

import (
	"fmt"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
)

// TestSelectOneHealthyInstance 测试选择健康实例
func TestSelectOneHealthyInstance(t *testing.T) {
	// 注意：此测试需要实际运行的Nacos服务器
	// 这里只是展示API用法

	if namingClient == nil {
		t.Skip("Nacos client not initialized, skipping test")
	}

	instance, err := SelectOneHealthyInstance("test-service")
	if err != nil {
		t.Logf("SelectOneHealthyInstance error (expected if no Nacos server): %v", err)
		return
	}

	if instance != nil {
		t.Logf("Selected instance: %s:%d", instance.Ip, instance.Port)
	}
}

// TestSelectInstances 测试查询实例列表
func TestSelectInstances(t *testing.T) {
	instances, err := SelectInstances("test-service", true)
	if err != nil {
		t.Logf("SelectInstances error (expected if no Nacos server): %v", err)
		return
	}

	t.Logf("Found %d healthy instances", len(instances))
	for i, inst := range instances {
		t.Logf("[%d] %s:%d, weight: %.2f", i+1, inst.Ip, inst.Port, inst.Weight)
	}
}

// TestGetAllServices 测试获取所有服务
func TestGetAllServices(t *testing.T) {
	serviceList, err := GetAllServices(1, 10)
	if err != nil {
		t.Logf("GetAllServices error (expected if no Nacos server): %v", err)
		return
	}

	t.Logf("Total services: %d", serviceList.Count)
	for i, name := range serviceList.Doms {
		t.Logf("[%d] %s", i+1, name)
	}
}

// TestGetServiceDetail 测试获取服务详情
func TestGetServiceDetail(t *testing.T) {
	service, err := GetServiceDetail("test-service", []string{"DEFAULT"})
	if err != nil {
		t.Logf("GetServiceDetail error (expected if no Nacos server): %v", err)
		return
	}

	if service != nil {
		t.Logf("Service: %s, Group: %s", service.Name, service.GroupName)
		t.Logf("Instance count: %d", len(service.Hosts))
	}
}

// TestSubscribe 测试订阅服务
func TestSubscribe(t *testing.T) {
	param := SubscribeParam{
		ServiceName: "test-service",
		SubscribeCallback: func(services []model.Instance, err error) {
			if err != nil {
				t.Logf("Subscribe callback error: %v", err)
				return
			}
			t.Logf("Received %d instances", len(services))
			for _, svc := range services {
				t.Logf("  - %s:%d", svc.Ip, svc.Port)
			}
		},
	}

	err := Subscribe(param)
	if err != nil {
		t.Logf("Subscribe error (expected if no Nacos server): %v", err)
		return
	}

	t.Log("Successfully subscribed to service")

	// 取消订阅
	err = Unsubscribe(param)
	if err != nil {
		t.Logf("Unsubscribe error: %v", err)
	}
}

// TestPublishConfig 测试发布配置
func TestPublishConfig(t *testing.T) {
	success, err := PublishConfig("test-config.yaml", "key: value\nname: test")
	if err != nil {
		t.Logf("PublishConfig error (expected if no Nacos server): %v", err)
		return
	}

	if success {
		t.Log("Config published successfully")
	}
}

// TestGetAndListenConfig 测试获取和监听配置
func TestGetAndListenConfig(t *testing.T) {
	// 获取配置
	config, err := GetConfig("test-config.yaml")
	if err != nil {
		t.Logf("GetConfig error (expected if no Nacos server): %v", err)
		return
	}

	t.Logf("Config content:\n%s", config)

	// 监听配置变化
	err = ListenConfig("test-config.yaml", func(namespace, group, dataId, data string) {
		t.Logf("Config changed:\nnamespace=%s, group=%s, dataId=%s\ndata=%s",
			namespace, group, dataId, data)
	})
	if err != nil {
		t.Logf("ListenConfig error: %v", err)
		return
	}

	t.Log("Successfully listening to config changes")

	// 取消监听
	err = CancelListenConfig("test-config.yaml")
	if err != nil {
		t.Logf("CancelListenConfig error: %v", err)
	}
}

// TestDeleteConfig 测试删除配置
func TestDeleteConfig(t *testing.T) {
	success, err := DeleteConfig("test-config.yaml")
	if err != nil {
		t.Logf("DeleteConfig error (expected if no Nacos server): %v", err)
		return
	}

	if success {
		t.Log("Config deleted successfully")
	}
}

// TestSearchConfig 测试搜索配置
func TestSearchConfig(t *testing.T) {
	configPage, err := SearchConfig("test", 1, 10)
	if err != nil {
		t.Logf("SearchConfig error (expected if no Nacos server): %v", err)
		return
	}

	t.Logf("Found %d configs", configPage.TotalCount)
	for i, item := range configPage.PageItems {
		t.Logf("[%d] DataId: %s, Group: %s", i+1, item.DataId, item.Group)
	}
}

// TestBatchRegisterInstances 测试批量注册
func TestBatchRegisterInstances(t *testing.T) {
	instances := []ServiceInfo{
		{
			Name:   "batch-test-service",
			Host:   "192.168.1.100",
			Port:   8080,
			Weight: 1.0,
			Metadata: map[string]string{
				"version": "1.0.0",
			},
		},
		{
			Name:   "batch-test-service",
			Host:   "192.168.1.101",
			Port:   8080,
			Weight: 1.0,
			Metadata: map[string]string{
				"version": "1.0.0",
			},
		},
	}

	err := BatchRegisterInstances(instances)
	if err != nil {
		t.Logf("BatchRegisterInstances error (expected if no Nacos server): %v", err)
		return
	}

	t.Log("Batch registration successful")

	// 批量注销
	err = BatchDeregisterInstances(instances)
	if err != nil {
		t.Logf("BatchDeregisterInstances error: %v", err)
	}
}

// TestGetClients 测试获取客户端
func TestGetClients(t *testing.T) {
	namingClient := GetNamingClient()
	if namingClient == nil {
		t.Error("Naming client should not be nil")
	} else {
		t.Log("Got naming client successfully")
	}

	configClient := GetConfigClient()
	if configClient == nil {
		t.Error("Config client should not be nil")
	} else {
		t.Log("Got config client successfully")
	}
}

// ExampleSelectOneHealthyInstance 使用示例
func ExampleSelectOneHealthyInstance() {
	// 选择一个健康的实例进行调用
	instance, err := SelectOneHealthyInstance("user-service")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// 使用选中的实例
	url := fmt.Sprintf("http://%s:%d/api/users", instance.Ip, instance.Port)
	fmt.Printf("Calling: %s\n", url)
}

// ExampleSubscribe 订阅示例
func ExampleSubscribe() {
	// 订阅服务变化
	param := SubscribeParam{
		ServiceName: "order-service",
		SubscribeCallback: func(services []model.Instance, err error) {
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				return
			}

			fmt.Printf("Service instances updated: %d\n", len(services))
			for _, svc := range services {
				fmt.Printf("  - %s:%d (healthy: %v)\n",
					svc.Ip, svc.Port, svc.Healthy)
			}
		},
	}

	err := Subscribe(param)
	if err != nil {
		fmt.Printf("Subscribe error: %v\n", err)
		return
	}

	fmt.Println("Successfully subscribed to order-service")

	// 记得在适当的时候取消订阅
	// Unsubscribe(param)
}

// ExamplePublishConfig 发布配置示例
func ExamplePublishConfig() {
	// 发布配置
	success, err := PublishConfig(
		"application.yaml",
		"server:\n  port: 8080\n  host: 0.0.0.0",
	)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if success {
		fmt.Println("Configuration published successfully")
	}
}
