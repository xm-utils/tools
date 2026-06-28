package nacos

import (
	"testing"
)

// TestNewConfigClient 测试配置客户端创建
func TestNewConfigClient(t *testing.T) {
	// 测试 nil 配置
	_, err := NewConfigClient(nil)
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 测试未启用的配置
	config := &Config{
		Enabled: false,
	}
	_, err = NewConfigClient(config)
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 注意：以下测试需要实际的 Nacos 服务器
	// 在实际环境中应该集成测试
}

// TestNewNamingClient 测试服务发现客户端创建
func TestNewNamingClient(t *testing.T) {
	// 测试 nil 配置
	_, err := NewNamingClient(nil)
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 测试未启用的配置
	config := &Config{
		Enabled: false,
	}
	_, err = NewNamingClient(config)
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}
}

// TestConfigClientWrapper 测试配置客户端包装器方法
func TestConfigClientWrapper(t *testing.T) {
	// 创建一个未初始化的客户端来测试空指针检查
	client := &ConfigClientWrapper{
		client:    nil,
		groupName: "DEFAULT_GROUP",
	}

	// 测试 GetConfig
	_, err := client.GetConfig("test.yaml")
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 测试 PublishConfig
	_, err = client.PublishConfig("test.yaml", "content")
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 测试 DeleteConfig
	_, err = client.DeleteConfig("test.yaml")
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 测试 ListenConfig
	err = client.ListenConfig("test.yaml", nil)
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 测试 CancelListenConfig
	err = client.CancelListenConfig("test.yaml")
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 测试 SearchConfig
	_, err = client.SearchConfig("", 1, 10)
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}
}

// TestNamingClientWrapper 测试服务发现客户端包装器方法
func TestNamingClientWrapper(t *testing.T) {
	// 创建一个未初始化的客户端来测试空指针检查
	client := &NamingClientWrapper{
		client:      nil,
		groupName:   "DEFAULT_GROUP",
		clusterName: "DEFAULT",
	}

	// 测试 RegisterInstance
	err := client.RegisterInstance(ServiceInstanceInfo{Name: "test"})
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 测试 DeregisterInstance
	_, err = client.DeregisterInstance("test")
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 测试 SelectOneHealthyInstance
	_, err = client.SelectOneHealthyInstance("test")
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 测试 SelectInstances
	_, err = client.SelectInstances("test", true)
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 测试 GetAllServices
	_, err = client.GetAllServices(1, 10)
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 测试 GetServiceDetail
	_, err = client.GetServiceDetail("test", nil)
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 测试 Subscribe
	err = client.Subscribe(SubscribeParam{})
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}

	// 测试 Unsubscribe
	err = client.Unsubscribe(SubscribeParam{})
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}
}

// TestGetGroupName 测试获取组名
func TestGetGroupName(t *testing.T) {
	configClient := &ConfigClientWrapper{
		groupName: "TEST_GROUP",
	}
	if configClient.GetGroupName() != "TEST_GROUP" {
		t.Errorf("期望 TEST_GROUP，得到 %s", configClient.GetGroupName())
	}

	namingClient := &NamingClientWrapper{
		groupName: "TEST_GROUP",
	}
	if namingClient.GetGroupName() != "TEST_GROUP" {
		t.Errorf("期望 TEST_GROUP，得到 %s", namingClient.GetGroupName())
	}
}

// TestGetClusterName 测试获取集群名称
func TestGetClusterName(t *testing.T) {
	namingClient := &NamingClientWrapper{
		clusterName: "TEST_CLUSTER",
	}
	if namingClient.GetClusterName() != "TEST_CLUSTER" {
		t.Errorf("期望 TEST_CLUSTER，得到 %s", namingClient.GetClusterName())
	}
}

// TestBatchRegisterInstances 测试批量注册
func TestBatchRegisterInstances(t *testing.T) {
	client := &NamingClientWrapper{
		client:      nil,
		groupName:   "DEFAULT_GROUP",
		clusterName: "DEFAULT",
	}

	// 测试空客户端的批量注册
	instances := []ServiceInstanceInfo{
		{Name: "test1", Host: "127.0.0.1", Port: 8080},
	}
	err := client.BatchRegisterInstances(instances)
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}
}

// TestBatchDeregisterInstances 测试批量注销
func TestBatchDeregisterInstances(t *testing.T) {
	client := &NamingClientWrapper{
		client:      nil,
		groupName:   "DEFAULT_GROUP",
		clusterName: "DEFAULT",
	}

	// 测试空客户端的批量注销
	instances := []ServiceInstanceInfo{
		{Name: "test1"},
	}
	err := client.BatchDeregisterInstances(instances)
	if err == nil {
		t.Error("期望返回错误，但得到 nil")
	}
}
