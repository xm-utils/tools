package nacos

import (
	"fmt"
	"testing"
	"time"
)

// 测试初始化Nacos客户端
func TestInitNacosClient(t *testing.T) {
	// 测试空配置
	err := InitClient(nil)
	if err == nil {
		t.Error("Expected error for nil config, got nil")
	}

	// 测试禁用状态
	disabledConfig := &Config{
		Enabled: false,
	}
	err = InitClient(disabledConfig)
	if err != nil {
		t.Errorf("Expected no error for disabled config, got: %v", err)
	}

	// 测试有效配置（使用模拟服务器地址）
	validConfig := &Config{
		Enabled: true,
		ServerConfig: []ServerConfig{
			{
				IpAddr: "127.0.0.1",
				Port:   8848,
			},
		},
		ClientConfig: ClientConfig{
			TimeoutMs:   10000,
			NamespaceId: "", // public namespace
			LogLevel:    "info",
		},
	}

	// 注意：这个测试需要实际的Nacos服务器运行，如果没有会失败
	// 在实际环境中，应该有一个测试用的Nacos实例
	err = InitClient(validConfig)
	if err != nil {
		t.Logf("InitNacosClient failed (expected if no Nacos server running): %v", err)
		// 不将其视为错误，因为可能没有运行的Nacos服务器
	} else {
		t.Log("InitNacosClient succeeded")
	}
}

func initClient() {
	// 首先初始化客户端
	config := &Config{
		Enabled: true,
		ServerConfig: []ServerConfig{
			{
				IpAddr: "192.168.3.85",
				Port:   8848,
			},
		},
		ClientConfig: ClientConfig{
			TimeoutMs:   10000,
			NamespaceId: "",
			LogLevel:    "info",
		},
	}

	err := InitClient(config)
	if err != nil {
		panic(fmt.Sprintf("Skipping TestGetConfig due to initialization failure: %v", err))
		return
	}
}

// 测试获取配置
func TestGetConfig(t *testing.T) {
	initClient()
	// 尝试获取一个可能不存在的配置
	data, err := GetConfig("nacos-config-demo1.yaml")
	if err != nil {
		t.Logf("GetConfig failed (expected if config doesn't exist): %v", err)
	} else {
		t.Logf("GetConfig succeeded, data: %s", data)
	}
}

// 测试监听配置
func TestListenConfig(t *testing.T) {
	// 首先初始化客户端
	initClient()

	// 定义监听器函数
	listener := func(namespace, group, dataId, data string) {
		t.Logf("Config changed - Namespace: %s, Group: %s, DataId: %s, Data: %s",
			namespace, group, dataId, data)
	}

	// 开始监听配置变化
	err := ListenConfig("nacos-config-demo1.yaml", listener)
	if err != nil {
		t.Logf("ListenConfig failed: %v", err)
	} else {
		t.Log("ListenConfig succeeded")

		// 等待一段时间以接收可能的配置变更通知
		time.Sleep(50 * time.Second)
	}
}

// 测试服务注册
func TestRegisterInstance(t *testing.T) {
	// 首先初始化客户端
	initClient()

	// 测试服务注册
	serviceInfo := ServiceInfo{
		Name:   "test-service",
		Port:   8080,
		Weight: 1.0,
		Metadata: map[string]string{
			"version": "1.0.0",
		},
	}

	if err := RegisterInstance(serviceInfo); err != nil {
		t.Logf("RegisterInstance failed (expected if no Nacos server running): %v", err)
	} else {
		t.Log("RegisterInstance succeeded")
	}

	time.Sleep(20 * time.Second)
}

// 测试服务注销
func TestDeregisterInstance(t *testing.T) {
	// 首先初始化客户端
	initClient()

	// 测试服务注销
	success, err := DeregisterInstance("test-service-flow")
	if err != nil {
		t.Logf("DeregisterInstance failed: %v", err)
	} else {
		t.Logf("DeregisterInstance succeeded, success: %t", success)
	}
}

// 测试完整的服务注册和注销流程
func TestServiceRegistrationFlow(t *testing.T) {
	// 首先初始化客户端
	initClient()

	// 注册服务
	serviceInfo := ServiceInfo{
		Name:   "test-service-flow",
		Port:   9090,
		Weight: 1.0,
		Metadata: map[string]string{
			"version": "1.0.0",
			"env":     "test",
		},
	}

	if err := RegisterInstance(serviceInfo); err != nil {
		t.Logf("RegisterInstance in flow test failed: %v", err)
		return
	}
	t.Log("Service registered successfully")

	// 短暂等待以确保注册完成
	time.Sleep(2 * time.Second)

	// 注销服务
	success, err := DeregisterInstance("test-service-flow")
	if err != nil {
		t.Logf("DeregisterInstance in flow test failed: %v", err)
	} else {
		t.Logf("Service deregistered successfully, success: %t", success)
	}
}

// 测试带自定义主机名的服务注册
func TestRegisterInstanceWithCustomHost(t *testing.T) {
	// 首先初始化客户端
	initClient()

	// 测试带有自定义主机名的服务注册
	serviceInfo := ServiceInfo{
		Name:   "test-service-custom-host",
		Host:   "192.168.3.87", // 自定义主机地址
		Port:   8080,
		Weight: 1.0,
		Metadata: map[string]string{
			"version": "1.0.0",
			"region":  "cn-north-1",
		},
	}

	if err := RegisterInstance(serviceInfo); err != nil {
		t.Logf("RegisterInstance with custom host failed: %v", err)
	} else {
		t.Log("RegisterInstance with custom host succeeded")

		time.Sleep(20 * time.Second)
		// 清理：注销服务
		DeregisterInstance("test-service-custom-host")
	}
}

// 基准测试 - 初始化客户端性能
func BenchmarkInitNacosClient(b *testing.B) {
	config := &Config{
		Enabled: true,
		ServerConfig: []ServerConfig{
			{
				IpAddr: "127.0.0.1",
				Port:   8848,
			},
		},
		ClientConfig: ClientConfig{
			TimeoutMs:   10000,
			NamespaceId: "",
			LogLevel:    "warn", // 减少日志输出以提高基准测试准确性
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = InitClient(config)
	}
}
