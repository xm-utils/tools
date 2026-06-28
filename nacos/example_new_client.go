package nacos

import (
	"fmt"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
)

// ExampleConfigClient 配置客户端使用示例
func ExampleConfigClient() {
	// 1. 创建配置
	config := &Config{
		Enabled: true,
		ServerConfig: []ServerConfig{
			{
				IpAddr: "127.0.0.1",
				Port:   8848,
			},
		},
		ClientConfig: ClientConfig{
			GroupName:   "DEFAULT_GROUP",
			NamespaceId: "", // public命名空间
			TimeoutMs:   10000,
			LogLevel:    "info",
		},
	}

	// 2. 创建配置客户端
	configClient, err := NewConfigClient(config)
	if err != nil {
		fmt.Printf("创建配置客户端失败: %v\n", err)
		return
	}

	// 3. 发布配置
	success, err := configClient.PublishConfig("app.yaml", `
server:
  port: 8080
  host: localhost
database:
  host: localhost
  port: 3306
`)
	if err != nil {
		fmt.Printf("发布配置失败: %v\n", err)
		return
	}
	fmt.Printf("发布配置成功: %v\n", success)

	// 4. 获取配置
	content, err := configClient.GetConfig("app.yaml")
	if err != nil {
		fmt.Printf("获取配置失败: %v\n", err)
		return
	}
	fmt.Printf("配置内容:\n%s\n", content)

	// 5. 监听配置变化
	err = configClient.ListenConfig("app.yaml", func(namespace, group, dataId, data string) {
		fmt.Printf("配置发生变化:\n")
		fmt.Printf("  namespace: %s\n", namespace)
		fmt.Printf("  group: %s\n", group)
		fmt.Printf("  dataId: %s\n", dataId)
		fmt.Printf("  data: %s\n", data)
	})
	if err != nil {
		fmt.Printf("监听配置失败: %v\n", err)
		return
	}

	// 保持程序运行以接收配置变化通知
	time.Sleep(1 * time.Hour)

	// 6. 取消监听（可选）
	// configClient.CancelListenConfig("app.yaml")
}

// ExampleNamingClient 服务发现客户端使用示例
func ExampleNamingClient() {
	// 1. 创建配置
	config := &Config{
		Enabled: true,
		ServerConfig: []ServerConfig{
			{
				IpAddr: "127.0.0.1",
				Port:   8848,
			},
		},
		ClientConfig: ClientConfig{
			GroupName:   "DEFAULT_GROUP",
			ClusterName: "DEFAULT",
			NamespaceId: "", // public命名空间
			TimeoutMs:   10000,
			LogLevel:    "info",
		},
	}

	// 2. 创建服务发现客户端
	namingClient, err := NewNamingClient(config)
	if err != nil {
		fmt.Printf("创建服务发现客户端失败: %v\n", err)
		return
	}

	// 3. 注册服务实例
	err = namingClient.RegisterInstance(ServiceInstanceInfo{
		Name:   "order-service",
		Host:   "192.168.1.100",
		Port:   8080,
		Weight: 1.0,
		Metadata: map[string]string{
			"version": "v1.0.0",
			"env":     "prod",
		},
	})
	if err != nil {
		fmt.Printf("注册服务实例失败: %v\n", err)
		return
	}
	fmt.Println("服务实例注册成功")

	// 4. 选择一个健康实例（负载均衡）
	instance, err := namingClient.SelectOneHealthyInstance("order-service")
	if err != nil {
		fmt.Printf("选择健康实例失败: %v\n", err)
		return
	}
	fmt.Printf("选择的实例: IP=%s, Port=%d, Weight=%.2f\n",
		instance.Ip, instance.Port, instance.Weight)

	// 5. 查询所有健康实例
	instances, err := namingClient.SelectInstances("order-service", true)
	if err != nil {
		fmt.Printf("查询实例列表失败: %v\n", err)
		return
	}
	fmt.Printf("健康实例数量: %d\n", len(instances))
	for i, inst := range instances {
		fmt.Printf("  [%d] IP=%s, Port=%d, Weight=%.2f\n",
			i+1, inst.Ip, inst.Port, inst.Weight)
	}

	// 6. 订阅服务变化
	subscribeParam := SubscribeParam{
		ServiceName: "order-service",
		SubscribeCallback: func(services []model.Instance, err error) {
			if err != nil {
				fmt.Printf("订阅回调错误: %v\n", err)
				return
			}
			fmt.Printf("服务实例发生变化，当前实例数: %d\n", len(services))
			for _, svc := range services {
				fmt.Printf("  - IP=%s, Port=%d, Healthy=%v\n",
					svc.Ip, svc.Port, svc.Healthy)
			}
		},
	}

	err = namingClient.Subscribe(subscribeParam)
	if err != nil {
		fmt.Printf("订阅服务失败: %v\n", err)
		return
	}
	fmt.Println("订阅服务成功")

	// 保持程序运行以接收服务变化通知
	time.Sleep(1 * time.Hour)

	// 7. 取消订阅（可选）
	// namingClient.Unsubscribe(subscribeParam)

	// 8. 注销服务实例（服务关闭时）
	// namingClient.DeregisterInstance("order-service")
}

// ExampleBothClients 同时使用两个客户端的示例
func ExampleBothClients() {
	// 1. 创建配置
	config := &Config{
		Enabled: true,
		ServerConfig: []ServerConfig{
			{
				IpAddr: "127.0.0.1",
				Port:   8848,
			},
		},
		ClientConfig: ClientConfig{
			GroupName:   "DEFAULT_GROUP",
			ClusterName: "DEFAULT",
			NamespaceId: "",
			TimeoutMs:   10000,
			LogLevel:    "info",
		},
	}

	// 2. 分别创建两个客户端
	configClient, err := NewConfigClient(config)
	if err != nil {
		fmt.Printf("创建配置客户端失败: %v\n", err)
		return
	}

	namingClient, err := NewNamingClient(config)
	if err != nil {
		fmt.Printf("创建服务发现客户端失败: %v\n", err)
		return
	}

	// 3. 从配置中心读取数据库配置
	dbConfig, err := configClient.GetConfig("database.yaml")
	if err != nil {
		fmt.Printf("获取数据库配置失败: %v\n", err)
		return
	}
	fmt.Printf("数据库配置:\n%s\n", dbConfig)

	// 4. 注册当前服务
	err = namingClient.RegisterInstance(ServiceInstanceInfo{
		Name:   "user-service",
		Host:   "", // 自动获取本地IP
		Port:   8081,
		Weight: 1.0,
		Metadata: map[string]string{
			"version": "v1.0.0",
		},
	})
	if err != nil {
		fmt.Printf("注册服务失败: %v\n", err)
		return
	}
	fmt.Println("服务注册成功")

	// 5. 发现其他服务
	userServiceInstance, err := namingClient.SelectOneHealthyInstance("user-service")
	if err != nil {
		fmt.Printf("发现用户服务失败: %v\n", err)
		return
	}
	fmt.Printf("用户服务地址: %s:%d\n", userServiceInstance.Ip, userServiceInstance.Port)

	fmt.Println("\n应用启动完成！")
}

// ExampleBatchRegister 批量注册示例
func ExampleBatchRegister() {
	config := &Config{
		Enabled: true,
		ServerConfig: []ServerConfig{
			{
				IpAddr: "127.0.0.1",
				Port:   8848,
			},
		},
		ClientConfig: ClientConfig{
			GroupName:   "DEFAULT_GROUP",
			ClusterName: "DEFAULT",
			NamespaceId: "",
			TimeoutMs:   10000,
		},
	}

	namingClient, err := NewNamingClient(config)
	if err != nil {
		fmt.Printf("创建客户端失败: %v\n", err)
		return
	}

	// 批量注册多个实例
	instances := []ServiceInstanceInfo{
		{
			Name:   "order-service",
			Host:   "192.168.1.100",
			Port:   8080,
			Weight: 1.0,
		},
		{
			Name:   "order-service",
			Host:   "192.168.1.101",
			Port:   8080,
			Weight: 1.0,
		},
		{
			Name:   "order-service",
			Host:   "192.168.1.102",
			Port:   8080,
			Weight: 0.5, // 权重较低，接收较少流量
		},
	}

	err = namingClient.BatchRegisterInstances(instances)
	if err != nil {
		fmt.Printf("批量注册失败: %v\n", err)
		return
	}
	fmt.Println("批量注册成功")

	// 查询注册的实例
	allInstances, err := namingClient.SelectInstances("order-service", true)
	if err != nil {
		fmt.Printf("查询实例失败: %v\n", err)
		return
	}
	fmt.Printf("当前健康实例数: %d\n", len(allInstances))
}
