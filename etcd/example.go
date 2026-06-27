package etcd

import (
	"fmt"
	"log"
	"time"
)

// 服务注册示例
func exampleRegister() {
	// 初始化etcd客户端
	config := &EtcdConfig{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}

	err := InitClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer Close()

	// 注册服务
	serviceInfo := ServiceInfo{
		Name:    "user-service",
		Version: "1.0.0",
		Host:    "", // 空字符串将自动获取本地IP
		Port:    8080,
		Metadata: map[string]string{
			"region": "cn-north-1",
			"zone":   "zone-a",
			"env":    "dev",
		},
		TTL: 10, // 租约时间10秒
	}

	err = RegisterService(serviceInfo)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("✓ Service registered successfully")
	fmt.Printf("  Service: %s\n", serviceInfo.Name)
	fmt.Printf("  Address: %s:%d\n", serviceInfo.Host, serviceInfo.Port)
}

// 服务发现示例
func exampleDiscovery() {
	// 初始化etcd客户端
	config := &EtcdConfig{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}

	err := InitClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer Close()

	// 发现服务
	services, err := DiscoverServices("user-service")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✓ Found %d service instance(s)\n", len(services))
	for i, service := range services {
		fmt.Printf("  [%d] %s:%d (version: %s)\n", i+1, service.Host, service.Port, service.Version)
		if len(service.Metadata) > 0 {
			fmt.Printf("      Metadata: %v\n", service.Metadata)
		}
	}

	// 获取服务实例地址列表
	instances, err := GetServiceInstances("user-service")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\n✓ Service instances: %v\n", instances)

	// 选择一个服务实例
	instance, err := SelectServiceInstance("user-service")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✓ Selected instance: %s:%d\n", instance.Host, instance.Port)
}

// 服务监听示例
func exampleWatch() {
	// 初始化etcd客户端
	config := &EtcdConfig{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}

	err := InitClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer Close()

	// 监听服务变化
	fmt.Println("✓ Watching for service changes...")
	err = WatchService("user-service", func(services []ServiceInfo) {
		fmt.Printf("\n[WATCH] Services updated, count: %d\n", len(services))
		for _, service := range services {
			fmt.Printf("  - %s:%d\n", service.Host, service.Port)
		}
	})

	if err != nil {
		log.Fatal(err)
	}

	// 保持程序运行以接收监听事件
	select {}
}

// 元数据过滤示例
func exampleFilterByMetadata() {
	// 初始化etcd客户端
	config := &EtcdConfig{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}

	err := InitClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer Close()

	// 根据元数据过滤服务
	filterMetadata := map[string]string{
		"region": "cn-north-1",
		"env":    "dev",
	}

	services, err := FilterServicesByMetadata("user-service", filterMetadata)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✓ Found %d service(s) matching filter\n", len(services))
	fmt.Printf("  Filter: %v\n", filterMetadata)
	for i, service := range services {
		fmt.Printf("  [%d] %s:%d\n", i+1, service.Host, service.Port)
		fmt.Printf("      Metadata: %v\n", service.Metadata)
	}
}

// 健康检查示例
func exampleHealthCheck() {
	// 初始化etcd客户端
	config := &EtcdConfig{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}

	err := InitClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer Close()

	// 创建健康检查器
	checker := NewHealthChecker(
		"user-service",
		"", // 将自动获取本地IP
		8080,
		5*time.Second, // 检查间隔
		2*time.Second, // 超时时间
	)

	// 启动健康检查
	checker.Start()
	fmt.Println("✓ Health checker started")

	// 模拟运行一段时间后停止
	time.Sleep(30 * time.Second)
	checker.Stop()
	fmt.Println("✓ Health checker stopped")
}

// 完整示例：注册、发现、注销
func exampleComplete() {
	// 初始化etcd客户端
	config := &EtcdConfig{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}

	err := InitClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer Close()

	// 1. 注册服务
	fmt.Println("=== Step 1: Register Service ===")
	serviceInfo := ServiceInfo{
		Name:    "order-service",
		Version: "1.0.0",
		Port:    8081,
		Metadata: map[string]string{
			"region": "cn-north-1",
			"env":    "prod",
		},
		TTL: 10,
	}

	err = RegisterService(serviceInfo)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Registered: %s at %s:%d\n", serviceInfo.Name, serviceInfo.Host, serviceInfo.Port)

	// 2. 发现服务
	fmt.Println("\n=== Step 2: Discover Service ===")
	time.Sleep(1 * time.Second) // 等待注册完成

	services, err := DiscoverServices("order-service")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✓ Found %d instance(s)\n", len(services))
	for _, service := range services {
		fmt.Printf("  - %s:%d\n", service.Host, service.Port)
	}

	// 3. 注销服务
	fmt.Println("\n=== Step 3: Deregister Service ===")
	localIP, _ := GetLocalIP()
	err = DeregisterService("order-service", localIP, 8081)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Deregistered: %s at %s:%d\n", serviceInfo.Name, localIP, 8081)

	// 4. 验证注销
	fmt.Println("\n=== Step 4: Verify Deregistration ===")
	time.Sleep(1 * time.Second)
	services, err = DiscoverServices("order-service")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Remaining instances: %d\n", len(services))
}

func main() {
	fmt.Println("ETCD Service Registry Examples")
	fmt.Println("==============================")
	fmt.Println()

	// 运行完整示例
	exampleComplete()

	// 取消注释以运行其他示例
	// exampleRegister()
	// exampleDiscovery()
	// exampleWatch()
	// exampleFilterByMetadata()
	// exampleHealthCheck()
}
