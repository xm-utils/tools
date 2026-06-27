package etcd

import (
	"testing"
	"time"
)

// TestEtcdConfig 测试配置
func TestEtcdConfig(t *testing.T) {
	config := DefaultEtcdConfig()
	if config == nil {
		t.Fatal("DefaultEtcdConfig should not be nil")
	}

	if len(config.Endpoints) == 0 {
		t.Error("Endpoints should not be empty")
	}

	if config.DialTimeout <= 0 {
		t.Error("DialTimeout should be greater than 0")
	}
}

// TestGetLocalIP 测试获取本地IP
func TestGetLocalIP(t *testing.T) {
	ip, err := GetLocalIP()
	if err != nil {
		t.Fatalf("GetLocalIP failed: %v", err)
	}

	if ip == "" {
		t.Error("Local IP should not be empty")
	}

	t.Logf("Local IP: %s", ip)
}

// TestServiceInfo 测试服务信息结构
func TestServiceInfo(t *testing.T) {
	info := ServiceInfo{
		Name:    "test-service",
		Version: "1.0.0",
		Host:    "192.168.1.100",
		Port:    8080,
		Metadata: map[string]string{
			"region": "cn-north-1",
			"zone":   "zone-a",
		},
		TTL: 10,
	}

	if info.Name != "test-service" {
		t.Errorf("Expected name 'test-service', got '%s'", info.Name)
	}

	if info.Port != 8080 {
		t.Errorf("Expected port 8080, got %d", info.Port)
	}

	if len(info.Metadata) != 2 {
		t.Errorf("Expected 2 metadata items, got %d", len(info.Metadata))
	}
}

// TestHealthChecker 测试健康检查器
func TestHealthChecker(t *testing.T) {
	checker := NewHealthChecker(
		"test-service",
		"192.168.1.100",
		8080,
		5*time.Second,
		2*time.Second,
	)

	if checker == nil {
		t.Fatal("HealthChecker should not be nil")
	}

	if checker.serviceName != "test-service" {
		t.Errorf("Expected serviceName 'test-service', got '%s'", checker.serviceName)
	}

	if checker.interval != 5*time.Second {
		t.Errorf("Expected interval 5s, got %v", checker.interval)
	}
}

// TestLeaseManager 测试租约管理器
func TestLeaseManager(t *testing.T) {
	manager := NewLeaseManager()
	if manager == nil {
		t.Fatal("LeaseManager should not be nil")
	}

	if manager.leases == nil {
		t.Error("leases map should be initialized")
	}

	if len(manager.leases) != 0 {
		t.Errorf("Expected 0 leases, got %d", len(manager.leases))
	}
}

// TestMatchMetadata 测试元数据匹配
func TestMatchMetadata(t *testing.T) {
	serviceMetadata := map[string]string{
		"region": "cn-north-1",
		"zone":   "zone-a",
		"env":    "prod",
	}

	// 测试完全匹配
	filter1 := map[string]string{
		"region": "cn-north-1",
		"zone":   "zone-a",
	}
	if !matchMetadata(serviceMetadata, filter1) {
		t.Error("Should match when all filters match")
	}

	// 测试部分匹配
	filter2 := map[string]string{
		"region": "cn-north-1",
	}
	if !matchMetadata(serviceMetadata, filter2) {
		t.Error("Should match when subset of filters match")
	}

	// 测试不匹配
	filter3 := map[string]string{
		"region": "cn-south-1",
	}
	if matchMetadata(serviceMetadata, filter3) {
		t.Error("Should not match when filter value differs")
	}

	// 测试空过滤器
	filter4 := map[string]string{}
	if !matchMetadata(serviceMetadata, filter4) {
		t.Error("Should match when filter is empty")
	}

	// 测试不存在的键
	filter5 := map[string]string{
		"version": "1.0.0",
	}
	if matchMetadata(serviceMetadata, filter5) {
		t.Error("Should not match when filter key doesn't exist")
	}
}
