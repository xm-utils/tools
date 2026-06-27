package etcd

import (
	"context"
	"fmt"
	"net"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// HealthChecker 健康检查器
type HealthChecker struct {
	serviceName string
	host        string
	port        int
	interval    time.Duration
	timeout     time.Duration
	stopChan    chan struct{}
}

// NewHealthChecker 创建健康检查器
func NewHealthChecker(serviceName, host string, port int, interval, timeout time.Duration) *HealthChecker {
	return &HealthChecker{
		serviceName: serviceName,
		host:        host,
		port:        port,
		interval:    interval,
		timeout:     timeout,
		stopChan:    make(chan struct{}),
	}
}

// Start 启动健康检查
func (hc *HealthChecker) Start() {
	go hc.checkLoop()
}

// Stop 停止健康检查
func (hc *HealthChecker) Stop() {
	close(hc.stopChan)
}

// checkLoop 健康检查循环
func (hc *HealthChecker) checkLoop() {
	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hc.checkHealth()
		case <-hc.stopChan:
			return
		}
	}
}

// checkHealth 执行健康检查
func (hc *HealthChecker) checkHealth() {
	if client == nil {
		return
	}

	key := fmt.Sprintf("/services/%s/%s:%d", hc.serviceName, hc.host, hc.port)

	// 设置较短的超时时间来检查服务是否可达
	ctx, cancel := context.WithTimeout(context.Background(), hc.timeout)
	defer cancel()

	resp, err := client.Get(ctx, key)
	if err != nil || len(resp.Kvs) == 0 {
		// 服务不可达，可能需要重新注册
		return
	}

	// 更新租约以保持服务活跃
	hc.refreshLease(key)
}

// refreshLease 刷新租约
func (hc *HealthChecker) refreshLease(key string) {
	// 获取当前服务信息
	resp, err := client.Get(context.Background(), key)
	if err != nil || len(resp.Kvs) == 0 {
		return
	}

	// 重新授予租约并更新
	leaseResp, err := client.Grant(context.Background(), 10) // 默认10秒租约
	if err != nil {
		return
	}

	_, err = client.Put(context.Background(), key, string(resp.Kvs[0].Value), clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return
	}
}

// GetLocalIP 获取本地IP地址
func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		// 检查IP地址判断是否是回环地址
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}

	return "", fmt.Errorf("no valid local IP found")
}

// LeaseManager 租约管理器
type LeaseManager struct {
	leases map[string]clientv3.LeaseID
}

// NewLeaseManager 创建租约管理器
func NewLeaseManager() *LeaseManager {
	return &LeaseManager{
		leases: make(map[string]clientv3.LeaseID),
	}
}

// CreateLease 创建租约
func (lm *LeaseManager) CreateLease(key string, ttl int64) (clientv3.LeaseID, error) {
	if client == nil {
		return 0, fmt.Errorf("etcd client not initialized")
	}

	leaseResp, err := client.Grant(context.Background(), ttl)
	if err != nil {
		return 0, err
	}

	lm.leases[key] = leaseResp.ID
	return leaseResp.ID, nil
}

// RevokeLease 撤销租约
func (lm *LeaseManager) RevokeLease(key string) error {
	if client == nil {
		return fmt.Errorf("etcd client not initialized")
	}

	leaseID, exists := lm.leases[key]
	if !exists {
		return fmt.Errorf("lease not found for key: %s", key)
	}

	_, err := client.Revoke(context.Background(), leaseID)
	if err != nil {
		return err
	}

	delete(lm.leases, key)
	return nil
}

// KeepAliveLease 保持租约活跃
func (lm *LeaseManager) KeepAliveLease(key string) error {
	if client == nil {
		return fmt.Errorf("etcd client not initialized")
	}

	leaseID, exists := lm.leases[key]
	if !exists {
		return fmt.Errorf("lease not found for key: %s", key)
	}

	_, err := client.KeepAliveOnce(context.Background(), leaseID)
	return err
}
