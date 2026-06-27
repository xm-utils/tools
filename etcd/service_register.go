package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// ServiceInfo 服务信息
type ServiceInfo struct {
	Name     string            `json:"name"`     // 服务名称
	Version  string            `json:"version"`  // 服务版本
	Host     string            `json:"host"`     // 服务主机
	Port     int               `json:"port"`     // 服务端口
	Metadata map[string]string `json:"metadata"` // 元数据
	TTL      int64             `json:"ttl"`      // 租约时间（秒）
}

// RegisterService 注册服务
func RegisterService(info ServiceInfo) error {
	if client == nil {
		return fmt.Errorf("etcd client not initialized")
	}

	if info.Host == "" {
		localIP, err := GetLocalIP()
		if err != nil {
			return fmt.Errorf("get local ip failed: %w", err)
		}
		info.Host = localIP
	}

	if info.TTL <= 0 {
		info.TTL = 10 // 默认10秒
	}

	// 创建租约
	leaseResp, err := client.Grant(context.Background(), info.TTL)
	if err != nil {
		return fmt.Errorf("create lease failed: %w", err)
	}

	// 构建服务值
	serviceValue, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal service info failed: %w", err)
	}

	// 构建服务key
	key := fmt.Sprintf("/services/%s/%s:%d", info.Name, info.Host, info.Port)

	// 注册服务
	_, err = client.Put(context.Background(), key, string(serviceValue), clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return fmt.Errorf("register service failed: %w", err)
	}

	// 启动心跳保持
	go keepAlive(leaseResp.ID, info.TTL)

	return nil
}

// DeregisterService 注销服务
func DeregisterService(name, host string, port int) error {
	if client == nil {
		return fmt.Errorf("etcd client not initialized")
	}

	key := fmt.Sprintf("/services/%s/%s:%d", name, host, port)
	_, err := client.Delete(context.Background(), key)
	if err != nil {
		return fmt.Errorf("deregister service failed: %w", err)
	}

	return nil
}

// keepAlive 保持租约活跃
func keepAlive(leaseID clientv3.LeaseID, ttl int64) {
	ch, err := client.KeepAlive(context.Background(), leaseID)
	if err != nil {
		return
	}

	// 持续接收心跳响应
	for range ch {
		time.Sleep(time.Duration(ttl/2) * time.Second)
	}
}
