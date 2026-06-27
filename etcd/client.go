package etcd

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	client *clientv3.Client
	once   sync.Once
)

// InitClient 初始化etcd客户端
func InitClient(config *EtcdConfig) error {
	var err error
	once.Do(func() {
		err = initClient(config)
	})
	return err
}

// initClient 内部初始化方法
func initClient(config *EtcdConfig) error {
	if config == nil {
		return fmt.Errorf("etcd config is nil")
	}

	tlsConfig, err := config.GetTLSConfig()
	if err != nil {
		return fmt.Errorf("get tls config failed: %w", err)
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.Endpoints,
		DialTimeout: config.DialTimeout,
		Username:    config.Username,
		Password:    config.Password,
		TLS:         tlsConfig,
		LogConfig: &zap.Config{
			Level:            zap.NewAtomicLevelAt(zap.ErrorLevel),
			Encoding:         "json",
			OutputPaths:      []string{"stderr"},
			ErrorOutputPaths: []string{"stderr"},
		},
	})
	if err != nil {
		return fmt.Errorf("create etcd client failed: %w", err)
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = cli.Get(ctx, "health")
	if err != nil {
		cli.Close()
		return fmt.Errorf("connect to etcd failed: %w", err)
	}

	client = cli
	return nil
}

// GetClient 获取etcd客户端实例
func GetClient() *clientv3.Client {
	return client
}

// Close 关闭etcd客户端连接
func Close() error {
	if client != nil {
		return client.Close()
	}
	return nil
}
