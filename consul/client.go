package consul

import (
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"
)

// Config Consul 配置结构
type Config struct {
	Address    string `yaml:"address" json:"address,omitempty"`       // Consul 地址，例如: 127.0.0.1:8500
	Scheme     string `yaml:"scheme" json:"scheme,omitempty"`         // 协议 scheme，默认 http
	Datacenter string `yaml:"datacenter" json:"datacenter,omitempty"` // 数据中心
	Token      string `yaml:"token" json:"token,omitempty"`           // ACL Token
	Timeout    int64  `yaml:"timeout" json:"timeout,omitempty"`       // 超时时间
}

// Client Consul 客户端
type Client struct {
	consulClient *api.Client
	config       *Config
}

// NewClient 创建新的 Consul 客户端
func NewClient(cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = &Config{
			Address: "127.0.0.1:8500",
			Scheme:  "http",
			Timeout: 10,
		}
	}

	if cfg.Address == "" {
		cfg.Address = "127.0.0.1:8500"
	}
	if cfg.Scheme == "" {
		cfg.Scheme = "http"
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10
	}

	consulConfig := api.DefaultConfig()
	consulConfig.Address = cfg.Address
	consulConfig.Scheme = cfg.Scheme
	consulConfig.Token = cfg.Token
	consulConfig.WaitTime = time.Duration(cfg.Timeout) * time.Second

	if cfg.Datacenter != "" {
		consulConfig.Datacenter = cfg.Datacenter
	}

	client, err := api.NewClient(consulConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consul client: %w", err)
	}

	// 测试连接
	if _, err := client.Agent().Self(); err != nil {
		return nil, fmt.Errorf("failed to connect to consul: %w", err)
	}

	return &Client{
		consulClient: client,
		config:       cfg,
	}, nil
}

// GetConsulClient 获取底层的 Consul API 客户端
func (c *Client) GetConsulClient() *api.Client {
	return c.consulClient
}

// GetConfig 获取配置
func (c *Client) GetConfig() *Config {
	return c.config
}
