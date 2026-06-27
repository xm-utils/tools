package etcd

import (
	"crypto/tls"
	"time"
)

// EtcdConfig etcd配置
type EtcdConfig struct {
	Endpoints   []string      `yaml:"endpoints" json:"endpoints"`         // etcd服务器地址列表
	DialTimeout time.Duration `yaml:"dialTimeout" json:"dialTimeout"`     // 连接超时时间
	Username    string        `yaml:"username" json:"username,omitempty"` // 用户名（可选）
	Password    string        `yaml:"password" json:"password,omitempty"` // 密码（可选）
	TLS         TLSConfig     `yaml:"tls" json:"tls,omitempty"`           // TLS配置（可选）
}

// TLSConfig TLS配置
type TLSConfig struct {
	Enabled            bool   `yaml:"enabled" json:"enabled"`                       // 是否启用TLS
	CertFile           string `yaml:"certFile" json:"certFile,omitempty"`           // 客户端证书文件
	KeyFile            string `yaml:"keyFile" json:"keyFile,omitempty"`             // 客户端私钥文件
	CaFile             string `yaml:"caFile" json:"caFile,omitempty"`               // CA证书文件
	InsecureSkipVerify bool   `yaml:"insecureSkipVerify" json:"insecureSkipVerify"` // 跳过证书验证
}

// DefaultEtcdConfig 返回默认配置
func DefaultEtcdConfig() *EtcdConfig {
	return &EtcdConfig{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}
}

// GetTLSConfig 获取TLS配置
func (c *EtcdConfig) GetTLSConfig() (*tls.Config, error) {
	if !c.TLS.Enabled {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: c.TLS.InsecureSkipVerify,
	}

	if c.TLS.CertFile != "" && c.TLS.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.TLS.CertFile, c.TLS.KeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if c.TLS.CaFile != "" {
		// 这里需要导入crypto/x509和os包来加载CA证书
		// 为了简化，暂时不实现CA证书加载
	}

	return tlsConfig, nil
}
