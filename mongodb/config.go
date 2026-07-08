package mongodb

import (
	"time"
)

// Config MongoDB 客户端配置
type Config struct {
	// URI MongoDB 连接字符串，例如: mongodb://user:password@host1:27017,host2:27017/database
	URI string `yaml:"uri" json:"uri,omitempty"`

	// Hosts 服务器地址列表（当不使用 URI 时）
	Hosts []string `yaml:"hosts" json:"hosts,omitempty"`

	// Database 默认数据库名
	Database string `yaml:"database" json:"database,omitempty"`

	// Username 用户名（可选）
	Username string `yaml:"username" json:"username,omitempty"`

	// Password 密码（可选）
	Password string `yaml:"password" json:"password,omitempty"`

	// AuthSource 认证数据库，默认为 admin
	AuthSource string `yaml:"auth_source" json:"auth_source,omitempty"`

	// MaxPoolSize 最大连接池大小，默认 100
	MaxPoolSize uint64 `yaml:"max_pool_size" json:"max_pool_size,omitempty"`

	// MinPoolSize 最小连接池大小，默认 0
	MinPoolSize uint64 `yaml:"min_pool_size" json:"min_pool_size,omitempty"`

	// ConnectTimeout 连接超时时间，默认 10 秒
	ConnectTimeout time.Duration `yaml:"connect_timeout" json:"connect_timeout,omitempty"`

	// SocketTimeout Socket 超时时间，默认 30 秒
	SocketTimeout time.Duration `yaml:"socket_timeout" json:"socket_timeout,omitempty"`

	// ServerSelectionTimeout 服务器选择超时时间，默认 30 秒
	ServerSelectionTimeout time.Duration `yaml:"server_selection_timeout" json:"server_selection_timeout,omitempty"`

	// HeartbeatInterval 心跳检测间隔，默认 10 秒
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval" json:"heartbeat_interval,omitempty"`

	// ReplicaSet 副本集名称（可选）
	ReplicaSet string `yaml:"replica_set" json:"replica_set,omitempty"`

	// Direct 是否直连，默认 false
	Direct bool `yaml:"direct" json:"direct,omitempty"`

	// TLS 是否启用 TLS/SSL，默认 false
	TLS bool `yaml:"tls" json:"tls,omitempty"`

	// TLSCAFile CA 证书文件路径（可选）
	TLSCAFile string `yaml:"tls_ca_file" json:"tls_ca_file,omitempty"`

	// TLSCertFile 客户端证书文件路径（可选）
	TLSCertFile string `yaml:"tls_cert_file" json:"tls_cert_file,omitempty"`

	// TLSKeyFile 客户端私钥文件路径（可选）
	TLSKeyFile string `yaml:"tls_key_file" json:"tls_key_file,omitempty"`
}

// GetDefaultConfig 获取默认配置
func GetDefaultConfig() *Config {
	return &Config{
		MaxPoolSize:            100,
		MinPoolSize:            0,
		ConnectTimeout:         10 * time.Second,
		SocketTimeout:          30 * time.Second,
		ServerSelectionTimeout: 30 * time.Second,
		HeartbeatInterval:      10 * time.Second,
		AuthSource:             "admin",
	}
}
