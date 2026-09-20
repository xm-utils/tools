package mongodb

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// recoverToErr 捕获 MongoDB 调用中可能出现的 panic，并将其转换为 error 返回，
// 避免因底层 panic 直接导致程序崩溃。
func recoverToErr(err *error) {
	if r := recover(); r != nil {
		var e error
		switch v := r.(type) {
		case error:
			e = v
		default:
			e = fmt.Errorf("%v", v)
		}
		if err != nil {
			*err = fmt.Errorf("panic recovered: %w\nstack: %s", e, debug.Stack())
		}
	}
}

// Client MongoDB 客户端
type Client struct {
	client   *mongo.Client
	database *mongo.Database
	config   *Config
}

var defaultManager *Client

// InitClient 初始化全局 MongoDB 客户端（单例模式）
func InitClient(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("config cannot be nil")
	}

	client, err := NewClient(cfg)
	if err != nil {
		return err
	}

	defaultManager = client
	return nil
}

// GetClient 获取全局 MongoDB 客户端
func GetClient() *Client {
	return defaultManager
}

// GetDatabase 获取全局默认数据库实例
func GetDatabase() *mongo.Database {
	if defaultManager == nil {
		return nil
	}
	return defaultManager.database
}

// NewClient 创建新的 MongoDB 客户端
func NewClient(cfg *Config) (result *Client, err error) {
	defer recoverToErr(&err)
	if cfg == nil {
		cfg = GetDefaultConfig()
	}

	// 设置默认值
	if cfg.AuthSource == "" {
		cfg.AuthSource = "admin"
	}
	if cfg.MaxPoolSize == 0 {
		cfg.MaxPoolSize = 100
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.SocketTimeout == 0 {
		cfg.SocketTimeout = 30 * time.Second
	}
	if cfg.ServerSelectionTimeout == 0 {
		cfg.ServerSelectionTimeout = 30 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}

	// 构建客户端选项
	clientOpts := options.Client().
		ApplyURI(cfg.URI).
		SetMaxPoolSize(cfg.MaxPoolSize).
		SetMinPoolSize(cfg.MinPoolSize).
		SetConnectTimeout(cfg.ConnectTimeout).
		SetServerSelectionTimeout(cfg.ServerSelectionTimeout).
		SetHeartbeatInterval(cfg.HeartbeatInterval)

	// 设置连接字符串或主机
	if cfg.URI != "" {
		// URI 已在上面设置
	} else if len(cfg.Hosts) > 0 {
		clientOpts.SetHosts(cfg.Hosts)
	} else {
		return nil, fmt.Errorf("either URI or Hosts must be provided")
	}

	// 设置认证信息
	if cfg.Username != "" && cfg.Password != "" {
		credential := options.Credential{
			Username:      cfg.Username,
			Password:      cfg.Password,
			AuthSource:    cfg.AuthSource,
			AuthMechanism: "", // 使用默认机制
		}
		clientOpts.SetAuth(credential)
	}

	// 设置连接池
	clientOpts.SetMaxPoolSize(cfg.MaxPoolSize)
	clientOpts.SetMinPoolSize(cfg.MinPoolSize)

	// 设置超时时间（v2 移除了 SetSocketTimeout）
	// Socket timeout 现在通过 context 控制

	// 设置副本集
	if cfg.ReplicaSet != "" {
		clientOpts.SetReplicaSet(cfg.ReplicaSet)
	}

	// 设置直连模式
	if cfg.Direct {
		clientOpts.SetDirect(true)
	}

	// 设置 TLS/SSL
	if cfg.TLS {
		// 在实际项目中，这里需要配置 tls.Config
		// tlsConfig := &tls.Config{}
		// clientOpts.SetTLSConfig(tlsConfig)
		_ = cfg.TLSCAFile   // 避免未使用警告
		_ = cfg.TLSCertFile // 避免未使用警告
		_ = cfg.TLSKeyFile  // 避免未使用警告
	}

	// 创建客户端
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	mc, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// 测试连接
	if err := mc.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	// 获取数据库实例
	var db *mongo.Database
	if cfg.Database != "" {
		db = mc.Database(cfg.Database)
	}

	return &Client{
		client:   mc,
		database: db,
		config:   cfg,
	}, nil
}

// GetMongoClient 获取底层的 MongoDB 客户端
func (c *Client) GetMongoClient() *mongo.Client {
	return c.client
}

// GetDatabase 获取数据库实例
func (c *Client) GetDatabase(dbName ...string) *mongo.Database {
	if len(dbName) > 0 && dbName[0] != "" {
		return c.client.Database(dbName[0])
	}
	return c.database
}

// GetConfig 获取配置
func (c *Client) GetConfig() *Config {
	return c.config
}

// Ping 检查 MongoDB 连接状态
func (c *Client) Ping(ctx context.Context) (err error) {
	defer recoverToErr(&err)
	if ctx == nil {
		ctx = context.Background()
	}
	return c.client.Ping(ctx, readpref.Primary())
}

// Close 关闭 MongoDB 连接
func (c *Client) Close(ctx context.Context) (err error) {
	defer recoverToErr(&err)
	if ctx == nil {
		ctx = context.Background()
	}
	return c.client.Disconnect(ctx)
}

// Collection 获取集合实例。
// 为保持向后兼容，当数据库未配置或底层发生 panic 时返回 nil，而不会导致程序崩溃。
// 若需要获取具体错误信息，请使用内部方法 getCollection。
func (c *Client) Collection(collectionName string, dbName ...string) (coll *mongo.Collection) {
	defer func() {
		if r := recover(); r != nil {
			coll = nil
		}
	}()
	db := c.GetDatabase(dbName...)
	if db == nil {
		return nil
	}
	return db.Collection(collectionName)
}

// getCollection 获取集合实例，当数据库未配置时返回 error 而非 panic，
// 供内部 CRUD 方法使用，以便将异常统一转换为 error 返回值。
func (c *Client) getCollection(collectionName string, dbName ...string) (*mongo.Collection, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("mongodb client is not initialized")
	}
	db := c.GetDatabase(dbName...)
	if db == nil {
		return nil, errors.New("database not configured, please specify database name")
	}
	return db.Collection(collectionName), nil
}
