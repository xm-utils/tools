package grpcx

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
)

// ClientCreator create a grpc client
type ClientCreator func(string, *grpc.ClientConn) interface{}

// ClientCreatorRegistry 客户端创建器注册表（全局单例）
type ClientCreatorRegistry struct {
	mu       sync.RWMutex
	creators map[string]ClientCreator
}

var registry = &ClientCreatorRegistry{
	creators: make(map[string]ClientCreator),
}

// RegisterCreator 注册 ClientCreator（线程安全）
// 如果名称已存在则覆盖旧注册
func RegisterCreator(name string, creator ClientCreator) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	logrus.Debugf("register client creator: %s", name)
	registry.creators[name] = creator
}

// GetCreator 根据名称获取已注册的 ClientCreator
func GetCreator(name string) (ClientCreator, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	creator, ok := registry.creators[name]
	if !ok {
		return nil, fmt.Errorf("client creator not found: %s", name)
	}
	return creator, nil
}

// HasCreator 检查指定名称的 ClientCreator 是否已注册
func HasCreator(name string) bool {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	_, ok := registry.creators[name]
	return ok
}

// ListCreators 返回所有已注册的 ClientCreator 名称
func ListCreators() []string {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	names := make([]string, 0, len(registry.creators))
	for name := range registry.creators {
		names = append(names, name)
	}
	return names
}

// GRPCClient is a grpc client
type GRPCClient struct {
	sync.RWMutex
	creator ClientCreator
	opts    *clientOptions
	clients map[string]interface{}
	conn    *grpc.ClientConn
}

// NewGRPCClient returns a GRPC Client
func NewGRPCClient(creator ClientCreator, opts ...ClientOption) *GRPCClient {
	copts := &clientOptions{}
	for _, opt := range opts {
		opt(copts)
	}

	return &GRPCClient{
		opts:    copts,
		creator: creator,
		clients: make(map[string]interface{}),
	}
}

// Close 关闭客户端连接
func (c *GRPCClient) Close() error {
	c.RLock()
	defer c.RUnlock()
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// GetState 获取 gRPC 连接的当前状态
func (c *GRPCClient) GetState() connectivity.State {
	c.RLock()
	defer c.RUnlock()
	if c.conn == nil {
		return connectivity.Shutdown
	}
	return c.conn.GetState()
}

// IsReady 快速检查连接是否处于就绪状态
func (c *GRPCClient) IsReady() bool {
	return c.GetState() == connectivity.Ready
}

// HealthCheck 在指定超时内尝试等待连接就绪，返回连接状态信息
// timeout 为 0 时使用默认超时 3s
func (c *GRPCClient) HealthCheck(timeout time.Duration) error {
	c.RLock()
	defer c.RUnlock()

	if c.conn == nil {
		return fmt.Errorf("connection not established")
	}

	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	state := c.conn.GetState()
	if state == connectivity.Ready {
		return nil
	}

	// 尝试等待连接就绪
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if c.conn.WaitForStateChange(ctx, state) {
		newState := c.conn.GetState()
		if newState == connectivity.Ready {
			return nil
		}
		return fmt.Errorf("connection state changed to %s, expected Ready", newState)
	}

	return fmt.Errorf("health check timed out, current state: %s", c.conn.GetState())
}

// HealthInfo 返回连接的健康信息摘要，方便日志和监控使用
func (c *GRPCClient) HealthInfo() map[string]interface{} {
	c.RLock()
	defer c.RUnlock()

	info := map[string]interface{}{
		"target": c.opts.prefix,
	}

	if c.conn == nil {
		info["state"] = connectivity.Shutdown.String()
		info["healthy"] = false
		return info
	}

	state := c.conn.GetState()
	info["state"] = state.String()
	info["healthy"] = state == connectivity.Ready

	return info
}

// GetServiceClient returns a grpc client
func (c *GRPCClient) GetServiceClient(name string) (interface{}, error) {
	c.RLock()
	if cli, ok := c.clients[name]; ok {
		c.RUnlock()
		return cli, nil
	}
	c.RUnlock()

	client, err := c.createClient(name)

	if err != nil {
		return nil, err
	}

	return client, nil
}

func (c *GRPCClient) createClient(name string) (interface{}, error) {
	c.Lock()
	defer c.Unlock()

	if cli, ok := c.clients[name]; ok {
		return cli, nil
	}

	if c.conn == nil {

		opts := append(c.opts.grpcOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))

		conn, err := grpc.NewClient(c.opts.host, opts...)
		if err != nil {
			logrus.Errorf("create grpc client failed: %v", err)
			return nil, err
		}
		c.conn = conn
	}

	cli := c.creator(name, c.conn)
	c.clients[name] = cli

	return cli, nil
}

var (
	managerInstance *ClientManager
	managerOnce     sync.Once
)

// ServiceConfig 服务配置
type ServiceConfig struct {
	Name    string        `yaml:"name"`
	Address string        `yaml:"address"`
	Timeout time.Duration `yaml:"timeout"` // 超时时间（秒）
}

// ClientManager 客户端管理器（单例）
type ClientManager struct {
	mu      sync.RWMutex
	clients map[string]*GRPCClient
	log     *logrus.Entry
}

// GetClientManager 获取客户端管理器单例
func GetClientManager() *ClientManager {
	managerOnce.Do(func() {
		managerInstance = &ClientManager{
			log:     logrus.WithField("module", "ClientManager"),
			clients: make(map[string]*GRPCClient),
		}
	})
	return managerInstance
}

// InitializeClients 初始化所有客户端
func (m *ClientManager) InitializeClients(configs []ServiceConfig) error {
	if configs == nil || len(configs) == 0 {
		return nil
	}
	m.log.Info("开始初始化所有gRPC客户端...")

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, config := range configs {
		m.log.Infof("开始初始化%s服务客户端...", config.Name)
		client := m.createClient(config)
		m.clients[config.Name] = client
		m.log.Debugf("初始化%s服务客户端成功", config.Name)
	}

	//m.manager = manager
	m.log.Info("所有gRPC客户端初始化完成")
	return nil
}

func (m *ClientManager) GetClient(name string) *GRPCClient {
	return m.clients[name]
}

// Close 关闭所有客户端连接
func (m *ClientManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.log.Info("开始关闭所有gRPC客户端连接...")

	var errs []error

	for i, client := range m.clients {
		if err := client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close %s client failed: %w", i, err))
		}
	}

	if len(errs) > 0 {
		m.log.Errorf("关闭部分客户端连接失败: %v", errs)
		return fmt.Errorf("关闭部分客户端连接失败: %v", errs)
	}

	m.log.Info("所有gRPC客户端连接已关闭")
	return nil
}

func (m *ClientManager) createClient(config ServiceConfig) *GRPCClient {
	name := config.Name
	creator, err := GetCreator(name)
	if err != nil {
		m.log.Errorf("创建客户端失败: %v", err)
		// 使用空 creator 作为兜底，允许服务启动但创建时失败
		creator = func(string, *grpc.ClientConn) interface{} { return nil }
	}
	return NewGRPCClient(creator,
		WidthHost(config.Address),
		WithTimeout(config.Timeout),
		WithStatsHandler(&customStatsHandler{}))
}

type customStatsHandler struct{}

func (c *customStatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return ctx
}

func (c *customStatsHandler) HandleConn(ctx context.Context, connStats stats.ConnStats) {

}

func (c *customStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, "RPCTAGINFO", info)
}

func (c *customStatsHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	switch s := s.(type) {
	case *stats.End:
		info := ctx.Value("RPCTAGINFO").(*stats.RPCTagInfo)
		orderNo := ""
		val := ctx.Value("orderNo")
		if val != nil {
			orderNo = val.(string)
		}
		logrus.Printf("RPC Method: %s, orderNo: %s Duration: %v\n", info.FullMethodName, orderNo, s.EndTime.Sub(s.BeginTime))
	}
}
