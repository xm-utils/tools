package nacos

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"google.golang.org/grpc/resolver"
)

const (
	// NacosScheme 是 Nacos resolver 的方案名称
	NacosScheme = "nacos"

	// 默认配置
	defaultGroup       = "DEFAULT_GROUP"
	defaultClusterName = "DEFAULT"
)

// ResolverBuilder Nacos Resolver 构建器
type ResolverBuilder struct {
	client *NamingClientWrapper
}

func NewResolverBuilder(config *Config) *ResolverBuilder {
	client, err := NewNamingClient(config)
	if err != nil {
		log.Fatalf("nacos客户端加载失败，error=%v", err)
		return nil
	}
	return &ResolverBuilder{
		client: client,
	}
}

// Scheme 实现 resolver.Builder 接口
func (b *ResolverBuilder) Scheme() string {
	return NacosScheme
}

// Build 实现 resolver.Builder 接口
func (b *ResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	// 解析服务名称
	serviceName := target.URL.Host
	if serviceName == "" {
		serviceName = target.Endpoint()
	}

	// 解析查询参数
	query := target.URL.Query()
	group := query.Get("group")
	if group == "" {
		group = defaultGroup
	}
	cluster := query.Get("cluster")
	if cluster == "" {
		cluster = defaultClusterName
	}

	log.Printf("[Nacos Resolver] Building resolver for service: %s, group: %s, cluster: %s", serviceName, group, cluster)

	// 创建 resolver 实例
	r := &nacosResolver{
		client:      b.client,
		serviceName: serviceName,
		group:       group,
		clusterName: cluster,
		cc:          cc,
		instances:   make([]resolver.Address, 0),
		cancelWatch: func() {},
	}

	// 立即解析一次
	r.ResolveNow(resolver.ResolveNowOptions{})

	// 启动服务发现监听
	r.startWatch()

	return r, nil
}

// nacosResolver Nacos Resolver 实现
type nacosResolver struct {
	client      *NamingClientWrapper
	mu          sync.Mutex
	serviceName string
	group       string
	clusterName string
	cc          resolver.ClientConn
	instances   []resolver.Address
	cancelWatch context.CancelFunc
}

// ResolveNow 实现 resolver.Resolver 接口
func (r *nacosResolver) ResolveNow(opts resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	log.Printf("[Nacos Resolver] Resolving service: %s", r.serviceName)

	// 使用 nacos 工具函数查询服务实例
	ins, err := r.client.SelectInstances(r.serviceName, true)
	if err != nil {
		log.Printf("[Nacos Resolver] Failed to select instances: %v", err)
		return
	}

	if len(ins) == 0 {
		log.Printf("[Nacos Resolver] No healthy instances found for service: %s", r.serviceName)
		r.updateAddresses([]resolver.Address{})
		return
	}

	// 转换为 gRPC Address
	addresses := make([]resolver.Address, 0, len(ins))
	for _, instance := range ins {
		addr := resolver.Address{
			Addr:       fmt.Sprintf("%s:%d", instance.Ip, instance.Port),
			ServerName: r.serviceName,
			Attributes: nil,
		}
		addresses = append(addresses, addr)
		log.Printf("[Nacos Resolver] Found instance: %s:%d, weight: %.2f, healthy: %v",
			instance.Ip, instance.Port, instance.Weight, instance.Healthy)
	}

	r.updateAddresses(addresses)
	log.Printf("[Nacos Resolver] Resolved %d instances for service: %s", len(addresses), r.serviceName)
}

// Close 实现 resolver.Resolver 接口
func (r *nacosResolver) Close() {
	r.cancelWatch()
	log.Printf("[Nacos Resolver] Closed resolver for service: %s", r.serviceName)
}

// startWatch 启动服务变化监听
func (r *nacosResolver) startWatch() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancelWatch = cancel

	callback := func(services []model.Instance, err error) {
		if err != nil {
			log.Printf("[Nacos Resolver] Watch callback error: %v", err)
			return
		}

		log.Printf("[Nacos Resolver] Service changed, instances count: %d", len(services))

		// 过滤健康实例
		addresses := make([]resolver.Address, 0)
		for _, instance := range services {
			if instance.Healthy {
				addr := resolver.Address{
					Addr:       fmt.Sprintf("%s:%d", instance.Ip, instance.Port),
					ServerName: r.serviceName,
				}
				addresses = append(addresses, addr)
			}
		}

		r.mu.Lock()
		defer r.mu.Unlock()
		r.updateAddresses(addresses)
	}

	// 订阅服务变化
	err := r.client.Subscribe(SubscribeParam{
		ServiceName:       r.serviceName,
		SubscribeCallback: callback,
	})
	if err != nil {
		log.Printf("[Nacos Resolver] Failed to subscribe service: %v", err)
		return
	}

	log.Printf("[Nacos Resolver] Started watching service: %s", r.serviceName)

	// 保持订阅直到取消
	go func() {
		<-ctx.Done()
		log.Printf("[Nacos Resolver] Stopped watching service: %s", r.serviceName)
	}()
}

// updateAddresses 更新地址列表
func (r *nacosResolver) updateAddresses(addresses []resolver.Address) {
	if err := r.cc.UpdateState(resolver.State{Addresses: addresses}); err != nil {
		log.Printf("[Nacos Resolver] Failed to update state: %v", err)
	}
}
