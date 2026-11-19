// Package registry 提供服务注册与发现的统一抽象层。
//
// 业务代码只依赖本包定义的 Registry / Discovery / Backend 接口，
// consul、nacos 等具体注册中心由子包（registry/consul、registry/nacos）
// 以适配器形式实现接口，切换注册中心时业务代码零改动。
//
// 本包不 import 任何注册中心 SDK，保持轻量。
package registry

import (
	"context"
	"errors"
	"time"
)

// 通用错误
var (
	// ErrServiceEmpty 服务名为空
	ErrServiceEmpty = errors.New("registry: service name cannot be empty")
	// ErrInstanceEmpty 实例信息缺失
	ErrInstanceEmpty = errors.New("registry: service instance is nil")
	// ErrNoopWatch 实现不支持持续监听
	ErrUnsupported = errors.New("registry: operation not supported by this backend")
)

// ServiceInstance 统一的服务实例模型，屏蔽 consul/nacos 字段差异。
type ServiceInstance struct {
	ID      string            // 实例唯一 ID（consul: ServiceID；nacos: InstanceId，为空时由实现生成）
	Name    string            // 服务名
	Host    string            // 实例地址（IP 或域名）
	Port    int               // 实例端口
	Tags    []string          // 服务标签（nacos 无原生 tag，适配器会将其并入 Meta["tags"]）
	Meta    map[string]string // 元数据/扩展字段
	Healthy bool              // 健康状态（仅查询结果有意义）
	Weight  float64           // 权重（consul 无此概念，恒为 0；nacos 有效，注册时 <=0 按 1 处理）
}

// Address 返回 host:port 形式的实例地址。
func (s ServiceInstance) Address() string {
	return joinHostPort(s.Host, s.Port)
}

// HealthCheckSpec 注册时声明的健康检查配置。
// nacos 采用客户端心跳（临时实例），适配器会忽略 Checks 并以后台心跳维持。
type HealthCheckSpec struct {
	HTTP     string        // HTTP 检查地址，如 http://host:port/health
	TCP      string        // TCP 检查地址，如 host:port
	TTL      time.Duration // TTL 检查：要求应用在 TTL 周期内主动上报（consul UpdateTTLCheck）
	Interval time.Duration // 检查间隔，如 10s（HTTP/TCP 型）
	Timeout  time.Duration // 单次检查超时，如 5s
}

// RegisterRequest 注册请求。
type RegisterRequest struct {
	Instance ServiceInstance
	Checks   []HealthCheckSpec // 为空时实现使用默认检查（如 consul 无检查、nacos 心跳）
}

// DiscoverRequest 服务发现查询条件。
type DiscoverRequest struct {
	Name        string   // 服务名，必填
	Tags        []string // 标签过滤，需全部命中（consul 原生支持；nacos 由适配器在结果内过滤 Meta["tags"]）
	HealthyOnly bool     // 仅返回健康实例
}

// Registry 服务注册/注销能力。所有方法均支持 ctx 取消。
type Registry interface {
	// Register 注册服务实例。Instance.Name/Host 为空、Port<=0 时返回错误。
	Register(ctx context.Context, req *RegisterRequest) error
	// Deregister 注销服务实例。consul 按 ID 注销；nacos 按 Host:Port 注销。
	Deregister(ctx context.Context, inst ServiceInstance) error
}

// Discovery 服务发现能力。
type Discovery interface {
	// Services 按条件查询服务实例列表。
	Services(ctx context.Context, req DiscoverRequest) ([]*ServiceInstance, error)
	// AllServices 返回所有已注册的服务名列表。
	AllServices(ctx context.Context) ([]string, error)
}

// Backend 完整后端能力：注册 + 发现 + 元信息。
type Backend interface {
	Registry
	Discovery
	// Name 返回后端类型标识："consul" / "nacos"。
	Name() string
	// Close 释放底层客户端资源（幂等）。
	Close() error
}
