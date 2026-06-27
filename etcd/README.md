# ETCD Service Registry & Discovery

基于 etcd 的服务注册与发现 Go 语言库，提供服务注册、服务发现、健康检查、元数据过滤等功能。

## 📋 目录

- [项目简介](#项目简介)
- [主要功能](#主要功能)
- [技术栈](#技术栈)
- [安装](#安装)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
- [API 文档](#api-文档)
- [使用示例](#使用示例)
- [测试](#测试)
- [项目结构](#项目结构)
- [许可证](#许可证)

## 🎯 项目简介

这是一个轻量级的服务注册与发现库，基于 etcd v3 实现。它提供了完整的服务治理能力，包括服务注册、服务发现、健康检查、租约管理等功能，适用于微服务架构中的服务治理场景。

## ✨ 主要功能

- ✅ **服务注册** - 支持服务实例注册到 etcd，自动获取本地 IP
- ✅ **服务发现** - 根据服务名称查询可用的服务实例列表
- ✅ **服务监听** - 实时监听服务实例的变化（新增/删除）
- ✅ **健康检查** - 定期检查服务实例的健康状态并刷新租约
- ✅ **租约管理** - 基于 etcd Lease 实现服务心跳保持
- ✅ **元数据过滤** - 支持根据自定义元数据过滤服务实例
- ✅ **TLS 支持** - 支持安全的 TLS 连接
- ✅ **认证支持** - 支持用户名密码认证

## 🛠 技术栈

- **Go**: 1.25.0+
- **etcd Client**: go.etcd.io/etcd/client/v3 v3.6.11
- **gRPC**: google.golang.org/grpc v1.79.3
- **日志**: go.uber.org/zap v1.27.0

## 📦 安装

```bash
go get github.com/XingMenTech/utils/etcd
```

或在 `go.mod` 中添加：

```go
require github.com/XingMenTech/utils/etcd latest
```

## 🚀 快速开始

### 1. 初始化 etcd 客户端

```go
import "github.com/XingMenTech/utils/etcd"

config := &etcd.EtcdConfig{
    Endpoints:   []string{"localhost:2379"},
    DialTimeout: 5 * time.Second,
}

err := etcd.InitClient(config)
if err != nil {
    log.Fatal(err)
}
defer etcd.Close()
```

### 2. 注册服务

```go
serviceInfo := etcd.ServiceInfo{
    Name:    "user-service",
    Version: "1.0.0",
    Host:    "", // 空字符串将自动获取本地IP
    Port:    8080,
    Metadata: map[string]string{
        "region": "cn-north-1",
        "env":    "prod",
    },
    TTL: 10, // 租约时间10秒
}

err = etcd.RegisterService(serviceInfo)
if err != nil {
    log.Fatal(err)
}
```

### 3. 发现服务

```go
// 获取所有服务实例
services, err := etcd.DiscoverServices("user-service")
if err != nil {
    log.Fatal(err)
}

for _, service := range services {
    fmt.Printf("Service: %s:%d (version: %s)\n", 
        service.Host, service.Port, service.Version)
}

// 或获取简化的实例地址列表
instances, err := etcd.GetServiceInstances("user-service")
```

### 4. 监听服务变化

```go
err = etcd.WatchService("user-service", func(services []etcd.ServiceInfo) {
    fmt.Printf("Services updated, count: %d\n", len(services))
    for _, service := range services {
        fmt.Printf("  - %s:%d\n", service.Host, service.Port)
    }
})
```

## ⚙️ 配置说明

### EtcdConfig 配置结构

```go
type EtcdConfig struct {
    Endpoints   []string      // etcd服务器地址列表
    DialTimeout time.Duration // 连接超时时间
    Username    string        // 用户名（可选）
    Password    string        // 密码（可选）
    TLS         TLSConfig     // TLS配置（可选）
}
```

### YAML 配置示例

创建 `config.yaml` 文件：

```yaml
etcd:
  endpoints:
    - "localhost:2379"
  
  dialTimeout: 5s
  
  # 认证信息（可选）
  # username: "admin"
  # password: "password"
  
  # TLS配置（可选）
  tls:
    enabled: false
    # certFile: "/path/to/client.crt"
    # keyFile: "/path/to/client.key"
    # caFile: "/path/to/ca.crt"
    # insecureSkipVerify: false
```

### 使用默认配置

```go
config := etcd.DefaultEtcdConfig()
// 返回: endpoints=["localhost:2379"], dialTimeout=5s
```

## 📖 API 文档

### 客户端管理

#### `InitClient(config *EtcdConfig) error`
初始化 etcd 客户端（单例模式）。

#### `GetClient() *clientv3.Client`
获取 etcd 客户端实例。

#### `Close() error`
关闭 etcd 客户端连接。

### 服务注册

#### `RegisterService(info ServiceInfo) error`
注册服务实例到 etcd。

**参数：**
- `info`: 服务信息结构体
  - `Name`: 服务名称
  - `Version`: 服务版本
  - `Host`: 服务主机（空则自动获取）
  - `Port`: 服务端口
  - `Metadata`: 元数据（键值对）
  - `TTL`: 租约时间（秒），默认10秒

#### `DeregisterService(name, host string, port int) error`
注销服务实例。

### 服务发现

#### `DiscoverServices(serviceName string) ([]ServiceInfo, error)`
发现指定服务的所有可用实例。

#### `GetServiceInstances(serviceName string) ([]string, error)`
获取服务实例的地址列表（格式：`host:port`）。

#### `SelectServiceInstance(serviceName string) (*ServiceInfo, error)`
选择一个可用的服务实例（当前返回第一个）。

#### `WatchService(serviceName string, callback func([]ServiceInfo)) error`
监听服务实例变化，当服务注册或注销时触发回调。

#### `FilterServicesByMetadata(serviceName string, metadata map[string]string) ([]ServiceInfo, error)`
根据元数据过滤服务实例。

### 健康检查

#### `NewHealthChecker(serviceName, host string, port int, interval, timeout time.Duration) *HealthChecker`
创建健康检查器。

**参数：**
- `serviceName`: 服务名称
- `host`: 服务主机
- `port`: 服务端口
- `interval`: 检查间隔
- `timeout`: 超时时间

#### `Start()` / `Stop()`
启动/停止健康检查。

### 租约管理

#### `NewLeaseManager() *LeaseManager`
创建租约管理器。

#### `CreateLease(key string, ttl int64) (clientv3.LeaseID, error)`
创建租约。

#### `RevokeLease(key string) error`
撤销租约。

#### `KeepAliveLease(key string) error`
保持租约活跃。

### 工具函数

#### `GetLocalIP() (string, error)`
获取本地非回环 IP 地址。

## 💡 使用示例

查看 [example.go](file:///Users/zhangyuan/workspase/xm/etcd/example.go) 获取完整示例：

- `exampleRegister()` - 服务注册示例
- `exampleDiscovery()` - 服务发现示例
- `exampleWatch()` - 服务监听示例
- `exampleFilterByMetadata()` - 元数据过滤示例
- `exampleHealthCheck()` - 健康检查示例
- `exampleComplete()` - 完整流程示例（注册→发现→注销）

运行示例：

```bash
go run example.go
```

## 🧪 测试

运行单元测试：

```bash
go test -v
```

测试覆盖：
- 配置初始化测试
- 本地 IP 获取测试
- 服务信息结构测试
- 健康检查器测试
- 租约管理器测试
- 元数据匹配测试

## 📁 项目结构

```
etcd/
├── client.go              # etcd 客户端初始化和连接管理
├── config.go              # 配置结构定义和 TLS 配置
├── service_register.go    # 服务注册与注销
├── service_discovery.go   # 服务发现、监听和过滤
├── health_check.go        # 健康检查和租约管理
├── example.go             # 使用示例代码
├── etcd_test.go           # 单元测试
├── config.example.yaml    # 配置文件示例
├── go.mod                 # Go 模块依赖
└── go.sum                 # 依赖校验文件
```

## 🔍 核心特性详解

### 服务注册流程

1. 创建 etcd Lease（租约）
2. 将服务信息序列化为 JSON
3. 以 `/services/{name}/{host}:{port}` 为 key 存储到 etcd
4. 启动后台协程保持租约活跃（心跳）

### 服务发现机制

- 使用前缀查询 `/services/{name}/` 获取所有实例
- 支持实时 Watch 监听服务变化
- 支持基于元数据的灵活过滤

### 健康检查策略

- 定时检查服务在 etcd 中的存在性
- 自动刷新租约以保持服务活跃
- 可配置检查间隔和超时时间

### 租约管理

- 基于 etcd Lease 实现 TTL 机制
- 服务下线后自动清理（租约过期）
- 支持手动撤销租约

## 📝 注意事项

1. **etcd 依赖**: 确保已部署 etcd 集群并可访问
2. **网络连接**: 保证应用与 etcd 服务器的网络连通性
3. **租约时间**: 合理设置 TTL，建议为检查间隔的 2-3 倍
4. **并发安全**: `InitClient` 使用 sync.Once 保证线程安全
5. **资源释放**: 使用完毕后调用 `Close()` 释放连接

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

本项目采用 MIT 许可证。

---

**作者**: XingMenTech  
**仓库**: github.com/XingMenTech/utils/etcd