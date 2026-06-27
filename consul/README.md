# Consul SDK - Go 语言实现

[![Go Version](https://img.shields.io/badge/go-1.26+-blue.svg)](https://golang.org/)
[![Consul API](https://img.shields.io/badge/consul--api-v1.34.2-green.svg)](https://github.com/hashicorp/consul/api)
[![License](https://img.shields.io/badge/license-MIT-orange.svg)](LICENSE)

这是一个基于 HashiCorp Consul API 的 Go 语言 SDK，提供了服务注册与发现、健康检查、KV 配置管理、分布式锁等核心功能。

## 📋 目录

- [功能特性](#-功能特性)
- [安装](#-安装)
- [快速开始](#-快速开始)
- [使用示例](#-使用示例)
- [API 文档](#-api-文档)
- [项目结构](#-项目结构)
- [依赖说明](#-依赖说明)
- [最佳实践](#-最佳实践)
- [常见问题](#-常见问题)

## ✨ 功能特性

### 🔧 核心功能

- **服务注册与发现** - 完整的服务生命周期管理
  - 支持 HTTP、TCP、TTL 等多种健康检查方式
  - 服务标签和元数据管理
  - 自动服务发现和负载均衡

- **健康检查** - 全方位的健康监控
  - 节点级别健康检查
  - 服务级别健康检查
  - TTL 检查状态更新
  - 自定义健康检查注册

- **KV 存储管理** - 灵活的配置管理
  - 配置的增删改查
  - 支持字符串和 JSON 格式
  - CAS 原子操作保证一致性
  - 阻塞查询实现配置热更新

- **分布式锁** - 可靠的分布式协调
  - 基于 Session 的分布式锁
  - 会话自动过期清理
  - 锁获取和释放

- **Watch 机制** - 实时监听变化
  - 服务变化监听
  - 配置变更监听
  - 阻塞查询减少轮询开销

## 📦 安装

### 前置要求

- Go 1.26 或更高版本
- Consul 服务器（用于测试和运行）

### 安装 SDK

在你的 Go 项目中：

```bash
go get github.com/XingMenTech/utils/consul
```

### 安装 Consul（本地测试）

**macOS:**
```bash
brew install consul
consul agent -dev
```

**Docker:**
```bash
docker run -d --name consul -p 8500:8500 consul:latest agent -dev -client=0.0.0.0
```

**Linux:**
```bash
curl -fsSL https://releases.hashicorp.com/consul/1.17.0/consul_1.17.0_linux_amd64.zip -o consul.zip
unzip consul.zip
sudo mv consul /usr/local/bin/
consul agent -dev
```

## 🚀 快速开始

### 1. 创建 Consul 客户端

```go
package main

import (
    "log"
    consul "github.com/XingMenTech/utils/consul"
)

func main() {
    // 创建客户端
    client, err := consul.NewClient(&consul.Config{
        Address: "127.0.0.1:8500",
        Scheme:  "http",
        Timeout: 10, // 超时时间（秒）
    })
    if err != nil {
        log.Fatalf("创建客户端失败: %v", err)
    }
    
    log.Println("✓ Consul 客户端创建成功")
}
```

### 2. 注册服务

```go
// 注册服务
err := client.RegisterService(&consul.ServiceRegistration{
    ServiceName: "web-service",
    ServiceID:   "web-service-1",
    Address:     "127.0.0.1",
    Port:        8080,
    Tags:        []string{"production", "v1"},
    Meta: map[string]string{
        "version": "1.0.0",
        "env":     "production",
    },
    Checks: []*consul.AgentCheck{
        {
            CheckID:   "http-health-check",
            Name:      "HTTP Health Check",
            HTTP:      "http://127.0.0.1:8080/health",
            Interval:  "10s",
            Timeout:   "5s",
            DeregisterCriticalServiceAfter: "30s",
        },
    },
})
if err != nil {
    log.Printf("注册服务失败: %v", err)
}
```

### 3. 发现服务

```go
// 发现健康的服务实例
services, err := client.GetHealthyServices("web-service")
if err != nil {
    log.Printf("发现服务失败: %v", err)
}

for _, svc := range services {
    fmt.Printf("服务地址: %s:%d\n", 
        svc.Service.Service.Address,
        svc.Service.Service.Port)
}

// 获取服务地址列表
addresses, _ := client.GetServiceAddresses("web-service", true)
fmt.Printf("可用地址: %v\n", addresses)
```

### 4. 配置管理

```go
// 写入配置
client.PutKV("config/app/name", []byte("my-app"))

// 读取配置
value, _ := client.GetKVString("config/app/name")
fmt.Printf("配置值: %s\n", value)

// 监听配置变化
lastIndex := uint64(0)
pair, newIndex, _ := client.WatchKV("config/app/name", lastIndex, 30*time.Second)
if pair != nil {
    fmt.Printf("配置已更新: %s\n", string(pair.Value))
}
```

## 💡 使用示例

### 服务注册完整示例

```go
package main

import (
    "fmt"
    "log"
    consul "github.com/XingMenTech/utils/consul"
)

func main() {
    // 创建客户端
    client, err := consul.NewClient(&consul.Config{
        Address: "127.0.0.1:8500",
        Scheme:  "http",
        Timeout: 10,
    })
    if err != nil {
        log.Fatal(err)
    }

    // 注册服务
    reg := &consul.ServiceRegistration{
        ServiceName: "api-gateway",
        ServiceID:   "api-gateway-1",
        Address:     "192.168.1.100",
        Port:        8080,
        Tags:        []string{"gateway", "v2"},
        Checks: []*consul.AgentCheck{
            {
                CheckID:                        "api-health",
                Name:                           "API Health Check",
                HTTP:                           "http://192.168.1.100:8080/health",
                Interval:                       "10s",
                Timeout:                        "5s",
                DeregisterCriticalServiceAfter: "60s",
            },
        },
    }

    if err := client.RegisterService(reg); err != nil {
        log.Fatal(err)
    }

    fmt.Println("✓ 服务注册成功")

    // 程序退出时注销服务
    defer client.DeregisterService("api-gateway-1")

    // 保持程序运行
    select {}
}
```

### TTL 健康检查示例

```go
// 注册 TTL 检查
check := &consul.AgentCheck{
    CheckID: "app-ttl-check",
    Name:    "Application TTL Check",
    TTL:     "30s",
    Notes:   "Application reports status via TTL",
}
client.RegisterCheck(check)

// 在 goroutine 中定期更新
go func() {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        err := client.UpdateTTLCheck("app-ttl-check", "passing", "OK")
        if err != nil {
            log.Printf("更新 TTL 失败: %v", err)
        }
    }
}()
```

### 分布式锁示例

```go
// 创建会话
sessionID, _ := client.CreateSession("my-lock", 10*time.Second)

// 获取锁
if acquired, _ := client.AcquireLock("locks/resource", sessionID); acquired {
    fmt.Println("✓ 获取锁成功")
    
    // 执行业务逻辑
    // ...
    
    // 释放锁
    client.ReleaseLock("locks/resource", sessionID)
    fmt.Println("✓ 释放锁成功")
}

// 销毁会话
client.DestroySession(sessionID)
```

### 配置热更新示例

```go
// 启动配置监听
go func() {
    lastIndex := uint64(0)
    for {
        pair, newIndex, err := client.WatchKV("config/settings", lastIndex, 30*time.Second)
        if err != nil {
            log.Printf("监听配置失败: %v", err)
            time.Sleep(5 * time.Second)
            continue
        }
        
        if pair != nil {
            var settings map[string]interface{}
            json.Unmarshal(pair.Value, &settings)
            fmt.Printf("配置已更新: %+v\n", settings)
            
            // 应用新配置
            applyConfig(settings)
        }
        
        lastIndex = newIndex
    }
}()
```

## 📚 API 文档

### Client 初始化

#### NewClient
创建 Consul 客户端。

```go
func NewClient(cfg *Config) (*Client, error)
```

**参数:**
- `cfg`: 客户端配置
  - `Address`: Consul 地址（默认: 127.0.0.1:8500）
  - `Scheme`: 协议类型（默认: http）
  - `Datacenter`: 数据中心（可选）
  - `Token`: ACL Token（可选）
  - `Timeout`: 超时时间（秒，默认: 10）

**示例:**
```go
client, err := consul.NewClient(&consul.Config{
    Address: "127.0.0.1:8500",
    Scheme:  "http",
    Timeout: 10,
})
```

### 服务管理

#### RegisterService
注册服务到 Consul。

```go
func (c *Client) RegisterService(reg *ServiceRegistration) error
```

#### DeregisterService
从 Consul 注销服务。

```go
func (c *Client) DeregisterService(serviceID string) error
```

#### GetLocalServices
获取本地代理上注册的所有服务。

```go
func (c *Client) GetLocalServices() (map[string]*api.AgentService, error)
```

### 服务发现

#### DiscoverServices
根据服务名称发现服务。

```go
func (c *Client) DiscoverServices(serviceName string, passingOnly bool) ([]*ServiceDiscovery, error)
```

**参数:**
- `serviceName`: 服务名称
- `passingOnly`: 是否只返回健康的服务

#### GetHealthyServices
获取所有健康的服务实例。

```go
func (c *Client) GetHealthyServices(serviceName string) ([]*ServiceDiscovery, error)
```

#### GetServiceAddresses
获取服务的地址列表。

```go
func (c *Client) GetServiceAddresses(serviceName string, passingOnly bool) ([]string, error)
```

**返回格式:** `["ip:port", "ip:port", ...]`

#### GetAllServices
获取所有已注册的服务。

```go
func (c *Client) GetAllServices() (map[string][]string, error)
```

#### WatchServices
监听服务变化（阻塞查询）。

```go
func (c *Client) WatchServices(serviceName string, passingOnly bool, lastIndex uint64, timeout string) ([]*ServiceDiscovery, uint64, error)
```

### 健康检查

#### GetNodeHealth
获取节点的健康状态。

```go
func (c *Client) GetNodeHealth(node string) ([]*CheckResult, error)
```

#### GetServiceHealth
获取服务的健康状态。

```go
func (c *Client) GetServiceHealth(serviceName string) ([]*CheckResult, error)
```

#### UpdateTTLCheck
更新 TTL 类型的健康检查状态。

```go
func (c *Client) UpdateTTLCheck(checkID, status, note string) error
```

**参数:**
- `checkID`: 检查 ID
- `status`: 状态（passing/warning/critical）
- `note`: 备注信息

#### RegisterCheck
注册自定义健康检查。

```go
func (c *Client) RegisterCheck(check *AgentCheck) error
```

### KV 存储

#### PutKV
写入 KV 配置。

```go
func (c *Client) PutKV(key string, value []byte) error
```

#### GetKV
获取 KV 配置。

```go
func (c *Client) GetKV(key string) (*KVPair, error)
```

#### GetKVString
获取字符串类型的 KV 配置。

```go
func (c *Client) GetKVString(key string) (string, error)
```

#### GetKVJSON
获取 JSON 类型的 KV 配置并解析。

```go
func (c *Client) GetKVJSON(key string, result interface{}) error
```

#### ListKV
列出指定前缀下的所有 KV。

```go
func (c *Client) ListKV(prefix string, recurse bool) ([]*KVPair, error)
```

#### DeleteKV
删除 KV 配置。

```go
func (c *Client) DeleteKV(key string) error
```

#### CompareAndSet
CAS 原子更新操作。

```go
func (c *Client) CompareAndSet(key string, value []byte, modifyIndex uint64) (bool, error)
```

#### WatchKV
监听 KV 变化（阻塞查询）。

```go
func (c *Client) WatchKV(key string, lastIndex uint64, timeout time.Duration) (*KVPair, uint64, error)
```

### 分布式锁

#### CreateSession
创建会话（用于分布式锁）。

```go
func (c *Client) CreateSession(name string, ttl time.Duration) (string, error)
```

#### AcquireLock
获取分布式锁。

```go
func (c *Client) AcquireLock(key string, sessionID string) (bool, error)
```

#### ReleaseLock
释放分布式锁。

```go
func (c *Client) ReleaseLock(key string, sessionID string) (bool, error)
```

#### DestroySession
销毁会话。

```go
func (c *Client) DestroySession(sessionID string) error
```

## 📁 项目结构

```
consul/
├── client.go           # Consul 客户端初始化和配置
├── service.go          # 服务注册与注销功能
├── discovery.go        # 服务发现功能
├── health.go           # 健康检查功能
├── kv.go               # KV 存储和配置管理
├── config.example.yaml # YAML 配置示例
├── go.mod              # Go 模块依赖
├── go.sum              # 依赖校验文件
└── .gitignore          # Git 忽略配置
```

### 核心文件说明

| 文件 | 说明 | 主要功能 |
|------|------|----------|
| `client.go` | 客户端核心 | 客户端创建、连接管理、配置处理 |
| `service.go` | 服务管理 | 服务注册、注销、查询 |
| `discovery.go` | 服务发现 | 服务发现、地址查询、Watch 机制 |
| `health.go` | 健康检查 | 健康状态查询、TTL 更新、检查注册 |
| `kv.go` | KV 存储 | 配置读写、CAS 操作、分布式锁 |

## 🔗 依赖说明

### 直接依赖

```go
require (
    github.com/hashicorp/consul/api v1.34.2  // Consul API 客户端
    gopkg.in/yaml.v3 v3.0.1                  // YAML 解析库
)
```

### 间接依赖

项目使用了以下间接依赖（由 Consul API 引入）：

- `github.com/hashicorp/go-cleanhttp` - HTTP 客户端
- `github.com/hashicorp/go-rootcerts` - CA 证书处理
- `github.com/mitchellh/mapstructure` - 结构体映射
- `github.com/fatih/color` - 终端颜色输出
- 其他 HashiCorp 生态库

## 🎯 最佳实践

### 1. 服务注册时机

在服务启动时注册，在关闭时注销：

```go
func main() {
    client, _ := consul.NewClient(config)
    
    // 注册服务
    client.RegisterService(reg)
    
    // 确保服务注销
    defer client.DeregisterService(serviceID)
    
    // 启动服务
    startServer()
}
```

### 2. 健康检查选择

- **HTTP 检查**: 适用于 Web 服务，最常用
- **TCP 检查**: 适用于数据库、缓存等服务
- **TTL 检查**: 适用于需要在应用层控制健康状态的场景

### 3. 配置管理策略

```go
// 使用分层配置
config/app/database    # 数据库配置
config/app/cache       # 缓存配置
config/app/logging     # 日志配置

// 使用前缀隔离不同环境
config/prod/app/...    # 生产环境
config/dev/app/...     # 开发环境
```

### 4. 分布式锁使用

```go
// 设置合理的会话 TTL
sessionID, _ := client.CreateSession("lock", 10*time.Second)

// 及时释放锁
if acquired, _ := client.AcquireLock(key, sessionID); acquired {
    defer client.ReleaseLock(key, sessionID)
    // 业务逻辑
}

// 定期续期（长时间任务）
go func() {
    ticker := time.NewTicker(5 * time.Second)
    for range ticker.C {
        client.RenewSession(sessionID)
    }
}()
```

### 5. Watch 机制优化

```go
// 使用阻塞查询减少轮询
lastIndex := uint64(0)
for {
    pair, newIndex, err := client.WatchKV(key, lastIndex, 30*time.Second)
    if err != nil {
        // 错误重试
        time.Sleep(5 * time.Second)
        continue
    }
    
    if pair != nil {
        // 处理变更
        handleUpdate(pair)
    }
    
    lastIndex = newIndex
}
```

## ❓ 常见问题

### Q: 连接 Consul 失败？

**A:** 检查以下几点：
1. Consul 是否正在运行：`consul members`
2. 地址和端口是否正确（默认 127.0.0.1:8500）
3. 防火墙是否阻止了连接
4. 如果使用 HTTPS，检查证书配置

### Q: 服务注册后找不到？

**A:** 可能的原因：
1. 等待几秒钟让 Consul 同步
2. 检查服务名称是否正确
3. 确认服务健康检查通过
4. 查看 Consul UI 或日志

### Q: TTL 检查一直显示 critical？

**A:** TTL 检查需要定期更新：
```go
// 每 10 秒更新一次（TTL 设置为 30s）
ticker := time.NewTicker(10 * time.Second)
for range ticker.C {
    client.UpdateTTLCheck(checkID, "passing", "OK")
}
```

### Q: 如何实现配置热更新？

**A:** 使用 WatchKV 方法：
```go
go func() {
    lastIndex := uint64(0)
    for {
        pair, newIndex, _ := client.WatchKV("config/key", lastIndex, 30*time.Second)
        if pair != nil {
            // 应用新配置
            applyConfig(pair.Value)
        }
        lastIndex = newIndex
    }
}()
```

### Q: 分布式锁如何避免死锁？

**A:** 
1. 设置合理的会话 TTL
2. 使用 defer 确保锁释放
3. 实现锁超时机制
4. 定期检查会话状态

### Q: 性能优化建议？

**A:**
1. 使用阻塞查询代替频繁轮询
2. 合理设置健康检查间隔
3. 避免过多的 Watch 连接
4. 使用服务标签过滤减少数据传输

## 📝 许可证

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📧 联系方式

如有问题或建议，请通过以下方式联系：
- 提交 GitHub Issue
- 发送邮件至维护者

---

**注意:** 使用前请确保 Consul 服务正在运行且可访问。更多详细信息请参考 [Consul 官方文档](https://www.consul.io/docs)。
