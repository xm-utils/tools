# Nacos 客户端工具库

基于 [nacos-sdk-go/v2](https://github.com/nacos-group/nacos-sdk-go) 封装的 Nacos 客户端工具库，采用闭包模式设计，将配置管理和服务发现拆分为独立的客户端实例。

## 目录

- [特性](#特性)
- [安装](#安装)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
  - [基础配置](#基础配置)
  - [服务器配置](#服务器配置)
  - [客户端配置](#客户端配置)
  - [TLS 配置](#tls-配置)
- [配置客户端 ConfigClient](#配置客户端-configclient)
  - [创建客户端](#创建配置客户端)
  - [获取配置](#获取配置)
  - [发布配置](#发布配置)
  - [删除配置](#删除配置)
  - [搜索配置](#搜索配置)
  - [监听配置变化](#监听配置变化)
  - [取消监听](#取消监听)
- [服务发现客户端 NamingClient](#服务发现客户端-namingclient)
  - [创建客户端](#创建服务发现客户端)
  - [注册服务实例](#注册服务实例)
  - [注销服务实例](#注销服务实例)
  - [选择健康实例（负载均衡）](#选择健康实例负载均衡)
  - [查询实例列表](#查询实例列表)
  - [获取服务详情](#获取服务详情)
  - [获取所有服务](#获取所有服务)
  - [订阅服务变化](#订阅服务变化)
  - [取消订阅](#取消订阅)
  - [批量注册/注销](#批量注册注销)
- [gRPC 服务发现](#grpc-服务发现)
- [高级用法](#高级用法)
  - [多集群支持](#多集群支持)
  - [获取底层客户端](#获取底层客户端)
- [最佳实践](#最佳实践)
- [API 参考](#api-参考)
- [文件结构](#文件结构)

## 特性

- ✅ **闭包模式**：ConfigClient 和 NamingClient 独立封装，互不依赖
- ✅ **多实例支持**：可同时创建多个客户端连接不同的 Nacos 集群
- ✅ **配置管理**：获取/发布/删除/搜索配置，支持配置变更监听
- ✅ **服务发现**：注册/注销实例，健康实例选择，负载均衡
- ✅ **服务订阅**：实时订阅服务实例变化，自动感知上下线
- ✅ **gRPC Resolver**：内置 gRPC 服务发现解析器，自动负载均衡
- ✅ **TLS 安全通信**：支持 mTLS 双向认证
- ✅ **多命名空间**：支持 Namespace 隔离，区分开发/测试/生产环境

## 安装

```bash
go get github.com/xm-utils/tools/nacos
```

**依赖要求**：
- Go 1.24.2+
- nacos-sdk-go/v2 v2.3.5

## 快速开始

```go
package main

import (
    "fmt"
    "github.com/xm-utils/tools/nacos"
)

func main() {
    config := &nacos.Config{
        Enabled: true,
        ServerConfig: []nacos.ServerConfig{
            {IpAddr: "127.0.0.1", Port: 8848},
        },
        ClientConfig: nacos.ClientConfig{
            GroupName:   "DEFAULT_GROUP",
            ClusterName: "DEFAULT",
            NamespaceId: "",
            TimeoutMs:   10000,
        },
    }

    // 使用配置客户端
    configClient, err := nacos.NewConfigClient(config)
    if err != nil {
        panic(err)
    }
    content, _ := configClient.GetConfig("app.yaml")
    fmt.Println(content)

    // 使用服务发现客户端
    namingClient, err := nacos.NewNamingClient(config)
    if err != nil {
        panic(err)
    }
    namingClient.RegisterInstance(nacos.ServiceInstanceInfo{
        Name: "user-service",
        Port: 8080,
    })
}
```

## 配置说明

### 基础配置

```go
config := &nacos.Config{
    Enabled: true,                    // 是否启用 Nacos
    ServerConfig: []nacos.ServerConfig{...}, // 服务器配置
    ClientConfig: nacos.ClientConfig{...},   // 客户端配置
}
```

### 服务器配置

```go
nacos.ServerConfig{
    Scheme:      "http",       // 协议，默认 http（2.0+ 非必需）
    IpAddr:      "127.0.0.1", // 服务器地址
    Port:        8848,         // 服务器端口
    GrpcPort:    9848,         // gRPC 端口，默认 port+1000（非必需）
    ContextPath: "/nacos",     // 上下文路径（2.0+ 非必需）
}
```

**多服务器高可用配置**：

```go
ServerConfig: []nacos.ServerConfig{
    {IpAddr: "192.168.1.100", Port: 8848},
    {IpAddr: "192.168.1.101", Port: 8848},
    {IpAddr: "192.168.1.102", Port: 8848},
}
```

### 客户端配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `GroupName` | string | `DEFAULT_GROUP` | 组名称 |
| `ClusterName` | string | `DEFAULT` | 集群名称 |
| `NamespaceId` | string | `""` (public) | 命名空间 ID |
| `TimeoutMs` | uint64 | 10000 | 请求超时时间（毫秒） |
| `BeatInterval` | int64 | 5000 | 心跳间隔（毫秒） |
| `Username` | string | `""` | 认证用户名 |
| `Password` | string | `""` | 认证密码 |
| `CacheDir` | string | 当前目录 | 本地缓存目录 |
| `LogDir` | string | 当前目录 | 日志目录 |
| `LogLevel` | string | `info` | 日志级别（debug/info/warn/error） |
| `DisableUseSnapShot` | bool | false | 获取失败时是否使用本地缓存 |
| `NotLoadCacheAtStart` | bool | false | 启动时是否加载缓存 |
| `UpdateCacheWhenEmpty` | bool | false | 空实例时是否更新缓存 |
| `UpdateThreadNum` | int | 20 | 更新 goroutine 数量 |
| `AppendToStdout` | bool | false | 日志是否输出到标准输出 |
| `AsyncUpdateService` | bool | false | 是否异步更新服务 |
| `AppName` | string | `""` | 应用名称 |
| `AppKey` | string | `""` | 客户端身份信息 |
| `Endpoint` | string | `""` | 地址服务器端点 |
| `AppConnLabels` | map[string]string | nil | 应用连接标签 |

### TLS 配置

```go
ClientConfig: nacos.ClientConfig{
    TLSCfg: nacos.TLSConfig{
        Appointed:          false,  // false: 从环境变量获取
        Enable:             true,   // 启用 TLS
        TrustAll:           false,  // 是否信任所有服务器
        CaFile:             "/etc/ssl/certs/ca.crt",
        CertFile:           "/etc/ssl/certs/client.crt",
        KeyFile:            "/etc/ssl/certs/client.key",
        ServerNameOverride: "",     // 服务器名称覆盖（仅测试用）
    },
}
```

---

## 配置客户端 ConfigClient

### 创建配置客户端

```go
configClient, err := nacos.NewConfigClient(config)
if err != nil {
    log.Fatalf("创建配置客户端失败: %v", err)
}
```

### 获取配置

```go
content, err := configClient.GetConfig("app.yaml")
if err != nil {
    log.Printf("获取配置失败: %v", err)
    return
}
fmt.Println(content)
```

### 发布配置

```go
success, err := configClient.PublishConfig("app.yaml", `
server:
  port: 8080
  host: localhost
`)
if err != nil {
    log.Printf("发布配置失败: %v", err)
    return
}
fmt.Printf("发布成功: %v\n", success)
```

### 删除配置

```go
success, err := configClient.DeleteConfig("app.yaml")
if err != nil {
    log.Printf("删除配置失败: %v", err)
}
```

### 搜索配置

```go
configPage, err := configClient.SearchConfig("app", 1, 10)
if err != nil {
    log.Printf("搜索配置失败: %v", err)
    return
}
fmt.Printf("找到 %d 个配置\n", configPage.TotalCount)
for _, item := range configPage.PageItems {
    fmt.Printf("  - DataId: %s, Group: %s\n", item.DataId, item.Group)
}
```

### 监听配置变化

```go
err := configClient.ListenConfig("app.yaml", func(namespace, group, dataId, data string) {
    fmt.Printf("配置发生变化:\n")
    fmt.Printf("  namespace: %s\n", namespace)
    fmt.Printf("  group: %s\n", group)
    fmt.Printf("  dataId: %s\n", dataId)
    fmt.Printf("  新内容:\n%s\n", data)
})
if err != nil {
    log.Printf("监听配置失败: %v", err)
}
```

### 取消监听

```go
err := configClient.CancelListenConfig("app.yaml")
if err != nil {
    log.Printf("取消监听失败: %v", err)
}
```

---

## 服务发现客户端 NamingClient

### 创建服务发现客户端

```go
namingClient, err := nacos.NewNamingClient(config)
if err != nil {
    log.Fatalf("创建服务发现客户端失败: %v", err)
}
```

### 注册服务实例

```go
err := namingClient.RegisterInstance(nacos.ServiceInstanceInfo{
    Name:   "order-service",
    Host:   "192.168.1.100", // 留空自动获取本地 IP
    Port:   8080,
    Weight: 1.0,
    Metadata: map[string]string{
        "version": "v1.0.0",
        "env":     "prod",
    },
})
if err != nil {
    log.Printf("注册失败: %v", err)
}
```

> **说明**：`Host` 留空时会自动获取本机非回环地址的 IPv4 地址。注册的实例为临时实例（Ephemeral），服务断开后 Nacos 会自动注销。

### 注销服务实例

```go
success, err := namingClient.DeregisterInstance("order-service")
if err != nil {
    log.Printf("注销失败: %v", err)
}
```

### 选择健康实例（负载均衡）

```go
instance, err := namingClient.SelectOneHealthyInstance("order-service")
if err != nil {
    log.Printf("选择实例失败: %v", err)
    return
}
fmt.Printf("选中实例: %s:%d (权重: %.2f)\n",
    instance.Ip, instance.Port, instance.Weight)
```

> Nacos SDK 内置基于权重的负载均衡算法，优先返回权重较高的健康实例。

### 查询实例列表

```go
// 查询所有健康实例
healthyInstances, err := namingClient.SelectInstances("order-service", true)

// 查询所有实例（包含不健康的）
allInstances, err := namingClient.SelectInstances("order-service", false)
```

### 获取服务详情

```go
service, err := namingClient.GetServiceDetail("order-service", []string{"DEFAULT"})
if err != nil {
    log.Printf("获取服务详情失败: %v", err)
}
fmt.Printf("服务名: %s\n", service.Name)
```

### 获取所有服务

```go
serviceList, err := namingClient.GetAllServices(1, 10) // pageNo, pageSize
if err != nil {
    log.Printf("获取服务列表失败: %v", err)
    return
}
fmt.Printf("服务总数: %d\n", serviceList.Count)
for _, name := range serviceList.Doms {
    fmt.Printf("  - %s\n", name)
}
```

### 订阅服务变化

```go
err := namingClient.Subscribe(nacos.SubscribeParam{
    ServiceName: "order-service",
    SubscribeCallback: func(services []model.Instance, err error) {
        if err != nil {
            log.Printf("订阅回调错误: %v", err)
            return
        }
        fmt.Printf("实例变化，当前数量: %d\n", len(services))
        for _, svc := range services {
            fmt.Printf("  - %s:%d healthy=%v\n",
                svc.Ip, svc.Port, svc.Healthy)
        }
    },
})
```

### 取消订阅

```go
err := namingClient.Unsubscribe(nacos.SubscribeParam{
    ServiceName:       "order-service",
    SubscribeCallback: callback, // 需要传入同一个回调函数
})
```

### 批量注册/注销

```go
// 批量注册
instances := []nacos.ServiceInstanceInfo{
    {Name: "order-service", Host: "192.168.1.100", Port: 8080, Weight: 1.0},
    {Name: "order-service", Host: "192.168.1.101", Port: 8080, Weight: 1.0},
    {Name: "order-service", Host: "192.168.1.102", Port: 8080, Weight: 0.5},
}
err := namingClient.BatchRegisterInstances(instances)

// 批量注销
err := namingClient.BatchDeregisterInstances(instances)
```

---

## gRPC 服务发现

内置 gRPC Resolver，基于 Nacos 实现服务自动发现和负载均衡：

```go
import (
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "github.com/xm-utils/tools/nacos"
)

// 创建 ResolverBuilder
builder := nacos.NewResolverBuilder(config)

// 注册到 gRPC resolver
resolver.Register(builder)

// 使用 nacos:// 协议连接服务
// 格式: nacos://服务名?group=组名&cluster=集群名
conn, err := grpc.Dial(
    "nacos:///order-service",
    grpc.WithTransportCredentials(insecure.NewCredentials()),
    grpc.WithResolvers(builder),
)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

// 调用 gRPC 服务...
```

**URL 参数说明**：

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `group` | 服务分组 | `DEFAULT_GROUP` |
| `cluster` | 集群名称 | `DEFAULT` |

**Resolver 工作流程**：

1. `Build` 阶段：解析服务名和查询参数，立即解析一次实例列表
2. `ResolveNow`：主动查询 Nacos 获取健康实例，更新 gRPC 连接状态
3. `startWatch`：启动后台 goroutine，通过 `Subscribe` 实时监听实例变化
4. 实例变化时自动更新 gRPC 连接的目标地址列表

---

## 高级用法

### 多集群支持

可同时创建多个客户端实例，连接不同的 Nacos 集群或命名空间：

```go
// 生产集群
prodConfig := &nacos.Config{
    Enabled: true,
    ServerConfig: []nacos.ServerConfig{
        {IpAddr: "nacos-prod.example.com", Port: 8848},
    },
    ClientConfig: nacos.ClientConfig{
        NamespaceId: "prod-namespace-id",
        GroupName:   "PROD_GROUP",
        Username:    "admin",
        Password:    "prod-password",
    },
}

// 测试集群
testConfig := &nacos.Config{
    Enabled: true,
    ServerConfig: []nacos.ServerConfig{
        {IpAddr: "127.0.0.1", Port: 8848},
    },
    ClientConfig: nacos.ClientConfig{
        NamespaceId: "",
        GroupName:   "TEST_GROUP",
    },
}

prodConfigClient, _ := nacos.NewConfigClient(prodConfig)
testConfigClient, _ := nacos.NewConfigClient(testConfig)

// 分别从不同集群读取配置
prodContent, _ := prodConfigClient.GetConfig("app.yaml")
testContent, _ := testConfigClient.GetConfig("app.yaml")
```

### 获取底层客户端

当需要使用 Nacos SDK 原生功能时，可通过 `GetClient()` 获取底层客户端：

```go
// 获取配置客户端底层实例
rawConfigClient := configClient.GetClient() // config_client.IConfigClient

// 获取命名客户端底层实例
rawNamingClient := namingClient.GetClient() // naming_client.INamingClient

// 使用原生 SDK 方法...
```

---

## 最佳实践

### 1. 客户端复用

建议每个 Nacos 集群只创建一个 ConfigClient 和一个 NamingClient，在应用全局复用：

```go
var (
    configClient *nacos.ConfigClientWrapper
    namingClient *nacos.NamingClientWrapper
)

func InitNacos(config *nacos.Config) error {
    var err error
    configClient, err = nacos.NewConfigClient(config)
    if err != nil {
        return err
    }
    namingClient, err = nacos.NewNamingClient(config)
    if err != nil {
        return err
    }
    return nil
}
```

### 2. 优雅关闭

在服务关闭时注销服务实例，避免流量继续路由到已关闭的节点：

```go
func GracefulShutdown(namingClient *nacos.NamingClientWrapper, serviceName string) {
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

    <-sigChan
    log.Println("正在关闭服务...")

    _, err := namingClient.DeregisterInstance(serviceName)
    if err != nil {
        log.Printf("注销服务失败: %v", err)
    } else {
        log.Println("服务已注销")
    }
}
```

### 3. 配置变更热更新

监听配置变化后，结合本地缓存实现热更新：

```go
type HotConfig struct {
    mu      sync.RWMutex
    cache   map[string]string
    client  *nacos.ConfigClientWrapper
}

func (h *HotConfig) Watch(dataId string) error {
    // 初始加载
    content, err := h.client.GetConfig(dataId)
    if err != nil {
        return err
    }
    h.mu.Lock()
    h.cache[dataId] = content
    h.mu.Unlock()

    // 监听变化
    return h.client.ListenConfig(dataId, func(namespace, group, dId, data string) {
        h.mu.Lock()
        h.cache[dId] = data
        h.mu.Unlock()
        log.Printf("配置已热更新: %s", dId)
    })
}

func (h *HotConfig) Get(dataId string) string {
    h.mu.RLock()
    defer h.mu.RUnlock()
    return h.cache[dataId]
}
```

### 4. 错误处理

始终检查并妥善处理错误：

```go
content, err := configClient.GetConfig("app.yaml")
if err != nil {
    log.Printf("获取配置失败，使用默认配置: %v", err)
    content = defaultConfig
}
```

### 5. 命名空间隔离

通过 NamespaceId 区分不同环境：

```go
// 开发环境
devConfig := &nacos.Config{
    ClientConfig: nacos.ClientConfig{
        NamespaceId: "",              // public 命名空间
        GroupName:   "DEV_GROUP",
    },
}

// 生产环境
prodConfig := &nacos.Config{
    ClientConfig: nacos.ClientConfig{
        NamespaceId: "your-prod-ns-id", // 生产命名空间
        GroupName:   "PROD_GROUP",
        Username:    "admin",
        Password:    "secure-password",
    },
}
```

---

## API 参考

### ConfigClientWrapper

| 方法 | 参数 | 返回值 | 说明 |
|------|------|--------|------|
| `GetConfig` | `dataId string` | `(string, error)` | 获取配置内容 |
| `PublishConfig` | `dataId, content string` | `(bool, error)` | 发布配置 |
| `DeleteConfig` | `dataId string` | `(bool, error)` | 删除配置 |
| `SearchConfig` | `search string, pageNo, pageSize uint32` | `(*model.ConfigPage, error)` | 搜索配置 |
| `ListenConfig` | `dataId string, listener func(...)` | `error` | 监听配置变化 |
| `CancelListenConfig` | `dataId string` | `error` | 取消监听 |
| `GetClient` | - | `config_client.IConfigClient` | 获取底层客户端 |
| `GetGroupName` | - | `string` | 获取组名 |

### NamingClientWrapper

| 方法 | 参数 | 返回值 | 说明 |
|------|------|--------|------|
| `RegisterInstance` | `info ServiceInstanceInfo` | `error` | 注册服务实例 |
| `DeregisterInstance` | `name string` | `(bool, error)` | 注销服务实例 |
| `SelectOneHealthyInstance` | `serviceName string` | `(*model.Instance, error)` | 选择健康实例 |
| `SelectInstances` | `serviceName string, healthy bool` | `([]model.Instance, error)` | 查询实例列表 |
| `GetAllServices` | `pageNo, pageSize uint32` | `(*model.ServiceList, error)` | 获取所有服务 |
| `GetServiceDetail` | `serviceName string, clusters []string` | `(*model.Service, error)` | 获取服务详情 |
| `Subscribe` | `param SubscribeParam` | `error` | 订阅服务变化 |
| `Unsubscribe` | `param SubscribeParam` | `error` | 取消订阅 |
| `BatchRegisterInstances` | `instances []ServiceInstanceInfo` | `error` | 批量注册 |
| `BatchDeregisterInstances` | `instances []ServiceInstanceInfo` | `error` | 批量注销 |
| `GetClient` | - | `naming_client.INamingClient` | 获取底层客户端 |
| `GetGroupName` | - | `string` | 获取组名 |
| `GetClusterName` | - | `string` | 获取集群名称 |

### ServiceInstanceInfo

| 字段 | 类型 | 说明 |
|------|------|------|
| `Name` | `string` | 服务名称 |
| `Host` | `string` | IP 地址（留空自动获取本地 IP） |
| `Port` | `uint64` | 端口号 |
| `Weight` | `float64` | 权重（0~100），用于负载均衡 |
| `Metadata` | `map[string]string` | 元数据键值对 |

---

## 文件结构

```
nacos/
├── config.go              # 配置结构定义（Config/ServerConfig/ClientConfig/TLSConfig）
├── config_client.go       # 配置客户端（ConfigClientWrapper 闭包实现）
├── naming_client.go       # 服务发现客户端（NamingClientWrapper 闭包实现）
├── grpc_resolver.go       # gRPC 服务发现解析器（NacosScheme Resolver）
├── config.example.yaml    # 配置文件示例
├── example_new_client.go  # 使用示例代码
├── new_client_test.go     # 单元测试
├── go.mod                 # 模块定义
└── go.sum                 # 依赖校验
```
