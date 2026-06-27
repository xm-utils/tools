# Nacos 客户端工具库

[![Go Version](https://img.shields.io/badge/go-1.22.10+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

一个基于 [nacos-sdk-go](https://github.com/nacos-group/nacos-sdk-go) 封装的 Go 语言 Nacos 客户端工具库，提供简洁易用的配置管理和服务发现功能。

## 📋 目录

- [项目简介](#项目简介)
- [主要特性](#主要特性)
- [安装](#安装)
- [快速开始](#快速开始)
- [核心功能](#核心功能)
  - [配置管理](#配置管理)
  - [服务发现](#服务发现)
- [配置说明](#配置说明)
- [使用示例](#使用示例)
- [API 参考](#api-参考)
- [测试](#测试)
- [依赖](#依赖)
- [许可证](#许可证)

## 📖 项目简介

本项目是对 Nacos Go SDK 的二次封装，旨在简化 Nacos 在 Go 项目中的使用。提供了配置管理（Config）和服务发现（Naming）两大核心功能的便捷接口，支持配置的增删改查、监听，以及服务的注册、发现、订阅等操作。

## ✨ 主要特性

- ✅ **简洁的 API**：封装复杂的 Nacos SDK，提供简单易用的接口
- ✅ **配置管理**：支持配置的获取、发布、删除、搜索和实时监听
- ✅ **服务发现**：支持服务注册、注销、健康检查、负载均衡
- ✅ **服务订阅**：实时监听服务实例变化
- ✅ **批量操作**：支持批量注册和注销服务实例
- ✅ **自动 IP 获取**：服务注册时自动获取本地 IP
- ✅ **灵活配置**：支持多服务器配置、命名空间、TLS 等高级配置
- ✅ **客户端访问**：提供原生客户端访问接口，支持高级用法

## 🚀 安装

```bash
go get github.com/XingMenTech/utils/nacos
```

### 依赖要求

- Go 1.22.10+
- github.com/nacos-group/nacos-sdk-go/v2 v2.3.5+

## 🎯 快速开始

### 初始化客户端

```go
package main

import (
    "fmt"
    "log"
    "github.com/XingMenTech/utils/nacos"
)

func main() {
    // 创建配置
    config := &nacos.Config{
        Enabled: true,
        ServerConfig: []nacos.ServerConfig{
            {
                IpAddr: "127.0.0.1",
                Port:   8848,
            },
        },
        ClientConfig: nacos.ClientConfig{
            TimeoutMs:   10000,
            NamespaceId: "", // public 命名空间填空字符串
            LogLevel:    "info",
        },
    }

    // 初始化客户端
    if err := nacos.InitClient(config); err != nil {
        log.Fatalf("Failed to init nacos client: %v", err)
    }

    fmt.Println("Nacos client initialized successfully")
}
```

## 🔧 核心功能

### 配置管理

#### 获取配置

```go
data, err := nacos.GetConfig("DEFAULT_GROUP", "app-config.yaml")
if err != nil {
    log.Printf("Failed to get config: %v", err)
    return
}
fmt.Printf("Config content: %s\n", data)
```

#### 发布配置

```go
success, err := nacos.PublishConfig(
    "app-config.yaml",
    "DEFAULT_GROUP",
    "server.port=8080\nserver.name=myapp",
)
if err != nil {
    log.Printf("Failed to publish config: %v", err)
    return
}
if success {
    fmt.Println("Config published successfully")
}
```

#### 删除配置

```go
success, err := nacos.DeleteConfig("app-config.yaml", "DEFAULT_GROUP")
if err != nil {
    log.Printf("Failed to delete config: %v", err)
    return
}
fmt.Printf("Config deleted: %t\n", success)
```

#### 监听配置变化

```go
listener := func(namespace, group, dataId, data string) {
    fmt.Printf("Config changed - Namespace: %s, Group: %s, DataId: %s\n", 
        namespace, group, dataId)
    fmt.Printf("New value: %s\n", data)
}

err := nacos.ListenConfig("DEFAULT_GROUP", "app-config.yaml", listener)
if err != nil {
    log.Printf("Failed to listen config: %v", err)
    return
}

// 保持程序运行以持续监听
select {}
```

#### 取消监听配置

```go
err := nacos.CancelListenConfig("DEFAULT_GROUP", "app-config.yaml")
if err != nil {
    log.Printf("Failed to cancel listen: %v", err)
}
```

#### 搜索配置

```go
configPage, err := nacos.SearchConfig("blur", 1, 10)
if err != nil {
    log.Printf("Failed to search config: %v", err)
    return
}

fmt.Printf("Found %d configs\n", configPage.TotalCount)
for _, item := range configPage.PageItems {
    fmt.Printf("  DataId: %s, Group: %s\n", item.DataId, item.Group)
}
```

### 服务发现

#### 注册服务实例

```go
serviceInfo := nacos.ServiceInfo{
    Name:      "user-service",
    GroupName: "DEFAULT_GROUP",
    Clusters:  "DEFAULT",
    Port:      8080,
    Weight:    1.0,
    Metadata: map[string]string{
        "version": "1.0.0",
        "env":     "production",
    },
    // Host 可选，不填写会自动获取本地 IP
}

if err := nacos.RegisterInstance(serviceInfo); err != nil {
    log.Printf("Failed to register instance: %v", err)
    return
}
fmt.Println("Service registered successfully")
```

#### 注销服务实例

```go
success, err := nacos.DeregisterInstance(
    "user-service",
    "DEFAULT_GROUP",
    "DEFAULT",
)
if err != nil {
    log.Printf("Failed to deregister instance: %v", err)
    return
}
fmt.Printf("Service deregistered: %t\n", success)
```

#### 选择健康实例（负载均衡）

```go
instance, err := nacos.SelectOneHealthyInstance("user-service", "DEFAULT_GROUP")
if err != nil {
    log.Printf("Failed to select healthy instance: %v", err)
    return
}

fmt.Printf("Selected instance: %s:%d\n", instance.Ip, instance.Port)
fmt.Printf("Weight: %f, Metadata: %v\n", instance.Weight, instance.Metadata)
```

#### 查询服务实例列表

```go
// 查询所有健康实例
instances, err := nacos.SelectInstances("user-service", "DEFAULT_GROUP", true)
if err != nil {
    log.Printf("Failed to select instances: %v", err)
    return
}

fmt.Printf("Found %d healthy instances\n", len(instances))
for _, inst := range instances {
    fmt.Printf("  %s:%d (weight: %f)\n", inst.Ip, inst.Port, inst.Weight)
}
```

#### 获取所有服务列表

```go
serviceList, err := nacos.GetAllServices(1, 10, "DEFAULT_GROUP")
if err != nil {
    log.Printf("Failed to get all services: %v", err)
    return
}

fmt.Printf("Total services: %d\n", serviceList.Count)
for _, serviceName := range serviceList.Doms {
    fmt.Printf("  Service: %s\n", serviceName)
}
```

#### 获取服务详细信息

```go
service, err := nacos.GetServiceDetail(
    "user-service",
    "DEFAULT_GROUP",
    []string{"DEFAULT"},
)
if err != nil {
    log.Printf("Failed to get service detail: %v", err)
    return
}

fmt.Printf("Service: %s, Group: %s\n", service.Name, service.GroupName)
fmt.Printf("Instance count: %d\n", len(service.Hosts))
```

#### 订阅服务变化

```go
subscribeCallback := func(services []model.Instance, err error) {
    if err != nil {
        log.Printf("Subscribe callback error: %v", err)
        return
    }
    
    fmt.Printf("Service instances changed, count: %d\n", len(services))
    for _, inst := range services {
        fmt.Printf("  %s:%d (healthy: %t)\n", 
            inst.Ip, inst.Port, inst.Healthy)
    }
}

param := nacos.SubscribeParam{
    ServiceName:       "user-service",
    GroupName:         "DEFAULT_GROUP",
    Clusters:          []string{"DEFAULT"},
    SubscribeCallback: subscribeCallback,
}

if err := nacos.Subscribe(param); err != nil {
    log.Printf("Failed to subscribe: %v", err)
    return
}

// 保持程序运行
select {}
```

#### 取消订阅服务

```go
param := nacos.SubscribeParam{
    ServiceName:       "user-service",
    GroupName:         "DEFAULT_GROUP",
    Clusters:          []string{"DEFAULT"},
    SubscribeCallback: subscribeCallback,
}

if err := nacos.Unsubscribe(param); err != nil {
    log.Printf("Failed to unsubscribe: %v", err)
}
```

#### 批量注册实例

```go
instances := []nacos.ServiceInfo{
    {
        Name:      "order-service",
        GroupName: "DEFAULT_GROUP",
        Clusters:  "DEFAULT",
        Host:      "192.168.1.101",
        Port:      8081,
        Weight:    1.0,
    },
    {
        Name:      "order-service",
        GroupName: "DEFAULT_GROUP",
        Clusters:  "DEFAULT",
        Host:      "192.168.1.102",
        Port:      8082,
        Weight:    2.0,
    },
}

if err := nacos.BatchRegisterInstances(instances); err != nil {
    log.Printf("Failed to batch register: %v", err)
    return
}
fmt.Println("Batch registration successful")
```

#### 批量注销实例

```go
instances := []nacos.ServiceInfo{
    {
        Name:      "order-service",
        GroupName: "DEFAULT_GROUP",
        Clusters:  "DEFAULT",
        Host:      "192.168.1.101",
        Port:      8081,
    },
    {
        Name:      "order-service",
        GroupName: "DEFAULT_GROUP",
        Clusters:  "DEFAULT",
        Host:      "192.168.1.102",
        Port:      8082,
    },
}

if err := nacos.BatchDeregisterInstances(instances); err != nil {
    log.Printf("Failed to batch deregister: %v", err)
    return
}
fmt.Println("Batch deregistration successful")
```

## ⚙️ 配置说明

### Config 结构

```go
type Config struct {
    Enabled      bool           // 是否启用 Nacos
    ServerConfig []ServerConfig // 服务器配置列表
    ClientConfig ClientConfig   // 客户端配置
}
```

### ServerConfig 服务器配置

| 字段 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| Scheme | string | Nacos 服务器协议 | http |
| IpAddr | string | Nacos 服务器地址 | - |
| Port | uint64 | Nacos 服务器端口 | - |
| GrpcPort | uint64 | gRPC 端口 | 服务器端口+1000 |
| ContextPath | string | 上下文路径 | /nacos |

### ClientConfig 客户端配置

| 字段 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| TimeoutMs | uint64 | 请求超时时间（毫秒） | 10000 |
| BeatInterval | int64 | 心跳间隔（毫秒） | 5000 |
| NamespaceId | string | 命名空间 ID | - |
| AppName | string | 应用名称 | - |
| Username | string | Nacos 认证用户名 | - |
| Password | string | Nacos 认证密码 | - |
| CacheDir | string | 持久化缓存目录 | 当前路径 |
| LogDir | string | 日志目录 | 当前路径 |
| LogLevel | string | 日志级别（debug/info/warn/error） | info |
| UpdateThreadNum | int | 更新服务的 goroutine 数量 | 20 |
| NotLoadCacheAtStart | bool | 启动时不加载缓存 | false |
| UpdateCacheWhenEmpty | bool | 空实例时也更新缓存 | false |
| AppendToStdout | bool | 日志追加到标准输出 | false |
| Endpoint | string | 获取服务器地址的端点 | - |

### TLSConfig TLS 配置

```go
type TLSConfig struct {
    Appointed          bool   // 是否指定，如果为 false，将从环境变量获取
    Enable             bool   // 启用 TLS
    TrustAll           bool   // 信任所有服务器
    CaFile             string // CA 证书文件
    CertFile           string // 客户端证书文件
    KeyFile            string // 客户端密钥文件
    ServerNameOverride string // 服务器名称覆盖（仅用于测试）
}
```

## 💡 使用示例

### 完整的微服务示例

```go
package main

import (
    "fmt"
    "log"
    "net/http"
    "github.com/XingMenTech/utils/nacos"
)

func main() {
    // 1. 初始化 Nacos 客户端
    config := &nacos.Config{
        Enabled: true,
        ServerConfig: []nacos.ServerConfig{
            {
                IpAddr: "127.0.0.1",
                Port:   8848,
            },
        },
        ClientConfig: nacos.ClientConfig{
            TimeoutMs:   10000,
            NamespaceId: "",
            LogLevel:    "info",
        },
    }

    if err := nacos.InitClient(config); err != nil {
        log.Fatalf("Failed to init nacos client: %v", err)
    }

    // 2. 从 Nacos 获取配置
    appConfig, err := nacos.GetConfig("DEFAULT_GROUP", "app-config.yaml")
    if err != nil {
        log.Printf("Warning: Failed to get config: %v", err)
    } else {
        fmt.Printf("Loaded config: %s\n", appConfig)
    }

    // 3. 监听配置变化
    nacos.ListenConfig("DEFAULT_GROUP", "app-config.yaml", 
        func(namespace, group, dataId, data string) {
            fmt.Printf("Config updated: %s\n", data)
        })

    // 4. 注册服务
    serviceInfo := nacos.ServiceInfo{
        Name:      "my-service",
        GroupName: "DEFAULT_GROUP",
        Clusters:  "DEFAULT",
        Port:      8080,
        Weight:    1.0,
        Metadata: map[string]string{
            "version": "1.0.0",
        },
    }

    if err := nacos.RegisterInstance(serviceInfo); err != nil {
        log.Fatalf("Failed to register service: %v", err)
    }
    defer func() {
        // 5. 服务关闭时注销
        nacos.DeregisterInstance("my-service", "DEFAULT_GROUP", "DEFAULT")
    }()

    // 6. 启动 HTTP 服务
    http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("OK"))
    })

    fmt.Println("Server starting on :8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

### 服务调用方示例

```go
package main

import (
    "fmt"
    "log"
    "github.com/XingMenTech/utils/nacos"
)

func callUserService() {
    // 选择一个健康的服务实例
    instance, err := nacos.SelectOneHealthyInstance("user-service", "DEFAULT_GROUP")
    if err != nil {
        log.Printf("Failed to select instance: %v", err)
        return
    }

    // 使用实例信息发起调用
    url := fmt.Sprintf("http://%s:%d/api/users", instance.Ip, instance.Port)
    fmt.Printf("Calling service at: %s\n", url)
    
    // TODO: 发起 HTTP 请求
}

func main() {
    // 初始化客户端
    config := &nacos.Config{
        Enabled: true,
        ServerConfig: []nacos.ServerConfig{
            {IpAddr: "127.0.0.1", Port: 8848},
        },
        ClientConfig: nacos.ClientConfig{
            TimeoutMs:   10000,
            NamespaceId: "",
        },
    }

    if err := nacos.InitClient(config); err != nil {
        log.Fatalf("Failed to init client: %v", err)
    }

    // 订阅服务变化
    param := nacos.SubscribeParam{
        ServiceName: "user-service",
        GroupName:   "DEFAULT_GROUP",
        Clusters:    []string{"DEFAULT"},
        SubscribeCallback: func(services []model.Instance, err error) {
            if err != nil {
                log.Printf("Subscribe error: %v", err)
                return
            }
            fmt.Printf("Available instances: %d\n", len(services))
        },
    }

    if err := nacos.Subscribe(param); err != nil {
        log.Printf("Failed to subscribe: %v", err)
    }

    // 定期调用服务
    callUserService()
}
```

## 📚 API 参考

### 初始化函数

| 函数 | 说明 |
|------|------|
| `InitClient(config *Config) error` | 初始化 Nacos 客户端 |

### 配置管理 API

| 函数 | 说明 |
|------|------|
| `GetConfig(group, dataId string) (string, error)` | 获取配置 |
| `PublishConfig(dataId, group, content string) (bool, error)` | 发布配置 |
| `DeleteConfig(dataId, group string) (bool, error)` | 删除配置 |
| `SearchConfig(search string, pageNo, pageSize uint32) (*model.ConfigPage, error)` | 搜索配置 |
| `ListenConfig(group, dataId string, listener func(...)) error` | 监听配置变化 |
| `CancelListenConfig(group, dataId string) error` | 取消监听配置 |

### 服务发现 API

| 函数 | 说明 |
|------|------|
| `RegisterInstance(info ServiceInfo) error` | 注册服务实例 |
| `DeregisterInstance(name, groupName, clusters string) (bool, error)` | 注销服务实例 |
| `SelectOneHealthyInstance(serviceName, groupName string) (*model.Instance, error)` | 选择一个健康实例 |
| `SelectInstances(serviceName, groupName string, healthy bool) ([]model.Instance, error)` | 查询服务实例列表 |
| `GetAllServices(pageNo, pageSize uint32, groupName string) (*model.ServiceList, error)` | 获取所有服务列表 |
| `GetServiceDetail(serviceName, groupName string, clusters []string) (*model.Service, error)` | 获取服务详细信息 |
| `Subscribe(param SubscribeParam) error` | 订阅服务变化 |
| `Unsubscribe(param SubscribeParam) error` | 取消订阅服务 |
| `BatchRegisterInstances(instances []ServiceInfo) error` | 批量注册实例 |
| `BatchDeregisterInstances(instances []ServiceInfo) error` | 批量注销实例 |

### 辅助函数

| 函数 | 说明 |
|------|------|
| `GetLocalIP() (string, error)` | 获取本地 IP 地址 |
| `GetNamingClient() naming_client.INamingClient` | 获取原生命名客户端 |
| `GetConfigClient() config_client.IConfigClient` | 获取原生配置客户端 |

### 数据结构

#### ServiceInfo

```go
type ServiceInfo struct {
    Name      string            // 服务名
    GroupName string            // 分组名
    Clusters  string            // 集群名
    Host      string            // 主机地址（可选）
    Port      uint64            // 端口
    Weight    float64           // 权重
    Metadata  map[string]string // 元数据
}
```

#### SubscribeParam

```go
type SubscribeParam struct {
    ServiceName       string                                     // 服务名
    GroupName         string                                     // 分组名
    Clusters          []string                                   // 集群列表
    SubscribeCallback func(services []model.Instance, err error) // 回调函数
}
```

## 🧪 测试

运行测试：

```bash
go test -v
```

运行基准测试：

```bash
go test -bench=.
```

运行特定测试：

```bash
go test -v -run TestRegisterInstance
```

## 📦 依赖

```go
require github.com/nacos-group/nacos-sdk-go/v2 v2.3.5
```

## 📄 许可证

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📧 联系方式

如有问题或建议，请通过以下方式联系：

- 提交 Issue
- 发送邮件

---

**注意**：使用前请确保已部署并运行 Nacos 服务器。下载地址：[Nacos Releases](https://github.com/alibaba/nacos/releases)
