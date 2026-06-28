# Nacos 客户端工具库

[![Go Version](https://img.shields.io/badge/go-1.24.2+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Nacos SDK](https://img.shields.io/badge/nacos--sdk--go-v2.3.5-blue.svg)](https://github.com/nacos-group/nacos-sdk-go)

一个基于 [nacos-sdk-go v2](https://github.com/nacos-group/nacos-sdk-go) 封装的高性能 Go 语言 Nacos 客户端工具库，提供简洁易用的配置管理和服务发现功能，支持微服务架构中的服务注册与发现、配置动态刷新等核心场景。

## 📋 目录

- [项目简介](#项目简介)
- [主要特性](#主要特性)
- [安装](#安装)
- [快速开始](#快速开始)
  - [基础初始化](#基础初始化)
  - [YAML 配置加载](#yaml-配置加载)
- [核心功能](#核心功能)
  - [配置管理](#配置管理)
  - [服务发现](#服务发现)
  - [服务订阅](#服务订阅)
  - [批量操作](#批量操作)
- [高级功能](#高级功能)
  - [获取原生客户端](#获取原生客户端)
  - [自动 IP 获取](#自动-ip-获取)
- [配置说明](#配置说明)
  - [Config 配置结构](#config-配置结构)
  - [ServerConfig 服务器配置](#serverconfig-服务器配置)
  - [ClientConfig 客户端配置](#clientconfig-客户端配置)
  - [TLSConfig TLS 配置](#tlsconfig-tls-配置)
- [使用示例](#使用示例)
  - [微服务端（服务提供者）](#微服务端服务提供者)
  - [调用端（服务消费者）](#调用端服务消费者)
  - [配置监听示例](#配置监听示例)
  - [多环境配置](#多环境配置)
- [API 参考](#api-参考)
  - [初始化函数](#初始化函数)
  - [配置管理 API](#配置管理-api)
  - [服务发现 API](#服务发现-api)
  - [辅助函数](#辅助函数)
  - [数据结构](#数据结构)
- [测试](#测试)
- [最佳实践](#最佳实践)
  - [生产环境建议](#生产环境建议)
  - [服务注册规范](#服务注册规范)
  - [配置管理规范](#配置管理规范)
- [常见问题](#常见问题)
- [依赖](#依赖)
- [许可证](#许可证)

## 📖 项目简介

本项目是对 Nacos Go SDK v2 的二次封装，旨在简化 Nacos 在 Go 微服务项目中的使用。提供了配置管理（Config）和服务发现（Naming）两大核心功能的便捷接口：

- **配置管理**：支持配置的增删改查、实时监听、搜索等功能
- **服务发现**：支持服务注册、注销、健康检查、负载均衡、服务订阅等操作
- **简化设计**：通过全局 GroupName 和 ClusterName 配置，减少重复参数传递

## ✨ 主要特性

- ✅ **简洁的 API**：封装复杂的 Nacos SDK，提供简单易用的接口
- ✅ **配置管理**：支持配置的获取、发布、删除、搜索和实时监听
- ✅ **服务发现**：支持服务注册、注销、健康检查、负载均衡
- ✅ **服务订阅**：实时监听服务实例变化，实现动态服务发现
- ✅ **批量操作**：支持批量注册和注销服务实例
- ✅ **自动 IP 获取**：服务注册时自动获取本地 IP，简化配置
- ✅ **灵活配置**：支持多服务器配置、命名空间、TLS 等高级配置
- ✅ **客户端访问**：提供原生客户端访问接口，支持高级用法
- ✅ **默认值优化**：GroupName 默认为 `DEFAULT_GROUP`，减少配置项
- ✅ **错误处理**：统一的错误包装和清晰的错误信息

## 🚀 安装

```bash
go get github.com/xm-utils/tools/nacos
```

### 依赖要求

- Go 1.24.2+
- github.com/nacos-group/nacos-sdk-go/v2 v2.3.5+

### 前置条件

使用前请确保已部署并运行 Nacos 服务器。下载地址：[Nacos Releases](https://github.com/alibaba/nacos/releases)

## 🎯 快速开始

### 基础初始化

```go
package main

import (
    "fmt"
    "log"
    "github.com/xm-utils/tools/nacos"
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

### YAML 配置加载

1. 创建配置文件 `nacos.yaml`：

```yaml
enabled: true

serverConfig:
  - scheme: "http"
    ipAddr: "127.0.0.1"
    port: 8848
    grpcPort: 9848
    contextPath: "/nacos"

clientConfig:
  timeoutMs: 10000
  beatInterval: 5000
  namespaceId: ""
  groupName: "DEFAULT_GROUP"
  clusterName: "DEFAULT"
  appName: "my-application"
  cacheDir: "/tmp/nacos/cache"
  logDir: "/tmp/nacos/log"
  logLevel: "info"
```

2. 在代码中加载配置（需配合配置加载库如 viper）：

```go
package main

import (
    "log"
    "github.com/spf13/viper"
    "github.com/xm-utils/tools/nacos"
)

func main() {
    // 使用 viper 加载 YAML 配置
    viper.SetConfigFile("nacos.yaml")
    if err := viper.ReadInConfig(); err != nil {
        log.Fatalf("Failed to read config: %v", err)
    }

    // 解析为 nacos.Config 结构
    var config nacos.Config
    if err := viper.Unmarshal(&config); err != nil {
        log.Fatalf("Failed to unmarshal config: %v", err)
    }

    // 初始化客户端
    if err := nacos.InitClient(&config); err != nil {
        log.Fatalf("Failed to init nacos client: %v", err)
    }

    log.Println("Nacos client initialized successfully")
}
```

## 🔧 核心功能

### 配置管理

#### 获取配置

```go
// 注意：GetConfig 只需要 dataId，groupName 从配置中自动获取
data, err := nacos.GetConfig("app-config.yaml")
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
success, err := nacos.DeleteConfig("app-config.yaml")
if err != nil {
    log.Printf("Failed to delete config: %v", err)
    return
}
fmt.Printf("Config deleted: %t\n", success)
```

#### 监听配置变化

```go
// 定义监听器回调函数
listener := func(namespace, group, dataId, data string) {
    fmt.Printf("Config changed:\n")
    fmt.Printf("  Namespace: %s\n", namespace)
    fmt.Printf("  Group: %s\n", group)
    fmt.Printf("  DataId: %s\n", dataId)
    fmt.Printf("  New value: %s\n", data)
    
    // 在这里处理配置变更逻辑，如重新加载应用配置
}

// 开始监听配置
err := nacos.ListenConfig("app-config.yaml", listener)
if err != nil {
    log.Printf("Failed to listen config: %v", err)
    return
}

// 保持程序运行以持续监听
select {}
```

#### 取消监听配置

```go
err := nacos.CancelListenConfig("app-config.yaml")
if err != nil {
    log.Printf("Failed to cancel listen: %v", err)
}
```

#### 搜索配置

```go
// 搜索配置，支持模糊查询
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
    Name:     "user-service",
    Port:     8080,
    Weight:   1.0,
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

#### 带自定义 IP 的服务注册

```go
serviceInfo := nacos.ServiceInfo{
    Name:     "order-service",
    Host:     "192.168.1.100", // 指定服务 IP
    Port:     8081,
    Weight:   1.0,
    Metadata: map[string]string{
        "version": "1.0.0",
        "region":  "cn-north-1",
    },
}

if err := nacos.RegisterInstance(serviceInfo); err != nil {
    log.Printf("Failed to register instance: %v", err)
    return
}
```

#### 注销服务实例

```go
success, err := nacos.DeregisterInstance("user-service")
if err != nil {
    log.Printf("Failed to deregister instance: %v", err)
    return
}
fmt.Printf("Service deregistered: %t\n", success)
```

#### 选择健康实例（负载均衡）

```go
// 自动选择一个健康的实例（基于权重和健康状态）
instance, err := nacos.SelectOneHealthyInstance("user-service")
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
instances, err := nacos.SelectInstances("user-service", true)
if err != nil {
    log.Printf("Failed to select instances: %v", err)
    return
}

fmt.Printf("Found %d healthy instances\n", len(instances))
for _, inst := range instances {
    fmt.Printf("  %s:%d (weight: %f, healthy: %t)\n", 
        inst.Ip, inst.Port, inst.Weight, inst.Healthy)
}

// 查询所有实例（包括不健康的）
allInstances, err := nacos.SelectInstances("user-service", false)
if err != nil {
    log.Printf("Failed to select all instances: %v", err)
    return
}
fmt.Printf("Total instances: %d\n", len(allInstances))
```

#### 获取所有服务列表

```go
serviceList, err := nacos.GetAllServices(1, 10)
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
    []string{"DEFAULT"}, // 集群列表
)
if err != nil {
    log.Printf("Failed to get service detail: %v", err)
    return
}

fmt.Printf("Service: %s, Group: %s\n", service.Name, service.GroupName)
fmt.Printf("Instance count: %d\n", len(service.Hosts))
for _, host := range service.Hosts {
    fmt.Printf("  Instance: %s:%d\n", host.Ip, host.Port)
}
```

### 服务订阅

#### 订阅服务变化

```go
// 定义订阅回调函数
subscribeCallback := func(services []model.Instance, err error) {
    if err != nil {
        log.Printf("Subscribe callback error: %v", err)
        return
    }
    
    fmt.Printf("Service instances changed, count: %d\n", len(services))
    for _, inst := range services {
        fmt.Printf("  %s:%d (healthy: %t, weight: %f)\n", 
            inst.Ip, inst.Port, inst.Healthy, inst.Weight)
    }
}

// 创建订阅参数
param := nacos.SubscribeParam{
    ServiceName:       "user-service",
    SubscribeCallback: subscribeCallback,
}

// 开始订阅
if err := nacos.Subscribe(param); err != nil {
    log.Printf("Failed to subscribe: %v", err)
    return
}

// 保持程序运行以接收订阅通知
select {}
```

#### 取消订阅服务

```go
param := nacos.SubscribeParam{
    ServiceName:       "user-service",
    SubscribeCallback: subscribeCallback,
}

if err := nacos.Unsubscribe(param); err != nil {
    log.Printf("Failed to unsubscribe: %v", err)
}
```

### 批量操作

#### 批量注册实例

```go
instances := []nacos.ServiceInfo{
    {
        Name:     "order-service",
        Host:     "192.168.1.101",
        Port:     8081,
        Weight:   1.0,
        Metadata: map[string]string{"version": "1.0.0"},
    },
    {
        Name:     "order-service",
        Host:     "192.168.1.102",
        Port:     8082,
        Weight:   2.0,
        Metadata: map[string]string{"version": "1.0.0"},
    },
    {
        Name:     "order-service",
        Host:     "192.168.1.103",
        Port:     8083,
        Weight:   1.5,
        Metadata: map[string]string{"version": "1.0.0"},
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
        Name: "order-service",
        Host: "192.168.1.101",
        Port: 8081,
    },
    {
        Name: "order-service",
        Host: "192.168.1.102",
        Port: 8082,
    },
}

if err := nacos.BatchDeregisterInstances(instances); err != nil {
    log.Printf("Failed to batch deregister: %v", err)
    return
}
fmt.Println("Batch deregistration successful")
```

## 🚀 高级功能

### 获取原生客户端

如果需要使用 Nacos SDK 的高级功能，可以直接获取原生的 Config 或 Naming 客户端：

```go
// 获取原生命名客户端
namingClient := nacos.GetNamingClient()
if namingClient != nil {
    // 使用原生 API，例如获取服务详情
    service, err := namingClient.GetService(vo.GetServiceParam{
        ServiceName: "user-service",
        GroupName:   "DEFAULT_GROUP",
        Clusters:    []string{"DEFAULT"},
    })
    if err != nil {
        log.Printf("Error: %v", err)
    }
    fmt.Printf("Service: %+v\n", service)
}

// 获取原生配置客户端
configClient := nacos.GetConfigClient()
if configClient != nil {
    // 使用原生 API
    config, err := configClient.GetConfig(vo.ConfigParam{
        DataId: "app-config.yaml",
        Group:  "DEFAULT_GROUP",
    })
    if err != nil {
        log.Printf("Error: %v", err)
    }
    fmt.Printf("Config: %s\n", config)
}
```

### 自动 IP 获取

服务注册时如果不指定 Host，会自动获取本地 IP：

```go
// 不指定 Host，自动获取
serviceInfo := nacos.ServiceInfo{
    Name: "auto-ip-service",
    Port: 8080,
}

// 内部会自动调用 GetLocalIP() 获取本机 IP
if err := nacos.RegisterInstance(serviceInfo); err != nil {
    log.Printf("Error: %v", err)
}

// 也可以手动获取本地 IP
localIP, err := nacos.GetLocalIP()
if err != nil {
    log.Printf("Failed to get local IP: %v", err)
} else {
    fmt.Printf("Local IP: %s\n", localIP)
}
```

## ⚙️ 配置说明

### Config 配置结构

```go
type Config struct {
    Enabled      bool           // 是否启用 Nacos
    ServerConfig []ServerConfig // 服务器配置列表
    ClientConfig ClientConfig   // 客户端配置
}
```

### ServerConfig 服务器配置

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| Scheme | string | 否 | http | Nacos 服务器协议（http/https） |
| IpAddr | string | 是 | - | Nacos 服务器地址 |
| Port | uint64 | 是 | - | Nacos 服务器端口 |
| GrpcPort | uint64 | 否 | Port+1000 | gRPC 端口（Nacos 2.x 使用） |
| ContextPath | string | 否 | /nacos | Nacos 服务器上下文路径 |

**多服务器配置示例：**

```go
ServerConfig: []ServerConfig{
    {
        IpAddr: "192.168.1.100",
        Port:   8848,
    },
    {
        IpAddr: "192.168.1.101",
        Port:   8848,
    },
    {
        IpAddr: "192.168.1.102",
        Port:   8848,
    },
}
```

### ClientConfig 客户端配置

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| TimeoutMs | uint64 | 否 | 10000 | 请求超时时间（毫秒） |
| BeatInterval | int64 | 否 | 5000 | 心跳间隔（毫秒） |
| NamespaceId | string | 否 | "" | 命名空间 ID，public 命名空间填空字符串 |
| GroupName | string | 否 | DEFAULT_GROUP | 分组名称 |
| ClusterName | string | 否 | "" | 集群名称 |
| AppName | string | 否 | "" | 应用名称 |
| AppKey | string | 否 | "" | 客户端身份信息 |
| Username | string | 否 | "" | Nacos 认证用户名 |
| Password | string | 否 | "" | Nacos 认证密码 |
| CacheDir | string | 否 | 当前路径 | 持久化缓存目录 |
| DisableUseSnapShot | bool | 否 | false | 获取远程配置失败时使用本地缓存 |
| UpdateThreadNum | int | 否 | 20 | 更新服务的 goroutine 数量 |
| NotLoadCacheAtStart | bool | 否 | false | 启动时不加载缓存 |
| UpdateCacheWhenEmpty | bool | 否 | false | 空实例时也更新缓存 |
| LogDir | string | 否 | 当前路径 | 日志目录 |
| LogLevel | string | 否 | info | 日志级别（debug/info/warn/error） |
| AppendToStdout | bool | 否 | false | 日志追加到标准输出 |
| TLSCfg | TLSConfig | 否 | - | TLS 配置 |
| AsyncUpdateService | bool | 否 | false | 异步更新服务 |
| Endpoint | string | 否 | "" | 获取服务器地址的端点 |
| EndpointContextPath | string | 否 | "" | 端点上下文路径 |
| EndpointQueryParams | string | 否 | "" | 端点查询参数 |
| AppConnLabels | map[string]string | 否 | - | 应用连接标签 |

### TLSConfig TLS 配置

```go
type TLSConfig struct {
    Appointed          bool   // 是否指定，如果为 false，将从环境变量获取
    Enable             bool   // 启用 TLS
    TrustAll           bool   // 信任所有服务器（仅用于测试）
    CaFile             string // CA 证书文件路径
    CertFile           string // 客户端证书文件路径
    KeyFile            string // 客户端密钥文件路径
    ServerNameOverride string // 服务器名称覆盖（仅用于测试）
}
```

**TLS 配置示例：**

```go
TLSCfg: TLSConfig{
    Enable:   true,
    TrustAll: false,
    CaFile:   "/etc/ssl/certs/ca.crt",
    CertFile: "/etc/ssl/certs/client.crt",
    KeyFile:  "/etc/ssl/private/client.key",
}
```

## 💡 使用示例

### 微服务端（服务提供者）

完整的微服务示例，包含配置加载、服务注册、配置监听等功能：

```go
package main

import (
    "fmt"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "github.com/xm-utils/tools/nacos"
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
            GroupName:   "DEFAULT_GROUP",
            ClusterName: "DEFAULT",
            LogLevel:    "info",
        },
    }

    if err := nacos.InitClient(config); err != nil {
        log.Fatalf("Failed to init nacos client: %v", err)
    }
    log.Println("✓ Nacos client initialized")

    // 2. 从 Nacos 获取应用配置
    appConfig, err := nacos.GetConfig("app-config.yaml")
    if err != nil {
        log.Printf("Warning: Failed to get config: %v", err)
    } else {
        log.Printf("✓ Loaded config: %s bytes", len(appConfig))
        // TODO: 解析配置并应用到应用中
    }

    // 3. 监听配置变化
    configListener := func(namespace, group, dataId, data string) {
        log.Printf("Config changed - DataId: %s", dataId)
        log.Printf("New config: %s", data)
        // TODO: 重新加载配置并热更新应用
    }

    if err := nacos.ListenConfig("app-config.yaml", configListener); err != nil {
        log.Printf("Warning: Failed to listen config: %v", err)
    } else {
        log.Println("✓ Config listener registered")
    }

    // 4. 注册服务实例
    serviceInfo := nacos.ServiceInfo{
        Name:     "user-service",
        Port:     8080,
        Weight:   1.0,
        Metadata: map[string]string{
            "version": "1.0.0",
            "env":     "production",
        },
    }

    if err := nacos.RegisterInstance(serviceInfo); err != nil {
        log.Fatalf("Failed to register service: %v", err)
    }
    log.Println("✓ Service registered")

    // 5. 服务关闭时注销
    defer func() {
        if _, err := nacos.DeregisterInstance("user-service"); err != nil {
            log.Printf("Failed to deregister service: %v", err)
        } else {
            log.Println("✓ Service deregistered")
        }
    }()

    // 6. 启动 HTTP 服务
    http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("OK"))
    })

    http.HandleFunc("/api/users", func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        w.Write([]byte(`{"users":[]}`))
    })

    server := &http.Server{Addr: ":8080"}
    
    // 7. 优雅退出
    go func() {
        log.Println("✓ Server starting on :8080")
        if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Fatalf("Server failed: %v", err)
        }
    }()

    // 等待中断信号
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit

    log.Println("Shutting down server...")
    if err := server.Close(); err != nil {
        log.Printf("Server close error: %v", err)
    }
}
```

### 调用端（服务消费者）

服务消费者示例，包含服务发现、负载均衡、服务订阅等功能：

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "time"
    "github.com/xm-utils/tools/nacos"
    "github.com/nacos-group/nacos-sdk-go/v2/model"
)

// UserServiceClient 用户服务客户端
type UserServiceClient struct {
    serviceName string
}

// NewUserServiceClient 创建服务客户端
func NewUserServiceClient(serviceName string) *UserServiceClient {
    return &UserServiceClient{
        serviceName: serviceName,
    }
}

// CallGetUsers 调用获取用户接口
func (c *UserServiceClient) CallGetUsers() error {
    // 1. 选择一个健康的服务实例（负载均衡）
    instance, err := nacos.SelectOneHealthyInstance(c.serviceName)
    if err != nil {
        return fmt.Errorf("select instance failed: %w", err)
    }

    // 2. 构建请求 URL
    url := fmt.Sprintf("http://%s:%d/api/users", instance.Ip, instance.Port)
    log.Printf("Calling service at: %s", url)

    // 3. 发起 HTTP 请求
    resp, err := http.Get(url)
    if err != nil {
        return fmt.Errorf("http request failed: %w", err)
    }
    defer resp.Body.Close()

    // 4. 读取响应
    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return fmt.Errorf("read response failed: %w", err)
    }

    log.Printf("Response: %s", string(body))
    return nil
}

func main() {
    // 1. 初始化 Nacos 客户端
    config := &nacos.Config{
        Enabled: true,
        ServerConfig: []nacos.ServerConfig{
            {IpAddr: "127.0.0.1", Port: 8848},
        },
        ClientConfig: nacos.ClientConfig{
            TimeoutMs:   10000,
            NamespaceId: "",
            GroupName:   "DEFAULT_GROUP",
            ClusterName: "DEFAULT",
        },
    }

    if err := nacos.InitClient(config); err != nil {
        log.Fatalf("Failed to init client: %v", err)
    }
    log.Println("✓ Nacos client initialized")

    // 2. 创建服务客户端
    client := NewUserServiceClient("user-service")

    // 3. 订阅服务变化
    subscribeCallback := func(services []model.Instance, err error) {
        if err != nil {
            log.Printf("Subscribe callback error: %v", err)
            return
        }
        
        log.Printf("Service instances changed:")
        log.Printf("  Available instances: %d", len(services))
        for _, inst := range services {
            if inst.Healthy {
                log.Printf("    ✓ %s:%d (weight: %f)", 
                    inst.Ip, inst.Port, inst.Weight)
            }
        }
    }

    param := nacos.SubscribeParam{
        ServiceName:       "user-service",
        SubscribeCallback: subscribeCallback,
    }

    if err := nacos.Subscribe(param); err != nil {
        log.Printf("Warning: Failed to subscribe: %v", err)
    } else {
        log.Println("✓ Service subscription registered")
    }

    // 4. 定期调用服务
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        if err := client.CallGetUsers(); err != nil {
            log.Printf("Call service failed: %v", err)
        } else {
            log.Println("✓ Call succeeded")
        }
    }
}
```

### 配置监听示例

动态配置监听的完整示例：

```go
package main

import (
    "encoding/json"
    "fmt"
    "log"
    "sync"
    "github.com/xm-utils/tools/nacos"
)

// AppConfig 应用配置结构
type AppConfig struct {
    ServerPort int    `json:"server_port"`
    ServerName string `json:"server_name"`
    DebugMode  bool   `json:"debug_mode"`
    MaxConn    int    `json:"max_conn"`
}

var (
    currentConfig AppConfig
    configMutex   sync.RWMutex
)

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

    // 1. 首次加载配置
    if err := loadConfig(); err != nil {
        log.Printf("Warning: Initial config load failed: %v", err)
    }

    // 2. 监听配置变化
    listener := func(namespace, group, dataId, data string) {
        log.Printf("Config changed detected:")
        log.Printf("  Namespace: %s", namespace)
        log.Printf("  Group: %s", group)
        log.Printf("  DataId: %s", dataId)
        
        // 重新加载配置
        if err := reloadConfig(data); err != nil {
            log.Printf("Reload config failed: %v", err)
        } else {
            log.Println("Config reloaded successfully")
        }
    }

    if err := nacos.ListenConfig("app-config.json", listener); err != nil {
        log.Fatalf("Failed to listen config: %v", err)
    }

    log.Println("Config listener started, waiting for changes...")
    
    // 保持程序运行
    select {}
}

// loadConfig 加载配置
func loadConfig() error {
    data, err := nacos.GetConfig("app-config.json")
    if err != nil {
        return err
    }
    return reloadConfig(data)
}

// reloadConfig 重新加载配置
func reloadConfig(data string) error {
    var newConfig AppConfig
    if err := json.Unmarshal([]byte(data), &newConfig); err != nil {
        return fmt.Errorf("unmarshal config failed: %w", err)
    }

    configMutex.Lock()
    oldConfig := currentConfig
    currentConfig = newConfig
    configMutex.Unlock()

    log.Printf("Config updated:")
    log.Printf("  ServerPort: %d -> %d", oldConfig.ServerPort, newConfig.ServerPort)
    log.Printf("  ServerName: %s -> %s", oldConfig.ServerName, newConfig.ServerName)
    log.Printf("  DebugMode: %t -> %t", oldConfig.DebugMode, newConfig.DebugMode)
    log.Printf("  MaxConn: %d -> %d", oldConfig.MaxConn, newConfig.MaxConn)

    return nil
}

// GetCurrentConfig 获取当前配置（线程安全）
func GetCurrentConfig() AppConfig {
    configMutex.RLock()
    defer configMutex.RUnlock()
    return currentConfig
}
```

### 多环境配置

不同环境的配置示例：

```go
package main

import (
    "log"
    "os"
    "github.com/xm-utils/tools/nacos"
)

// GetNacosConfig 根据环境获取 Nacos 配置
func GetNacosConfig() *nacos.Config {
    env := os.Getenv("APP_ENV")
    if env == "" {
        env = "development"
    }

    switch env {
    case "production":
        return &nacos.Config{
            Enabled: true,
            ServerConfig: []nacos.ServerConfig{
                {
                    Scheme: "https",
                    IpAddr: "nacos.example.com",
                    Port:   443,
                },
            },
            ClientConfig: nacos.ClientConfig{
                TimeoutMs:   10000,
                NamespaceId: "prod-namespace-id",
                GroupName:   "PROD_GROUP",
                Username:    "nacos",
                Password:    os.Getenv("NACOS_PASSWORD"),
                LogLevel:    "warn",
                CacheDir:    "/var/lib/nacos/cache",
                LogDir:      "/var/log/nacos",
            },
        }

    case "staging":
        return &nacos.Config{
            Enabled: true,
            ServerConfig: []nacos.ServerConfig{
                {
                    IpAddr: "192.168.1.200",
                    Port:   8848,
                },
            },
            ClientConfig: nacos.ClientConfig{
                TimeoutMs:   10000,
                NamespaceId: "staging-namespace-id",
                GroupName:   "STAGING_GROUP",
                LogLevel:    "info",
            },
        }

    default: // development
        return &nacos.Config{
            Enabled: true,
            ServerConfig: []nacos.ServerConfig{
                {
                    IpAddr: "127.0.0.1",
                    Port:   8848,
                },
            },
            ClientConfig: nacos.ClientConfig{
                TimeoutMs:    5000,
                NamespaceId:  "",
                GroupName:    "DEV_GROUP",
                LogLevel:     "debug",
                AppendToStdout: true,
            },
        }
    }
}

func main() {
    config := GetNacosConfig()
    
    if err := nacos.InitClient(config); err != nil {
        log.Fatalf("Failed to init client: %v", err)
    }
    
    log.Println("Nacos client initialized for environment:", os.Getenv("APP_ENV"))
}
```

## 📚 API 参考

### 初始化函数

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `InitClient(config *Config) error` | 初始化 Nacos 客户端 | error: 初始化错误 |

**示例：**
```go
err := nacos.InitClient(config)
if err != nil {
    log.Fatal(err)
}
```

### 配置管理 API

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `GetConfig(dataId string) (string, error)` | 获取配置内容 | data: 配置内容, error: 错误 |
| `PublishConfig(dataId, content string) (bool, error)` | 发布配置 | success: 是否成功, error: 错误 |
| `DeleteConfig(dataId string) (bool, error)` | 删除配置 | success: 是否成功, error: 错误 |
| `SearchConfig(search string, pageNo, pageSize uint32) (*model.ConfigPage, error)` | 搜索配置 | configPage: 配置分页结果, error: 错误 |
| `ListenConfig(dataId string, listener func(namespace, group, dataId, data string)) error` | 监听配置变化 | error: 错误 |
| `CancelListenConfig(dataId string) error` | 取消监听配置 | error: 错误 |

**注意：** 配置管理 API 中的 `group` 参数已从函数签名中移除，统一使用配置中的 `GroupName`。

### 服务发现 API

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `RegisterInstance(info ServiceInfo) error` | 注册服务实例 | error: 错误 |
| `DeregisterInstance(name string) (bool, error)` | 注销服务实例 | success: 是否成功, error: 错误 |
| `SelectOneHealthyInstance(serviceName string) (*model.Instance, error)` | 选择一个健康实例（负载均衡） | instance: 实例信息, error: 错误 |
| `SelectInstances(serviceName string, healthy bool) ([]model.Instance, error)` | 查询服务实例列表 | instances: 实例列表, error: 错误 |
| `GetAllServices(pageNo, pageSize uint32) (*model.ServiceList, error)` | 获取所有服务列表 | serviceList: 服务列表, error: 错误 |
| `GetServiceDetail(serviceName string, clusters []string) (*model.Service, error)` | 获取服务详细信息 | service: 服务详情, error: 错误 |
| `Subscribe(param SubscribeParam) error` | 订阅服务变化 | error: 错误 |
| `Unsubscribe(param SubscribeParam) error` | 取消订阅服务 | error: 错误 |
| `BatchRegisterInstances(instances []ServiceInfo) error` | 批量注册实例 | error: 错误 |
| `BatchDeregisterInstances(instances []ServiceInfo) error` | 批量注销实例 | error: 错误 |

**注意：** 服务发现 API 中的 `groupName` 和 `clusters` 参数已从函数签名中移除，统一使用配置中的 `GroupName` 和 `ClusterName`。

### 辅助函数

| 函数签名 | 说明 | 返回值 |
|---------|------|--------|
| `GetLocalIP() (string, error)` | 获取本地 IP 地址 | ip: IP 地址, error: 错误 |
| `GetNamingClient() naming_client.INamingClient` | 获取原生命名客户端 | client: 命名客户端 |
| `GetConfigClient() config_client.IConfigClient` | 获取原生配置客户端 | client: 配置客户端 |

### 数据结构

#### ServiceInfo

服务实例信息结构：

```go
type ServiceInfo struct {
    Name     string            // 服务名（必填）
    Host     string            // 主机地址（可选，为空时自动获取）
    Port     uint64            // 端口（必填）
    Weight   float64           // 权重，范围 0-100（必填）
    Metadata map[string]string // 元数据，用于存储版本、环境等信息（可选）
}
```

**字段说明：**
- `Name`: 服务名称，用于服务发现和调用
- `Host`: 服务 IP 地址，不填则自动获取本地 IP
- `Port`: 服务端口号
- `Weight`: 实例权重，影响负载均衡的选择概率，范围 0-100，值越大被选中的概率越高
- `Metadata`: 服务元数据，可以存储版本、环境、区域等信息，用于服务筛选和路由

**示例：**
```go
serviceInfo := nacos.ServiceInfo{
    Name:     "user-service",
    Port:     8080,
    Weight:   1.0,
    Metadata: map[string]string{
        "version": "1.0.0",
        "env":     "production",
        "region":  "cn-north-1",
    },
}
```

#### SubscribeParam

服务订阅参数结构：

```go
type SubscribeParam struct {
    ServiceName       string                                     // 服务名
    SubscribeCallback func(services []model.Instance, err error) // 回调函数
}
```

**字段说明：**
- `ServiceName`: 要订阅的服务名称
- `SubscribeCallback`: 服务实例变化时的回调函数
  - `services`: 当前的服务实例列表
  - `err`: 错误信息（如果有）

**示例：**
```go
param := nacos.SubscribeParam{
    ServiceName: "user-service",
    SubscribeCallback: func(services []model.Instance, err error) {
        if err != nil {
            log.Printf("Subscribe error: %v", err)
            return
        }
        log.Printf("Available instances: %d", len(services))
    },
}
```

#### model.Instance

Nacos 服务实例模型（来自 nacos-sdk-go）：

```go
type Instance struct {
    InstanceId  string            // 实例 ID
    Ip          string            // IP 地址
    Port        uint64            // 端口
    Weight      float64           // 权重
    Healthy     bool              // 是否健康
    Enabled     bool              // 是否启用
    Ephemeral   bool              // 是否为临时实例
    ClusterName string            // 集群名称
    ServiceName string            // 服务名称
    Metadata    map[string]string // 元数据
}
```

#### model.ConfigPage

配置搜索结果分页模型（来自 nacos-sdk-go）：

```go
type ConfigPage struct {
    TotalCount int           // 总记录数
    PageNumber int           // 当前页码
    PagesAvailable int      // 可用页数
    PageItems  []ConfigItem  // 配置项列表
}

type ConfigItem struct {
    Id      string // 配置 ID
    DataId  string // 配置 DataId
    Group   string // 配置分组
    Content string // 配置内容
}
```

#### model.ServiceList

服务列表模型（来自 nacos-sdk-go）：

```go
type ServiceList struct {
    Count int      // 服务总数
    Doms  []string // 服务名称列表
}
```

#### model.Service

服务详情模型（来自 nacos-sdk-go）：

```go
type Service struct {
    Name        string     // 服务名称
    GroupName   string     // 分组名称
    Clusters    string     // 集群名称
    CacheMillis uint64     // 缓存时间
    Hosts       []Instance // 实例列表
    LastRefTime uint64     // 最后引用时间
    Checksum    string     // 校验和
}
```

## 🧪 测试

### 运行测试

```bash
cd nacos
go test -v
```

### 运行特定测试

```bash
# 测试初始化
go test -v -run TestInitNacosClient

# 测试配置获取
go test -v -run TestGetConfig

# 测试服务注册
go test -v -run TestRegisterInstance

# 测试完整流程
go test -v -run TestServiceRegistrationFlow
```

### 运行基准测试

```bash
go test -bench=. -benchmem
```

**示例输出：**
```
BenchmarkInitNacosClient-8    1000    1234567 ns/op    12345 B/op    123 allocs/op
```

### 测试注意事项

1. **需要 Nacos 服务器**：大部分测试需要运行中的 Nacos 服务器
2. **配置测试服务器地址**：修改 `client_test.go` 中的 `initClient()` 函数，设置正确的 Nacos 服务器地址
3. **测试隔离**：测试会使用真实的服务名称，注意避免与生产环境冲突

```go
// 修改测试中的 Nacos 服务器地址
func initClient() {
    config := &Config{
        Enabled: true,
        ServerConfig: []ServerConfig{
            {
                IpAddr: "your-nacos-server-ip", // 修改这里
                Port:   8848,
            },
        },
        ClientConfig: ClientConfig{
            TimeoutMs:   10000,
            NamespaceId: "",
            LogLevel:    "info",
        },
    }
    InitClient(config)
}
```

## 📖 最佳实践

### 生产环境建议

#### 1. 高可用配置

```go
// 使用多个 Nacos 服务器节点
ServerConfig: []ServerConfig{
    {IpAddr: "192.168.1.100", Port: 8848},
    {IpAddr: "192.168.1.101", Port: 8848},
    {IpAddr: "192.168.1.102", Port: 8848},
}
```

#### 2. 启用 TLS

```go
ClientConfig: nacos.ClientConfig{
    TLSCfg: nacos.TLSConfig{
        Enable:   true,
        TrustAll: false,
        CaFile:   "/etc/ssl/certs/ca.crt",
    },
}
```

#### 3. 配置合理的超时和重试

```go
ClientConfig: nacos.ClientConfig{
    TimeoutMs:    10000,  // 10秒超时
    BeatInterval: 5000,   // 5秒心跳
}
```

#### 4. 日志配置

```go
ClientConfig: nacos.ClientConfig{
    LogLevel:       "warn",  // 生产环境使用 warn 或 error
    LogDir:         "/var/log/nacos",
    AppendToStdout: false,
}
```

#### 5. 命名空间隔离

```go
// 不同环境使用不同的命名空间
ClientConfig: nacos.ClientConfig{
    NamespaceId: "prod-namespace-id", // 生产环境
    // NamespaceId: "dev-namespace-id",  // 开发环境
}
```

### 服务注册规范

#### 1. 服务命名规范

```go
// 推荐格式：<业务域>-<服务名>
serviceInfo := nacos.ServiceInfo{
    Name: "user-center-service",
    // 或
    Name: "order-payment-service",
}
```

#### 2. 元数据使用

```go
serviceInfo := nacos.ServiceInfo{
    Name: "user-service",
    Port: 8080,
    Metadata: map[string]string{
        "version":    "1.0.0",      // 服务版本
        "env":        "production", // 环境标识
        "region":     "cn-north-1", // 区域
        "protocol":   "http",       // 服务协议
        "healthPath": "/health",    // 健康检查路径
    },
}
```

#### 3. 优雅退出

```go
// 程序退出时注销服务
defer func() {
    if _, err := nacos.DeregisterInstance("my-service"); err != nil {
        log.Printf("Failed to deregister: %v", err)
    }
}()
```

#### 4. 权重配置

```go
// 根据服务器性能配置不同权重
serviceInfo := nacos.ServiceInfo{
    Name:   "compute-service",
    Port:   8080,
    Weight: 2.0, // 高性能服务器设置更高权重
}
```

### 配置管理规范

#### 1. 配置分组

```go
// 使用分组区分不同类型的配置
ClientConfig: nacos.ClientConfig{
    GroupName: "DATABASE_CONFIG",  // 数据库配置
    // GroupName: "CACHE_CONFIG",   // 缓存配置
    // GroupName: "BUSINESS_CONFIG", // 业务配置
}
```

#### 2. 配置文件格式

推荐使用 YAML 或 JSON 格式：

```yaml
# app-config.yaml
server:
  port: 8080
  name: user-service

database:
  host: localhost
  port: 3306
  name: user_db

cache:
  enabled: true
  ttl: 3600
```

#### 3. 配置版本管理

在元数据中标记配置版本：

```go
metadata := map[string]string{
    "configVersion": "v1.2.0",
    "lastUpdated":   "2024-01-01T00:00:00Z",
}
```

## ❓ 常见问题

### Q1: 如何禁用 Nacos？

```go
config := &nacos.Config{
    Enabled: false, // 设置为 false 即可禁用
}
nacos.InitClient(config) // 不会报错，直接返回
```

### Q2: 如何处理配置监听的性能问题？

配置监听是异步的，不会影响主流程性能。但如果监听器中有耗时操作，建议在监听器中使用 goroutine：

```go
listener := func(namespace, group, dataId, data string) {
    go func() {
        // 异步处理配置变更
        reloadConfig(data)
    }()
}
```

### Q3: 服务注册后多久能被其他服务发现？

通常在 1-2 秒内可以被发现。可以通过调整心跳间隔来优化：

```go
ClientConfig: nacos.ClientConfig{
    BeatInterval: 3000, // 3秒心跳，更快的检测频率
}
```

### Q4: 如何实现灰度发布？

通过服务元数据和权重配置实现：

```go
// 新版本实例，设置较低权重
serviceInfo := nacos.ServiceInfo{
    Name:     "user-service",
    Port:     8081,
    Weight:   0.1, // 低权重，少量流量
    Metadata: map[string]string{
        "version": "2.0.0-beta",
        "gray":    "true",
    },
}
```

消费者可以根据元数据筛选实例：

```go
instances, _ := nacos.SelectInstances("user-service", true)
for _, inst := range instances {
    if inst.Metadata["gray"] == "true" {
        // 灰度实例
        continue
    }
    // 使用稳定版本实例
}
```

### Q5: 如何处理 Nacos 服务器不可用的情况？

Nacos SDK 会自动重试并使用本地缓存：

```go
ClientConfig: nacos.ClientConfig{
    DisableUseSnapShot: false, // 允许使用本地缓存
    CacheDir:           "/var/lib/nacos/cache",
}
```

### Q6: GroupName 和 ClusterName 如何使用？

本库在初始化时从配置中读取 GroupName 和 ClusterName，后续所有 API 调用都会自动使用这些值，无需每次传递：

```go
config := &nacos.Config{
    ClientConfig: nacos.ClientConfig{
        GroupName:   "MY_GROUP",
        ClusterName: "MY_CLUSTER",
    },
}
nacos.InitClient(config)

// 后续调用自动使用 MY_GROUP 和 MY_CLUSTER
nacos.GetConfig("app-config.yaml")
nacos.RegisterInstance(serviceInfo)
```

如果需要针对不同分组或集群操作，需要创建多个客户端实例（目前本库使用全局客户端，如需多客户端支持，请使用原生 SDK）。

## 📦 依赖

```go
require github.com/nacos-group/nacos-sdk-go/v2 v2.3.5
```

**间接依赖：**
- google.golang.org/grpc v1.67.3
- go.uber.org/zap v1.21.0
- github.com/prometheus/client_golang v1.12.2
- 更多依赖请参考 [go.mod](go.mod)

## 📄 许可证

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

### 贡献指南

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

### 开发建议

- 遵循 Go 代码规范
- 添加必要的单元测试
- 更新文档
- 保持 API 向后兼容

## 📧 联系方式

如有问题或建议，请通过以下方式联系：

- 提交 [Issue](../../issues)
- 发送邮件至项目维护者

## 🔗 相关链接

- [Nacos 官方文档](https://nacos.io/zh-cn/docs/what-is-nacos.html)
- [Nacos Go SDK](https://github.com/nacos-group/nacos-sdk-go)
- [Nacos GitHub](https://github.com/alibaba/nacos)

---

**注意**：使用前请确保已部署并运行 Nacos 服务器。下载地址：[Nacos Releases](https://github.com/alibaba/nacos/releases)

**版本信息**：
- 当前版本：v1.0.0
- Nacos SDK 版本：v2.3.5
- Go 版本要求：1.24.2+
