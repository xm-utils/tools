# registry — 注册中心与配置中心统一抽象

`github.com/xm-utils/tools/registry`

为微服务业务代码提供与具体注册中心解耦的 **服务注册发现**（`registry.Backend`）与 **配置读写监听**（`config.ConfigSource`）抽象层。业务只依赖接口，consul / nacos 由子包以适配器形式实现，切换注册中心时业务代码零改动。


## 特性

- **双抽象、双适配器**：服务治理（注册/发现/监听）与配置治理（读写/热更新）各一组接口，consul / nacos 各一套实现
- **统一实例模型**：`ServiceInstance` 屏蔽 consul/nacos 字段差异（Tags、Weight、健康判定、地址回退）
- **通用服务 Watch**：基于 `Discovery` 接口的轮询 + 排序指纹变更检测，适配器无需各写一份，顺序变化不触发推送；生命周期仅由 ctx 终止（`Backend.Close` 不负责终止在跑的 Watch）
- **Context 全链路**：所有对外 API 第一参数为 `context.Context`，取消可传播到后台 goroutine
- **panic 转 error**：对外 API 不向调用方抛出 panic
- **可 mock**：核心接口零外部依赖（仅 stdlib），单测面向接口写 fake 即可，无需真实中间件

## 安装

```bash
go get github.com/xm-utils/tools/registry
```

## 包结构

```
registry/                                  # 独立 go.mod
├── registry.go        # package registry：ServiceInstance / Registry / Discovery / Backend 接口与模型
├── watch.go           # package registry：通用轮询 Watch（变更检测 + 顺序无关）
├── config/
│   └── config.go      # package config：ConfigSource 接口、Change、错误定义
├── consul/
│   ├── adapter.go     # Consul 注册发现适配器（Backend）
│   └── configsource.go# Consul KV 配置源适配器（ConfigSource）
└── nacos/
    ├── adapter.go     # Nacos 服务注册发现适配器（Backend）
    └── configsource.go# Nacos 配置中心适配器（ConfigSource）
```

## API 参考

### package registry

```go
type ServiceInstance struct {
    ID, Name, Host string; Port int
    Tags []string; Meta map[string]string
    Healthy bool; Weight float64
}
func (s ServiceInstance) Address() string   // "host:port"

type Registry interface {
    Register(ctx context.Context, req *RegisterRequest) error
    Deregister(ctx context.Context, inst ServiceInstance) error
}

type Discovery interface {
    Services(ctx context.Context, req DiscoverRequest) ([]*ServiceInstance, error)
    AllServices(ctx context.Context) ([]string, error)
}

type Backend interface{ Registry; Discovery; Name() string; Close() error }

// 通用服务监听：仅实例集合变化时推送全量快照；ctx 取消后 channel 关闭
func Watch(ctx context.Context, d Discovery, service string,
    cfg WatchConfig, onError func(error)) (<-chan []*ServiceInstance, error)
```

### package config

```go
type ConfigSource interface {
    Get(ctx context.Context, key string) ([]byte, error)   // 不存在返回 ErrKeyNotFound
    Put(ctx context.Context, key string, value []byte) error
    Delete(ctx context.Context, key string) error
    Watch(ctx context.Context, key string) (<-chan Change, error) // 仅推后续变更，不推当前值
    Name() string
    Close() error   // 幂等，终止所有 Watch
}
```

### 适配器构造（四个入口 × 两族）

| 构造函数 | 说明 |
|---|---|
| `consul.New(cfg)` / `nacos.New(cfg)` | 由配置新建，返回 `registry.Backend`；nacos 要求 `cfg.Enabled=true` |
| `consul.NewFromClient(c)` / `nacos.NewFromNamingClient(c)` | 复用 `xm-utils/tools/consul`、`/nacos` 组件已有客户端 |
| `consul.NewConfigAdapter(cfg)` / `nacos.NewConfigAdapter(cfg)` | 配置源，返回 `config.ConfigSource` |
| `consul.NewConfigAdapterFromClient(c)` / `nacos.NewConfigAdapterFromClient(c)` | 复用已有 KV / Config 客户端 |

## 双后端差异语义

| 能力 | consul 适配器 | nacos 适配器 |
|---|---|---|
| Register 健康检查 | HTTP/TCP/TTL 转 `AgentCheck` | 忽略 `Checks`，临时实例由 SDK 心跳维持 |
| Register ID/Weight | `ID` 为空自动生成 `name-host-port` | 无 ServiceID 概念；`Weight<=0` 按 1  |
| Deregister | 按 ServiceID（ID 空回退默认 ID） | 按 Ip+Port 直连 SDK 注销（Host/Port 必填） |
| Tags | 原生 tag 过滤 | 无原生 tag：存入 `Metadata["tags"]`（逗号分隔），查询时拆回并结果内过滤 |
| 健康判定 | Checks 全 `passing`（无 Checks 视为健康）；空 `Address` 回退节点地址 | `Healthy && Enable` |
| AllServices | Catalog 服务名表 | 分页拉取（每页 200）|
| Services/AllServices | 共用 `registry.Watch` 轮询监听（由 ctx 终止，非 `Backend.Close`） | 同左 |
| Config key | KV 路径 | dataId；Group 固定取客户端配置 `GroupName` |
| Config 不存在 | `GetKV` 返回 nil → `ErrKeyNotFound` | 空内容 → `ErrKeyNotFound` |
| Config Watch | blocking query，`Get` 建索引基线；ctx 取消最迟 ~10s 关闭 | SDK `ListenConfig` 回调；同 key 重复 Watch 为**覆盖**语义 |
| Close | SDK 无需释放，恒 nil（幂等） | 注销全部监听并终止 Watch（幂等） |

## 错误约定

| 错误 | 场景 |
|---|---|
| `registry.ErrServiceEmpty` | 服务名为空 |
| `registry.ErrInstanceEmpty` | 注册请求 / 实例为 nil |
| `registry.ErrUnsupported` | `Watch` 传入 nil Discovery 等不支持操作 |
| `config.ErrKeyEmpty` | 配置键为空 |
| `config.ErrKeyNotFound` | 配置键不存在（用 `errors.Is` 判定） |

## 依赖与构建说明

- 基座依赖：`github.com/xm-utils/tools/consul`（已发布 `consul/v1.0.0` tag）、`github.com/xm-utils/tools/nacos`（已发布 `nacos/v1.0.0` tag），均直接按版本引用，无仓内 replace。
- 本模块 go.mod 仅保留一条 `replace`：`genproto => v0.0.0-20241118…`，把整体式 genproto 钉到 2023-05 拆分后的版本，消除与 `genproto/googleapis/rpc` 的 `rpc/status` 包歧义。用 replace 而非 require 钉版，因为 require 会被 `go mod tidy` 剪除（无包直接引用它）。
- 本地改动联动由根目录 `go.work` 负责（不提交）；无 go.work 的独立构建（CI 等）按已发布版本解析。
- go 指令与本仓库所有组件统一为 1.24.2。

## 测试

```bash
cd registry
go test -race ./...     # 38 用例：Watch 推送/去重/错误恢复/取消、双适配器映射与校验、配置源覆盖/注销/并发取消回归、Close 幂等等
```

## 快速上手

见 [QUICKSTART.md](QUICKSTART.md)（5 分钟：注册 → 发现 → 监听 → 配置热更新）。

## 许可证

MIT
