# xm-utils/tools

Go 微服务基础设施工具集，提供数据库访问、缓存、消息队列、服务治理、重试容错等开箱即用的通用组件。各组件以独立 Go Module 发布，可按需引入，互不耦合。

## 环境要求

- Go 1.24.2+
- 各组件的外部依赖（MySQL、Redis、Kafka、MongoDB、Consul、Nacos 等）按实际使用场景部署

## 模块总览

| 模块路径 | 说明 | 文档 |
|---------|------|------|
| `gorm/database` | GORM 数据库工具库：泛型 CRUD、链式 Context API、一主多从读写分离（DBResolver）、条件构建器、事务 | [README](gorm/database/README.md) · [QUICKSTART](gorm/database/QUICKSTART.md) |
| `gorm/database/sharding` | 分库分表：取模分片 / 时间范围分片、Snowflake 主键、自动路由 | [README](gorm/database/sharding/README.md) · [QUICKSTART](gorm/database/sharding/QUICKSTART.md) |
| `beego/database` | Beego ORM 数据库工具库（兼容旧项目） | — |
| `redis` | Redis 客户端封装：String / Hash / List / Set / SortedSet / Stream 全数据结构操作 | [README](redis/README.md) |
| `mq/core` | 消息队列统一门面（纯契约，零第三方依赖）：Message / Handler / Producer / Consumer 抽象与多后端选型矩阵 | [README](mq/core/README.md) |
| `mq/redis` | Redis 侧适配器四形态：list 安全队列 / zset 延迟队列 / stream 消费者组 / pubsub 广播；含 `stream/queue` 队列本体（当前仅启用 Stream 基础读写，装饰器 / Pending 巡检 / 指标已注释保留） | [README](mq/redis/README.md) · [QUICKSTART](mq/redis/QUICKSTART.md) |
| `mq/kafka` | Kafka 侧适配器：手动 offset 模式生产 / 消费与额度内重投；含 `client` 客户端本体（Worker Pool、消息去重、指标监控） | [README](mq/kafka/README.md) · [client](mq/kafka/client/README.md) |
| `mq/pipequeue` | pipeline 引擎 × MQ 门面桥接队列：推送→拉取转换、终态钩子回写确认 | [README](mq/pipequeue/README.md) |
| `mongodb` | MongoDB 客户端：连接管理、泛型 CRUD、聚合管道 | [README](mongodb/README.md) · [QUICKSTART](mongodb/QUICKSTART.md) |
| `pipeline` | 泛型消息消费执行引擎：步骤链编排、错误分级、步骤超时、有界停机、同 Key 串行 | [README](pipeline/README.md) · [QUICKSTART](pipeline/QUICKSTART.md) |
| `retry` | 非阻塞重试执行器：多策略（固定 / 指数 / 线性）、批量并发、监控回调 | [README](retry/README.md) · [QUICKSTART](retry/QUICKSTART.md) |
| `breaker` | 熔断器：滑动窗口指标、状态机（Closed / Open / Half-Open）、告警、与 Retry 联动 | [README](breaker/README.md) · [QUICKSTART](breaker/QUICKSTART.md) |
| `ratelimit` | 限流与降级：令牌桶 / 滑动窗口 / 漏桶三算法、本地内存与 Redis(Lua 原子) 双后端、gRPC/HTTP 入口中间件 | [README](ratelimit/README.md) · [QUICKSTART](ratelimit/QUICKSTART.md) |
| `deadletter` | 死信队列：自定义持久化（GORM）、自动恢复重试、实时监控告警 | [README](deadletter/README.md) · [QUICKSTART](deadletter/QUICKSTART.md) |
| `consul` | Consul SDK 封装：服务注册 / 发现、健康检查、KV 配置 | [README](consul/README.md) |
| `nacos` | Nacos SDK 封装：配置管理、服务发现（闭包模式独立客户端） | [README](nacos/README.md) |
| `registry` | 注册中心/配置中心统一抽象：consul / nacos 双适配器，服务注册发现、实例 Watch、配置热更新 | [README](registry/README.md) · [QUICKSTART](registry/QUICKSTART.md) |
| `grpcx` | gRPC 服务工具：Server / Client 快速构建、cmux 端口复用（gRPC + HTTP）、优雅停机 | — |
| `httpclient` | HTTP 客户端：连接池、JSON / 表单 / 文件上传、请求追踪 | — |
| `common` | 通用工具函数：JWT、字符串 / 切片 / 时间 / 网络、RSA 加解密、密码哈希、统一响应体 | — |

## 快速引入

各模块独立 `go.mod`，按需 `go get`：

```bash
# 示例：引入 GORM 数据库组件
go get github.com/xm-utils/tools/gorm/database

# 示例：引入 Redis 组件
go get github.com/xm-utils/tools/redis

# 示例：引入重试执行器
go get github.com/xm-utils/tools/retry

# 示例：引入 MQ 门面 + 择一后端适配器
go get github.com/xm-utils/tools/mq/core
go get github.com/xm-utils/tools/mq/redis
```

> 模块路径统一前缀：`github.com/xm-utils/tools/<模块目录>`；`mq` 族为两级路径（`mq/core`、
> `mq/redis`、`mq/kafka`、`mq/pipequeue`），顶层 `mq/` 仅为容器目录、自身不是模块。

## 目录结构

```
tools/
├── beego/database/       # Beego ORM 封装
├── breaker/              # 熔断器
├── common/               # 通用工具函数
├── consul/               # Consul 服务治理
├── deadletter/           # 死信队列
├── gorm/database/        # GORM 数据库（含 sharding 子包）
├── grpcx/                # gRPC 工具
├── httpclient/           # HTTP 客户端
├── mongodb/              # MongoDB 客户端
├── mq/                   # 消息队列族（顶层容器，自身无 go.mod）
│   ├── core/             # 统一门面（纯契约，package mq）
│   ├── redis/            # Redis 四形态适配器（含 stream/queue 队列本体）
│   ├── kafka/            # Kafka 适配器（含 client 客户端本体）
│   └── pipequeue/        # MQ × pipeline 桥接队列
├── nacos/                # Nacos 配置/注册中心
├── pipeline/             # 消息消费执行引擎
├── ratelimit/            # 限流与降级（含 redisstore / grpcmiddleware / httpmiddleware 子包）
├── redis/                # Redis 客户端
├── registry/             # 注册中心/配置中心统一抽象（consul/nacos 双适配器）
└── retry/                # 重试执行器
```

## 设计约定

- **独立模块**：每个组件拥有独立 `go.mod`，可单独版本管理与引入，不强制拉取整个仓库；`mq` 族拆为四个同级模块（共用同一门面契约），按需引入以免把无关后端的 SDK 拉进依赖树。
- **panic 转 error**：对外 API 不向调用方抛出 panic；内部 recover 后包装为 error 返回。
- **配置模板**：需要外部配置的组件提供 `config.example.yaml`（如 `gorm/database`、`consul`、`nacos`），字段与结构体 yaml tag 一一对应；`ratelimit` 根包为零依赖，其模板描述的是装配层同构 struct 的键位。
- **文档配套**：核心组件配备 README（完整 API 参考）+ QUICKSTART（5 分钟上手）。
- **测试覆盖**：关键路径均有单元测试，`go test ./...` 即可验证。

## 开发

```bash
# 克隆仓库
git clone https://github.com/xm-utils/tools.git
cd tools

# 进入目标模块开发/测试（各模块独立）
cd gorm/database
go build ./...
go test ./...
go vet ./...
```

## 许可证

MIT