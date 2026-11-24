# grpcx — gRPC 服务与客户端工具

gRPC 服务端 / 客户端的快速构建层：**双形态 `IServer`（gRPC + HTTP(Gin）+ cmux 端口复用 + 优先级优雅停机框架 + 客户端惰性建连与注册表式管理**，并内置与 `trace` 组件的拦截器装配。

- 模块路径：`github.com/xm-utils/tools/grpcx`
- 技术选型：`google.golang.org/grpc` v1.79.3、`github.com/soheilhy/cmux` v0.1.5（同端口复用 gRPC + HTTP）、`github.com/gin-gonic/gin` v1.10.0、`github.com/sirupsen/logrus` v1.9.4
- 仓内依赖：`github.com/xm-utils/tools/trace`（仅用于拦截器装配，当前以 `replace ../trace` 引用，trace 发 tag 后转 pseudo-version）
- 发布状态：最新 tag **`grpcx/v1.0.1`**（2026-07-06，commit `44642f6`）；此后合入的 trace 装配（`trace.go`）**尚未随 tag 发布**，下次发布建议 `v1.0.2`
- 测试现状：**本模块暂无 `_test.go`**，行为以源码为唯一真源，下文所有断言均据实现逐条核对

## 目录结构

```
grpcx/
├── server.go      # IServer 抽象、GrpcServer / HTTPServer 两实现、Server 聚合与 cmux 复用
├── client.go      # ClientCreator 注册表、GRPCClient（惰性建连 + 健康检查）、ClientManager
├── option.go      # ClientOption：WidthHost / WithTimeout / WithResolver / WithStatsHandler 等
├── shutdown.go    # 优雅停机框架：ShutdownHook / ImplementShutdown / CustomHook / Shutdown 单例
├── trace.go       # 与 trace 组件的装配面：TraceServerOptions / WithClientTrace / TraceClientDialOptions
└── ip.go          # 内网 IPv4 探测（当前仅被未接线的 adjustAddr 使用，见已知边界）
```

## 快速上手

### 服务端（30 秒）

```go
package main

import (
    "github.com/xm-utils/tools/grpcx"
    "google.golang.org/grpc"
)

func main() {
    // register 里注册你的 pb 服务
    srv := grpcx.NewGrpcServer("order-svc", ":9000", func(gs *grpc.Server) {
        // pb.RegisterOrderServer(gs, &OrderImpl{})
    })

    // 挂到停机框架：收到 TERM/INT 等信号时自动 GracefulStop
    grpcx.GetShutdown().InstallShutdownHook(srv)

    go func() {
        if err := srv.Start(); err != nil {
            panic(err)
        }
    }()
    grpcx.GetShutdown().WaitShutdown() // 阻塞直到收到终止信号
}
```

### 客户端（30 秒）

```go
cli := grpcx.NewGRPCClient(
    func(name string, conn *grpc.ClientConn) interface{} {
        return pb.NewUserClient(conn) // creator：按名字返回你的 pb 客户端
    },
    grpcx.WidthHost("127.0.0.1:9000"),
    grpcx.WithClientTrace("order-svc"),
)
defer cli.Close()

u, err := cli.GetServiceClient("user") // 首次调用才建连，结果按 name 缓存
if err != nil {
    return err
}
userCli := u.(pb.UserClient) // GetServiceClient 返回 interface{}，需自行断言
```

## API 参考

### 服务端：IServer 双形态

| 构造 | 行为 |
|------|------|
| `NewGrpcServer(name, addr string, register ServiceRegister, opts ...grpc.ServerOption) *GrpcServer` | 构造期即 `grpc.NewServer(opts...)` 并执行 `register(srv)`；**此时不监听**，`Start()` 才 `net.Listen` |
| `NewHTTPServer(name, addr string, httpSetup func(engine *gin.Engine)) *HTTPServer` | 构造期 `gin.Default()` 并调用 `httpSetup(engine)`；`httpSetup` 传 `nil` 会在构造时 panic |
| `WithGrpcServer(...)` / `WithHTTPServer(...)` | 同上两构造器的 `IServer` 版本，便于混装进 `[]IServer` |
| `Server = NewServer([]IServer)` | 多服务聚合器，提供 `Start()` 与 `StartWithListener(l)` 两条启动路径 |

停机侧：`GrpcServer.stop()` → `GracefulStop()`；`HTTPServer.stop()` → `server.Shutdown(context.Background())`（忽略 `http.ErrServerClosed`）。两者都实现了 `BeforeShutdown()`，可直接作为停机钩子。

> `IServer` 接口含未导出方法（`stop` / `getAddr` / `typeCode`），因此**外部包无法自行实现该接口**，只能使用本包提供的两种实现（密封接口手法）。

### 端口复用（cmux）

```go
m := cmux 用的监听器（自己 net.Listen 或使用 NewServer 的 StartWithListener）
server := grpcx.NewServer([]grpcx.IServer{
    grpcx.WithGrpcServer("order-svc", ":9000", register), // 必须放在前面
    grpcx.WithHTTPServer("gateway", ":9000", func(e *gin.Engine) { ... }),
})
_ = server.StartWithListener(lis)
```

匹配规则由各自的 `Match()` 决定：

- `GrpcServer.Match()` → `cmux.HTTP2HeaderField("content-type", "application/grpc")`
- `HTTPServer.Match()` → `cmux.Any()`（兜底，吃掉所有未被前面匹配的流量）

因此**切片顺序必须 gRPC 在前、HTTP 在后**：`cmux.Any()` 一旦先注册，后续匹配器永远拿不到连接，gRPC 请求会被当成 HTTP 处理而失败。

`Start()` 路径（非复用）则把每个子 server 各起一个 goroutine 调 `Start()`，各自监听自己的 `addr`，子服务启动失败只打日志、`Start()` 始终返回 `nil`。

### 优雅停机框架

| 符号 | 说明 |
|------|------|
| `ShutdownHook` | 接口：`Name()` / `ShutdownPriority()` / `BeforeShutdown()`（`AfterShutdown` 已在接口中注释掉，未启用） |
| `ImplementShutdown` | 内嵌即拿到 `Name()` / `ShutdownPriority()` 两个实现（字段 `Priority`、`EventName`） |
| `CustomHook` / `NewCustomHook(eventName string, priority int, shutdown func())` | 用一个闭包快速挂钩子 |
| `NewShutdown()` / `GetShutdown()` | 全局单例（`sync.Once`）；`GetShutdown` 未初始化时会懒建 |
| `InstallShutdownHook(hook) *Shutdown` | 追加钩子（传 `nil` 被忽略），返回自身支持链式 |
| `WaitShutdown()` | 阻塞等信号，命中 `SIGHUP/SIGINT/SIGQUIT/SIGABRT/SIGTERM` 后依次执行钩子并 `os.Exit(0)` |
| `IsShutdown()` | 是否已进入关闭流程 |
| `TimeOut()` | 60 秒强制 `os.Exit(0)` 的兜底；**内部是无条件的 `time.After(60s)`，从被调用那一刻起计时**，与是否进入关闭流程无关。`WaitShutdown` 里的 `go object.TimeOut()` 已被注释（改由 docker-compose 控制停机超时），因此现状下不会被自动启用；若在启动期直接 `go TimeOut()`，进程会在 60 秒后自杀 |

优先级常量为 `iota` 序列，钩子按 **`ShutdownPriority()` 数值降序**执行（数值越大越先关闭）：

```go
WorkerPoolPriority = 0
RoutinePoolPriority = 1
LogPriority = 2
CronPriority = 3
GrpcServerPriority = 4        // GrpcServer / Server / NewServer 默认取此值
HttpServerShutdownPriority = 5 // HTTPServer 取此值 → 比 gRPC 先关
```

即默认行为是「先停 HTTP 入口，再停 gRPC 服务」，最后才轮到优先级 0~3 的 worker/日志/定时任务类钩子。

> 两点必须知道的副作用：① `signal.Notify(object.signalCh)` **没有传信号列表**，会转发全部信号，未匹配的走 default 分支被丢弃——因此 `SIGUSR1` 等信号的默认进程行为（终止/打印堆栈）被抑制；② 收尾是 `os.Exit(0)`，**业务里注册的 `defer` 不会执行**，需要收尾的动作必须做成 `ShutdownHook`。

### 客户端：GRPCClient

| 方法 | 说明 |
|------|------|
| `NewGRPCClient(creator ClientCreator, opts ...ClientOption) *GRPCClient` | 只做选项装配，不建连 |
| `GetServiceClient(name string) (interface{}, error)` | 首次调用才建连（`grpc.NewClient(host, ...)`），结果按 `name` 缓存复用；返回值是 `interface{}`，调用方断言成 pb 客户端 |
| `Close() error` | 关闭底层连接；尚未建连（`conn == nil`）时安全返回 `nil` |
| `GetState() connectivity.State` | 未建连时返回 `connectivity.Shutdown` |
| `IsReady() bool` | 等价于 `GetState() == Ready` |
| `HealthCheck(timeout time.Duration) error` | 未建连直接报错 `connection not established`；`timeout <= 0` 用默认 3s；内部 `WaitForStateChange` 一次，状态变化但仍非 Ready 也返回 error |
| `HealthInfo() map[string]interface{}` | 键：`target`（取 `opts.prefix`，仅由 `WithDirectAddresses` 写入）、`state`、`healthy` |

建连细节（`createClient`）：实际拨号目标只取 `WidthHost` 设的 `opts.host`；`grpc.NewClient` 的选项顺序是「业务传入的 grpcOptions → 恒定追加 `insecure.NewCredentials()`」，**后写覆盖前写，因此无法经 grpcx 配置 TLS**（需要 TLS 请自行 `grpc.NewClient` 后使用 `TraceClientDialOptions` 等拦截器）。

### ClientOption 一览

| 选项 | 实际作用 | 注意 |
|------|----------|------|
| `WidthHost(host string)` | 设置拨号目标（唯一决定连到哪儿） | 拼写沿用既有 API（`Width` 非 `With`），勿误写 |
| `WithDirectAddresses(addrs ...string)` | 仅把 `addrs[0]` 记进 `opts.prefix`，供 `HealthInfo` 的 `target` 展示 | **不改变拨号目标**；源码中预留的 resolver 装配被注释掉，属未完成能力 |
| `WithTimeout(timeout time.Duration)` | 追加 `grpc.WithTimeout` | 该 DialOption 在 grpc 中已 deprecated，只影响建连阶段；调用超时请用 `ctx` |
| `WithResolver(resolver.Builder)` | 追加 `grpc.WithResolvers` | 与 `registry` / `nacos` 的服务发现 resolver 配合使用 |
| `WithStatsHandler(stats.Handler)` | 追加 `grpc.WithStatsHandler` | — |
| `WithClientTrace(tracerName string)` | 追加一元 + 流客户端拦截器 | 见「链路追踪装配」 |

### 客户端注册表与 ClientManager

```go
grpcx.RegisterCreator("user", func(name string, conn *grpc.ClientConn) interface{} {
    return pb.NewUserClient(conn)
})

err := grpcx.GetClientManager().InitializeClients([]grpcx.ServiceConfig{
    {Name: "user", Address: "127.0.0.1:9000", Timeout: 3 * time.Second},
})
defer grpcx.GetClientManager().Close()

cli := grpcx.GetClientManager().GetClient("user")
```

- `RegisterCreator(name, creator)`：同名**覆盖**旧注册；`GetCreator(name)` 未注册返回 error；另有 `HasCreator` / `ListCreators`。
- `InitializeClients([]ServiceConfig)`：`configs` 为空直接返回 `nil`；每个 name 通过 `GetCreator` 取 creator，**取不到时兜底为空 creator（返回 nil）并只记日志、仍返回 nil error**——即“服务照常启动，用到时才拿到 nil 客户端”。
- 管理器创建的客户端固定装配 `WidthHost(config.Address)` + `WithTimeout(config.Timeout)` + `WithStatsHandler(&customStatsHandler{})`。
- `customStatsHandler` 会在每条 RPC 结束时 `logrus.Printf` 打印方法名与耗时，并尝试从 ctx 取业务键 `"orderNo"`（与业务耦合的调试埋点，生产日志噪音需自行评估）。
- `GetClient(name)` 直接索引 map，**未加读锁**：与并发执行的 `InitializeClients` 同时调用存在数据竞争（只读期无碍）。
- `Close()` 逐个关闭并聚合错误（`关闭部分客户端连接失败: [...]`）。
- `ServiceConfig` 带 yaml tag（`name` / `address` / `timeout`），可被配置源直接反序列化；`timeout` 是 `time.Duration`，yaml.v3 只接受 `3s` 这类时长串（写整数会报错），详见 `config.example.yaml` 头部说明。

### 链路追踪装配

```go
// 服务端：一元 + 流拦截器一次性展开
grpcx.NewGrpcServer(name, addr, register, grpcx.TraceServerOptions(name)...)

// 客户端：GRPCClient 走 Option
grpcx.NewGRPCClient(creator, grpcx.WidthHost(host), grpcx.WithClientTrace("order-svc"))

// 客户端：自行拨号时拿原始 DialOption
conn, _ := grpc.NewClient(addr, grpcx.TraceClientDialOptions("order-svc")...)
```

三者的实际语义与 Span 命名、属性、收口时机详见 [trace/README.md](../trace/README.md)；未调用 `trace.Init` 时拦截器为透传零行为，可无条件常驻。与业务既有拦截器共存时建议把 `TraceServerOptions` 置于链首，最先提取上游 `traceparent`。

## 与其他组件的关系

| 方向 | 组件 | 说明 |
|------|------|------|
| 依赖 | `trace` | 拦截器装配面（`trace.go`）；未 Init 时零行为 |
| 协作 | `registry` / `consul` / `nacos` | 服务发现 resolver 经 `WithResolver` 注入；注册本身不在 grpcx 职责内 |
| 协作 | `ratelimit/grpcmiddleware` | 限流服务端拦截器与本包拦截器同属 `grpc.ServerOption` 链，可叠加装配 |
| 协作 | `metrics` | 如需 `/metrics` 出口，可在 `NewHTTPServer` 的 Gin 引擎上挂 `metrics.Handler()` |

## 已知边界与待办

| 项 | 现状 | 影响 / 处理 |
|----|------|-------------|
| 无单元测试 | 本目录无 `_test.go` | 回归依赖调用方集成环境；建议补 cmux 顺序、停机排序、惰性建连三组用例 |
| TLS 不可配 | `createClient` 恒定追加 `insecure` 且在选项末尾 | 需要 TLS 时绕过 `GRPCClient` 自行 `grpc.NewClient` |
| `WithDirectAddresses` 名不副实 | 只写 `prefix`（仅影响 `HealthInfo.target`） | 直连多地址未接线，拨号目标仍由 `WidthHost` 决定 |
| 空 creator 静默兜底 | `InitializeClients` 遇到未注册 name 只记 error 日志 | 表现为「启动成功但客户端为 nil」，排障先看启动日志 |
| `WaitShutdown` 吞全部信号 | `signal.Notify(ch)` 未传信号列表 | `SIGUSR1` 等默认行为被抑制；自定义信号处理需与本函数错开 |
| `os.Exit(0)` 收尾 | 钩子跑完直接退出 | 业务 `defer` 不执行；需收尾的动作要做成 `ShutdownHook` |
| `adjustAddr` / `intranetIP` 未接线 | 定义了地址补全逻辑但无调用点（死代码） | `":9000"` 不会被补成内网 IP；需要时自行调用或清理该死代码 |
| 子服务启动错误不外传 | `Server.Start()` 恒返回 `nil`，`StartWithListener` 内部 `log.Fatalf` | 端口占用等失败会让进程直接退出而非返回 error |
| `GetClient` 未加锁 | 直接索引 map | 运行期热更新配置时勿与 `InitializeClients` 并发 |
| `TimeOut()` 计时起点易误用 | 无条件 `time.After(60s)`，不看 `isShutdown` | 只能在确认已进入停机后调用；启动期 `go TimeOut()` 等于 60 秒后自杀 |
| trace 依赖未发布 | 仍以 `replace ../trace` 引用 | trace 发 tag 后转 pseudo-version 并删除 replace |

## 测试

本模块暂无测试文件。手工验证路径：

```bash
cd grpcx
GOWORK=off go vet ./...
GOWORK=off go build ./...
```

行为验证建议覆盖：`StartWithListener` 的 gRPC/HTTP 匹配顺序、`InstallShutdownHook` 的降序执行、`GetServiceClient` 的建连幂等与缓存、`HealthCheck` 未建连与超时两个分支。

## 许可

MIT
