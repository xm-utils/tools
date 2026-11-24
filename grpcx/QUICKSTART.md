# grpcx 快速上手

目标：5 分钟把 gRPC 服务跑起来并完成客户端装配，然后按需叠加端口复用、优雅停机与链路追踪。

前置：Go 1.24.2+；已有编译好的 protobuf 客户端/服务端代码（下文以 `pb` 代指）。

## 第 1 步：起一个 gRPC 服务

```go
package main

import (
    "log"

    "github.com/xm-utils/tools/grpcx"
    "google.golang.org/grpc"
)

func main() {
    srv := grpcx.NewGrpcServer("order-svc", ":9000", func(gs *grpc.Server) {
        pb.RegisterOrderServer(gs, &OrderImpl{}) // 在此注册全部服务
    })

    // 注册即 GracefulStop：挂到停机框架后收到终止信号会自动收流
    grpcx.GetShutdown().InstallShutdownHook(srv)

    go func() {
        if err := srv.Start(); err != nil { // Start 内部才 net.Listen
            log.Fatal(err)
        }
    }()

    grpcx.GetShutdown().WaitShutdown() // 阻塞，等 HUP/INT/QUIT/ABRT/TERM
}
```

## 第 2 步：客户端调用

```go
cli := grpcx.NewGRPCClient(
    func(name string, conn *grpc.ClientConn) interface{} {
        return pb.NewUserClient(conn) // name 用于区分多服务，可忽略
    },
    grpcx.WidthHost("127.0.0.1:9000"), // 拨号目标只由 WidthHost 决定
)
defer cli.Close()

v, err := cli.GetServiceClient("user")
if err != nil {
    log.Fatal(err)
}
userCli, ok := v.(pb.UserClient) // 返回 interface{}，必须自行断言
if !ok {
    log.Fatal("creator 返回类型不符")
}

ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()
resp, err := userCli.GetUserInfo(ctx, &pb.GetUserInfoReq{Id: 1}) // 超时用 ctx，不靠 DialOption
```

连接是**惰性**建立的：`GetServiceClient` 首次调用才 `grpc.NewClient`。想提前探活：

```go
if err := cli.HealthCheck(3 * time.Second); err != nil { // 未建连直接报错
    log.Println("not ready:", err)
}
```

## 第 3 步：多服务客户端用注册表 + ClientManager

```go
// 启动早期注册 creator（名字与配置里的 name 对应）
grpcx.RegisterCreator("user", func(_ string, conn *grpc.ClientConn) interface{} {
    return pb.NewUserClient(conn)
})
grpcx.RegisterCreator("pay", func(_ string, conn *grpc.ClientConn) interface{} {
    return pb.NewPayClient(conn)
})

mgr := grpcx.GetClientManager()
if err := mgr.InitializeClients([]grpcx.ServiceConfig{
    {Name: "user", Address: "127.0.0.1:9001", Timeout: 3 * time.Second},
    {Name: "pay", Address: "127.0.0.1:9002", Timeout: 5 * time.Second},
}); err != nil {
    log.Fatal(err)
}
defer mgr.Close()

pay := mgr.GetClient("pay") // 名字未初始化时返回 nil，必须判空
if pay == nil {
    log.Fatal("pay 客户端未初始化（检查配置 name 与已注册的 creator 是否一致）")
}
v, err := pay.GetServiceClient("pay") // 返回 interface{}，后续自行断言成 pb 客户端
```

配置源写法与 `time.Duration` 的反序列化陷阱（必须写 `3s` 这类时长串，写整数会报错）见 [config.example.yaml](config.example.yaml)。

## 第 4 步：gRPC 与 HTTP 共用一个端口（cmux）

```go
lis, err := net.Listen("tcp", ":9000")
if err != nil {
    log.Fatal(err)
}

server := grpcx.NewServer([]grpcx.IServer{
    // 顺序关键：gRPC 必须在前。HTTPServer 的匹配是 cmux.Any()，会吞掉后续所有流量
    grpcx.WithGrpcServer("order-svc", ":9000", func(gs *grpc.Server) {
        pb.RegisterOrderServer(gs, &OrderImpl{})
    }),
    grpcx.WithHTTPServer("gateway", ":9000", func(e *gin.Engine) {
        e.GET("/health", func(c *gin.Context) { c.String(200, "ok") })
    }),
})

grpcx.GetShutdown().InstallShutdownHook(server) // Server 的 BeforeShutdown 会逐个 stop
log.Fatal(server.StartWithListener(lis))        // 阻塞直到监听器出错
```

不需要复用端口时改用 `server.Start()`，各子服务按自己的 `addr` 分别监听。

## 第 5 步：自定义停机钩子

```go
grpcx.GetShutdown().
    InstallShutdownHook(grpcx.NewCustomHook("worker-pool", grpcx.WorkerPoolPriority, func() {
        pool.Stop() // 优先级 0 → 最后执行
    })).
    InstallShutdownHook(grpcx.NewCustomHook("flush-metrics", 10, func() {
        _ = trace.Flush(context.Background()) // 自定义数值更大 → 更早执行
    }))
```

钩子按 `ShutdownPriority()` **降序**执行：`HttpServerShutdownPriority(5)` > `GrpcServerPriority(4)` > `CronPriority(3)` > … > `WorkerPoolPriority(0)`。

兜底强退**不会被自动启用**（`WaitShutdown` 内的 `go object.TimeOut()` 已被注释，停机超时改由 docker-compose 控制）。需要自己拉起时注意：`TimeOut()` 内部是无条件的 `time.After(60s)`，**从被调用那一刻起计时，不看是否已进入停机**——启动期直接 `go TimeOut()` 等于让进程 60 秒后自杀。

```go
// 正确做法：先等停机标志，再启动 60s 看门狗
go func() {
    for !grpcx.GetShutdown().IsShutdown() { // 钩子卡住时的兜底
        time.Sleep(100 * time.Millisecond)
    }
    grpcx.GetShutdown().TimeOut() // 此后 60s 内钩子未跑完则 os.Exit(0)
}()
```

## 第 6 步：接上链路追踪

```go
import "github.com/xm-utils/tools/trace"

func main() {
    ctx := context.Background()
    _ = trace.Init(ctx, trace.Config{
        ServiceName: "order-svc",
        Exporter:    "otlp",
        Endpoint:    "localhost:4317",
        Insecure:    true,
    })
    defer trace.Shutdown(context.Background())

    srv := grpcx.NewGrpcServer("order-svc", ":9000", register,
        grpcx.TraceServerOptions("order-svc")...) // 一元 + 流拦截器一次展开

    cli := grpcx.NewGRPCClient(creator, grpcx.WidthHost("127.0.0.1:9001"),
        grpcx.WithClientTrace("order-svc"))
}
```

未调用 `trace.Init` 时这些拦截器是透传零行为，所以接入代码可以常驻。跨进程贯通细节（metadata 载体、Span 命名与属性）见 [trace/QUICKSTART.md](../trace/QUICKSTART.md)。

## 常见错误与排查

| 现象 | 原因 | 处理 |
|------|------|------|
| `v.(pb.UserClient)` 断言 panic | creator 返回的类型与断言不符，或走的是 `ClientManager` 的空 creator 兜底（返回 `nil`） | 确认 `RegisterCreator` 的名字与 `ServiceConfig.Name` 一致；断言前先判 `v == nil` |
| `mgr.GetClient("x")` 返回 nil | 该 name 未 `InitializeClients` | 名字大小写与配置对齐；`GetClient` 不做存在性校验 |
| 配置里 `timeout: 3` 报 `cannot unmarshal !!int ... into time.Duration` | yaml.v3 对 `time.Duration` 只接受时长串 | 改写 `timeout: 3s`（或装配层用 string 影子 struct + `time.ParseDuration`） |
| 想连 TLS 端点却仍是明文 | `createClient` 恒定在最后追加 `insecure` 凭据，覆盖业务传入项 | 绕过 `GRPCClient`，自行 `grpc.NewClient` + `grpcx.TraceClientDialOptions` |
| 复用端口后 gRPC 全部失败 | `HTTPServer` 的 `cmux.Any()` 排在切片前面，吞掉了 HTTP2 流量 | 调整顺序：`WithGrpcServer` 在前、`WithHTTPServer` 在后 |
| 兜底看门狗把正常进程杀了 | `TimeOut()` 从被调用起无条件计时 60s | 只在 `IsShutdown()` 为 true 后再调用（见第 5 步） |
| HTTP 接口 404 且启动无报错 | `NewHTTPServer` 的 `httpSetup` 里没注册路由（传 `nil` 会在构造期 panic，进程直接起不来） | 在 `httpSetup(engine)` 内完成注册；`httpSetup` 不可为 nil |
| `defer f.Close()` 没生效 | 停机收尾是 `os.Exit(0)`，跳过所有 `defer` | 把收尾动作改为 `InstallShutdownHook` |
| 发 `SIGUSR1` 进程无反应 | `signal.Notify(ch)` 未过滤信号，全部被 grpcx 收下并丢弃 | 需自定义信号语义时不要用 `WaitShutdown`，或改用其他信号 |
| 端口被占用时进程直接退出 | `StartWithListener` 子服务失败走 `log.Fatalf`；`adjustAddr` 失败同样 `logrus.Fatalf` | 需要优雅降级就自行接管 listener 与错误处理 |
| `HealthInfo()` 的 `target` 为空 | `target` 取的是 `opts.prefix`，只有 `WithDirectAddresses` 会写它 | 需要展示地址就同时传 `WithDirectAddresses(addr)`，拨号仍靠 `WidthHost` |
| 超时不生效 / 报 `grpc.WithTimeout` 弃用 | `WithTimeout` 是已弃用的 DialOption，只影响建连阶段 | 每次调用用 `context.WithTimeout` 控制 |
