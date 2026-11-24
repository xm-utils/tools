# trace 快速上手

目标：5 分钟把链路追踪跑起来并看到 Span，然后按需要在 gRPC / HTTP / MQ 三个入口贯通。

前置：Go 1.24.2+；`trace` 尚未打 tag，仓外项目引用需临时 `replace github.com/xm-utils/tools/trace => ../trace`（仓内 go.work 联调无需处理）。

## 第 1 步：最小可跑（stdout 导出，零外部依赖）

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/xm-utils/tools/trace"
)

func main() {
    ctx := context.Background()

    if err := trace.Init(ctx, trace.Config{
        ServiceName: "demo",
        Exporter:    "stdout", // JSON 打印到标准输出，便于确认链路确实生效
    }); err != nil {
        fmt.Println("init failed:", err)
        return
    }
    // 批量导出器有缓冲，进程退出前必须 Shutdown（短任务也可只 Flush）
    defer func() {
        sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
        defer cancel()
        _ = trace.Shutdown(sctx)
    }()

    ctx, span := trace.Start(ctx, "demo", "handleOrder")
    fmt.Println("traceId =", trace.TraceIDFromCtx(ctx))
    doWork(ctx)
    span.End()
}

func doWork(ctx context.Context) {
    _, child := trace.Start(ctx, "demo", "db.query")
    defer child.End()
}
```

`go run .` 后 stdout 会出现两段 JSON（`handleOrder` 与 `db.query`），二者 `traceID` 相同、`parentSpanID` 串联 — 说明父子关系正确。

## 第 2 步：只想在日志里带 traceId（不接观测后端）

`Exporter` 留空即为 `none`：SDK Provider 仍注册，**TraceID/SpanID 照常生成与传播**，只是不外送数据。配合 `logging` 组件即可零成本获得全链路 grep 锚点：

```go
// 链路侧：只注册 Provider，不外送数据
_ = trace.Init(ctx, trace.Config{ServiceName: "demo"}) // Exporter 留空即 none

// 日志侧：无需为 trace 做任何额外配置
_ = logging.Init(logging.Config{Service: "demo"})      // TraceKey 留空即 trace_id
logging.Context(ctx).Info("order created")              // 自动附加 trace_id 字段
```

## 第 3 步：gRPC 入口贯通（经 grpcx 一行装配）

```go
// 服务端：拦截器置于链首，最先提取上游 traceparent
srv := grpcx.NewGrpcServer("order-svc", ":9000", register,
    grpcx.TraceServerOptions("order-svc")...)

// 客户端：自动开 CLIENT span 并注入出站 metadata
cli := grpcx.NewGRPCClient(creator, grpcx.WidthHost("127.0.0.1:9000"),
    grpcx.WithClientTrace("order-svc"))

// 不用 GRPCClient、自行拨号时
conn, _ := grpc.NewClient(addr, grpcx.TraceClientDialOptions("order-svc")...)
```

handler 内直接 `trace.TraceIDFromCtx(ctx)` 取 ID；服务端流 handler 用 `stream.Context()` 也能拿到（拦截器已包装）。

## 第 4 步：HTTP 入口贯通

```go
// 入站：包一层中间件，还原上游并开 SERVER span
mux := http.NewServeMux()
mux.HandleFunc("/order", createOrder)
handler := trace.Middleware("order-svc", "", mux) // operation 留空则用 "METHOD /path"
http.ListenAndServe(":8080", handler)

// 出站：httpclient 为全局单例，Init 时打开追踪开关（LogKeeper 可选，传 nil 即可）
httpclient.Init(&httpclient.Config{
    EnableTracing: true,
    TracerName:    "order-svc", // 留空默认 "httpclient"
}, nil)

req := httpclient.NewHttpRequest(1001, "pay-svc")
req.Method, req.URL, req.Body = "POST", "http://127.0.0.1:9001/pay", body
req.Ctx = ctx // 关键：不带 Ctx 则出站 span 挂不到当前链路上
resp, err := httpclient.GetClient().DoRequest(req)
```

服务端也可用 `httpclient` 提供的等价别名（内部就是转调 `trace.Middleware`，避免多 import 一个包）：

```go
mux.Handle("/order", httpclient.TracedHandler("order-svc", "POST /order", orderHandler))
```

不用 `httpclient` 时，直接包装标准客户端：`http.DefaultClient.Transport = trace.NewTracedTransport("order-svc", nil)`。

## 第 5 步：MQ 链路（适配器已内置，理解形态即可）

`mq/kafka` 与 `mq/redis`（list / zset / stream / pubsub）已在收发处内置 PRODUCER / CONSUMER span，业务侧**无需额外代码**：

```go
// 生产端：Headers 需已初始化，traceparent 才有地方写（Message.ID 由适配器签发，业务不填）
err := producer.Produce(ctx, "order", &mq.Message{
    Key:     "order-1",
    Body:    []byte(`{"id":1}`),
    Headers: map[string]string{}, // nil 时注入静默无效
})

// 消费端：mq.Handler 是 func(ctx, msg) error，收到的 ctx 已挂 CONSUMER span
var handler mq.Handler = func(cctx context.Context, msg *mq.Message) error {
    // 此处 TraceID 与生产端一致
    _ = trace.TraceIDFromCtx(cctx)
    return nil
}
```

自己在其他队列上埋点时用两个高阶函数（不暴露 otel 类型）：

```go
pctx, endP := trace.StartProducerSpan(ctx, "order-svc", "mq produce order", headers)
defer endP()

cctx, endC := trace.StartConsumerSpan(ctx, "order-svc", "mq receive order", headers)
defer endC()
```

## 第 6 步：切到 OTLP Collector（Jaeger / Tempo / otel-collector）

```bash
# 本地起一套 Jaeger（OTLP 4317 + UI 16686）验证
docker run --rm -p 4317:4317 -p 16686:16686 jaegertracing/all-in-one:latest
```

```go
_ = trace.Init(ctx, trace.Config{
    ServiceName: "order-svc",
    Environment: "prod",
    Exporter:    "otlp",
    Endpoint:    "localhost:4317",
    Insecure:    true,   // 同集群内网明文；跨网走 TLS 时置 false
    SampleRatio: 0.2,    // 起点按 20% 采样，下游服务沿用上游决策
})
```

打开 <http://localhost:16686> 即可看到跨服务串起来的调用链。

## 常见错误与排查

| 现象 | 原因 | 处理 |
|------|------|------|
| 没有任何 Span 输出 | 未调 `Init`，或 `Exporter` 留空（=`none` 本身不外送） | 确认 `Init` 成功返回 nil；调试期改 `stdout` |
| 日志/响应里 traceId 恒为空 | `Init` 未执行，或 `ctx` 在链路上被 `context.Background()` 替换 | 保持 ctx 一路透传；`HasSpanContext(ctx)` 可自检 |
| 客户端与服务端链路断开（两个独立 Trace） | 出站未带 ctx：`HttpRequest.Ctx` 留空，或 gRPC 调用绕过了拦截器 | 出站传 ctx；确认拦截器在链首且服务端也挂了拦截器 |
| Span 时有时无 | 批量导出器缓冲未刷新就退出 | 退出前 `Shutdown(ctx)`；长任务阶段性 `Flush(ctx)` |
| `Init` 报 `Exporter=otlp 要求 Config.Endpoint 非空` | OTLP 缺 `Endpoint` | 补 `Endpoint`；`stdout` 只需 `ServiceName` |
| `Init` 报 `SampleRatio=1.5 超出 [0,1]` | 传了百分比而非小数 | 用 `1.0` / `0.2`；想全采样传 `0`（会归一为 1.0） |
| MQ 消息头里没有 traceparent | 传入的 `headers` 为 `nil`，无法写回 | 生产端初始化 `Headers: map[string]string{}` |
| 只有 CLIENT span、无 SERVER span | 对端未装拦截器/中间件 | 两端都要接；服务端可用 `grpcx.TraceServerOptions` 或 `trace.Middleware` |
| 纯上行 gRPC 流缺 Span | 客户端流 Span 靠 `RecvMsg` 收口，从未调用则不 End | 该场景改手动埋点或确保读流 |
