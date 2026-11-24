# trace — 链路追踪与指标计量组件

基于 **OpenTelemetry** 的统一封装：`Init` 一次装配全局 TracerProvider / MeterProvider / 传播器，`Inject*` / `Extract*` 覆盖 gRPC metadata、HTTP Header、MQ 消息头三类载体，并把拦截器与中间件内置在本组件，使业务与其他组件**无需 import otel 类型**即可贯通全链路。对应架构路线图 3.1.1 / 3.1.2。

- 模块路径：`github.com/xm-utils/tools/trace`
- 技术选型：`go.opentelemetry.io/otel` v1.34.0 全家桶（sdk / sdk/metric / otlptracegrpc / otlpmetricgrpc / stdouttrace / stdoutmetric）+ `google.golang.org/grpc` v1.71.1（仅为 metadata 与拦截器类型）
- **不引入 `otelgrpc` / `otelhttp`**：gRPC 拦截器与 HTTP RoundTripper 全部自写适配（含 otel 官方缺失的 `mdCarrier`），避免新增两条 contrib 依赖链
- 零行为契约：**未调用 `Init` 时全局为 noop Provider 与空传播器**，所有拦截器、注入提取、取 ID 的函数均安全退化，接入代码可无条件常驻，业务无需判空
- 仓内依赖方向：**trace 不依赖任何仓内模块**，被 `grpcx`、`httpclient`、`logging`、`metrics`、`mq/kafka`、`mq/redis` 依赖
- 发布状态：**尚未打 tag**，上述消费方目前以临时 `replace ../trace` 引用；发 `trace/v0.1.0` 后应转 pseudo-version 并删除 replace

## 目录结构

```
trace/
├── trace.go              # 包文档、Tracer / Meter / Start / MetricName / TraceID·SpanID 提取
├── init.go               # Config + Init / Shutdown / Flush / TracerProvider / MeterProvider
├── propagation.go        # 三类载体的 Inject / Extract（grpc metadata、HTTP Header、map）+ TraceParentFromCtx
├── grpc.go               # 一元 / 流 × 服务端 / 客户端 四组拦截器
├── http.go               # NewTracedTransport（客户端）+ Middleware（服务端）
├── mq.go                 # StartProducerSpan / StartConsumerSpan 高阶封装
├── trace_test.go         # 生命周期、配置校验、三类载体往返、MetricName（7 用例）
└── instrument_test.go    # 拦截器与中间件的注入提取、错误落状态（6 用例）
```

## 快速上手（30 秒）

```go
package main

import (
    "context"
    "github.com/xm-utils/tools/trace"
)

func main() {
    ctx := context.Background()

    // 1. 进程启动时装配一次（Export=none 时无外部依赖，可安全用于任何环境）
    if err := trace.Init(ctx, trace.Config{
        ServiceName: "order-svc",
        Environment: "prod",
        Exporter:    "otlp",            // none / stdout / otlp
        Endpoint:    "localhost:4317",
        Insecure:    true,
    }); err != nil {
        panic(err) // Init 内部 panic 已转 error，此处仅为示意
    }
    defer trace.Shutdown(context.Background()) // 2. 退出前刷新缓冲，避免丢 Span

    // 3. 业务埋点
    ctx, span := trace.Start(ctx, "order-svc", "createOrder")
    defer span.End()

    // 4. 取 TraceID 关联日志 / 响应体
    _ = trace.TraceIDFromCtx(ctx)
}
```

## API 参考

### 生命周期

| 函数 | 说明 |
|------|------|
| `Init(ctx, cfg Config) error` | 装配全局 TracerProvider / MeterProvider / TextMapPropagator |
| `Shutdown(ctx) error` | 刷新并关停当前 Provider，释放导出器后台协程；未 Init、重复调用均安全 |
| `Flush(ctx) error` | 立即导出已缓冲的 Span 与指标，**不关停** Provider；短生命周期任务收尾、测试断言时用 |
| `TracerProvider() trace.TracerProvider` | 返回当前装配的 Provider；未 Init 返回 `nil` |
| `MeterProvider() metric.MeterProvider` | 同上，指标侧 |

`Init` 的四条语义（实现在 `init.go`，勿凭直觉推断）：

1. **幂等可重入**：重复调用先构造新 Provider，全局注册替换完成后才关停**上一轮** Provider（不会留下无 Provider 的窗口，也不会误关停本轮刚建好的导出器）；适合配置热更新。
2. **panic 转 error**：内部 `recover` 后包装为 `trace: init panic 转 error: ...` 返回，不击穿调用方。
3. **失败不改状态**：任一步构造失败即返回 error，既有全局注册与 Provider 保持不动（构造中途失败的 `tp` / `mp` 会被就地关停回收）。
4. **批量导出**：`stdout` / `otlp` 均走 `WithBatcher`（指标走 `PeriodicReader`），因此**进程或任务结束前必须 `Flush` 或 `Shutdown`**，否则缓冲中的 Span 看不到。

### Config 字段

| 字段 | 类型 | 默认 | 约束 |
|------|------|------|------|
| `ServiceName` | `string` | 空 | `Exporter` 为 `stdout`/`otlp` 时**必填**；写入 resource 属性 `service.name` |
| `Environment` | `string` | 空 | 可选；写入 `deployment.environment`（如 `prod` / `staging`） |
| `Exporter` | `string` | `none` | 取 `none` / `stdout` / `otlp`（大小写与首尾空格自动归一）；非法值报错 |
| `Endpoint` | `string` | 空 | `Exporter=otlp` 时**必填**，形如 `localhost:4317`、`otel-collector:4318` |
| `Insecure` | `bool` | `false` | 为 `true` 时用明文 gRPC 连采集器；同集群内网部署通常为 `true` |
| `SampleRatio` | `float64` | `1.0` | 生效区间 `(0,1]`；`<=0` 归一为 `1.0`（全采样），**`>1` 直接报错** |
| `Propagators` | `[]string` | `["tracecontext","baggage"]` | 可选值 `tracecontext`、`baggage`；出现其他值报错（不静默忽略） |

`ServiceName` 与 `Environment` 均为空时沿用 otel 默认 resource；否则构造带 `WithHost` / `WithProcess` / `WithTelemetrySDK` 的 resource 并与默认值合并（导出的 Span 会含主机名、进程 PID 等属性）。

### 取用 Tracer / Meter

```go
tracer := trace.Tracer("order-svc")            // name 建议传组件名/服务名
meter := trace.Meter("order-svc")              // 未 Init 时返回 noop，调用安全
ctx, span := trace.Start(ctx, "order-svc", "db.query") // 便捷开 Span，调用方负责 span.End()

counter, _ := meter.Int64Counter(trace.MetricName("order", "request_total"),
    metric.WithDescription("请求总数"))
counter.Add(ctx, 1)
```

`MetricName(component, name)` 按仓库规范拼 `xmt_<component>_<name>`（如 `xmt_breaker_open_total`）。指标的统一注册表、命名校验与 `/metrics` 暴露由 `metrics` 组件负责，本函数只是轻量拼接工具（`metrics/naming.go` 另有带校验的实现）。

### 从上下文取标识

| 函数 | 返回 | 无有效上下文时 |
|------|------|----------------|
| `TraceIDFromCtx(ctx)` | 32 位十六进制 TraceID | 空串（`ctx == nil` 亦安全） |
| `SpanIDFromCtx(ctx)` | 16 位十六进制 SpanID | 空串 |
| `TraceParentFromCtx(ctx)` | W3C 单值串 `00-<traceId>-<spanId>-01` | 空串 |
| `HasSpanContext(ctx)` | 是否携带有效链路上下文 | `false` |

`logging` 组件在格式化阶段调 `TraceIDFromCtx` 自动附加 `trace_id` 字段；`common` 的统一响应体同样以 TraceID 作为排障锚点。

### 三类跨进程载体

| 载体 | 注入 | 提取 | 注意点 |
|------|------|------|--------|
| gRPC metadata | `InjectGRPC(ctx) context.Context` | `ExtractGRPC(ctx) context.Context` | 自写 `mdCarrier` 适配 `metadata.MD`（otel v1.34 无 `MDCarrier`）；注入前 `md.Copy()`，**既有 metadata 键保留不动** |
| HTTP Header | `InjectHTTP(ctx, *http.Request)` | `ExtractHTTP(ctx, *http.Request) context.Context` | 用官方 `propagation.HeaderCarrier`；`req == nil` 安全返回 |
| 消息头 / map | `InjectMap(ctx, map[string]string)` | `ExtractMap(ctx, map[string]string) context.Context` | 载体是 `mq.Message.Headers` 本身，**trace 不 import mq/core**；`carrier == nil` 时无法写回、静默无效，调用方需保证已初始化 |

`Inject*` / `Extract*` 都直接调用 `otel.GetTextMapPropagator()`，因此写入的键由 `Init` 装配的传播协议决定（默认 `traceparent` + `tracestate` + `baggage`）。

### gRPC 拦截器

```go
// 服务端
grpc.NewServer(
    grpc.ChainUnaryInterceptor(trace.UnaryServerInterceptor("order-svc")),
    grpc.ChainStreamInterceptor(trace.StreamServerInterceptor("order-svc")),
)

// 客户端
conn, _ := grpc.NewClient(addr,
    grpc.WithChainUnaryInterceptor(trace.UnaryClientInterceptor("order-svc")),
    grpc.WithChainStreamInterceptor(trace.StreamClientInterceptor("order-svc")),
)
```

| 拦截器 | Span 名 | Kind | 关键属性 | 收口时机 |
|--------|---------|------|----------|----------|
| `UnaryServerInterceptor` | `FullMethod`（`/pkg.Service/Method`） | SERVER | `rpc.system` / `rpc.service` / `rpc.method` / `net.peer.name` | handler 返回后；error 时 `RecordError` + Error 状态 + `rpc.grpc.status_code` |
| `StreamServerInterceptor` | `FullMethod` | SERVER | 同上 | 流 handler 返回时（正常 `io.EOF` 或出错） |
| `UnaryClientInterceptor` | `method` | CLIENT | 同上 + `rpc.target` | `defer span.End()`（调用返回即收口） |
| `StreamClientInterceptor` | `method` | CLIENT | `rpc.system` / `rpc.service` / `rpc.method` | 建流失败立即 End；成功则由包装流的 `RecvMsg` 在 `io.EOF`/error 时 End |

服务端两个拦截器额外做两件事：`ExtractGRPC` 先还原上游上下文（父级优先采样据此保持一致），服务端流再用 `serverStream` 覆写 `Context()`，使 handler 内 `s.Context()` 携带该 Span。

### HTTP 埋点

```go
// 服务端：还原上游链路 + 开 SERVER span
http.Handle("/order", trace.Middleware("order-svc", "POST /order", mux))

// 客户端：出站开 CLIENT span + 注入 traceparent
client := &http.Client{Transport: trace.NewTracedTransport("httpclient", http.DefaultTransport)}
```

- `Middleware(tracerName, operation, next)`：`operation` 为空回退为 `"METHOD /path"`；属性记 `http.request.method` / `url.path` / `http.response.status_code`，状态码 ≥500 置 Error。`statusRecorder` 内嵌 `http.ResponseWriter`，`Flush`/`Hijack` 等方法由接口提升透传，SSE 与 WebSocket 场景不受影响。
- `NewTracedTransport(tracerName, base)`：`base == nil` 时用 `http.DefaultTransport`；Span 名为 `"METHOD Host"`，属性记 `url.full` / `http.response.status_code`；传输 error 与 ≥500 均置 Error。注入时用带 Span 的 ctx 派生新请求，保证 `traceparent` 对应的是本 CLIENT span。

### MQ 侧高阶封装

```go
// 生产：headers 必须已初始化（nil 无法写回）
ctx, end := trace.StartProducerSpan(ctx, "order-svc", "kafka produce order", msg.Headers)
defer end()

// 消费：还原上游后开 CONSUMER span，handler 用返回的 ctx
cctx, end := trace.StartConsumerSpan(ctx, "order-svc", "kafka receive order", msg.Headers)
defer end()
_ = cctx
```

两函数返回 `(ctx, end func())`，**不暴露 otel Span 类型**，mq 适配器只需 import 本包。批量发送想只挂一个父 Span 时，按 `mq/kafka`、`mq/redis/list` 的既有手法：`StartProducerSpan(ctx, name, "...", nil)` 开整批父 Span，循环内对每条 `trace.InjectMap(batchCtx, m.Headers)`。

### 各组件已内置的接入面

| 组件 | 接入方式 | 说明 |
|------|----------|------|
| `grpcx` | `grpcx.TraceServerOptions(name)...` / `grpcx.WithClientTrace(name)` / `grpcx.TraceClientDialOptions(name)...` | 把本包四组拦截器包成 grpcx Option；与业务既有拦截器共存时建议置于链首，最先提取上游 `traceparent` |
| `httpclient` | `Config.EnableTracing = true`（可配 `Config.TracerName`，默认 `httpclient`）；服务端别名 `httpclient.TracedHandler(tracerName, operation, next)` | 装配时包装 `Transport`；`HttpRequest.Ctx` 字段用于承接上游上下文，使出站 Span 挂到调用链下（留空则链路从 background 起，父子关系断开）；`TracedHandler` 内部直接转调 `trace.Middleware` |
| `logging` | 无需配置 | Format 阶段自动 `TraceIDFromCtx(entry.Context)` 附加 `trace_id`（键名可由 `Config.TraceKey` 改，默认 `trace_id`） |
| `mq/kafka` | 内置 | Producer 在序列化前 `InjectMap`；Consumer 派发前开 CONSUMER span 包裹 `InvokeHandler` |
| `mq/redis` | 内置 | list / zset / stream / pubsub 四形态均在发送前注入、派发前开消费者 span；`traceparent` 经 envelope 的 Headers 天然承载 |
| `metrics` | 协作关系 | 指标统一注册表与命名规范在 metrics 组件；`trace.MetricName` 与之口径一致 |

MQ 适配器内置 Span 的 tracer 名是各包的包级常量（如 `mq/kafka`、`mq/redis/list`、`mq/redis/pubsub`），**不对外暴露配置入口**；需按服务名区分时请改用 `StartProducerSpan` / `StartConsumerSpan` 自行埋点。

### HTTP 服务端的两种选择

`trace.Middleware` 适合手写 `net/http` 服务；用 `httpclient` 的调用方只需 `EnableTracing`，不必再包一层。若你的服务已由框架或自建中间件链管理，也可只调 `ExtractHTTP` + `Start` 自行开 Span，两者不能叠加使用（会产生父子重复的 SERVER span）。

## 采样与传播语义

- 采样器为 `ParentBased(TraceIDRatioBased(SampleRatio))`：**上游已做决策的链路不重新采样**，保证同一条 Trace 跨服务后判定一致；只有链路起点按 `SampleRatio` 概率采样。
- 传播协议默认组合 W3C `traceparent` + `tracestate` + `baggage`；只留 `["tracecontext"]` 可关掉 baggage 透传。
- `Exporter=none` 时仍注册 SDK TracerProvider：**TraceID/SpanID 正常生成与传播**，仅不外送数据，适合本地开发与未接观测后端的部署（日志关联照常可用）。
- 远端动态采样（按 Collector 下发的策略）**尚未实现**，需要时应在 `Init` 之外自行装配 Provider。

## 已知边界与待办

| 项 | 现状 | 影响 |
|----|------|------|
| 客户端流只写不读 | `StreamClientInterceptor` 的 Span 靠 `RecvMsg` 收口 | 纯上行流若从不调 `RecvMsg`，该 Span 不会 End（缓冲中被丢弃）；此类场景请改用一元或自行埋点 |
| URL 属性未脱敏 | 客户端 Span 记 `url.full`（含 query） | query 中带 token / 手机号等敏感参数会进观测后端，需在上层自控 |
| 无 Span 过滤/白名单 | 所有请求一律开 Span（仅采样率可调） | 健康检查、metrics 抓取等噪音链路需业务侧自行跳过 |
| MQ span 动词不统一 | `mq/redis/pubsub` 用 `publish`，其余后端用 `produce` | 按 span 名做聚合看板时需兼顾两种写法 |
| 未发布 tag | 六个消费方仍是 `replace ../trace` | 发 `trace/v0.1.0` 后须逐个转 pseudo-version |
| 无 README 之外的示例程序 | 本目录未提供 `example.go` | 可运行示例见 QUICKSTART 的三步最小工程 |

## 测试

```bash
cd trace
go vet ./...
go test -race -count=1 ./...
```

13 个用例分两个文件：`trace_test.go` 覆盖未 Init 的 noop 安全性、`Config` 校验分支（非法 Exporter / 缺 ServiceName / 缺 Endpoint / `SampleRatio>1`）、stdout 导出与 Flush/Shutdown、三类载体的注入-提取往返、`MetricName`；`instrument_test.go` 用真实 gRPC 内存连接与 `httptest` 断言拦截器的 `traceparent` 注入/提取、SERVER Span 命名与错误落状态、`splitGRPCMethod` 解析。

## 许可

MIT
