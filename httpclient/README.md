# httpclient — 弹性 HTTP 客户端

全局单例的出站 HTTP 客户端：把**重试（指数退避）+ 熔断（滑动窗口）+ 请求日志回写 + 链路追踪**四件事串成一条 `DoRequest` 调用链，支持 JSON / 表单 / multipart 文件上传三种请求体。

- 模块路径：`github.com/xm-utils/tools/httpclient`
- 技术选型：标准库 `net/http` + `github.com/google/go-querystring`（form 序列化）+ `github.com/google/uuid`（请求 ID）+ `github.com/sirupsen/logrus`
- 仓内依赖：`breaker` v1.0.2（熔断）、`retry` v1.0.2（重试执行器）、`trace`（出站注入，当前 `replace ../trace` 待转 tag）
- 发布状态：最新 tag **`httpclient/v0.0.1`**（2026-11-17，commit `51b72b4`）；此后合入的 trace 接入（`EnableTracing` / `HttpRequest.Ctx` / `TracedHandler`）**尚未随 tag 发布**
- 测试现状：仅 `trace_test.go` 两个用例（出站注入、服务端还原），**重试、熔断、序列化路径无单元测试**

## 目录结构

```
httpclient/
├── http_client.go   # Init / GetClient / newHttpClient、DoRequest 主链路、prepareRequest / executeRequest / recordLog
├── models.go        # Config / HttpRequest / HttpResponse / HttpRequestLog / MultipartField / LogKeeper
├── trace.go         # TracedHandler（trace.Middleware 的别名，省一次 import）
└── trace_test.go    # 链路贯通用例
```

## 快速上手

```go
package main

import (
    "context"
    "time"

    "github.com/xm-utils/tools/httpclient"
)

type payResp struct {
    Code int    `json:"code"`
    Msg  string `json:"msg"`
}

func main() {
    // 单例装配：LogKeeper 可选，传 nil 即不回写请求日志
    httpclient.Init(&httpclient.Config{
        Timeout:              10 * time.Second,
        MaxRetries:           2,
        EnableCircuitBreaker: true,
        ErrorThreshold:       0.5,
        EnableTracing:        true,
    }, nil)

    req := httpclient.NewHttpRequest(1001, "pay")
    req.Method = "POST"
    req.URL = "https://example.com/pay"
    req.Ctx = context.Background() // 链路追踪/取消控制都依赖它
    req.Body = map[string]any{"order": "A1"}

    resp, err := httpclient.GetClient().DoRequest(req)
    if err != nil {
        return // resp 可能非 nil（可重试状态码耗尽时会带回最后一次响应）
    }

    var out payResp
    if err := httpclient.GetClient().ParseJSONResponse(resp, &out); err != nil {
        return
    }
}
```

## API 参考

### 装配

| 符号 | 说明 |
|------|------|
| `Init(cnf *Config, lk LogKeeper)` | `sync.Once` 包裹，**只有第一次生效**；`cnf == nil` 时取 `DefaultConfig()` |
| `GetClient() *HttpClient` | 返回单例；`Init` 之前调用返回 `nil`，直接调方法会 panic |
| `DefaultConfig() *Config` | `Timeout 30s`、`MaxRetries 3`、`EnableCircuitBreaker false`、`ErrorThreshold 0.5`、`SlowCallDuration 5s`、`WindowSize 60s`、`MinRequests 10` |

需要多实例并行（不同域名不同策略）时本包无法满足：单例是既有设计，`newHttpClient` 未对外暴露，只能自行组合 `breaker` + `retry` + `net/http`。

### Config 字段

| 字段 | yaml | 默认 | 生效说明 |
|------|------|------|----------|
| `Timeout` | `timeout` | 30s（DefaultConfig） | 赋给 `http.Client.Timeout`，是**单次尝试的真实上限**，也是整条重试链的每次调用上限 |
| `MaxRetries` | `max_retries` | 3 | 赋给 `retry.Config.MaxRetries`；**总尝试次数 = MaxRetries + 1** |
| `EnableCircuitBreaker` | `enable_circuit_breaker` | false | 关闭时 `DoRequest` 走纯重试路径（仍会创建一个极端阈值的熔断器实例，只是不参与判定） |
| `ErrorThreshold` | `error_threshold` | 0.5 | 仅当 `> 0` 才覆盖 breaker 默认值 |
| `SlowCallDuration` | `slow_call_duration` | 5s | 同上，`> 0` 才覆盖 |
| `WindowSize` | `window_size` | 60s | 同上 |
| `MinRequests` | `min_requests` | 10 | 同上 |
| `EnableTracing` | `enable_tracing` | false | 为 true 时把 `Transport` 换成 `trace.NewTracedTransport(tracerName, http.DefaultTransport)` |
| `TracerName` | `tracer_name` | 空 → `httpclient` | 出站 CLIENT span 所属 tracer 名 |

breaker 侧另有三个**固定不可配**的值（写在 `newHttpClient` 内）：`SlowCallThreshold = 0.8`、`WaitDurationInOpenState = 30s`、`HalfOpenMaxRequests = 5`；熔断器名固定 `"http-client"`，状态变更回调只打 logrus Warn。重试退避同样固定：`ExponentialBackoffStrategy{InitialDelay: 1s, MaxDelay: 10s, Multiplier: 2.0}`，`retry.Config.Timeout` 沿用 `retry.DefaultRetryConfig()` 的 10s。

配置源侧提醒：`Config` 自带 yaml tag，可整段直接反序列化；但 `Timeout` / `SlowCallDuration` / `WindowSize` 是 `time.Duration`，gopkg.in/yaml.v3 只接受 `10s` 这类时长串（写纳秒整数会报 `cannot unmarshal !!int ... into time.Duration`，连 `0` 都不能如直觉写）；走 viper 需加 `mapstructure.StringToTimeDurationHookFunc`。另注意：**整段配置存在而某个键缺失时，该字段是 Go 零值而非 `DefaultConfig` 默认值**（`Timeout: 0` = 不超时），只有 `Config` 指针本身为 `nil` 才会退化到默认。详见 [config.example.yaml](config.example.yaml)。

### 请求：HttpRequest

| 字段 | 说明 |
|------|------|
| `Method` | 请求方法；留空时由 `net/http` 按 `GET` 处理 |
| `URL` | 完整地址 |
| `Headers` | 请求头；`Content-Type` 决定序列化方式（见下） |
| `Body` | `interface{}`：JSON 走 `json.Marshal`；`application/x-www-form-urlencoded` 走 `query.Values`（依赖结构体的 `url` tag）后 `Encode()` |
| `Files` | `[]MultipartField`，非空时**覆盖** `Body` 的序列化结果并改写 `Content-Type` 为 multipart |
| `ServiceType` / `AppId` | 仅用于请求日志记录 |
| `RequestId` | 业务可传入以串联排查；留空由客户端 `uuid.NewString()` 兜底 |
| `Ctx` | 调用方上下文，可为 nil（回退 `context.Background()`）；链路追踪与超时取消都挂在它上面 |

`NewHttpRequest(appId, serviceType)` 预置 `AppId` / `ServiceType` / `RequestId`（自动生成 UUID），其余字段自行填。

`MultipartField`：`FileName` 为空 → 写普通文本字段（`FieldName` / `Value`）；非空 → `CreateFormFile(FieldName, FileName)` 并把 `FileData`（`io.Reader`）拷入。`ContentType` 字段当前**未被使用**（文件部分的 MIME 由 `CreateFormFile` 决定）。

### 响应与解析

```go
type HttpResponse struct {
    StatusCode int
    Body       []byte
    Headers    http.Header
}
```

`ParseJSONResponse(resp, result)`：`StatusCode != 200` 直接返回 error（消息含响应体），**2xx 中的 201/204 也按失败处理**；`result == nil` 时跳过解析返回 nil；`resp.Body` 已被完整读入内存（`io.ReadAll`），不支持流式处理。

### 重试与熔断语义

`DoRequest` 的执行序（`http_client.go` 实现）：

1. `RequestId` 兜底生成。
2. `prepareRequest`：序列化 + `http.NewRequestWithContext(req.ctxOrBackground(), ...)` + 逐个 `Header.Set`。**这一步的错误不重试**，直接返回 `(nil, err)` 并记日志。
3. 重试任务 `retryTask` 只包住 `executeRequest`（`client.Do` + `io.ReadAll` + 状态码判定）。
4. 启用熔断时用 `circuitBreaker.ExecuteWithContext(context.Background(), ...)` **把整条重试链作为一次熔断样本**（即：一次业务调用 = 一次成功/失败计数，而非每次 HTTP 尝试计一次）。

判定规则：

| 情况 | 行为 |
|------|------|
| 网络 IO 错误 / 读响应错误 | 触发重试，退避 1s → 2s → 4s（上限 10s） |
| 状态码 5xx、408、429 | 视为瞬时故障：内部构造 `retryableResponseError`（携带该次响应）触发重试；**耗尽后 `(resp, err)` 同时非 nil**，resp 是最后一次响应 |
| 其他 4xx | `executeRequest` 返回 `(resp, nil)`：不重试、`DoRequest` 的 `err == nil`，业务必须自查 `StatusCode` |
| 重试全部失败 | 错误消息形如 `重试4次后仍然失败: http request failed: ...` |
| 熔断打开 | 返回 `fmt.Errorf("circuit breaker is open: %w", breaker.ErrCircuitOpen)`，`errors.Is(err, breaker.ErrCircuitOpen)` 成立；本次响应为 `nil` |

两个容易踩的语义：

- **单次尝试的超时来自 `Config.Timeout`（`http.Client.Timeout`）**。重试执行器虽然给每次尝试包了 10s 的 `taskCtx`，但 `retryTask` 忽略传入的 ctx、仍用 `prepareRequest` 里由 `HttpRequest.Ctx` 派生的请求，因此那 10s 不起作用。想让上游取消生效，就传带 deadline 的 `Ctx`。
- **重试复用同一个 `*http.Request`**：body 缓冲在首次尝试后已耗尽，带 `Body` 的 POST 从第二次尝试起可能发出空请求体。对非幂等接口建议 `MaxRetries = 0` 或在业务层自己控制重试。

### 熔断运维接口

| 方法 | 未启用熔断时 |
|------|--------------|
| `GetCircuitBreakerState() breaker.State` | 恒返回 `breaker.StateClosed` |
| `GetMetrics() *breaker.CircuitBreakerMetrics` | 返回 `nil`（判空再用） |
| `ResetCircuitBreaker()` | 只打一条 Warn 日志，不做任何操作 |

启用时分别透传 breaker 的状态、指标快照与 `Reset()`。

### 请求日志回写（LogKeeper）

```go
type LogKeeper interface {
    KeepHttpLog(log *HttpRequestLog)
}
```

- 传 `nil` 即完全跳过（`recordLog` 首行判空）。
- **每次请求起一个 goroutine 异步回写**，不阻塞主流程，也不保证顺序与落库完成。
- `HttpRequestLog` 字段：`RequestId` / `AppId` / `ServiceType` / `Method` / `URL` / `ReqHeaders`（JSON 串）/ `ReqBody`（string、[]byte 原样，其他类型 `json.Marshal`）/ `RespStatus` / `RespBody` / `Duration`（毫秒）/ `Attempts`（`int8`，重试次数上限勿超 127）/ `Status`（0 成功 1 失败）/ `ErrorMsg`（超 1000 字符截断）。
- 注意：失败路径调用 `recordLog` 时 `respStatus` 传的是字面量 `0`，**即使拿到了 5xx 响应，失败记录的 `RespStatus` 仍是 0**（真实状态码在 `RespBody` / `ErrorMsg` 里可见）。

### 链路追踪

```go
httpclient.Init(&httpclient.Config{EnableTracing: true, TracerName: "order-svc"}, nil)
// 出站自动开 CLIENT span 并注入 traceparent —— 前提是 HttpRequest.Ctx 带上了上游上下文
```

服务端侧可直接用本包别名（内部转调 `trace.Middleware`，省一次 import）：

```go
mux.Handle("/callback", httpclient.TracedHandler("pay-svc", "POST /callback", handler))
```

`trace.Init` 未调用时 transport 为透传零行为，`EnableTracing` 可无条件打开。Span 属性与命名细节见 [trace/README.md](../trace/README.md)。

## 与其他组件的关系

| 组件 | 关系 |
|------|------|
| `breaker` | 熔断状态机与滑动窗口指标的实现方；本包只做装配与运维透传 |
| `retry` | 重试执行器与退避策略的实现方；本包固定用指数退避 |
| `trace` | 出站 `NewTracedTransport`；服务端 `TracedHandler` 别名 |
| 使用方 | `LogKeeper` 由业务实现（落库 / 入队），本包不关心存储 |

## 已知边界与待办

| 项 | 现状 | 影响 / 处理 |
|----|------|-------------|
| `Files` 非空但 `Headers == nil` | `prepareRequest` 直接 `req.Headers["Content-Type"] = ct` | **向 nil map 赋值会 panic**；上传前务必初始化 `Headers: map[string]string{}`（可修复为惰性初始化，待确认） |
| 单例不可重建 | `Init` 用 `sync.Once` | 配置变更需重启进程；`GetClient()` 在 `Init` 前为 `nil` |
| Transport 不可自定义 | 无 `Transport` 配置项，连接池沿用 `http.DefaultTransport` | 需要自定义 DialContext / TLS / 代理时无法经本包注入 |
| 4xx 不重试且无 error | `executeRequest` 对非可重试状态码返回 `(resp, nil)` | 业务必须自查 `StatusCode`；`ParseJSONResponse` 会在非 200 时报错 |
| 非 200 的 2xx 判失败 | `ParseJSONResponse` 只认 `StatusCode == 200` | 201/204 场景改用手写 `json.Unmarshal(resp.Body, ...)` |
| 重试复用请求体 | 同一 `*http.Request` 多次 `Do` | 带 body 的重试可能发空体，非幂等接口慎用 |
| 失败记录状态码为 0 | 失败路径 `recordLog(..., 0, ...)` | 排查时看 `ErrorMsg` / `RespBody`；可修复为透传真实码 |
| `retry` 的 10s 单次超时不生效 | `retryTask` 忽略传入 ctx | 单次上限实际是 `Config.Timeout` |
| 熔断样本粒度 | 整条重试链计一次 | 熔断阈值反映的是「业务调用成功率」而非「HTTP 请求成功率」 |
| 响应全量入内存 | `io.ReadAll` | 大文件下载不适合走本客户端 |
| 测试覆盖薄 | 仅 2 个 trace 用例 | 重试/熔断/序列化三块建议补 httptest 用例 |
| trace 依赖未发布 | `replace ../trace` | trace 打 tag 后转 pseudo-version 并删除 replace |

## 测试

```bash
cd httpclient
GOWORK=off go vet ./...
GOWORK=off go test -race -count=1 ./...
```

现有 `trace_test.go` 两个用例：`TestTracedTransportInjectsTraceparent`（`EnableTracing` 后出站 Header 确实带 `traceparent`）、`TestTracedHandlerRoundTrip`（服务端中间件还原上游链路，handler 内 TraceID 与入站一致）。其余行为（重试次数、退避、熔断跳转、form/multipart 序列化）目前依赖调用方集成验证。

## 许可

MIT
