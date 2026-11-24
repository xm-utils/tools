# httpclient 快速上手

目标：5 分钟发出第一个带重试的请求，然后按需叠加熔断、请求日志与链路追踪。

前置：Go 1.24.2+。本包是**进程级单例**，`Init` 只在第一次调用生效，装配代码要放在 `main` / 初始化阶段，不能放在按请求执行的路径里。

## 第 1 步：装配单例

```go
package main

import (
    "time"

    "github.com/xm-utils/tools/httpclient"
)

func init() {
    // LogKeeper 传 nil 即不回写请求日志；Config 传 nil 等价于 DefaultConfig()
    httpclient.Init(&httpclient.Config{
        Timeout:              10 * time.Second, // 单次尝试的真实上限
        MaxRetries:           2,                // 总尝试次数 = 2 + 1 = 3
        EnableCircuitBreaker: false,
    }, nil)

    // Init 之后才能取实例；Init 之前 GetClient() 返回 nil
    _ = httpclient.GetClient()
}
```

只要 `Init` 没被调用过，`GetClient().DoRequest(...)` 就是 nil 指针 panic —— 单例语义无法绕过，需要多实例请自行组合 `net/http` + `breaker` + `retry`。

## 第 2 步：发一个 JSON POST

```go
type payResp struct {
    Code int    `json:"code"`
    Msg  string `json:"msg"`
}

req := httpclient.NewHttpRequest(1001, "pay") // 预置 AppId / ServiceType / RequestId(uuid)
req.Method = "POST"
req.URL = "https://example.com/pay"
req.Headers = map[string]string{"Content-Type": "application/json"}
req.Body = map[string]any{"order": "A1"}
req.Ctx = ctx // 可选，但链路追踪与上游取消都挂在它上面

resp, err := httpclient.GetClient().DoRequest(req)
if err != nil {
    // resp 不一定为 nil：5xx/408/429 重试耗尽时会带上最后一次响应
}

var out payResp
if err := httpclient.GetClient().ParseJSONResponse(resp, &out); err != nil {
    // 非 200 一律报错（含 201/204），需要放宽时自行 json.Unmarshal(resp.Body, &out)
}
```

`Content-Type` 决定序列化方式：留空或 `application/json` 走 `json.Marshal`；`application/x-www-form-urlencoded` 走 `query.Values(req.Body)`，此时 `Body` 需是带 `url` tag 的结构体或 map，否则序列化结果为空表单。

## 第 3 步：表单与文件上传

```go
// form：只改 Content-Type，序列化自动切到 urlencoded
req.Headers = map[string]string{"Content-Type": "application/x-www-form-urlencoded"}
req.Body = struct {
    Mobile string `url:"mobile"`
    Code   string `url:"code"`
}{Mobile: "13800000000", Code: "8888"}

// multipart：Files 非空会覆盖 Body 的序列化结果，并自动改写 Content-Type
upload := httpclient.NewHttpRequest(1001, "ocr")
upload.Method = "POST"
upload.URL = "https://example.com/ocr"
upload.Headers = map[string]string{} // 必须非 nil：内部要写 Headers["Content-Type"]
f, _ := os.Open("/path/to/a.png")
defer f.Close()
upload.Files = []httpclient.MultipartField{
    {FieldName: "type", Value: "idcard"},            // FileName 为空 → 普通文本字段
    {FieldName: "file", FileName: "a.png", FileData: f}, // FileName 非空 → CreateFormFile
}
```

`MultipartField.ContentType` 当前未被使用，文件部分的 MIME 由 `CreateFormFile` 决定。

## 第 4 步：调重试与超时

```go
httpclient.Init(&httpclient.Config{
    Timeout:    3 * time.Second,
    MaxRetries: 0, // 非幂等接口：不重试
}, nil)
```

固定行为（无需配置也无法配置）：

- 退避为指数：1s → 2s → 4s …… 单次上限 10s。
- 触发重试的只有**网络 IO 错误、读响应错误、状态码 5xx/408/429**；序列化失败、`URL` 非法、其他 4xx 都不重试。
- 重试耗尽的错误形如 `重试4次后仍然失败: ...`（数字 = `MaxRetries + 1`，用 `errors.As` 拿不到状态码，直接判断返回的 `resp.StatusCode` 更稳）。
- 带 `Body` 的请求重试时复用同一个 `*http.Request`，**第二次尝试起请求体可能为空**，非幂等接口务必 `MaxRetries = 0`。

想让上游取消/超时生效，传带 deadline 的 `HttpRequest.Ctx`；`retry` 内部的 10s 单次超时对本客户端不起作用，真正的单次上限是 `Config.Timeout`。

## 第 5 步：开启熔断

```go
httpclient.Init(&httpclient.Config{
    EnableCircuitBreaker: true,
    ErrorThreshold:       0.5,               // 错误率 ≥50% 触发
    SlowCallDuration:     2 * time.Second,   // 慢调用判定
    WindowSize:           60 * time.Second,  // 滑动窗口
    MinRequests:          20,                // 窗口内至少 20 次才评估
}, nil)

resp, err := httpclient.GetClient().DoRequest(req)
if errors.Is(err, breaker.ErrCircuitOpen) {
    // 熔断打开：本次直接拒绝，resp 为 nil
}

// 运维三接口
_ = httpclient.GetClient().GetCircuitBreakerState() // 未启用熔断时恒为 Closed
_ = httpclient.GetClient().GetMetrics()             // 未启用时返回 nil，判空再用
httpclient.GetClient().ResetCircuitBreaker()        // 手动恢复
```

两点语义：熔断器名固定 `"http-client"`；**一次 `DoRequest`（含全部重试）只算一个熔断样本**，所以阈值反映的是业务调用成功率而非 HTTP 请求成功率。`SlowCallThreshold=0.8`、`WaitDurationInOpenState=30s`、`HalfOpenMaxRequests=5` 为写死值，状态变更只输出 logrus Warn 日志。

## 第 6 步：接链路追踪 + 请求日志

```go
// 1) 先装配 trace 全局 Provider（不调用则出站为透传零行为，EnableTracing 可无条件打开）
_ = trace.Init(ctx, trace.Config{ServiceName: "order-svc"})
defer trace.Shutdown(context.Background())

// 2) 出站：开启追踪，span 挂在 HttpRequest.Ctx 上
httpclient.Init(&httpclient.Config{EnableTracing: true, TracerName: "order-svc"}, myKeeper)
req.Ctx = ctx // 关键：不带 Ctx 则出站 span 挂不到当前链路上

// 3) 本服务作为被调方：直接用本包别名（内部转调 trace.Middleware）
mux.Handle("/callback", httpclient.TracedHandler("order-svc", "POST /callback", cbHandler))

// 4) 请求日志：实现 LogKeeper，每次请求起 goroutine 异步回写
type myKeeper struct{}

func (myKeeper) KeepHttpLog(l *httpclient.HttpRequestLog) {
    log.Printf("http=%s %s status=%d attempts=%d cost=%dms err=%s",
        l.Method, l.URL, l.RespStatus, l.Attempts, l.Duration, l.ErrorMsg)
}
```

排查提示：失败记录的 `RespStatus` 恒为 0（真实码在 `RespBody` / `ErrorMsg` 里）；`ErrorMsg` 超 1000 字符会被截断；`Attempts` 是 `int8`，重试次数别超过 127。

本地验证链路是否打通：

```bash
docker run --rm -p 4317:4317 -p 16686:16686 jaegertracing/all-in-one:latest
# Exporter=otlp + Endpoint=localhost:4317 后发起请求，Jaeger UI 应看到
# CLIENT span（出站，名为 "METHOD Host"，如 "POST example.com"）与被调方的 SERVER span 串成一条链
```

## 常见问题排查

| 现象 | 原因 | 处理 |
|------|------|------|
| `GetClient()` 返回 nil / nil pointer panic | 未调用 `Init` | 在 `main` / `init()` 里先 `Init` |
| 改了配置但不生效 | `Init` 被 `sync.Once` 包住 | 重启进程；不要指望二次 `Init` |
| 上传文件时 panic：assignment to entry in nil map | `Files` 非空但 `Headers == nil` | 初始化 `Headers: map[string]string{}` |
| form 请求对端收到空参数 | `Body` 类型不带 `url` tag | 改用带 `url` tag 的结构体/map |
| `ParseJSONResponse` 报 `http status code: 204` | 只认 200 | 自行 `json.Unmarshal(resp.Body, &out)` |
| 4xx 时 `err == nil` 却解析失败 | 非可重试状态码不产生 error | 自查 `resp.StatusCode` |
| 第二次重试请求体为空 | 复用同一 `*http.Request` | `MaxRetries = 0` 或在业务层控制重试 |
| 大文件下载内存暴涨 | `io.ReadAll` 全量入内存 | 该场景绕过本包，直连 `net/http` |
| 出站 span 是孤立根 span | `HttpRequest.Ctx` 没带上上游 ctx | `req.Ctx = ctx`（gRPC handler / `TracedHandler` 的 ctx） |
| 熔断一直不触发 | 样本粒度 = 一次 `DoRequest`，且 `MinRequests` 未达 | 降低 `MinRequests` 或按业务调用量评估阈值 |

## 下一步

- 完整 API 与语义细节：[README.md](README.md)
- 配置键位约定：[config.example.yaml](config.example.yaml)
- 链路侧配置：[../trace/README.md](../trace/README.md)
- 熔断参数含义：[../breaker/README.md](../breaker/README.md)
