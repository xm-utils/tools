# metrics — 5 分钟上手

## 1. 引入与初始化

```go
import "github.com/xm-utils/tools/metrics"

func main() {
    // 可选：不调用即默认（前缀 xmt + Go 运行时指标）
    if err := metrics.Init(metrics.Config{}); err != nil {
        panic(err) // 启动期失败直接退出属于业务选择，组件本身不 panic
    }
}
```

## 2. 打自己的业务指标

```go
fails, _ := metrics.NewCounterVec("order", "create_fail_total", "下单失败数", "code")
depth, _ := metrics.NewGaugeVec("order", "pending", "待处理订单数", "shard")

fails.WithLabelValues("500").Inc()
depth.WithLabelValues("s1").Set(float64(n))
```

命名自动规范为 `xmt_order_create_fail_total` / `xmt_order_pending`，构造即注册。

## 3. 收录组件私有指标

```go
import (
    "github.com/xm-utils/tools/breaker"
    breakermetrics "github.com/xm-utils/tools/metrics/adapter/breaker"
    dlmetrics "github.com/xm-utils/tools/metrics/adapter/deadletter"
    kafkametrics "github.com/xm-utils/tools/metrics/adapter/kafka"
)

metrics.Register(breakermetrics.NewFromManager(breaker.GetManager())) // 全部熔断器
metrics.Register(kafkametrics.New(consumer.GetMetrics(), "order-group"))
metrics.Register(dlmetrics.New(queueMetrics))
```

## 4. 暴露 /metrics

```go
// 独立端口
go http.ListenAndServe(":9090", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    metrics.Handler().ServeHTTP(w, r)
}))

// 或复用 grpcx cmux 的 HTTP 端口（同端口跑 gRPC + HTTP）：
// mux.Handle(cmux.MatcherHTTP(), ...) 内部路由 "/metrics" → metrics.Handler()
```

## 5. 验证

```bash
curl -s localhost:9090/metrics | grep xmt_
# xmt_breaker_requests_total{name="order-api",result="success"} 1024
# xmt_kafka_consumer_messages_total{consumer="order-group"} 88
# xmt_order_create_fail_total{code="500"} 3
```

## 常见错误

| 现象 | 原因 |
|---|---|
| `metrics: collector already registered` | 同名指标重复注册；`Init` 重建注册表后需重新 Register 既有 collector |
| Vec 构造报「至少需要一个 label 名」 | Vec 类型必须带 label；无 label 用 `NewGaugeFunc` 或原生 prometheus 构造 |
| `/metrics` 里 `xmt_deadletter_*` 抓取报错 | Redis / 存储层未初始化，GetStats panic 已被兜底为抓取错误，先完成组件初始化 |
