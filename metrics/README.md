# metrics — 统一指标出口组件

全仓库统一的 Prometheus 指标出口：**集中注册表 + `xmt_<component>_<name>` 命名规范 + `/metrics` HTTP Handler + 组件私有指标适配器**。对应架构路线图 3.1.2。

- 模块路径：`github.com/xm-utils/tools/metrics`
- 技术选型：`prometheus/client_golang`（v1.22.0），不引入其他指标链
- 兼容策略：**不删除、不修改** `breaker` / `mq/kafka/client` / `deadletter` 各自 `metrics.go` 的私有实现；接入一律经 `metrics/adapter/*` 薄适配器（拉取式 `prometheus.Collector`），组件本体零依赖本包

## 目录结构

```
metrics/
├── metrics.go                 # 统一注册表、Register、Handler、Exporter、规范构造器
├── naming.go                  # xmt_<component>_<name> 命名规范与校验
├── config.go                  # Config + Init（前缀、Go/进程 collector）
├── adapter/
│   ├── breaker/               # CircuitBreaker(Metrics) / Manager 快照 → xmt_breaker_*
│   ├── kafka/                 # client.ConsumerMetrics 累计+窗口 → xmt_kafka_consumer_*
│   └── deadletter/            # QueueMetrics.GetStats → xmt_deadletter_*（panic 安全）
└── config.example.yaml
```

## API 参考

### 初始化（可选）

```go
import "github.com/xm-utils/tools/metrics"

err := metrics.Init(metrics.Config{
    Namespace:            "xmt", // 留空即 xmt
    EnableProcessCollector: true,
})
```

未调用 `Init` 时即为默认配置（前缀 `xmt`、启用 Go collector），所有 API 可直接使用。`Init` 应在注册任何业务指标之前调用；重复调用会重建注册表。

### 创建规范指标

```go
fails, err := metrics.NewCounterVec("order", "handler_fail_total", "处理失败数", "topic")
depth, err := metrics.NewGaugeVec("order", "queue_depth", "积压深度", "queue")
latency, err := metrics.NewHistogramVec("order", "handle_seconds", "处理耗时", nil, "op")
snap, err := metrics.NewGaugeFunc("order", "workers", "当前 worker 数", func() float64 { return float64(n) })
```

构造即注册进统一注册表；命名非法、label 重复、同名重复注册均返回 `error`，不 panic。Vec 构造器要求至少一个 label，无 label 场景用 `NewGaugeFunc` 或原生 `prometheus.NewXxx` + `metrics.Register`。

### 暴露 /metrics

```go
http.Handle("/metrics", metrics.Handler()) // 标准库
// Gin: r.GET("/metrics", gin.WrapH(metrics.Handler()))
// grpcx：挂到 cmux 的 HTTP 端口，与 gRPC 同端口复用
```

`Handler()` 每次抓取实时解析当前注册表，`Init` 之后无需重建。

### 收录组件采集器

```go
type Exporter interface {
    MetricsCollectors() []prometheus.Collector
}

// 适配器均实现 MetricsCollectors()，两种接入方式等价：
err := metrics.RegisterExporter(breakermetrics.NewFromManager(xmbreaker.GetManager()))
err := metrics.Register(breakermetrics.NewFromManager(xmbreaker.GetManager()))
```

组件（或适配器）实现 `MetricsCollectors()` 即可被 `RegisterExporter` 收录；组件本体无需 import 本包——结构类型天然解耦。

## 组件适配器与指标清单

### breaker（`metrics/adapter/breaker`）

```go
import xmbreaker "github.com/xm-utils/tools/breaker"
import breakermetrics "github.com/xm-utils/tools/metrics/adapter/breaker"

// 单个熔断器
metrics.Register(breakermetrics.New(cb))
// 或整个管理器（推荐，动态覆盖 manager 下全部熔断器）
metrics.Register(breakermetrics.NewFromManager(xmbreaker.GetManager()))
```

| 指标 | 类型 | label |
|---|---|---|
| `xmt_breaker_requests_total` | counter | `name`, `result=success\|failure\|rejected` |
| `xmt_breaker_request_duration_seconds` | gauge | `name`, `stat=min\|avg\|max`（无成功请求时不产出） |
| `xmt_breaker_request_rate` | gauge | `name`, `kind=success\|failure\|rejection` |
| `xmt_breaker_state_change_total` | counter | `name`, `transition`（如 `closed_to_open`） |
| `xmt_breaker_last_update_timestamp_seconds` | gauge | `name` |

### kafka consumer（`metrics/adapter/kafka`）

```go
import kafkaclient "github.com/xm-utils/tools/mq/kafka/client"
import kafkametrics "github.com/xm-utils/tools/metrics/adapter/kafka"

metrics.Register(kafkametrics.New(consumer.GetMetrics(), "order-group"))
```

| 指标 | 类型 | label |
|---|---|---|
| `xmt_kafka_consumer_messages_total` / `fetches_total` / `errors_total` | counter | `consumer` |
| `xmt_kafka_consumer_rate` | gauge | `consumer`, `kind=message_avg\|message_recent\|fetch_avg\|fetch_recent` |
| `xmt_kafka_consumer_process_time_seconds` | gauge | `consumer`, `stat=avg` |
| `xmt_kafka_consumer_uptime_seconds` | gauge | `consumer` |
| `xmt_kafka_consumer_window` | gauge | `consumer`, `kind=messages\|fetches\|errors` |
| `xmt_kafka_consumer_window_process_time_seconds` | gauge | `consumer`, `stat=total\|avg` |

### deadletter（`metrics/adapter/deadletter`）

```go
import dlmetrics "github.com/xm-utils/tools/metrics/adapter/deadletter"

metrics.Register(dlmetrics.New(queueMetrics)) // *deadletter.QueueMetrics，须业务侧持有引用
```

抓取时实时调用 `GetStats`（含 Redis 队列长度与存储层统计）；存储层/Redis 未初始化引发的 **panic 被兜底为抓取错误**（InvalidMetric），不击穿抓取方。

| 指标 | 类型 | label |
|---|---|---|
| `xmt_deadletter_messages_total` | counter | `queue_key`, `kind=dead\|recovered\|abandoned` |
| `xmt_deadletter_queue_length` | gauge | `queue_key` |
| `xmt_deadletter_db_messages` | gauge | `queue_key`, `status=pending\|processing\|processed\|abandoned` |
| `xmt_deadletter_db_avg_retry_count` | gauge | `queue_key` |
| `xmt_deadletter_last_check_timestamp_seconds` | gauge | `queue_key`（未经巡检器更新时不产出） |

## 设计说明

1. **拉取式适配器**：适配器不包装、不拦截组件写入路径，仅在抓取瞬间读快照——组件私有 `metrics.go` 保持原语义，独立可引。
2. **零全局副作用**：本包不写 `prometheus.DefaultRegisterer`；需要使用默认注册表的场景请 `metrics.Registry()` 取表自行桥接。
3. **panic 转 error**：`Register` / `RegisterExporter` / 各构造器全部返回 error；`Init` 非法前缀报错并保持原配置。
4. **与 trace 的关系**：metrics 是 P0 可观测性第二块；`/metrics` 端点复用 `grpcx` cmux HTTP 端口或 `httpclient` server 侧，无需新端口。

## 开发状态

- `github.com/xm-utils/tools/trace` 尚未发布 tag，`go.mod` 中临时 `replace ../trace`（经 mq/kafka 间接依赖）；trace 发版后删除该 replace 并转 pseudo-version。
- 本模块未打 tag；接口冻结后发布 `metrics/v0.1.0`。

## 测试

```bash
go test -race ./...        # 根包 + 三个适配器；deadletter 用例基于 miniredis 离线运行
```
