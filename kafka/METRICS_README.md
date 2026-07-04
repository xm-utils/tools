# Kafka 消费者性能监控

## 📊 功能概述

Kafka 消费者内置了完整的性能监控功能，可以实时统计和分析消费者的各项性能指标：

### 监控指标

1. **消费速率** - 固定时间窗口内消费的消息数量（条/秒）
2. **处理耗时** - 消息平均处理耗时
3. **拉取频率** - 消费者多久拉取一次消息（次/秒）
4. **每次拉取数量** - 每次拉取的消息条数
5. **错误统计** - 处理失败的消息数量

## 🔧 使用方法

### 1. 基础用法

```go
package main

import (
    "fmt"
    "github.com/xm-utils/tools/kafka"
)

func main() {
    // 初始化消费者
    config := &kafka.ConsumerConfig{
        CommonConfig: kafka.CommonConfig{
            Brokers: []string{"localhost:9092"},
        },
        Topic:   "my-topic",
        GroupID: "my-group",
    }
    
    kafka.InitConsumer(config)
    consumer := kafka.GetConsumer()
    
    // 获取性能指标收集器
    metrics := consumer.GetMetrics()
    
    // 打印当前统计信息
    fmt.Println(metrics.FormatStats())
}
```

### 2. 定期打印统计信息

```go
// 启动定时打印协程
go func() {
    ticker := time.NewTicker(30 * time.Second) // 每30秒打印一次
    defer ticker.Stop()
    
    for range ticker.C {
        fmt.Println("\n========== 性能统计 ==========")
        consumer.PrintStats()
    }
}()
```

### 3. 获取详细统计数据

```go
metrics := consumer.GetMetrics()

// 获取总体统计
stats := metrics.GetOverallStats()
fmt.Printf("总消息数: %d\n", stats.TotalMessages)
fmt.Printf("运行时长: %v\n", stats.Uptime)
fmt.Printf("平均速率: %.2f 条/秒\n", stats.MessageRate)
fmt.Printf("平均处理耗时: %v\n", stats.AvgProcessTime)
fmt.Printf("最近1分钟速率: %.2f 条/秒\n", stats.RecentMessageRate)

// 获取当前窗口统计
window := metrics.GetCurrentWindow()
fmt.Printf("当前窗口消息数: %d\n", window.MessageCount)
fmt.Printf("当前窗口速率: %.2f 条/秒\n", window.MessageRate)
fmt.Printf("当前窗口平均耗时: %v\n", window.AvgProcessTime)

// 获取历史窗口数据
history := metrics.GetHistoryWindows()
for i, w := range history {
    fmt.Printf("窗口 %d: 消息数=%d, 速率=%.2f\n", i+1, w.MessageCount, w.MessageRate)
}
```

### 4. 使用 ConsumerManager 监控多业务

```go
manager := kafka.GetConsumerManager()

// 注册多个业务消费者
manager.RegisterConsumer("order", orderConfig)
manager.RegisterConsumer("user", userConfig)

// 定期打印各业务的性能统计
go func() {
    ticker := time.NewTicker(60 * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        consumers := manager.ListConsumers()
        for business, consumer := range consumers {
            fmt.Printf("\n【%s】业务统计:\n", business)
            consumer.PrintStats()
        }
    }
}()
```

## 📈 统计窗口说明

### 时间窗口机制

- **默认窗口大小**: 1分钟
- **窗口切换**: 当当前窗口时长达到配置值时自动切换到新窗口
- **历史窗口**: 保留最近60个窗口的数据用于趋势分析

### 自定义窗口大小

```go
// 创建5分钟窗口的指标收集器
customMetrics := kafka.NewConsumerMetrics(5 * time.Minute)
```

## 📊 输出示例

```
=== 消费者性能统计 ===
启动时间: 2026-07-04 12:30:00
运行时长: 10分30秒

--- 累计统计 ---
总消息数: 15234
总拉取次数: 15234
总错误数: 12
平均处理耗时: 25ms
平均消息速率: 24.18 条/秒
平均拉取速率: 24.18 次/秒

--- 最近1分钟 ---
消息速率: 28.50 条/秒
拉取速率: 28.50 次/秒

--- 当前窗口 (45秒) ---
消息数量: 1283
拉取次数: 1283
错误数量: 0
平均耗时: 22ms
消息速率: 28.51 条/秒
拉取速率: 28.51 次/秒
========================
```

## 🎯 监控场景

### 1. 性能告警

```go
go func() {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        metrics := consumer.GetMetrics()
        stats := metrics.GetOverallStats()
        window := metrics.GetCurrentWindow()
        
        // 消费速率过低告警
        if stats.RecentMessageRate < 10 {
            log.Warn("⚠️ 消费速率过低!")
        }
        
        // 处理耗时过长告警
        if window.AvgProcessTime > 1*time.Second {
            log.Warn("⚠️ 处理耗时过长!")
        }
        
        // 错误率过高告警
        errorRate := float64(stats.TotalErrors) / float64(stats.TotalMessages) * 100
        if errorRate > 5 {
            log.Warnf("⚠️ 错误率过高: %.2f%%", errorRate)
        }
    }
}()
```

### 2. 性能趋势分析

```go
// 获取历史窗口数据分析趋势
history := metrics.GetHistoryWindows()

var rates []float64
for _, window := range history {
    rates = append(rates, window.MessageRate)
}

// 计算平均速率、最大速率、最小速率
avgRate := calculateAverage(rates)
maxRate := calculateMax(rates)
minRate := calculateMin(rates)

fmt.Printf("平均速率: %.2f, 最大速率: %.2f, 最小速率: %.2f\n", 
    avgRate, maxRate, minRate)
```

### 3. 重置统计数据

```go
// 在特定时间点重置统计数据（如每天零点）
consumer.ResetStats()
```

## 🔍 关键指标解读

### 1. 消费速率 (Message Rate)

- **含义**: 每秒处理的消息数量
- **正常范围**: 根据业务而定，一般 10-100 条/秒
- **异常判断**: 突然下降可能表示处理能力不足或上游消息减少

### 2. 拉取频率 (Fetch Rate)

- **含义**: 每秒从 Kafka 拉取消息的次数
- **正常范围**: 通常与消费速率相近（每次拉取1条）
- **异常判断**: 远高于消费速率可能表示批量拉取但未及时处理

### 3. 平均处理耗时 (Avg Process Time)

- **含义**: 单条消息的平均处理时间
- **正常范围**: 毫秒级（< 100ms）
- **异常判断**: 超过1秒可能需要优化业务逻辑

### 4. 错误数量 (Error Count)

- **含义**: 处理失败的消息总数
- **正常范围**: 应该接近0
- **异常判断**: 持续增长表示存在系统性问题

## 💡 最佳实践

### 1. 监控频率

- **开发环境**: 每30秒打印一次
- **测试环境**: 每60秒打印一次
- **生产环境**: 每5-10分钟记录到监控系统

### 2. 窗口大小选择

- **实时监控**: 使用较小的窗口（30秒-1分钟）
- **趋势分析**: 使用较大的窗口（5-10分钟）
- **长期统计**: 依赖累计统计数据

### 3. 告警阈值设置

```go
// 推荐的告警阈值
const (
    MinMessageRate    = 10.0  // 最低消费速率（条/秒）
    MaxProcessTime    = 1 * time.Second  // 最大处理耗时
    MaxErrorRate      = 5.0   // 最大错误率（%）
)
```

### 4. 集成监控系统

可以将统计数据导出到 Prometheus、Grafana 等监控系统：

```go
func exportToPrometheus(metrics *kafka.ConsumerMetrics) {
    stats := metrics.GetOverallStats()
    
    // 上报到 Prometheus
    prometheus.GaugeVec.WithLabelValues("message_rate").Set(stats.MessageRate)
    prometheus.GaugeVec.WithLabelValues("avg_process_time").Set(float64(stats.AvgProcessTime))
    prometheus.CounterVec.WithLabelValues("total_errors").Add(float64(stats.TotalErrors))
}
```

## 📝 注意事项

1. **线程安全**: 所有指标收集方法都是线程安全的，可以在并发环境中使用
2. **内存占用**: 历史窗口会占用一定内存，默认保留60个窗口
3. **性能影响**: 指标收集对性能影响极小（< 1%）
4. **时间精度**: 统计基于系统时间，确保服务器时间同步

## 🚀 完整示例

查看 `metrics_example.go` 文件获取更多完整的使用示例：

- `ExampleConsumerMetrics` - 基础用法示例
- `ExampleConsumerMetricsWithManager` - 多业务监控示例
- `ExampleCustomMetricsWindow` - 自定义窗口示例
- `ExampleMonitorConsumerLag` - 延迟监控示例
