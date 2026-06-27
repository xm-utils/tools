# Circuit Breaker 熔断器组件索引

## 📁 文件结构

```
internal/common/circuitbreaker/
├── circuit_breaker.go      # 核心状态机和滑动窗口实现
├── breaker.go              # 熔断器主逻辑 (Open/Closed/Half-Open)
├── metrics.go              # 监控指标统计
├── alert.go                # 告警机制
├── manager.go              # 熔断器管理器（单例模式）
├── example.go              # 使用示例代码
├── circuit_breaker_test.go # 单元测试
├── README.md               # 详细文档
└── QUICKSTART.md           # 快速开始指南
```

## 📄 文件说明

### 核心实现

| 文件 | 说明 | 主要功能 |
|------|------|----------|
| `circuit_breaker.go` | 基础定义 | State枚举、Config配置、SlidingWindow滑动窗口 |
| `breaker.go` | 熔断器核心 | 状态机逻辑、Execute执行方法、状态转换 |

### 辅助功能

| 文件 | 说明 | 主要功能 |
|------|------|----------|
| `metrics.go` | 监控指标 | 请求统计、成功率、响应时间、状态变更统计 |
| `alert.go` | 告警管理 | Open状态检测、拒绝率告警、自定义告警回调 |
| `manager.go` | 统一管理 | 单例管理器、多熔断器管理、全局指标查询 |

### 测试与示例

| 文件 | 说明 | 内容 |
|------|------|------|
| `example.go` | 使用示例 | 8个典型场景示例代码 |
| `circuit_breaker_test.go` | 单元测试 | 10个测试用例 + 性能测试 |

### 文档

| 文件 | 说明 | 适用人群 |
|------|------|----------|
| `README.md` | 完整文档 | 深入了解原理和API细节 |
| `QUICKSTART.md` | 快速开始 | 新手快速上手 |

## 🚀 快速导航

### 我是新手，想快速上手
👉 阅读 [QUICKSTART.md](QUICKSTART.md)

### 我想了解详细API和原理
👉 阅读 [README.md](README.md)

### 我想看代码示例
👉 查看 [example.go](example.go)

### 我想运行测试
```bash
go test -v ./internal/common/circuitbreaker/...
```

### 我想在实际项目中使用
👉 参考 [QUICKSTART.md 第7节](QUICKSTART.md#7-完整实战示例)

## 🎯 核心概念速查

### 三种状态

| 状态 | 说明 | 行为 |
|------|------|------|
| **CLOSED** | 关闭状态（正常） | 允许所有请求通过，实时监控指标 |
| **OPEN** | 打开状态（熔断） | 拒绝所有请求，直接返回降级结果 |
| **HALF_OPEN** | 半开状态（探测） | 允许少量探测请求，验证服务恢复 |

### 关键配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `ErrorThreshold` | 0.5 | 错误率阈值（50%） |
| `MinRequests` | 10 | 最小请求数才评估 |
| `WaitDurationInOpenState` | 30s | Open状态持续时间 |
| `WindowSize` | 10s | 时间窗口大小 |
| `SlowCallDuration` | 3s | 慢调用判定阈值 |

### 执行流程

```
请求到达
   ↓
检查熔断器状态
   ↓
┌─ OPEN ─────► 执行降级函数
│
└─ CLOSED/HALF_OPEN
      ↓
   调用下游服务
      ↓
   记录结果并更新状态
```

## 📊 监控指标

### 可用指标

- `TotalRequests`: 总请求数
- `SuccessRequests`: 成功请求数
- `FailedRequests`: 失败请求数
- `RejectedRequests`: 被拒绝的请求数
- `SuccessRate`: 成功率
- `FailureRate`: 失败率
- `RejectionRate`: 拒绝率
- `MinDuration/MaxDuration/AvgDuration`: 响应时间统计
- `StateChanges`: 状态变更次数

### 获取指标

```go
manager := circuitbreaker.GetManager()
allMetrics := manager.GetAllMetrics()

for name, metrics := range allMetrics {
    fmt.Printf("[%s] 成功率: %.2f%%\n", name, metrics.SuccessRate*100)
}
```

## 🔧 常见配置模板

### 模板1: 核心业务（支付、订单）

```go
config := circuitbreaker.DefaultCircuitBreakerConfig()
config.ErrorThreshold = 0.3
config.MinRequests = 20
config.WaitDurationInOpenState = 60 * time.Second
config.FallbackFunc = yourFallbackFunction
```

### 模板2: 非核心业务（通知、日志）

```go
config := circuitbreaker.DefaultCircuitBreakerConfig()
config.ErrorThreshold = 0.7
config.MinRequests = 10
config.WaitDurationInOpenState = 10 * time.Second
```

## ⚠️ 注意事项

### ✅ 推荐做法

1. **为每个下游服务创建独立熔断器**
   ```go
   paymentCB := manager.GetOrCreateBreaker("payment-service", config)
   orderCB := manager.GetOrCreateBreaker("order-service", config)
   ```

2. **实现降级函数**
   ```go
   config.FallbackFunc = func(args ...interface{}) (interface{}, error) {
       return cachedData, nil  // 返回缓存或默认值
   }
   ```

3. **启用告警监控**
   ```go
   alertConfig.AlertCallback = func(alert *AlertEvent) {
       sendNotification(alert.Message)
   }
   ```

### ❌ 避免做法

1. **不要将MinRequests设置过小**
   ```go
   // ❌ 容易误判
   config.MinRequests = 2
   
   // ✅ 合理设置
   config.MinRequests = 10~50
   ```

## 🐛 故障排查

### 问题1: 编译错误

**错误信息**: `undefined: logger.LOG`

**解决方案**: 确保项目已初始化logger模块
```go
import "gitlab.novgate.com/xm/pay/internal/common/logger"
```

### 问题2: 熔断器频繁打开

**可能原因**:
- 下游服务确实故障
- ErrorThreshold设置过低
- MinRequests设置过小

**解决方案**:
```go
// 调整配置
config.ErrorThreshold = 0.6
config.MinRequests = 50
```

## 📚 相关组件

熔断器通常与以下组件配合使用：

- **重试策略**: `internal/common/retry/`
  - 指数退避重试
  - 批量重试执行器
  
- **死信队列**: `internal/common/deadletter/`
  - Redis + 数据库双写
  - 自动恢复机制
  
- **缓存组件**: `internal/common/cache/`
  - 用于降级时返回缓存数据

## 🎓 学习路径

1. **入门** (30分钟)
   - 阅读 [QUICKSTART.md](QUICKSTART.md) 第1-3节
   - 运行基础示例代码

2. **进阶** (1小时)
   - 阅读 [README.md](README.md) 核心章节
   - 查看 [example.go](example.go) 所有示例
   - 理解状态机流转

3. **精通** (2小时)
   - 阅读源代码实现
   - 运行单元测试
   - 根据业务场景调优配置
   - 集成监控系统

## 💡 最佳实践总结

1. **独立熔断器** - 每个下游服务一个
2. **实现降级** - 提供fallback保证可用性
3. **监控告警** - 及时发现问题
4. **配置调优** - 根据业务特点调整参数
5. **定期演练** - 测试熔断器是否正常工作

## 🔗 参考资料

- [Martin Fowler - Circuit Breaker](https://martinfowler.com/articles/circuitBreaker.html)
- [Resilience4j Circuit Breaker](https://resilience4j.readme.io/docs/circuitbreaker)
- [Hystrix Documentation](https://github.com/Netflix/Hystrix/wiki)

---

**版本**: v1.0.0  
**最后更新**: 2026-06-19  
**维护者**: XM Pay Team
