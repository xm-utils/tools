# 死信队列组件 - 快速开始指南

## 🚀 5分钟快速上手

### 第一步:选择持久化方案

死信队列支持多种持久化方案,根据你的需求选择:

**方案A: 无持久化(仅 Redis)** - 适合测试或非关键业务
```bash
# 无需额外配置,直接使用
```

**方案B: MySQL/PostgreSQL (使用 GORM)** - 推荐生产环境使用
```bash
# 1. 执行SQL脚本创建表
mysql -u root -p your_database < sql/dead_letter_queue.sql

# 2. 确保已安装 GORM 和对应的数据库驱动
go get gorm.io/gorm
go get gorm.io/driver/mysql  # 或 gorm.io/driver/postgres
```

**方案C: 自定义持久化** - 适合特殊需求
```bash
# 实现 PersistenceStore 接口即可
```

### 第二步:基本使用示例

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "time"
    
    "github.com/xm-utils/tools/deadletter"
)

// 1. 定义你的消息结构
type OrderMessage struct {
    OrderNo  string `json:"orderNo"`
    Amount   uint64 `json:"amount"`
    Merchant int64  `json:"merchant"`
}

func main() {
    // 2. 定义消息处理器(当死信恢复时会调用这个函数)
    handler := func(ctx context.Context, messageData string) error {
        var order OrderMessage
        if err := json.Unmarshal([]byte(messageData), &order); err != nil {
            return err
        }
        
        log.Printf("重新处理订单: %s, 金额: %d", order.OrderNo, order.Amount)
        
        // TODO: 在这里执行你的业务逻辑
        // 例如:重新调用第三方支付接口、更新订单状态等
        
        return nil // 返回nil表示处理成功
    }
    
    // 3. 创建配置
    config := deadletter.DefaultConfig("order_callback")
    config.MaxRetry = 3                      // 最多重试3次
    config.RecoveryInterval = 5 * time.Minute // 每5分钟检查一次死信队列
    config.BatchSize = 10                     // 每次批量处理10条
    
    // 4. 选择持久化方案
    
    // 方案A: 不使用持久化(传nil)
    manager := deadletter.NewQueueManager(config, handler, nil)
    
    // 方案B: 使用GORM持久化
    // db, _ := gorm.Open(mysql.Open("dsn"), &gorm.Config{})
    // store := deadletter.NewGormPersistenceStore(db, "dead_letter_queue")
    // manager := deadletter.NewQueueManager(config, handler, store)
    
    // 5. 启动恢复服务(自动从死信队列恢复消息并重试)
    manager.StartRecovery()
    
    // 6. 在你的业务代码中,当消息处理失败时推入死信队列
    orderMsg := OrderMessage{
        OrderNo:  "ORD20260617001",
        Amount:   10000,
        Merchant: 1001,
    }
    messageData, _ := json.Marshal(orderMsg)
    
    // 假设这个消息处理失败了
    err := manager.PushToDeadLetter(
        "msg_001",              // 消息唯一ID
        string(messageData),    // 消息数据(JSON字符串)
        "第三方接口超时",        // 失败原因
        0,                      // 当前重试次数(第一次失败传0)
    )
    
    if err != nil {
        log.Printf("推入死信队列失败: %v", err)
    } else {
        log.Println("✅ 消息已移入死信队列,将自动恢复重试")
    }
    
    // 7. 应用关闭时停止服务
    defer manager.Stop()
    
    // 保持程序运行
    select {}
}
```

### 第三步:启用监控告警(可选但推荐)

```go
// 定义告警回调函数
alertFunc := func(stats *deadletter.QueueStats) {
    log.Printf("⚠️ 死信队列告警!")
    log.Printf("  队列: %s", stats.QueueKey)
    log.Printf("  长度: %d", stats.CurrentLength)
    log.Printf("  待处理: %d", stats.PendingCount)
    log.Printf("  恢复率: %.2f%%", 
        float64(stats.TotalRecovered)*100/float64(stats.TotalDeadLetters))
    
    // TODO: 发送告警通知
    // sendDingTalkAlert(stats)
    // sendEmailAlert(stats)
}

// 创建监控服务(每分钟检查一次)
monitor := deadletter.NewMetricsMonitor(manager, 1*time.Minute, alertFunc)
monitor.Start()

defer monitor.Stop()
```

### 第四步:查看监控信息

```go
// 获取队列统计信息
stats := manager.GetMetrics().GetStats(context.Background())

log.Printf("📊 队列统计:")
log.Printf("  当前长度: %d", stats.CurrentLength)
log.Printf("  总死信数: %d", stats.TotalDeadLetters)
log.Printf("  总恢复数: %d", stats.TotalRecovered)
log.Printf("  待处理数: %d", stats.PendingCount)
log.Printf("  平均重试: %.2f", stats.AvgRetryCount)
```

## 💼 完整实战示例:订单回调服务

```go
package order

import (
    "context"
    "encoding/json"
    "log"
    "time"
    
    "github.com/xm-utils/tools/deadletter"
    "gorm.io/gorm"
)

// OrderCallback 订单回调消息
type OrderCallback struct {
    OrderNo   string `json:"orderNo"`
    Status    string `json:"status"`
    Amount    uint64 `json:"amount"`
    Timestamp int64  `json:"timestamp"`
}

// CallbackService 回调服务
type CallbackService struct {
    dlqManager *deadletter.QueueManager
}

// NewCallbackService 创建回调服务
func NewCallbackService(db *gorm.DB) *CallbackService {
    // 定义消息处理器
    handler := func(ctx context.Context, messageData string) error {
        var callback OrderCallback
        if err := json.Unmarshal([]byte(messageData), &callback); err != nil {
            return err
        }
        
        log.Printf("🔄 重新处理订单回调: orderNo=%s", callback.OrderNo)
        
        // 执行业务逻辑
        // 1. 验证签名
        // 2. 更新订单状态
        // 3. 通知商户
        return processCallback(&callback)
    }
    
    // 创建配置
    config := deadletter.DefaultConfig("order_callback")
    config.MaxRetry = 5                      // 订单重要,多试几次
    config.RecoveryInterval = 2 * time.Minute // 每2分钟检查
    config.BatchSize = 20                     // 批量处理20条
    
    // 创建持久化存储
    store := deadletter.NewGormPersistenceStore(db, "dead_letter_queue")
    
    // 创建管理器
    manager := deadletter.NewQueueManager(config, handler, store)
    
    // 启动恢复服务
    manager.StartRecovery()
    
    // 启动监控
    alertFunc := func(stats *deadletter.QueueStats) {
        log.Printf("⚠️ 订单回调死信队列告警: %+v", stats)
        // TODO: 发送告警通知
    }
    monitor := deadletter.NewMetricsMonitor(manager, 1*time.Minute, alertFunc)
    monitor.Start()
    
    return &CallbackService{
        dlqManager: manager,
    }
}

// HandleCallback 处理订单回调
func (s *CallbackService) HandleCallback(callback *OrderCallback) error {
    messageData, _ := json.Marshal(callback)
    
    // 尝试处理回调
    err := processCallback(callback)
    if err != nil {
        log.Printf("❌ 订单回调处理失败: orderNo=%s, err=%v", callback.OrderNo, err)
        
        // 推入死信队列
        s.dlqManager.PushToDeadLetter(
            callback.OrderNo,       // 使用订单号作为消息ID
            string(messageData),
            err.Error(),
            0,
        )
        
        return err
    }
    
    log.Printf("✅ 订单回调处理成功: orderNo=%s", callback.OrderNo)
    return nil
}

// Shutdown 关闭服务
func (s *CallbackService) Shutdown() {
    if s.dlqManager != nil {
        s.dlqManager.Stop()
    }
}

// processCallback 实际处理逻辑
func processCallback(callback *OrderCallback) error {
    // TODO: 实现具体的业务逻辑
    // 1. 验证签名
    // 2. 更新订单状态
    // 3. 通知商户
    return nil
}
```

## 🎯 多队列使用场景

不同业务使用独立的死信队列,互不影响:

```go
// 支付回调队列 - 重要业务
paymentConfig := deadletter.DefaultConfig("payment_callback")
paymentConfig.MaxRetry = 5
paymentConfig.RecoveryInterval = 2 * time.Minute
paymentStore := deadletter.NewGormPersistenceStore(db, "dead_letter_payment")
paymentManager := deadletter.NewQueueManager(paymentConfig, paymentHandler, paymentStore)
paymentManager.StartRecovery()

// 退款回调队列 - 重要业务
refundConfig := deadletter.DefaultConfig("refund_callback")
refundConfig.MaxRetry = 5
refundConfig.RecoveryInterval = 2 * time.Minute
refundStore := deadletter.NewGormPersistenceStore(db, "dead_letter_refund")
refundManager := deadletter.NewQueueManager(refundConfig, refundHandler, refundStore)
refundManager.StartRecovery()

// 系统日志队列 - 非重要业务
logConfig := deadletter.DefaultConfig("system_log")
logConfig.MaxRetry = 1
logConfig.RecoveryInterval = 30 * time.Minute
logManager := deadletter.NewQueueManager(logConfig, logHandler, nil) // 不需要持久化
logManager.StartRecovery()

// 分别使用对应的manager
paymentManager.PushToDeadLetter(...)
refundManager.PushToDeadLetter(...)
logManager.PushToDeadLetter(...)
```

## ❓ 常见问题

### Q1: 消息会丢失吗?

**A:** 取决于你的持久化方案:

- **无持久化(nil)**: 仅存储在 Redis,Redis 重启后数据丢失
- **GORM/自定义持久化**: 双重存储(Redis + 数据库),即使 Redis 重启,数据仍在数据库中

**建议**: 生产环境务必启用持久化!

### Q2: 如何手动处理死信消息?

**A:** 有两种方式:

**方式1: 查询数据库手动处理**
```sql
-- 查看待处理的死信消息
SELECT * FROM dead_letter_queue 
WHERE status = 1 
ORDER BY created_at DESC 
LIMIT 10;

-- 手动标记为已处理(如果确认无法恢复)
UPDATE dead_letter_queue 
SET status = 4, processed_time = NOW() 
WHERE id = xxx;

-- 或者重置为待处理,让系统自动重试
UPDATE dead_letter_queue 
SET status = 1, retry_count = 0, next_retry_time = NOW() 
WHERE id = xxx;
```

**方式2: 等待自动恢复**
- 恢复服务会定期(默认5分钟)从 Redis 读取消息并重试
- 如果重试成功,自动标记为已处理
- 如果超过最大重试次数,标记为已放弃

### Q3: 如何调整恢复频率?

**A:** 修改配置中的 `RecoveryInterval`:

```go
config := deadletter.DefaultConfig("my_queue")
config.RecoveryInterval = 1 * time.Minute  // 改为1分钟
config.RecoveryInterval = 10 * time.Minute // 或改为10分钟
```

**建议**:
- 核心业务: 1-2 分钟
- 普通业务: 5-10 分钟
- 非核心业务: 30 分钟以上

### Q4: 不同业务需要不同的重试策略怎么办?

**A:** 为每个业务创建独立的死信队列管理器:

```go
// 订单回调:重要,多试几次
orderConfig := deadletter.DefaultConfig("order_callback")
orderConfig.MaxRetry = 5
orderConfig.RecoveryInterval = 2 * time.Minute

// 系统日志:不重要,少试几次
logConfig := deadletter.DefaultConfig("system_log")
logConfig.MaxRetry = 1
logConfig.RecoveryInterval = 30 * time.Minute

// 分别创建管理器
orderManager := deadletter.NewQueueManager(orderConfig, orderHandler, store)
logManager := deadletter.NewQueueManager(logConfig, logHandler, nil)
```

### Q5: 性能怎么样?

**A:** 参考性能数据:

| 操作 | 耗时 | 说明 |
|------|------|------|
| 推入死信队列 | ~5ms | Redis LPUSH + DB INSERT |
| 恢复消息 | ~10ms/条 | 取决于业务逻辑 |
| 监控查询 | ~50ms | DB 统计查询 |
| 内存占用 | 1-5KB/条 | 1000条约1-5MB |

**优化建议**:
- 批量大小不超过 50
- 恢复间隔不低于 1 分钟
- 定期清理历史数据(保留30天)
- 为数据库表建立合适的索引

### Q6: 如何实现消息幂等性?

**A:** 在消息处理器中检查是否已处理:

```go
handler := func(ctx context.Context, messageData string) error {
    var msg Message
    json.Unmarshal([]byte(messageData), &msg)
    
    // 检查是否已处理(基于消息ID)
    if isProcessed(msg.MessageID) {
        log.Printf("消息已处理,跳过: %s", msg.MessageID)
        return nil
    }
    
    // 执行业务逻辑
    processBusiness(msg)
    
    // 标记已处理
    markAsProcessed(msg.MessageID)
    
    return nil
}
```

### Q7: 如何区分可重试和不可重试错误?

**A:** 在消息处理器中分类处理:

```go
handler := func(ctx context.Context, messageData string) error {
    err := processMessage(messageData)
    
    // 不可重试错误(如数据格式错误、业务规则违反)
    if isPermanentError(err) {
        log.Printf("不可重试错误,直接放弃: %v", err)
        // 返回特殊错误码,可以在外部捕获并直接标记为放弃
        return ErrPermanent
    }
    
    // 可重试错误(如网络超时、第三方服务暂时不可用)
    return err
}
```

## 🎓 配置调优建议

### 核心业务(支付、订单)

```go
config := deadletter.DefaultConfig("payment")
config.MaxRetry = 5                    // 多试几次
config.RecoveryInterval = 2 * time.Minute // 快速恢复
config.BatchSize = 20                  // 批量处理
// 务必启用持久化!
store := deadletter.NewGormPersistenceStore(db, "dead_letter_payment")
manager := deadletter.NewQueueManager(config, handler, store)
```

### 非核心业务(通知、日志)

```go
config := deadletter.DefaultConfig("notification")
config.MaxRetry = 2                    // 少试几次
config.RecoveryInterval = 10 * time.Minute // 慢速恢复
config.BatchSize = 10
// 可以不启用持久化
manager := deadletter.NewQueueManager(config, handler, nil)
```

### 高QPS场景

```go
config := deadletter.DefaultConfig("high_qps")
config.MaxRetry = 3
config.RecoveryInterval = 1 * time.Minute // 频繁检查
config.BatchSize = 50                     // 大批量处理
store := deadletter.NewGormPersistenceStore(db, "dead_letter_high_qps")
manager := deadletter.NewQueueManager(config, handler, store)
```

## 📚 下一步

- 📖 阅读 [完整文档](README.md) 了解更多高级特性
- 🔧 查看 [自定义持久化示例](custom_persistence_example.go) 了解如何实现 MongoDB/PostgreSQL
- 🧪 运行测试: `go test ./deadletter/...`
- 💡 查看 [集成示例](integration_examples.go) 了解如何在现有项目中集成

## 🆘 技术支持

如有问题,请:
1. 查看 [README.md](README.md) 完整文档
2. 查看 [MIGRATION.md](MIGRATION.md) 迁移指南
3. 提交 Issue 或联系开发团队

---

**提示**: 建议先运行基本示例,熟悉后再尝试高级功能! 🎉
