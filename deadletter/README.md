# Dead Letter Queue - 死信队列组件

基于 Redis 的通用死信队列组件,支持自定义持久化存储、灵活的消息处理、自动恢复重试和实时监控告警。

## 🎯 核心特性

- ✅ **灵活的持久化接口** - 支持 MySQL、PostgreSQL、MongoDB 或无持久化(仅 Redis)
- ✅ **自定义消息处理器** - 通过函数回调实现业务逻辑
- ✅ **自动恢复机制** - 定期从死信队列恢复消息并重试
- ✅ **双重存储架构** - Redis List(快速恢复) + 自定义持久化(数据兜底)
- ✅ **实时监控告警** - 队列长度、恢复率、平均重试次数等指标监控
- ✅ **多队列支持** - 不同业务使用独立的死信队列
- ✅ **幂等保证** - 基于唯一消息 ID 防止重复处理

## 📦 安装

```bash
go get github.com/xm-utils/tools/deadletter
```

## 🏗️ 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                     业务服务层                                │
│  (Order Service / Payment Service / Account Service)        │
└──────────────────┬──────────────────────────────────────────┘
                   │ 消息处理失败
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                    QueueManager                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  PushToDeadLetter()                                  │   │
│  │  - 序列化消息                                         │   │
│  │  - 写入 Redis List (快速恢复)                         │   │
│  │  - 持久化到自定义存储 (可选)                           │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────┬──────────────────────────────────────────┘
                   │
         ┌─────────┴─────────┐
         ▼                   ▼
┌─────────────────┐  ┌─────────────────┐
│  Redis List     │  │  Custom Store   │
│  dead_letter:   │  │  (Optional)     │
│  {queue_key}    │  │                 │
│                 │  │  - MySQL        │
│  [msg1]         │  │  - PostgreSQL   │
│  [msg2]         │  │  - MongoDB      │
│  [msg3]         │  │  - ...          │
│  ...            │  │                 │
└────────┬────────┘  └─────────────────┘
         │
         │ 定期恢复
         ▼
┌─────────────────────────────────────────────────────────────┐
│              Recovery Service                                │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  StartRecovery()                                     │   │
│  │  - 定时从 Redis LRange 读取消息                       │   │
│  │  - 调用自定义 MessageHandler 重试1次                  │   │
│  │  - 成功: 标记已处理,从队列移除                        │   │
│  │  - 失败: 更新重试计数,重新入队尾部                    │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│              Metrics Monitor                                 │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  GetStats()                                          │   │
│  │  - 队列长度(Redis LLEN)                               │   │
│  │  - 各状态统计(Custom Store)                           │   │
│  │  - 平均重试次数(Custom Store)                         │   │
│  │  - 恢复率计算                                         │   │
│  │                                                      │   │
│  │  Alert Conditions:                                   │   │
│  │  - 队列长度 > 1000                                   │   │
│  │  - 待处理数 > 500                                    │   │
│  │  - 平均重试 > 2.5                                    │   │
│  │  - 恢复率 < 50%                                      │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 快速开始

### 1. 基本使用(无持久化)

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "time"
    
    "github.com/xm-utils/tools/deadletter"
)

// 定义消息结构
type OrderMessage struct {
    OrderNo  string `json:"orderNo"`
    Amount   uint64 `json:"amount"`
    Merchant int64  `json:"merchant"`
}

func main() {
    // 1. 定义消息处理器
    handler := func(ctx context.Context, messageData string) error {
        var order OrderMessage
        if err := json.Unmarshal([]byte(messageData), &order); err != nil {
            return err
        }
        
        log.Printf("处理订单: %s, 金额: %d", order.OrderNo, order.Amount)
        
        // 执行业务逻辑
        // 例如:调用第三方支付接口、更新订单状态等
        return processOrder(order)
    }
    
    // 2. 创建配置
    config := deadletter.DefaultConfig("order_callback")
    config.MaxRetry = 3                      // 最大重试3次
    config.RecoveryInterval = 5 * time.Minute // 每5分钟检查一次
    config.BatchSize = 10                     // 每次批量处理10条
    
    // 3. 创建管理器(不使用持久化,传nil)
    manager := deadletter.NewQueueManager(config, handler, nil)
    
    // 4. 启动恢复服务
    manager.StartRecovery()
    
    // 5. 在业务代码中使用
    // 当消息处理失败时推入死信队列
    orderMsg := OrderMessage{
        OrderNo:  "ORD20260617001",
        Amount:   10000,
        Merchant: 1001,
    }
    messageData, _ := json.Marshal(orderMsg)
    
    err := manager.PushToDeadLetter(
        "msg_001",           // 消息ID
        string(messageData), // 消息数据
        "支付接口超时",       // 错误信息
        0,                   // 当前重试次数
    )
    if err != nil {
        log.Printf("推入死信队列失败: %v", err)
    }
    
    // 6. 应用关闭时停止服务
    defer manager.Stop()
    
    // 保持程序运行
    select {}
}

func processOrder(order OrderMessage) error {
    // TODO: 实现订单处理逻辑
    return nil
}
```

### 2. 使用 GORM 持久化(MySQL/PostgreSQL)

```go
package main

import (
    "github.com/xm-utils/tools/deadletter"
    "gorm.io/gorm"
    // 导入对应的数据库驱动
    _ "gorm.io/driver/mysql"
)

func main() {
    // 1. 初始化数据库连接
    db, err := gorm.Open(mysql.Open("user:pass@tcp(localhost:3306)/dbname"), &gorm.Config{})
    if err != nil {
        panic(err)
    }
    
    // 2. 创建持久化存储
    store := deadletter.NewGormPersistenceStore(db, "dead_letter_queue")
    
    // 3. 创建管理器
    config := deadletter.DefaultConfig("order_callback")
    handler := func(ctx context.Context, messageData string) error {
        // 处理消息
        return nil
    }
    
    manager := deadletter.NewQueueManager(config, handler, store)
    manager.StartRecovery()
    
    defer manager.Stop()
    select {}
}
```

### 3. 启用监控告警

```go
// 创建监控服务
alertFunc := func(stats *deadletter.QueueStats) {
    log.Printf("⚠️ 死信队列告警:")
    log.Printf("  队列: %s", stats.QueueKey)
    log.Printf("  长度: %d", stats.CurrentLength)
    log.Printf("  待处理: %d", stats.PendingCount)
    log.Printf("  恢复率: %.2f%%", 
        float64(stats.TotalRecovered)*100/float64(stats.TotalDeadLetters))
    
    // TODO: 发送告警通知(钉钉、企业微信、邮件等)
    sendAlertNotification(stats)
}

monitor := deadletter.NewMetricsMonitor(manager, 1*time.Minute, alertFunc)
monitor.Start()

defer monitor.Stop()
```

### 4. 多队列使用

不同业务使用独立的死信队列:

```go
// 支付回调队列
paymentConfig := deadletter.DefaultConfig("payment_callback")
paymentManager := deadletter.NewQueueManager(paymentConfig, paymentHandler, store)
paymentManager.StartRecovery()

// 退款回调队列
refundConfig := deadletter.DefaultConfig("refund_callback")
refundManager := deadletter.NewQueueManager(refundConfig, refundHandler, store)
refundManager.StartRecovery()

// 账户事件队列
accountConfig := deadletter.DefaultConfig("account_event")
accountManager := deadletter.NewQueueManager(accountConfig, accountHandler, store)
accountManager.StartRecovery()

// 分别使用对应的manager
paymentManager.PushToDeadLetter(...)
refundManager.PushToDeadLetter(...)
accountManager.PushToDeadLetter(...)
```

## 📖 API 参考

### Config 配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| QueueKey | string | "default" | 队列Key标识,用于区分不同业务 |
| DeadLetterStream | string | "dead_letter:{QueueKey}" | Redis Key |
| MaxRetry | int | 3 | 最大重试次数 |
| RetryInterval | time.Duration | 1s | 重试间隔时间 |
| RecoveryInterval | time.Duration | 5m | 恢复检查间隔 |
| BatchSize | int | 10 | 批量处理大小 |

### QueueManager 方法

```go
// 创建管理器
func NewQueueManager(config *Config, handler MessageHandler, store PersistenceStore) *QueueManager

// 推入死信队列
func (m *QueueManager) PushToDeadLetter(messageID, messageData, errorMessage string, retryCount int) error

// 启动恢复服务
func (m *QueueManager) StartRecovery()

// 获取队列长度
func (m *QueueManager) GetQueueLength() (int64, error)

// 停止服务
func (m *QueueManager) Stop()
```

### PersistenceStore 接口

```go
type PersistenceStore interface {
    // 保存死信消息记录
    Save(ctx context.Context, record *QueueMsgRecord) error
    
    // 更新消息状态
    UpdateStatus(ctx context.Context, queueKey, messageID string, 
                 status QueueStatus, processedTime *time.Time) error
    
    // 更新重试信息
    UpdateRetryInfo(ctx context.Context, queueKey, messageID string, 
                    retryCount int, errorMessage string, 
                    lastErrorTime, nextRetryTime time.Time) error
    
    // 根据消息ID查询记录
    FindByMessageID(ctx context.Context, queueKey, messageID string) (*QueueMsgRecord, error)
    
    // 获取队列统计信息(用于监控服务)
    GetStats(ctx context.Context, queueKey string) (*DatabaseStats, error)
}
```

### 监控指标

| 指标 | 说明 | 告警阈值 |
|------|------|----------|
| CurrentLength | 当前队列长度(Redis) | > 1000 |
| PendingCount | 待处理消息数 | > 500 |
| AvgRetryCount | 平均重试次数 | > 2.5 |
| RecoveryRate | 恢复率(已恢复/总死信) | < 50% |

## 🔧 自定义持久化实现

### MongoDB 实现示例

```go
type MongoPersistenceStore struct {
    collection *mongo.Collection
}

func NewMongoPersistenceStore(client *mongo.Client, dbName, collectionName string) *MongoPersistenceStore {
    return &MongoPersistenceStore{
        collection: client.Database(dbName).Collection(collectionName),
    }
}

func (s *MongoPersistenceStore) Save(ctx context.Context, record *deadletter.QueueMsgRecord) error {
    _, err := s.collection.InsertOne(ctx, record)
    return err
}

func (s *MongoPersistenceStore) UpdateStatus(ctx context.Context, queueKey, messageID string, 
    status deadletter.QueueStatus, processedTime *time.Time) error {
    filter := bson.M{"queue_key": queueKey, "message_id": messageID}
    update := bson.M{"$set": bson.M{"status": status}}
    if processedTime != nil {
        update["$set"].(bson.M)["processed_time"] = processedTime
    }
    _, err := s.collection.UpdateOne(ctx, filter, update)
    return err
}

// 实现其他方法...

// 使用
mongoStore := NewMongoPersistenceStore(mongoClient, "deadletter", "messages")
manager := deadletter.NewQueueManager(config, handler, mongoStore)
```

### PostgreSQL 实现示例

```go
import "github.com/jackc/pgx/v5"

type PostgresPersistenceStore struct {
    conn *pgx.Conn
}

func (s *PostgresPersistenceStore) Save(ctx context.Context, record *deadletter.QueueMsgRecord) error {
    query := `INSERT INTO dead_letter_queue (...) VALUES (...)`
    _, err := s.conn.Exec(ctx, query, /* 参数 */)
    return err
}

// 实现其他方法...

// 使用
pgStore := &PostgresPersistenceStore{conn: pgConn}
manager := deadletter.NewQueueManager(config, handler, pgStore)
```

## 🔄 消息流转过程

```
1. 消息处理失败
   ↓
2. PushToDeadLetter()
   ├─→ Redis LPUSH dead_letter:{queue_key}
   └─→ Custom Store Save (如果配置了持久化)
   ↓
3. 定期恢复服务(每5分钟)
   ├─→ Redis LRANGE 获取批量消息
   ├─→ 调用 MessageHandler 重试1次
   ├─→ 成功: LREM 移除 + 更新状态为已处理
   └─→ 失败: LREM + RPUSH 重新入队 + 更新重试计数
   ↓
4. 超过MaxRetry → 标记为已放弃
   ↓
5. 监控服务实时统计
   ├─→ Redis LLEN 获取队列长度
   ├─→ Custom Store GetStats 统计各状态数量
   └─→ 触发告警条件时发送通知
```

## 💡 最佳实践

### 1. 消息幂等性

确保消息处理器是幂等的,避免重复处理导致数据不一致:

```go
handler := func(ctx context.Context, messageData string) error {
    var msg Message
    json.Unmarshal([]byte(messageData), &msg)
    
    // 检查是否已处理
    if isProcessed(msg.MessageID) {
        return nil
    }
    
    // 执行业务逻辑
    processBusiness(msg)
    
    // 标记已处理
    markAsProcessed(msg.MessageID)
    
    return nil
}
```

### 2. 错误分类处理

区分可重试和不可重试错误:

```go
handler := func(ctx context.Context, messageData string) error {
    err := processMessage(messageData)
    
    // 不可重试错误(如数据格式错误)
    if isPermanentError(err) {
        // 直接返回特殊错误,标记为放弃
        return deadletter.ErrPermanent
    }
    
    // 可重试错误(如网络超时)
    return err
}
```

### 3. 配置调优建议

**核心业务**(支付、订单):
```go
config.MaxRetry = 5
config.RecoveryInterval = 2 * time.Minute
config.BatchSize = 20
```

**非核心业务**(通知、日志):
```go
config.MaxRetry = 3
config.RecoveryInterval = 10 * time.Minute
config.BatchSize = 10
```

**高QPS场景**:
```go
config.BatchSize = 50
config.RecoveryInterval = 1 * time.Minute
```

### 4. 人工介入

对于长期无法恢复的消息,需要人工介入处理:

```sql
-- 查询已放弃的消息
SELECT * FROM dead_letter_queue 
WHERE status = 4 
ORDER BY created_at DESC 
LIMIT 100;

-- 手动重新处理
UPDATE dead_letter_queue 
SET status = 1, next_retry_time = NOW() 
WHERE id = xxx;
```

## ⚠️ 注意事项

1. **持久化为可选**: 传入 `nil` 将禁用持久化,仅使用 Redis
2. **幂等性保证**: 消息处理器必须是幂等的
3. **资源管理**: 应用关闭时务必调用 `Stop()` 方法
4. **错误处理**: 持久化失败不会影响 Redis 操作,仅记录日志
5. **监控性能**: 监控服务会定期查询持久化存储,注意性能影响
6. **数据清理**: 建议定期清理已处理的历史数据(保留30天)

## 🐛 故障排查

### 问题1: 消息未进入死信队列

**检查**:
- Redis 连接是否正常
- 持久化存储连接是否正常(如果配置了)
- 消息 ID 是否唯一

### 问题2: 恢复服务未执行

**检查**:
- `StartRecovery()` 是否已调用
- `RecoveryInterval` 配置是否合理
- 日志中是否有错误信息

### 问题3: 监控数据不准确

**检查**:
- 持久化存储的统计查询是否正确
- Redis 和持久化存储数据是否一致
- 监控间隔是否过短

### 问题4: 队列积压严重

**解决方案**:
1. 增加 `BatchSize` 提高单次处理量
2. 缩短 `RecoveryInterval` 加快恢复频率
3. 检查消息处理器性能,优化处理逻辑
4. 考虑增加消费者实例

## 📊 性能参考

- **Redis 操作**: O(1) ~ O(N), N为批量大小
- **持久化操作**: 依赖具体实现,建议建立合适的索引
- **内存占用**: 每个消息约 1-5KB, 1000条消息约 1-5MB
- **恢复频率**: 建议 5-10 分钟,避免频繁查询

## 📚 扩展阅读

- [Redis List 命令文档](https://redis.io/commands/#list)
- [GORM 官方文档](https://gorm.io/)
- [消息队列最佳实践](https://www.rabbitmq.com/blog/2015/04/16/scheduling-messages-with-rabbitmq/)
- [死信队列模式](https://www.enterpriseintegrationpatterns.com/patterns/messaging/DeadLetterChannel.html)

## 🤝 贡献

欢迎提交 Issue 和 Pull Request!

## 📄 许可证

MIT License
