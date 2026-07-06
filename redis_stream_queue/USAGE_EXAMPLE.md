# Redis Stream Queue 使用示例

## 基本用法

### 1. 使用默认配置

```go
package main

import (
    "context"
    "fmt"
    redis_stream "github.com/xm-utils/tools/redis_stream_queue"
)

func main() {
    // 使用默认配置创建队列（默认连接 localhost:6379）
    config := redis_stream.DefaultQueueConfig("mystream", "mygroup")
    
    queue, err := redis_stream.NewStreamQueue(config)
    if err != nil {
        panic(err)
    }
    
    ctx := context.Background()
    
    // 初始化 Stream 和消费者组
    if err := queue.InitStream(ctx); err != nil {
        panic(err)
    }
    
    fmt.Println("Stream Queue 初始化成功")
}
```

### 2. 自定义 Redis 配置

```go
package main

import (
    "context"
    "time"
    redis_stream "github.com/xm-utils/tools/redis_stream_queue"
)

func main() {
    config := &redis_stream.QueueConfig{
        StreamKey:      "order-stream",
        ConsumerGroup:  "order-processor",
        ReadBlockMs:    5 * time.Second,
        ReadBatchCount: 100,
        Redis: &redis_stream.RedisConfig{
            Addr:         "redis-server:6379",
            Password:     "your-password",
            DB:           1,
            PoolSize:     20,
            MinIdleConns: 10,
            MaxRetries:   5,
            DialTimeout:  10 * time.Second,
            ReadTimeout:  5 * time.Second,
            WriteTimeout: 5 * time.Second,
        },
    }
    
    queue, err := redis_stream.NewStreamQueue(config)
    if err != nil {
        panic(err)
    }
    
    // 使用 queue...
}
```

### 3. 发送消息

```go
msg := &redis_stream.StreamMessage{
    RequestID: "req-12345",
    Payload:   `{"order_id": 1001, "amount": 99.99}`,
}

messageID, err := queue.EnqueueMessage(ctx, msg)
if err != nil {
    log.Printf("消息入队失败: %v", err)
    return
}

log.Printf("消息入队成功: messageId=%s", messageID)
```

### 4. 消费消息

```go
// 启动消费者
consumerName := "consumer-1"

for {
    messages, err := queue.ReadMessages(ctx, consumerName)
    if err != nil {
        log.Printf("读取消息失败: %v", err)
        continue
    }
    
    for _, msg := range messages {
        log.Printf("收到消息: requestId=%s, payload=%s", 
            msg.RequestID, msg.Payload)
        
        // 处理业务逻辑
        if err := processMessage(msg); err != nil {
            log.Printf("消息处理失败: %v", err)
            continue
        }
        
        // 确认消息
        if err := queue.AckMessage(ctx, msg.MessageID); err != nil {
            log.Printf("消息确认失败: %v", err)
        }
    }
}
```

### 5. 完整的消费者示例

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"
    
    redis_stream "github.com/xm-utils/tools/redis_stream_queue"
)

type Order struct {
    OrderID int64   `json:"order_id"`
    Amount  float64 `json:"amount"`
}

func main() {
    // 创建队列
    config := redis_stream.DefaultQueueConfig("orders", "order-processors")
    queue, err := redis_stream.NewStreamQueue(config)
    if err != nil {
        log.Fatalf("创建队列失败: %v", err)
    }
    
    ctx := context.Background()
    
    // 初始化
    if err := queue.InitStream(ctx); err != nil {
        log.Fatalf("初始化失败: %v", err)
    }
    
    // 优雅关闭
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    
    go func() {
        <-sigChan
        log.Println("收到退出信号，正在关闭...")
        os.Exit(0)
    }()
    
    // 启动消费者
    consumerName := "consumer-1"
    log.Printf("启动消费者: %s", consumerName)
    
    for {
        select {
        case <-ctx.Done():
            return
        default:
            messages, err := queue.ReadMessages(ctx, consumerName)
            if err != nil {
                log.Printf("读取消息失败: %v", err)
                time.Sleep(1 * time.Second)
                continue
            }
            
            for _, msg := range messages {
                processOrderMessage(queue, ctx, msg)
            }
        }
    }
}

func processOrderMessage(queue *redis_stream.StreamQueue, ctx context.Context, msg redis_stream.StreamMessage) {
    var order Order
    if err := json.Unmarshal([]byte(msg.Payload), &order); err != nil {
        log.Printf("解析订单消息失败: %v", err)
        return
    }
    
    log.Printf("处理订单: OrderID=%d, Amount=%.2f", order.OrderID, order.Amount)
    
    // 模拟业务处理
    time.Sleep(100 * time.Millisecond)
    
    // 确认消息
    if err := queue.AckMessage(ctx, msg.MessageID); err != nil {
        log.Printf("确认消息失败: %v", err)
    } else {
        log.Printf("订单处理完成: OrderID=%d", order.OrderID)
    }
}
```

## 高级功能

### 1. 获取 Pending 消息

```go
// 获取长时间未处理的消息
pending, err := queue.GetPendingMessages(ctx)
if err != nil {
    log.Printf("获取 Pending 消息失败: %v", err)
    return
}

for _, p := range pending {
    log.Printf("Pending 消息: ID=%s, 空闲时间=%v, 重试次数=%d",
        p.ID, p.Idle, p.RetryCount)
}
```

### 2. 认领超时消息

```go
// 认领超过 5 分钟未处理的消息
claimedMsg, err := queue.ClaimMessage(ctx, "consumer-2", messageID)
if err != nil {
    log.Printf("认领消息失败: %v", err)
    return
}

if claimedMsg != nil {
    log.Printf("成功认领消息: %s", claimedMsg.MessageID)
    // 处理认领的消息...
}
```

### 3. 获取 Stream 长度

```go
length, err := queue.GetStreamLength(ctx)
if err != nil {
    log.Printf("获取 Stream 长度失败: %v", err)
    return
}

log.Printf("Stream 当前消息数: %d", length)
```

### 4. 删除消息

```go
if err := queue.DelMessage(ctx, messageID); err != nil {
    log.Printf("删除消息失败: %v", err)
}
```

## 配置说明

### RedisConfig 参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| Addr | string | localhost:6379 | Redis 服务器地址 |
| Password | string | "" | Redis 密码（可选） |
| DB | int | 0 | Redis 数据库编号 |
| PoolSize | int | 10 | 连接池大小 |
| MinIdleConns | int | 5 | 最小空闲连接数 |
| MaxRetries | int | 3 | 最大重试次数 |
| DialTimeout | time.Duration | 5s | 连接超时时间 |
| ReadTimeout | time.Duration | 3s | 读取超时时间 |
| WriteTimeout | time.Duration | 3s | 写入超时时间 |

### QueueConfig 参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| StreamKey | string | - | Stream 名称 |
| ConsumerGroup | string | - | 消费者组名称 |
| ReadBlockMs | time.Duration | 5s | XREADGROUP 阻塞时间 |
| ReadBatchCount | int64 | 100 | 每次读取的消息数量 |
| Redis | *RedisConfig | 见上 | Redis 连接配置 |

## 最佳实践

### 1. 连接池配置

根据并发量调整连接池大小：

```go
// 高并发场景
Redis: &RedisConfig{
    PoolSize:     50,
    MinIdleConns: 20,
}

// 低并发场景
Redis: &RedisConfig{
    PoolSize:     10,
    MinIdleConns: 5,
}
```

### 2. 超时设置

根据网络环境和业务需求调整超时：

```go
Redis: &RedisConfig{
    DialTimeout:  10 * time.Second,  // 慢网络增加连接超时
    ReadTimeout:  5 * time.Second,   // 大数据量增加读取超时
    WriteTimeout: 5 * time.Second,
}
```

### 3. 错误处理

始终检查错误并进行适当的重试：

```go
for {
    messages, err := queue.ReadMessages(ctx, consumerName)
    if err != nil {
        log.Printf("读取失败，1秒后重试: %v", err)
        time.Sleep(1 * time.Second)
        continue
    }
    // 处理消息...
}
```

### 4. 优雅关闭

使用信号处理实现优雅关闭：

```go
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
<-sigChan
// 执行清理操作...
```

## 注意事项

1. **幂等性**: 确保消息处理器是幂等的，因为消息可能会被重复处理
2. **ACK 机制**: 处理完消息后务必调用 `AckMessage`，否则消息会一直停留在 Pending 状态
3. **消费者组**: 同一消费者组内的消费者会竞争消费消息，不同组的消费者会各自独立消费
4. **消息顺序**: Redis Stream 保证单个消费者组内的消息顺序，但不保证跨组的顺序
5. **资源释放**: 应用退出时应该正确关闭 Redis 连接（Go 的垃圾回收会自动处理）
