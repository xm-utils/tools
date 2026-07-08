# MongoDB 客户端快速开始指南

## 1. 安装

```bash
go get github.com/xm-utils/tools/mongodb
```

## 2. 基础使用

### 初始化客户端

```go
package main

import (
    "context"
    "fmt"
    "log"
    "github.com/xm-utils/tools/mongodb"
)

func main() {
    // 创建配置
    cfg := &mongodb.Config{
        URI:      "mongodb://localhost:27017",
        Database: "test_db",
    }

    // 初始化客户端
    client, err := mongodb.NewClient(cfg)
    if err != nil {
        log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close(context.Background())

    fmt.Println("Connected to MongoDB!")
}
```

### CRUD 操作

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    "github.com/xm-utils/tools/mongodb"
    "go.mongodb.org/mongo-driver/v2/bson"
)

type User struct {
    Name  string    `bson:"name"`
    Email string    `bson:"email"`
    Age   int       `bson:"age"`
    CreatedAt time.Time `bson:"created_at"`
}

func main() {
    cfg := &mongodb.Config{
        URI:      "mongodb://localhost:27017",
        Database: "test_db",
    }

    client, err := mongodb.NewClient(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close(context.Background())

    ctx := context.Background()

    // 插入
    user := User{
        Name:      "张三",
        Email:     "zhangsan@example.com",
        Age:       25,
        CreatedAt: time.Now(),
    }
    
    result, _ := client.InsertOne(ctx, "users", user)
    fmt.Printf("Inserted ID: %v\n", result.InsertedID)

    // 查询
    var found User
    client.FindOne(ctx, "users", bson.M{"name": "张三"}, &found)
    fmt.Printf("Found: %+v\n", found)

    // 更新
    client.UpdateOne(ctx, "users", 
        bson.M{"name": "张三"},
        bson.M{"$set": bson.M{"age": 26}})

    // 删除
    client.DeleteOne(ctx, "users", bson.M{"name": "张三"})
}
```

## 3. 更多示例

查看 [example.go](example.go) 获取完整的使用示例，包括：
- 批量操作
- 分页查询
- 索引管理
- 事务处理
- 聚合查询
- 连接池配置
- 副本集配置

## 4. 文档

完整的 API 文档请查看 [README.md](README.md)
