package mongodb

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// User 示例用户结构
type User struct {
	ID        string    `bson:"_id,omitempty" json:"id"`
	Name      string    `bson:"name" json:"name"`
	Email     string    `bson:"email" json:"email"`
	Age       int       `bson:"age" json:"age"`
	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

// ExampleBasicUsage 基础使用示例
func ExampleBasicUsage() {
	// 1. 初始化客户端
	cfg := &Config{
		URI:      "mongodb://localhost:27017",
		Database: "test_db",
	}

	client, err := NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close(context.Background())

	ctx := context.Background()

	// 2. 插入单个文档
	user := User{
		Name:      "张三",
		Email:     "zhangsan@example.com",
		Age:       25,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	result, err := client.InsertOne(ctx, "users", user)
	if err != nil {
		log.Printf("Insert failed: %v", err)
		return
	}
	fmt.Printf("Inserted ID: %v\n", result.InsertedID)

	// 3. 查询单个文档
	var foundUser User
	err = client.FindOne(ctx, "users", bson.M{"name": "张三"}, &foundUser)
	if err != nil {
		log.Printf("Find failed: %v", err)
	} else {
		fmt.Printf("Found user: %+v\n", foundUser)
	}

	// 4. 更新文档
	updateResult, err := client.UpdateOne(
		ctx,
		"users",
		bson.M{"name": "张三"},
		bson.M{"$set": bson.M{"age": 26, "updated_at": time.Now()}},
	)
	if err != nil {
		log.Printf("Update failed: %v", err)
	} else {
		fmt.Printf("Modified count: %d\n", updateResult.ModifiedCount)
	}

	// 5. 删除文档
	deleteResult, err := client.DeleteOne(ctx, "users", bson.M{"name": "张三"})
	if err != nil {
		log.Printf("Delete failed: %v", err)
	} else {
		fmt.Printf("Deleted count: %d\n", deleteResult.DeletedCount)
	}
}

// ExampleBatchOperations 批量操作示例
func ExampleBatchOperations() {
	cfg := &Config{
		URI:      "mongodb://localhost:27017",
		Database: "test_db",
	}

	client, err := NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close(context.Background())

	ctx := context.Background()

	// 1. 批量插入
	var users []interface{}
	for i := 1; i <= 10; i++ {
		users = append(users, User{
			Name:      fmt.Sprintf("用户%d", i),
			Email:     fmt.Sprintf("user%d@example.com", i),
			Age:       20 + i,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		})
	}

	insertResult, err := client.InsertMany(ctx, "users", users)
	if err != nil {
		log.Printf("Batch insert failed: %v", err)
		return
	}
	fmt.Printf("Inserted %d documents\n", len(insertResult.InsertedIDs))

	// 2. 批量查询
	allUsers, err := client.Find(ctx, "users", bson.M{})
	if err != nil {
		log.Printf("Find failed: %v", err)
	} else {
		fmt.Printf("Found %d users\n", len(allUsers))
	}

	// 3. 条件查询
	filter := bson.M{"age": bson.M{"$gte": 25}}
	usersOver25, err := client.Find(ctx, "users", filter)
	if err != nil {
		log.Printf("Find failed: %v", err)
	} else {
		fmt.Printf("Users over 25: %d\n", len(usersOver25))
	}

	// 4. 批量更新
	updateResult, err := client.UpdateMany(
		ctx,
		"users",
		bson.M{"age": bson.M{"$lt": 25}},
		bson.M{"$set": bson.M{"updated_at": time.Now()}},
	)
	if err != nil {
		log.Printf("Update many failed: %v", err)
	} else {
		fmt.Printf("Modified %d documents\n", updateResult.ModifiedCount)
	}

	// 5. 批量删除
	deleteResult, err := client.DeleteMany(ctx, "users", bson.M{"age": bson.M{"$gt": 28}})
	if err != nil {
		log.Printf("Delete many failed: %v", err)
	} else {
		fmt.Printf("Deleted %d documents\n", deleteResult.DeletedCount)
	}
}

// ExampleAdvancedQuery 高级查询示例
func ExampleAdvancedQuery() {
	cfg := &Config{
		URI:      "mongodb://localhost:27017",
		Database: "test_db",
	}

	client, err := NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close(context.Background())

	ctx := context.Background()

	// 1. 分页查询
	page := int64(1)
	pageSize := int64(10)
	results, total, err := client.FindWithPagination(ctx, "users", bson.M{}, page, pageSize)
	if err != nil {
		log.Printf("Pagination query failed: %v", err)
	} else {
		fmt.Printf("Total: %d, Current page results: %d\n", total, len(results))
	}

	// 2. 排序查询
	opts := options.Find().SetSort(bson.M{"age": -1}).SetLimit(5)
	top5Users, err := client.FindWithOptions(ctx, "users", bson.M{}, opts)
	if err != nil {
		log.Printf("Sorted query failed: %v", err)
	} else {
		fmt.Printf("Top 5 oldest users: %d\n", len(top5Users))
	}

	// 3. 投影查询（只返回指定字段）
	projOpts := options.Find().
		SetProjection(bson.M{"name": 1, "email": 1, "_id": 0}).
		SetLimit(10)
	projectionResults, err := client.FindWithOptions(ctx, "users", bson.M{}, projOpts)
	if err != nil {
		log.Printf("Projection query failed: %v", err)
	} else {
		fmt.Printf("Projection results: %d\n", len(projectionResults))
	}

	// 4. 聚合查询
	pipeline := []bson.M{
		{"$match": bson.M{"age": bson.M{"$gte": 25}}},
		{"$group": bson.M{
			"_id":    nil,
			"count":  bson.M{"$sum": 1},
			"avgAge": bson.M{"$avg": "$age"},
		}},
	}

	aggResults, err := client.Aggregate(ctx, "users", pipeline)
	if err != nil {
		log.Printf("Aggregation failed: %v", err)
	} else {
		fmt.Printf("Aggregation results: %+v\n", aggResults)
	}

	// 5. Distinct 查询
	distinctAges, err := client.Distinct(ctx, "users", "age", bson.M{})
	if err != nil {
		log.Printf("Distinct query failed: %v", err)
	} else {
		fmt.Printf("Distinct ages: %v\n", distinctAges)
	}
}

// ExampleIndexManagement 索引管理示例
func ExampleIndexManagement() {
	cfg := &Config{
		URI:      "mongodb://localhost:27017",
		Database: "test_db",
	}

	client, err := NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close(context.Background())

	ctx := context.Background()

	// 1. 创建单字段索引
	indexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "email", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("idx_email"),
	}

	indexName, err := client.CreateIndex(ctx, "users", indexModel)
	if err != nil {
		log.Printf("Create index failed: %v", err)
	} else {
		fmt.Printf("Created index: %s\n", indexName)
	}

	// 2. 创建复合索引
	compoundIndex := mongo.IndexModel{
		Keys:    bson.D{{Key: "age", Value: 1}, {Key: "name", Value: 1}},
		Options: options.Index().SetName("idx_age_name"),
	}

	_, err = client.CreateIndex(ctx, "users", compoundIndex)
	if err != nil {
		log.Printf("Create compound index failed: %v", err)
	}

	// 3. 创建 TTL 索引（自动过期）
	ttlIndex := mongo.IndexModel{
		Keys:    bson.D{{Key: "expire_at", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(3600).SetName("idx_expire"),
	}

	_, err = client.CreateIndex(ctx, "sessions", ttlIndex)
	if err != nil {
		log.Printf("Create TTL index failed: %v", err)
	}

	// 4. 列出所有索引
	indexes, err := client.ListIndexes(ctx, "users")
	if err != nil {
		log.Printf("List indexes failed: %v", err)
	} else {
		fmt.Printf("Indexes on users collection: %d\n", len(indexes))
		for _, idx := range indexes {
			fmt.Printf("  - %v\n", idx)
		}
	}

	// 5. 删除索引
	err = client.DropIndex(ctx, "users", "idx_age_name")
	if err != nil {
		log.Printf("Drop index failed: %v", err)
	}
}

// ExampleTransaction 事务示例
func ExampleTransaction() {
	cfg := &Config{
		URI:      "mongodb://localhost:27017/?replicaSet=myReplicaSet",
		Database: "test_db",
	}

	client, err := NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close(context.Background())

	ctx := context.Background()

	// 在事务中执行多个操作
	result, err := client.WithTransaction(ctx, func(sc context.Context) (interface{}, error) {
		// 操作 1: 插入用户
		user := User{
			Name:      "事务用户",
			Email:     "transaction@example.com",
			Age:       30,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		_, err := client.InsertOne(sc, "users", user)
		if err != nil {
			return nil, err
		}

		// 操作 2: 更新统计信息
		_, err = client.UpdateOne(
			sc,
			"statistics",
			bson.M{"_id": "user_count"},
			bson.M{"$inc": bson.M{"count": 1}},
		)
		if err != nil {
			return nil, err
		}

		return "success", nil
	})

	if err != nil {
		log.Printf("Transaction failed: %v", err)
	} else {
		fmt.Printf("Transaction result: %v\n", result)
	}
}

// ExampleGlobalClient 全局客户端示例
func ExampleGlobalClient() {
	// 1. 初始化全局客户端
	cfg := &Config{
		URI:      "mongodb://localhost:27017",
		Database: "test_db",
	}

	if err := InitClient(cfg); err != nil {
		log.Fatalf("Failed to init global client: %v", err)
	}

	// 2. 获取全局客户端
	client := GetClient()
	defer client.Close(context.Background())

	ctx := context.Background()

	// 3. 直接使用全局客户端进行操作
	count, err := client.CountDocuments(ctx, "users", bson.M{})
	if err != nil {
		log.Printf("Count failed: %v", err)
	} else {
		fmt.Printf("Total users: %d\n", count)
	}
}

// ExampleConnectionPool 连接池配置示例
func ExampleConnectionPool() {
	cfg := &Config{
		URI:                    "mongodb://localhost:27017",
		Database:               "test_db",
		MaxPoolSize:            50, // 最大连接数
		MinPoolSize:            10, // 最小连接数
		ConnectTimeout:         5 * time.Second,
		SocketTimeout:          10 * time.Second,
		ServerSelectionTimeout: 5 * time.Second,
		HeartbeatInterval:      5 * time.Second,
	}

	client, err := NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close(context.Background())

	// 测试连接
	ctx := context.Background()
	if err := client.Ping(ctx); err != nil {
		log.Printf("Ping failed: %v", err)
	} else {
		fmt.Println("MongoDB connected successfully")
	}
}

// ExampleReplicaSet 副本集配置示例
func ExampleReplicaSet() {
	cfg := &Config{
		Hosts: []string{
			"mongo1.example.com:27017",
			"mongo2.example.com:27017",
			"mongo3.example.com:27017",
		},
		Database:    "test_db",
		Username:    "admin",
		Password:    "password",
		AuthSource:  "admin",
		ReplicaSet:  "myReplicaSet",
		MaxPoolSize: 100,
	}

	client, err := NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close(context.Background())

	fmt.Println("Connected to MongoDB replica set")
}

// ExampleTLS TLS/SSL 连接示例
func ExampleTLS() {
	cfg := &Config{
		URI:         "mongodb://localhost:27017",
		Database:    "test_db",
		TLS:         true,
		TLSCAFile:   "/path/to/ca.pem",
		TLSCertFile: "/path/to/client-cert.pem",
		TLSKeyFile:  "/path/to/client-key.pem",
	}

	client, err := NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close(context.Background())

	fmt.Println("Connected to MongoDB with TLS")
}
