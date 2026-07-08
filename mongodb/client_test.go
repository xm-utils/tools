package mongodb

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// TestNewClient 测试客户端创建
func TestNewClient(t *testing.T) {
	cfg := &Config{
		URI:      "mongodb://localhost:27017",
		Database: "test_db",
	}

	client, err := NewClient(cfg)
	if err != nil {
		t.Skipf("MongoDB not available, skipping test: %v", err)
		return
	}
	defer client.Close(context.Background())

	// 测试连接
	ctx := context.Background()
	if err := client.Ping(ctx); err != nil {
		t.Errorf("Ping failed: %v", err)
	}

	t.Log("MongoDB client created successfully")
}

// TestCRUDOperations 测试 CRUD 操作
func TestCRUDOperations(t *testing.T) {
	cfg := &Config{
		URI:      "mongodb://localhost:27017",
		Database: "test_db",
	}

	client, err := NewClient(cfg)
	if err != nil {
		t.Skipf("MongoDB not available, skipping test: %v", err)
		return
	}
	defer client.Close(context.Background())

	ctx := context.Background()
	collection := "test_users"

	// 清理测试数据
	defer func() {
		_, _ = client.DeleteMany(ctx, collection, bson.M{})
	}()

	// 1. 测试插入
	user := User{
		Name:      "测试用户",
		Email:     "test@example.com",
		Age:       25,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	result, err := client.InsertOne(ctx, collection, user)
	if err != nil {
		t.Fatalf("InsertOne failed: %v", err)
	}
	t.Logf("Inserted document with ID: %v", result.InsertedID)

	// 2. 测试查询
	var foundUser User
	err = client.FindOne(ctx, collection, bson.M{"email": "test@example.com"}, &foundUser)
	if err != nil {
		t.Fatalf("FindOne failed: %v", err)
	}

	if foundUser.Name != user.Name {
		t.Errorf("Expected name %s, got %s", user.Name, foundUser.Name)
	}
	t.Logf("Found user: %+v", foundUser)

	// 3. 测试更新
	updateResult, err := client.UpdateOne(
		ctx,
		collection,
		bson.M{"email": "test@example.com"},
		bson.M{"$set": bson.M{"age": 26}},
	)
	if err != nil {
		t.Fatalf("UpdateOne failed: %v", err)
	}
	t.Logf("Modified count: %d", updateResult.ModifiedCount)

	// 4. 测试统计
	count, err := client.CountDocuments(ctx, collection, bson.M{})
	if err != nil {
		t.Fatalf("CountDocuments failed: %v", err)
	}
	t.Logf("Document count: %d", count)

	// 5. 测试删除
	deleteResult, err := client.DeleteOne(ctx, collection, bson.M{"email": "test@example.com"})
	if err != nil {
		t.Fatalf("DeleteOne failed: %v", err)
	}
	t.Logf("Deleted count: %d", deleteResult.DeletedCount)
}

// TestBatchOperations 测试批量操作
func TestBatchOperations(t *testing.T) {
	cfg := &Config{
		URI:      "mongodb://localhost:27017",
		Database: "test_db",
	}

	client, err := NewClient(cfg)
	if err != nil {
		t.Skipf("MongoDB not available, skipping test: %v", err)
		return
	}
	defer client.Close(context.Background())

	ctx := context.Background()
	collection := "test_batch"

	// 清理测试数据
	defer func() {
		_, _ = client.DeleteMany(ctx, collection, bson.M{})
	}()

	// 批量插入
	var users []interface{}
	for i := 1; i <= 5; i++ {
		users = append(users, User{
			Name:      "用户" + string(rune('0'+i)),
			Email:     "user" + string(rune('0'+i)) + "@example.com",
			Age:       20 + i,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		})
	}

	result, err := client.InsertMany(ctx, collection, users)
	if err != nil {
		t.Fatalf("InsertMany failed: %v", err)
	}
	t.Logf("Inserted %d documents", len(result.InsertedIDs))

	// 批量查询
	allUsers, err := client.Find(ctx, collection, bson.M{})
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	t.Logf("Found %d users", len(allUsers))

	// 批量更新
	updateResult, err := client.UpdateMany(
		ctx,
		collection,
		bson.M{"age": bson.M{"$lt": 25}},
		bson.M{"$set": bson.M{"updated_at": time.Now()}},
	)
	if err != nil {
		t.Fatalf("UpdateMany failed: %v", err)
	}
	t.Logf("Modified %d documents", updateResult.ModifiedCount)
}

// TestPagination 测试分页查询
func TestPagination(t *testing.T) {
	cfg := &Config{
		URI:      "mongodb://localhost:27017",
		Database: "test_db",
	}

	client, err := NewClient(cfg)
	if err != nil {
		t.Skipf("MongoDB not available, skipping test: %v", err)
		return
	}
	defer client.Close(context.Background())

	ctx := context.Background()
	collection := "test_pagination"

	// 准备测试数据
	var users []interface{}
	for i := 1; i <= 25; i++ {
		users = append(users, User{
			Name:      "用户" + string(rune('0'+i%10)),
			Email:     "user" + string(rune('0'+i)) + "@example.com",
			Age:       20 + i%10,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		})
	}

	_, err = client.InsertMany(ctx, collection, users)
	if err != nil {
		t.Fatalf("InsertMany failed: %v", err)
	}

	// 清理测试数据
	defer func() {
		_, _ = client.DeleteMany(ctx, collection, bson.M{})
	}()

	// 测试分页
	page := int64(1)
	pageSize := int64(10)
	results, total, err := client.FindWithPagination(ctx, collection, bson.M{}, page, pageSize)
	if err != nil {
		t.Fatalf("FindWithPagination failed: %v", err)
	}

	t.Logf("Total: %d, Page: %d, Results: %d", total, page, len(results))

	if total != 25 {
		t.Errorf("Expected total 25, got %d", total)
	}

	if len(results) != 10 {
		t.Errorf("Expected 10 results, got %d", len(results))
	}
}

// TestIndexManagement 测试索引管理
func TestIndexManagement(t *testing.T) {
	cfg := &Config{
		URI:      "mongodb://localhost:27017",
		Database: "test_db",
	}

	client, err := NewClient(cfg)
	if err != nil {
		t.Skipf("MongoDB not available, skipping test: %v", err)
		return
	}
	defer client.Close(context.Background())

	ctx := context.Background()
	collection := "test_indexes"

	// 创建索引
	indexModel := mongo.IndexModel{
		Keys: bson.M{"email": 1},
	}

	indexName, err := client.CreateIndex(ctx, collection, indexModel)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}
	t.Logf("Created index: %s", indexName)

	// 列出索引
	indexes, err := client.ListIndexes(ctx, collection)
	if err != nil {
		t.Fatalf("ListIndexes failed: %v", err)
	}
	t.Logf("Found %d indexes", len(indexes))

	// 删除索引
	err = client.DropIndex(ctx, collection, indexName)
	if err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}
	t.Log("Index dropped successfully")
}
