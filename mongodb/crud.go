package mongodb

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// InsertOne 插入单个文档
func (c *Client) InsertOne(ctx context.Context, collectionName string, document interface{}, dbName ...string) (*mongo.InsertOneResult, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}
	return coll.InsertOne(ctx, document)
}

// InsertMany 批量插入文档
func (c *Client) InsertMany(ctx context.Context, collectionName string, documents []interface{}, dbName ...string) (*mongo.InsertManyResult, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	if len(documents) == 0 {
		return nil, errors.New("documents cannot be empty")
	}

	return coll.InsertMany(ctx, documents)
}

// FindOne 查询单个文档
func (c *Client) FindOne(ctx context.Context, collectionName string, filter interface{}, result interface{}, dbName ...string) error {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	err := coll.FindOne(ctx, filter).Decode(result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return errors.New("document not found")
		}
		return err
	}

	return nil
}

// FindOneWithOptions 使用选项查询单个文档
func (c *Client) FindOneWithOptions(ctx context.Context, collectionName string, filter interface{}, result interface{}, opts *options.FindOneOptions, dbName ...string) error {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	// v2 API: FindOne 不再直接支持 options，需要使用 Find 并限制为 1
	findOpts := options.Find().SetLimit(1)
	if opts != nil {
		// 复制 FindOneOptions 到 FindOptions
		if opts.Sort != nil {
			findOpts.SetSort(opts.Sort)
		}
		if opts.Projection != nil {
			findOpts.SetProjection(opts.Projection)
		}
	}

	cursor, err := coll.Find(ctx, filter, findOpts)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	if !cursor.Next(ctx) {
		return errors.New("document not found")
	}

	return cursor.Decode(result)
}

// Find 查询多个文档
func (c *Client) Find(ctx context.Context, collectionName string, filter interface{}, dbName ...string) ([]map[string]interface{}, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []map[string]interface{}
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

// FindWithDecode 查询多个文档并解码到指定类型
func (c *Client) FindWithDecode(ctx context.Context, collectionName string, filter interface{}, result interface{}, dbName ...string) error {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, result); err != nil {
		return err
	}

	return nil
}

// FindWithOptions 使用选项查询多个文档
func (c *Client) FindWithOptions(ctx context.Context, collectionName string, filter interface{}, opts *options.FindOptionsBuilder, dbName ...string) ([]map[string]interface{}, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []map[string]interface{}
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

// UpdateOne 更新单个文档
func (c *Client) UpdateOne(ctx context.Context, collectionName string, filter interface{}, update interface{}, dbName ...string) (*mongo.UpdateResult, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	return coll.UpdateOne(ctx, filter, update)
}

// UpdateOneWithOptions 使用选项更新单个文档
func (c *Client) UpdateOneWithOptions(ctx context.Context, collectionName string, filter interface{}, update interface{}, opts *options.UpdateOneOptionsBuilder, dbName ...string) (*mongo.UpdateResult, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	return coll.UpdateOne(ctx, filter, update, opts)
}

// UpdateMany 更新多个文档
func (c *Client) UpdateMany(ctx context.Context, collectionName string, filter interface{}, update interface{}, dbName ...string) (*mongo.UpdateResult, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	return coll.UpdateMany(ctx, filter, update)
}

// ReplaceOne 替换单个文档
func (c *Client) ReplaceOne(ctx context.Context, collectionName string, filter interface{}, replacement interface{}, dbName ...string) (*mongo.UpdateResult, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	return coll.ReplaceOne(ctx, filter, replacement)
}

// DeleteOne 删除单个文档
func (c *Client) DeleteOne(ctx context.Context, collectionName string, filter interface{}, dbName ...string) (*mongo.DeleteResult, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	return coll.DeleteOne(ctx, filter)
}

// DeleteMany 删除多个文档
func (c *Client) DeleteMany(ctx context.Context, collectionName string, filter interface{}, dbName ...string) (*mongo.DeleteResult, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	return coll.DeleteMany(ctx, filter)
}

// CountDocuments 统计文档数量
func (c *Client) CountDocuments(ctx context.Context, collectionName string, filter interface{}, dbName ...string) (int64, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	return coll.CountDocuments(ctx, filter)
}

// EstimatedDocumentCount 获取估算的文档数量
func (c *Client) EstimatedDocumentCount(ctx context.Context, collectionName string, dbName ...string) (int64, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	return coll.EstimatedDocumentCount(ctx)
}

// Aggregate 聚合查询
func (c *Client) Aggregate(ctx context.Context, collectionName string, pipeline interface{}, dbName ...string) ([]map[string]interface{}, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []map[string]interface{}
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

// AggregateWithDecode 聚合查询并解码到指定类型
func (c *Client) AggregateWithDecode(ctx context.Context, collectionName string, pipeline interface{}, result interface{}, dbName ...string) error {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, result); err != nil {
		return err
	}

	return nil
}

// Distinct 获取 distinct 值
func (c *Client) Distinct(ctx context.Context, collectionName string, fieldName string, filter interface{}, dbName ...string) ([]interface{}, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	result := coll.Distinct(ctx, fieldName, filter)
	if err := result.Err(); err != nil {
		return nil, err
	}

	var values []interface{}
	err := result.Decode(&values)
	return values, err
}

// CreateIndex 创建索引
func (c *Client) CreateIndex(ctx context.Context, collectionName string, model mongo.IndexModel, dbName ...string) (string, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	return coll.Indexes().CreateOne(ctx, model)
}

// CreateManyIndexes 创建多个索引
func (c *Client) CreateManyIndexes(ctx context.Context, collectionName string, models []mongo.IndexModel, dbName ...string) ([]string, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	return coll.Indexes().CreateMany(ctx, models)
}

// DropIndex 删除索引
func (c *Client) DropIndex(ctx context.Context, collectionName string, indexName string, dbName ...string) error {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	return coll.Indexes().DropOne(ctx, indexName)
}

// DropAllIndexes 删除所有索引（除了 _id 索引）
func (c *Client) DropAllIndexes(ctx context.Context, collectionName string, dbName ...string) error {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	return coll.Indexes().DropAll(ctx)
}

// ListIndexes 列出所有索引
func (c *Client) ListIndexes(ctx context.Context, collectionName string, dbName ...string) ([]bson.M, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var indexes []bson.M
	if err = cursor.All(ctx, &indexes); err != nil {
		return nil, err
	}

	return indexes, nil
}

// BulkWrite 批量写操作
func (c *Client) BulkWrite(ctx context.Context, collectionName string, models []mongo.WriteModel, dbName ...string) (*mongo.BulkWriteResult, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	if len(models) == 0 {
		return nil, errors.New("models cannot be empty")
	}

	return coll.BulkWrite(ctx, models)
}

// Watch 监听集合变更（需要副本集或分片集群）
func (c *Client) Watch(ctx context.Context, collectionName string, pipeline interface{}, dbName ...string) (*mongo.ChangeStream, error) {
	coll := c.Collection(collectionName, dbName...)
	if ctx == nil {
		ctx = context.Background()
	}

	return coll.Watch(ctx, pipeline)
}

// StartSession 启动会话
func (c *Client) StartSession(ctx context.Context) (*mongo.Session, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	session, err := c.client.StartSession()
	if err != nil {
		return nil, err
	}

	return session, nil
}

// WithTransaction 在事务中执行操作
func (c *Client) WithTransaction(ctx context.Context, fn func(context.Context) (interface{}, error), dbName ...string) (interface{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var result interface{}
	err := c.client.UseSession(ctx, func(sc context.Context) error {
		var err error
		result, err = fn(sc)
		return err
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// ==================== 便捷方法 ====================

// InsertOneWithTTL 插入带过期时间的文档（需要集合有 TTL 索引）
func (c *Client) InsertOneWithTTL(ctx context.Context, collectionName string, document interface{}, ttl time.Duration, dbName ...string) (*mongo.InsertOneResult, error) {
	// 添加过期时间字段
	docMap, ok := document.(bson.M)
	if !ok {
		// 尝试转换为 bson.M
		data, err := bson.Marshal(document)
		if err != nil {
			return nil, err
		}
		docMap = make(bson.M)
		if err := bson.Unmarshal(data, &docMap); err != nil {
			return nil, err
		}
	}

	docMap["expire_at"] = time.Now().Add(ttl)

	return c.InsertOne(ctx, collectionName, docMap, dbName...)
}

// FindByID 根据 _id 查询文档
func (c *Client) FindByID(ctx context.Context, collectionName string, id interface{}, result interface{}, dbName ...string) error {
	filter := bson.M{"_id": id}
	return c.FindOne(ctx, collectionName, filter, result, dbName...)
}

// UpdateByID 根据 _id 更新文档
func (c *Client) UpdateByID(ctx context.Context, collectionName string, id interface{}, update interface{}, dbName ...string) (*mongo.UpdateResult, error) {
	filter := bson.M{"_id": id}
	return c.UpdateOne(ctx, collectionName, filter, update, dbName...)
}

// DeleteByID 根据 _id 删除文档
func (c *Client) DeleteByID(ctx context.Context, collectionName string, id interface{}, dbName ...string) (*mongo.DeleteResult, error) {
	filter := bson.M{"_id": id}
	return c.DeleteOne(ctx, collectionName, filter, dbName...)
}

// FindWithPagination 分页查询
func (c *Client) FindWithPagination(ctx context.Context, collectionName string, filter interface{}, page int64, pageSize int64, dbName ...string) ([]map[string]interface{}, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 1000 {
		pageSize = 10
	}

	skip := (page - 1) * pageSize

	opts := options.Find().
		SetSkip(skip).
		SetLimit(pageSize).
		SetSort(bson.M{"_id": -1})

	results, err := c.FindWithOptions(ctx, collectionName, filter, opts, dbName...)
	if err != nil {
		return nil, 0, err
	}

	total, err := c.CountDocuments(ctx, collectionName, filter, dbName...)
	if err != nil {
		return nil, 0, err
	}

	return results, total, nil
}

// UpsertOne 插入或更新单个文档
func (c *Client) UpsertOne(ctx context.Context, collectionName string, filter interface{}, update interface{}, dbName ...string) (*mongo.UpdateResult, error) {
	opts := options.UpdateOne().SetUpsert(true)
	return c.UpdateOneWithOptions(ctx, collectionName, filter, update, opts, dbName...)
}
