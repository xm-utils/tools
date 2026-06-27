package redis

import (
	"context"
	"testing"
)

// TestZAdd 测试向有序集合添加成员
func TestZAdd(t *testing.T) {

	key := "test_zadd"
	ctx := context.Background()
	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	pairs := map[interface{}]float64{
		"member1": 10.0,
		"member2": 20.5,
		"member3": 30.8,
	}

	err := ZAdd(ctx, key, pairs)
	if err != nil {
		t.Errorf("ZAdd failed: %v", err)
		return
	}

	// 验证添加的成员
	card, err := ZCard(ctx, key)
	if err != nil {
		t.Errorf("ZCard failed: %v", err)
		return
	}

	if card != 3 {
		t.Errorf("Expected cardinality 3, got %d", card)
	}

	// 验证分数
	score, err := ZScore(ctx, key, "member1")
	if err != nil {
		t.Errorf("ZScore failed: %v", err)
		return
	}

	if score != 10.0 {
		t.Errorf("Expected score 10.0, got %f", score)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZAdd test passed")
}

// TestZAddByScore 测试通过分数添加成员
func TestZAddByScore(t *testing.T) {
	ctx := context.Background()
	key := "test_zadd_score"

	// 清空可能存在的键
	Delete(ctx, key)

	err := ZAddByScore(ctx, key, "member1", 15.5)
	if err != nil {
		t.Errorf("ZAddByScore failed: %v", err)
		return
	}

	// 验证添加的成员
	card, err := ZCard(ctx, key)
	if err != nil {
		t.Errorf("ZCard failed: %v", err)
		return
	}

	if card != 1 {
		t.Errorf("Expected cardinality 1, got %d", card)
	}

	// 验证分数
	score, err := ZScore(ctx, key, "member1")
	if err != nil {
		t.Errorf("ZScore failed: %v", err)
		return
	}

	if score != 15.5 {
		t.Errorf("Expected score 15.5, got %f", score)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZAddByScore test passed")
}

// TestZCard 测试获取有序集合成员数
func TestZCard(t *testing.T) {
	ctx := context.Background()
	key := "test_zcard"

	// 清空可能存在的键
	Delete(ctx, key)

	// 测试空集合
	card, err := ZCard(ctx, key)
	if err != nil {
		t.Errorf("ZCard failed: %v", err)
		return
	}

	if card != 0 {
		t.Errorf("Expected cardinality 0 for empty set, got %d", card)
	}

	// 添加成员
	ZAddByScore(ctx, key, "member1", 10.0)
	ZAddByScore(ctx, key, "member2", 20.0)

	// 测试非空集合
	card, err = ZCard(ctx, key)
	if err != nil {
		t.Errorf("ZCard failed: %v", err)
		return
	}

	if card != 2 {
		t.Errorf("Expected cardinality 2, got %d", card)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZCard test passed")
}

// TestZCount 测试计算指定区间分数的成员数
func TestZCount(t *testing.T) {
	ctx := context.Background()
	key := "test_zcount"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	ZAddByScore(ctx, key, "member1", 10.0)
	ZAddByScore(ctx, key, "member2", 20.0)
	ZAddByScore(ctx, key, "member3", 30.0)
	ZAddByScore(ctx, key, "member4", 40.0)

	// 计算分数在 15-35 之间的成员数
	count, err := ZCount(ctx, key, "15", "35")
	if err != nil {
		t.Errorf("ZCount failed: %v", err)
		return
	}

	if count != 2 { // member2 (20.0) 和 member3 (30.0)
		t.Errorf("Expected count 2, got %d", count)
	}

	// 计算所有成员数
	count, err = ZCount(ctx, key, "-inf", "+inf")
	if err != nil {
		t.Errorf("ZCount failed: %v", err)
		return
	}

	if count != 4 {
		t.Errorf("Expected count 4, got %d", count)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZCount test passed")
}

// TestZScore 测试获取成员分数
func TestZScore(t *testing.T) {
	ctx := context.Background()
	key := "test_zscore"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	ZAddByScore(ctx, key, "member1", 25.5)

	// 获取存在的成员分数
	score, err := ZScore(ctx, key, "member1")
	if err != nil {
		t.Errorf("ZScore failed: %v", err)
		return
	}

	if score != 25.5 {
		t.Errorf("Expected score 25.5, got %f", score)
	}

	// 尝试获取不存在的成员分数
	score, err = ZScore(ctx, key, "nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent member")
	}

	if score != 0 {
		t.Errorf("Expected score 0 for nonexistent member, got %f", score)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZScore test passed")
}

// TestZIncrBy 测试增加成员分数
func TestZIncrBy(t *testing.T) {

	ctx := context.Background()
	key := "test_zincrby"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加初始成员
	ZAddByScore(ctx, key, "member1", 10.0)

	// 增加分数
	newScore, err := ZIncrBy(ctx, key, "member1", 5.5)
	if err != nil {
		t.Errorf("ZIncrBy failed: %v", err)
		return
	}

	if newScore != 15.5 {
		t.Errorf("Expected new score 15.5, got %f", newScore)
	}

	// 验证实际分数
	actualScore, err := ZScore(ctx, key, "member1")
	if err != nil {
		t.Errorf("ZScore failed: %v", err)
		return
	}

	if actualScore != 15.5 {
		t.Errorf("Expected actual score 15.5, got %f", actualScore)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZIncrBy test passed")
}

// TestZInterStore 测试有序集合交集并存储
func TestZInterStore(t *testing.T) {

	ctx := context.Background()
	key1 := "test_zinterstore1"
	key2 := "test_zinterstore2"
	dest := "test_zinterstore_dest"

	// 清空可能存在的键
	Delete(ctx, key1)
	Delete(ctx, key2)
	Delete(ctx, dest)

	// 添加测试数据
	ZAdd(ctx, key1, map[interface{}]float64{
		"a": 1,
		"b": 2,
		"c": 3,
	})

	ZAdd(ctx, key2, map[interface{}]float64{
		"b": 1,
		"c": 2,
		"d": 4,
	})

	// 计算交集并存储
	count, err := ZInterStore(ctx, dest, key1, key2)
	if err != nil {
		t.Errorf("ZInterStore failed: %v", err)
		return
	}

	// 验证结果数量
	if count != 2 { // "b" 和 "c" 是两个集合的交集
		t.Errorf("Expected count 2, got %d", count)
	}

	// 验证交集成员
	card, err := ZCard(ctx, dest)
	if err != nil {
		t.Errorf("ZCard failed: %v", err)
		return
	}

	if card != 2 {
		t.Errorf("Expected destination cardinality 2, got %d", card)
	}

	// 清理
	Delete(ctx, key1)
	Delete(ctx, key2)
	Delete(ctx, dest)
	t.Log("ZInterStore test passed")
}

// TestZLexCount 测试字典区间成员计数
func TestZLexCount(t *testing.T) {

	ctx := context.Background()
	key := "test_zlexcount"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据，使用相同的分数以便测试字典序
	ZAdd(ctx, key, map[interface{}]float64{
		"apple":  0,
		"banana": 0,
		"cherry": 0,
		"date":   0,
	})

	// 计算字典区间内的成员数 [apple, cherry]
	count, err := ZLexCount(ctx, key, "[apple", "[cherry")
	if err != nil {
		t.Errorf("ZLexCount failed: %v", err)
		return
	}

	if count != 3 { // apple, banana, cherry
		t.Errorf("Expected lex count 3, got %d", count)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZLexCount test passed")
}

// TestZRange 测试通过索引区间获取成员
func TestZRange(t *testing.T) {
	ctx := context.Background()
	key := "test_zrange"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	ZAdd(ctx, key, map[interface{}]float64{
		"member1": 10,
		"member2": 20,
		"member3": 30,
		"member4": 40,
	})

	// 获取索引 0-2 的成员
	members, err := ZRange[string](ctx, key, 0, 2)
	if err != nil {
		t.Errorf("ZRange failed: %v", err)
		return
	}

	if len(members) != 3 {
		t.Errorf("Expected 3 members, got %d", len(members))
	}

	// 验证顺序（按分数从小到大）
	if members[0] != "member1" || members[1] != "member2" || members[2] != "member3" {
		t.Errorf("Expected members in order [member1, member2, member3], got %v", members)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZRange test passed")
}

// TestZRangeWithScores 测试获取带分数的成员
func TestZRangeWithScores(t *testing.T) {
	ctx := context.Background()
	key := "test_zrangewithscores"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	ZAdd(ctx, key, map[interface{}]float64{
		"member1": 10,
		"member2": 20,
		"member3": 30,
	})

	// 获取带分数的成员
	zMembers, err := ZRangeWithScores(ctx, key, 0, -1)
	if err != nil {
		t.Errorf("ZRangeWithScores failed: %v", err)
		return
	}

	if len(zMembers) != 3 {
		t.Errorf("Expected 3 members with scores, got %d", len(zMembers))
	}

	// 验证分数
	if zMembers[0].Score != 10 || zMembers[1].Score != 20 || zMembers[2].Score != 30 {
		t.Errorf("Unexpected scores: %v", zMembers)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZRangeWithScores test passed")
}

// TestZRangeByLex 测试通过字典区间获取成员
func TestZRangeByLex(t *testing.T) {
	ctx := context.Background()
	key := "test_zrangebylex"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据，使用相同分数以便测试字典序
	ZAdd(ctx, key, map[interface{}]float64{
		"apple":  0,
		"banana": 0,
		"cherry": 0,
		"date":   0,
	})

	// 获取字典区间内的成员
	members, err := ZRangeByLex(ctx, key, "[apple", "[cherry")
	if err != nil {
		t.Errorf("ZRangeByLex failed: %v", err)
		return
	}

	if len(members) != 3 { // apple, banana, cherry
		t.Errorf("Expected 3 members, got %d", len(members))
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZRangeByLex test passed")
}

// TestZRangeByScore 测试通过分数区间获取成员
func TestZRangeByScore(t *testing.T) {
	ctx := context.Background()
	key := "test_zrangebyscore"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	ZAdd(ctx, key, map[interface{}]float64{
		"low":    5,
		"medium": 15,
		"high":   25,
		"top":    35,
	})

	// 获取分数在 10-30 之间的成员
	members, err := ZRangeByScore[string](ctx, key, "10", "30")
	if err != nil {
		t.Errorf("ZRangeByScore failed: %v", err)
		return
	}

	if len(members) != 2 { // medium (15) 和 high (25)
		t.Errorf("Expected 2 members, got %d", len(members))
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZRangeByScore test passed")
}

// TestZRangeByScoreWithScores 测试通过分数区间获取带分数的成员
func TestZRangeByScoreWithScores(t *testing.T) {
	ctx := context.Background()
	key := "test_zrangebyscorewithscores"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	ZAdd(ctx, key, map[interface{}]float64{
		"low":    5,
		"medium": 15,
		"high":   25,
	})

	// 获取分数在 10-20 之间的带分数成员
	zMembers, err := ZRangeByScoreWithScores(ctx, key, "10", "20")
	if err != nil {
		t.Errorf("ZRangeByScoreWithScores failed: %v", err)
		return
	}

	if len(zMembers) != 1 { // medium (15)
		t.Errorf("Expected 1 member with score, got %d", len(zMembers))
	}

	if zMembers[0].Member != "medium" || zMembers[0].Score != 15 {
		t.Errorf("Expected member 'medium' with score 15, got %v", zMembers[0])
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZRangeByScoreWithScores test passed")
}

// TestZRank 测试获取成员索引
func TestZRank(t *testing.T) {
	ctx := context.Background()
	key := "test_zrank"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	ZAdd(ctx, key, map[interface{}]float64{
		"first":  10,
		"second": 20,
		"third":  30,
	})

	// 获取成员排名（从0开始）
	rank, err := ZRank(ctx, key, "first")
	if err != nil {
		t.Errorf("ZRank failed: %v", err)
		return
	}

	if rank != 0 { // first 应该排在第0位
		t.Errorf("Expected rank 0, got %d", rank)
	}

	rank, err = ZRank(ctx, key, "second")
	if err != nil {
		t.Errorf("ZRank failed: %v", err)
		return
	}

	if rank != 1 { // second 应该排在第1位
		t.Errorf("Expected rank 1, got %d", rank)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZRank test passed")
}

// TestZRevRank 测试获取成员反向排名
func TestZRevRank(t *testing.T) {
	ctx := context.Background()
	key := "test_zrevrank"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	ZAdd(ctx, key, map[interface{}]float64{
		"lowest":  10,
		"medium":  20,
		"highest": 30,
	})

	// 获取反向排名（分数从高到低）
	rank, err := ZRevRank(ctx, key, "highest") // 最高分应该是第0名
	if err != nil {
		t.Errorf("ZRevRank failed: %v", err)
		return
	}

	if rank != 0 {
		t.Errorf("Expected reverse rank 0 for highest score, got %d", rank)
	}

	rank, err = ZRevRank(ctx, key, "lowest") // 最低分应该是第2名
	if err != nil {
		t.Errorf("ZRevRank failed: %v", err)
		return
	}

	if rank != 2 {
		t.Errorf("Expected reverse rank 2 for lowest score, got %d", rank)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZRevRank test passed")
}

// TestZRevRange 测试反向索引区间获取成员
func TestZRevRange(t *testing.T) {
	ctx := context.Background()
	key := "test_zrevrange"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	ZAdd(ctx, key, map[interface{}]float64{
		"first":  10,
		"second": 20,
		"third":  30,
		"fourth": 40,
	})

	// 获取反向排序的前2个成员（分数从高到低）
	members, err := ZRevRange[string](ctx, key, 0, 1)
	if err != nil {
		t.Errorf("ZRevRange failed: %v", err)
		return
	}

	if len(members) != 2 {
		t.Errorf("Expected 2 members, got %d", len(members))
	}

	// 验证顺序（分数从高到低）
	if members[0] != "fourth" || members[1] != "third" {
		t.Errorf("Expected members in reverse order [fourth, third], got %v", members)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZRevRange test passed")
}

// TestZRevRangeByScore 测试反向分数区间获取成员
func TestZRevRangeByScore(t *testing.T) {
	ctx := context.Background()
	key := "test_zrevrangebyscore"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	ZAdd(ctx, key, map[interface{}]float64{
		"very_low":  5,
		"low":       10,
		"medium":    20,
		"high":      30,
		"very_high": 35,
	})

	// 获取分数在 35 到 15 之间（从高到低）的成员
	members, err := ZRevRangeByScore[string](ctx, key, "35", "15")
	if err != nil {
		t.Errorf("ZRevRangeByScore failed: %v", err)
		return
	}

	if len(members) != 3 { // very_high (35), high (30), medium (20)
		t.Errorf("Expected 3 members, got %d", len(members))
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZRevRangeByScore test passed")
}

// TestZRevRangeByScoreWithScores 测试反向分数区间获取带分数的成员
func TestZRevRangeByScoreWithScores(t *testing.T) {
	ctx := context.Background()
	key := "test_zrevrangebyscorewithscores"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	ZAdd(ctx, key, map[interface{}]float64{
		"low":    10,
		"medium": 20,
		"high":   30,
	})

	// 获取分数在 30 到 15 之间（从高到低）的带分数成员
	zMembers, err := ZRevRangeByScoreWithScores(ctx, key, "30", "15")
	if err != nil {
		t.Errorf("ZRevRangeByScoreWithScores failed: %v", err)
		return
	}

	// 应该返回 high (30) 和 medium (20)
	if len(zMembers) != 2 {
		t.Errorf("Expected 2 members with scores, got %d", len(zMembers))
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZRevRangeByScoreWithScores test passed")
}

// TestZRem 测试移除有序集合成员
func TestZRem(t *testing.T) {
	ctx := context.Background()
	key := "test_zrem"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	ZAdd(ctx, key, map[interface{}]float64{
		"member1": 10,
		"member2": 20,
		"member3": 30,
		"member4": 40,
	})

	// 移除两个成员
	removed, err := ZRem(ctx, key, "member1", "member3")
	if err != nil {
		t.Errorf("ZRem failed: %v", err)
		return
	}

	if removed != 2 {
		t.Errorf("Expected 2 removals, got %d", removed)
	}

	// 验证剩余成员数量
	card, err := ZCard(ctx, key)
	if err != nil {
		t.Errorf("ZCard failed: %v", err)
		return
	}

	if card != 2 { // member2 和 member4 仍存在
		t.Errorf("Expected cardinality 2 after removal, got %d", card)
	}

	// 尝试移除不存在的成员
	removed, err = ZRem(ctx, key, "nonexistent")
	if err != nil {
		t.Errorf("ZRem failed: %v", err)
		return
	}

	if removed != 0 {
		t.Errorf("Expected 0 removals for nonexistent member, got %d", removed)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZRem test passed")
}

// TestZRemRangeByLex 测试移除字典区间内的成员
func TestZRemRangeByLex(t *testing.T) {
	ctx := context.Background()
	key := "test_zremrangebylex"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据，使用相同分数以便测试字典序
	ZAdd(ctx, key, map[interface{}]float64{
		"apple":  0,
		"banana": 0,
		"cherry": 0,
		"date":   0,
	})

	// 移除字典区间 [banana, date] 内的成员
	removed, err := ZRemRangeByLex(ctx, key, "[banana", "[date")
	if err != nil {
		t.Errorf("ZRemRangeByLex failed: %v", err)
		return
	}

	if removed != 3 { // banana, cherry, date
		t.Errorf("Expected 3 removals, got %d", removed)
	}

	// 验证剩余成员
	card, err := ZCard(ctx, key)
	if err != nil {
		t.Errorf("ZCard failed: %v", err)
		return
	}

	if card != 1 { // 只剩下 apple
		t.Errorf("Expected cardinality 1 after removal, got %d", card)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZRemRangeByLex test passed")
}

// TestZRemRangeByRank 测试移除排名区间内的成员
func TestZRemRangeByRank(t *testing.T) {
	ctx := context.Background()
	key := "test_zremrangebyrank"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	ZAdd(ctx, key, map[interface{}]float64{
		"first":  10,
		"second": 20,
		"third":  30,
		"fourth": 40,
	})

	// 移除排名 1-2 的成员（second 和 third）
	removed, err := ZRemRangeByRank(ctx, key, 1, 2)
	if err != nil {
		t.Errorf("ZRemRangeByRank failed: %v", err)
		return
	}

	if removed != 2 {
		t.Errorf("Expected 2 removals, got %d", removed)
	}

	// 验证剩余成员
	card, err := ZCard(ctx, key)
	if err != nil {
		t.Errorf("ZCard failed: %v", err)
		return
	}

	if card != 2 { // first 和 fourth 仍存在
		t.Errorf("Expected cardinality 2 after removal, got %d", card)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZRemRangeByRank test passed")
}

// TestZRemRangeByScore 测试移除分数区间内的成员
func TestZRemRangeByScore(t *testing.T) {
	ctx := context.Background()
	key := "test_zremrangebyscore"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	ZAdd(ctx, key, map[interface{}]float64{
		"very_low":  5,
		"low":       10,
		"medium":    20,
		"high":      30,
		"very_high": 35,
	})

	// 移除分数 10-30 之间的成员
	removed, err := ZRemRangeByScore(ctx, key, 10, 30)
	if err != nil {
		t.Errorf("ZRemRangeByScore failed: %v", err)
		return
	}

	if removed != 3 { // low (10), medium (20), high (30)
		t.Errorf("Expected 3 removals, got %d", removed)
	}

	// 验证剩余成员
	card, err := ZCard(ctx, key)
	if err != nil {
		t.Errorf("ZCard failed: %v", err)
		return
	}

	if card != 2 { // very_low (5) 和 very_high (35) 仍存在
		t.Errorf("Expected cardinality 2 after removal, got %d", card)
	}

	// 清理
	Delete(ctx, key)
	t.Log("ZRemRangeByScore test passed")
}

// TestZUnionStore 测试有序集合并集并存储
func TestZUnionStore(t *testing.T) {
	ctx := context.Background()
	key1 := "test_zunionstore1"
	key2 := "test_zunionstore2"
	dest := "test_zunionstore_dest"

	// 清空可能存在的键
	Delete(ctx, key1)
	Delete(ctx, key2)
	Delete(ctx, dest)

	// 添加测试数据
	ZAdd(ctx, key1, map[interface{}]float64{
		"a": 1,
		"b": 2,
	})

	ZAdd(ctx, key2, map[interface{}]float64{
		"b": 1, // 注意：b 在两个集合中都有，分数不同
		"c": 3,
	})

	// 计算并集并存储
	count, err := ZUnionStore(ctx, dest, key1, key2)
	if err != nil {
		t.Errorf("ZUnionStore failed: %v", err)
		return
	}

	// 并集应该包含 a, b, c 三个成员
	if count != 3 {
		t.Errorf("Expected union count 3, got %d", count)
	}

	card, err := ZCard(ctx, dest)
	if err != nil {
		t.Errorf("ZCard failed: %v", err)
		return
	}

	if card != 3 {
		t.Errorf("Expected destination cardinality 3, got %d", card)
	}

	// 清理
	Delete(ctx, key1)
	Delete(ctx, key2)
	Delete(ctx, dest)
	t.Log("ZUnionStore test passed")
}
