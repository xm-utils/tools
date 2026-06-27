package redis

import (
	"context"
	"testing"
	"time"
)

const (
	set_key = "test_set_key"
)

// TestSAdd 测试向集合添加成员
func TestSAdd(t *testing.T) {
	ctx, cannel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cannel()
	// 清空可能存在的键
	if err := Delete(ctx, set_key); err != nil {
		t.Errorf("Delete failed: %v", err)
		return
	}

	// 添加字符串成员
	count, err := SAdd(ctx, set_key, "member1", "member2", "member3")
	if err != nil {
		t.Errorf("SAdd failed: %v", err)
		return
	}

	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}

	// 验证集合大小
	card := SCard(ctx, set_key)
	if card != 3 {
		t.Errorf("Expected cardinality 3, got %d", card)
	}

	// 添加重复成员
	count, err = SAdd(ctx, set_key, "member1", "member4") // member1 已存在
	if err != nil {
		t.Errorf("SAdd failed: %v", err)
		return
	}

	if count != 1 { // 只有 member4 是新添加的
		t.Errorf("Expected count 1, got %d", count)
	}

	// 清理
	if err = Delete(ctx, set_key); err != nil {
		t.Errorf("Delete failed: %v", err)
		return
	}
	t.Log("SAdd test passed")
}

// TestSCard 测试获取集合成员数量
func TestSCard(t *testing.T) {

	ctx := context.Background()
	// 清空可能存在的键
	Delete(ctx, set_key)

	// 空集合
	card := SCard(ctx, set_key)
	if card != 0 {
		t.Errorf("Expected cardinality 0 for empty set, got %d", card)
	}

	// 添加成员
	SAdd(ctx, set_key, "member1", "member2")

	// 非空集合
	card = SCard(ctx, set_key)
	if card != 2 {
		t.Errorf("Expected cardinality 2, got %d", card)
	}

	// 清理
	Delete(ctx, set_key)
	t.Log("SCard test passed")
}

// TestSDiff 测试集合差集
func TestSDiff(t *testing.T) {
	ctx := context.Background()

	key1 := "test_set_diff1"
	key2 := "test_set_diff2"

	// 清空可能存在的键
	Delete(ctx, key1)
	Delete(ctx, key2)

	// 添加测试数据
	SAdd(ctx, key1, "a", "b", "c")
	SAdd(ctx, key2, "b", "c", "d")

	// 计算差集
	diff, err := SDiff[string](ctx, key1, key2)
	if err != nil {
		t.Errorf("SDiff failed: %v", err)
		return
	}

	if len(diff) != 1 || diff[0] != "a" {
		t.Errorf("Expected diff ['a'], got %v", diff)
	}

	// 清理
	Delete(ctx, key1)
	Delete(ctx, key2)
	t.Log("SDiff test passed")
}

// TestSDiffStore 测试集合差集并存储
func TestSDiffStore(t *testing.T) {
	ctx := context.Background()
	key1 := "test_set_diffstore1"
	key2 := "test_set_diffstore2"
	dest := "test_set_diffstore_dest"

	// 清空可能存在的键
	Delete(ctx, key1)
	Delete(ctx, key2)
	Delete(ctx, dest)

	// 添加测试数据
	SAdd(ctx, key1, "a", "b", "c")
	SAdd(ctx, key2, "b", "c", "d")

	// 计算差集并存储
	count, err := SDiffStore(ctx, dest, key1, key2)
	if err != nil {
		t.Errorf("SDiffStore failed: %v", err)
		return
	}

	if count != 1 {
		t.Errorf("Expected count 1, got %d", count)
	}

	// 验证结果
	card := SCard(ctx, dest)
	if card != 1 {
		t.Errorf("Expected stored set cardinality 1, got %d", card)
	}

	// 清理
	Delete(ctx, key1)
	Delete(ctx, key2)
	Delete(ctx, dest)
	t.Log("SDiffStore test passed")
}

// TestSInter 测试集合交集
func TestSInter(t *testing.T) {
	ctx := context.Background()
	key1 := "test_set_inter1"
	key2 := "test_set_inter2"

	// 清空可能存在的键
	Delete(ctx, key1)
	Delete(ctx, key2)

	// 添加测试数据
	SAdd(ctx, key1, "a", "b", "c")
	SAdd(ctx, key2, "b", "c", "d")

	// 计算交集
	inter, err := SInter[string](ctx, key1, key2)
	if err != nil {
		t.Errorf("SInter failed: %v", err)
		return
	}

	if len(inter) != 2 {
		t.Errorf("Expected 2 intersection elements, got %d", len(inter))
	}

	// 验证元素
	expected := map[string]bool{"b": true, "c": true}
	for _, item := range inter {
		if !expected[item] {
			t.Errorf("Unexpected intersection element: %s", item)
		}
	}

	// 清理
	Delete(ctx, key1)
	Delete(ctx, key2)
	t.Log("SInter test passed")
}

// TestSInterStore 测试集合交集并存储
func TestSInterStore(t *testing.T) {

	ctx := context.Background()
	key1 := "test_set_interstore1"
	key2 := "test_set_interstore2"
	dest := "test_set_interstore_dest"

	// 清空可能存在的键
	Delete(ctx, key1)
	Delete(ctx, key2)
	Delete(ctx, dest)

	// 添加测试数据
	SAdd(ctx, key1, "a", "b", "c")
	SAdd(ctx, key2, "b", "c", "d")

	// 计算交集并存储
	count, err := SInterStore(ctx, dest, key1, key2)
	if err != nil {
		t.Errorf("SInterStore failed: %v", err)
		return
	}

	if count != 2 {
		t.Errorf("Expected count 2, got %d", count)
	}

	// 验证结果
	card := SCard(ctx, dest)
	if card != 2 {
		t.Errorf("Expected stored set cardinality 2, got %d", card)
	}

	// 清理
	Delete(ctx, key1)
	Delete(ctx, key2)
	Delete(ctx, dest)
	t.Log("SInterStore test passed")
}

// TestSIsMember 测试判断元素是否属于集合
func TestSIsMember(t *testing.T) {

	ctx := context.Background()
	key := "test_set_ismember"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	SAdd(ctx, key, "member1", "member2")

	// 测试存在的成员
	isMember, err := SIsMember(ctx, key, "member1")
	if err != nil {
		t.Errorf("SIsMember failed: %v", err)
		return
	}

	if !isMember {
		t.Error("Expected member1 to be in set")
	}

	// 测试不存在的成员
	isMember, err = SIsMember(ctx, key, "member3")
	if err != nil {
		t.Errorf("SIsMember failed: %v", err)
		return
	}

	if isMember {
		t.Error("Expected member3 not to be in set")
	}

	// 清理
	Delete(ctx, key)
	t.Log("SIsMember test passed")
}

// TestSMembers 测试获取集合所有成员
func TestSMembers(t *testing.T) {
	ctx := context.Background()
	key := "test_set_members"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	SAdd(ctx, key, "member1", "member2", "member3")

	// 获取所有成员
	members, err := SMembers[string](ctx, key)
	if err != nil {
		t.Errorf("SMembers failed: %v", err)
		return
	}

	if len(members) != 3 {
		t.Errorf("Expected 3 members, got %d", len(members))
	}

	// 验证包含预期成员
	expected := map[string]bool{"member1": true, "member2": true, "member3": true}
	for _, member := range members {
		if !expected[member] {
			t.Errorf("Unexpected member: %s", member)
		}
	}

	// 清理
	Delete(ctx, key)
	t.Log("SMembers test passed")
}

// TestSMove 测试移动集合成员
func TestSMove(t *testing.T) {
	ctx := context.Background()
	source := "test_set_move_source"
	dest := "test_set_move_dest"

	// 清空可能存在的键
	Delete(ctx, source)
	Delete(ctx, dest)

	// 添加测试数据
	SAdd(ctx, source, "member1", "member2")
	SAdd(ctx, dest, "dest_member1")

	// 移动成员
	moved, err := SMove(ctx, source, dest, "member1")
	if err != nil {
		t.Errorf("SMove failed: %v", err)
		return
	}

	if !moved {
		t.Error("Expected move to succeed")
	}

	// 验证源集合中不再包含该成员
	isSourceMember, _ := SIsMember(ctx, source, "member1")
	if isSourceMember {
		t.Error("member1 should not be in source after move")
	}

	// 验证目标集合中包含该成员
	isDestMember, _ := SIsMember(ctx, dest, "member1")
	if !isDestMember {
		t.Error("member1 should be in destination after move")
	}

	// 尝试移动不存在的成员
	moved, err = SMove(ctx, source, dest, "nonexistent")
	if err != nil {
		t.Errorf("SMove failed: %v", err)
		return
	}

	if moved {
		t.Error("Expected move to fail for nonexistent member")
	}

	// 清理
	Delete(ctx, source)
	Delete(ctx, dest)
	t.Log("SMove test passed")
}

// TestSPop 测试随机弹出集合成员
func TestSPop(t *testing.T) {
	ctx := context.Background()
	key := "test_set_pop"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	SAdd(ctx, key, "member1", "member2", "member3")

	// 弹出一个成员
	popped, err := SPop[string](ctx, key)
	if err != nil {
		t.Errorf("SPop failed: %v", err)
		return
	}

	if popped == "" {
		t.Error("Expected to pop a member")
	}

	// 验证弹出的成员确实不在集合中了
	isMember, _ := SIsMember(ctx, key, popped)
	if isMember {
		t.Error("Popped member should not be in set anymore")
	}

	// 验证集合大小减少
	card := SCard(ctx, key)
	if card != 2 {
		t.Errorf("Expected cardinality 2 after pop, got %d", card)
	}

	// 清理
	Delete(ctx, key)
	t.Log("SPop test passed")
}

// TestSRandMember 测试随机获取集合成员
func TestSRandMember(t *testing.T) {
	ctx := context.Background()
	key := "test_set_randmember"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	SAdd(ctx, key, "member1", "member2", "member3", "member4", "member5")

	// 获取一个随机成员
	members, err := SRandMember[string](ctx, key, 1)
	if err != nil {
		t.Errorf("SRandMember failed: %v", err)
		return
	}

	if len(members) != 1 {
		t.Errorf("Expected 1 random member, got %d", len(members))
	}

	// 获取多个随机成员
	members, err = SRandMember[string](ctx, key, 3)
	if err != nil {
		t.Errorf("SRandMember failed: %v", err)
		return
	}

	if len(members) != 3 {
		t.Errorf("Expected 3 random members, got %d", len(members))
	}

	// 获取超过集合大小的成员数
	members, err = SRandMember[string](ctx, key, 10)
	if err != nil {
		t.Errorf("SRandMember failed: %v", err)
		return
	}

	if len(members) != 5 { // 集合只有5个成员
		t.Errorf("Expected 5 random members (all), got %d", len(members))
	}

	// 清理
	Delete(ctx, key)
	t.Log("SRandMember test passed")
}

// TestSRem 测试移除集合成员
func TestSRem(t *testing.T) {
	ctx := context.Background()
	key := "test_set_rem"

	// 清空可能存在的键
	Delete(ctx, key)

	// 添加测试数据
	SAdd(ctx, key, "member1", "member2", "member3")

	// 移除存在的成员
	removed, err := SRem(ctx, key, "member1", "member2")
	if err != nil {
		t.Errorf("SRem failed: %v", err)
		return
	}

	if removed != 2 {
		t.Errorf("Expected 2 removals, got %d", removed)
	}

	// 验证成员已被移除
	isMember, _ := SIsMember(ctx, key, "member1")
	if isMember {
		t.Error("member1 should be removed")
	}

	isMember, _ = SIsMember(ctx, key, "member2")
	if isMember {
		t.Error("member2 should be removed")
	}

	// 验证未移除的成员仍在
	isMember, _ = SIsMember(ctx, key, "member3")
	if !isMember {
		t.Error("member3 should still be in set")
	}

	// 尝试移除不存在的成员
	removed, err = SRem(ctx, key, "nonexistent")
	if err != nil {
		t.Errorf("SRem failed: %v", err)
		return
	}

	if removed != 0 {
		t.Errorf("Expected 0 removals for nonexistent member, got %d", removed)
	}

	// 清理
	Delete(ctx, key)
	t.Log("SRem test passed")
}

// TestSUnion 测试集合并集
func TestSUnion(t *testing.T) {
	ctx := context.Background()
	key1 := "test_set_union1"
	key2 := "test_set_union2"

	// 清空可能存在的键
	Delete(ctx, key1)
	Delete(ctx, key2)

	// 添加测试数据
	SAdd(ctx, key1, "a", "b", "c")
	SAdd(ctx, key2, "c", "d", "e")

	// 计算并集
	union, err := SUnion[string](ctx, key1, key2)
	if err != nil {
		t.Errorf("SUnion failed: %v", err)
		return
	}

	if len(union) != 5 { // a, b, c, d, e
		t.Errorf("Expected 5 union elements, got %d", len(union))
	}

	// 验证包含预期成员
	expected := map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true}
	for _, item := range union {
		if !expected[item] {
			t.Errorf("Unexpected union element: %s", item)
		}
	}

	// 清理
	Delete(ctx, key1)
	Delete(ctx, key2)
	t.Log("SUnion test passed")
}

// TestSUnionStore 测试集合并集并存储
func TestSUnionStore(t *testing.T) {
	ctx := context.Background()
	key1 := "test_set_unionstore1"
	key2 := "test_set_unionstore2"
	dest := "test_set_unionstore_dest"

	// 清空可能存在的键
	Delete(ctx, key1)
	Delete(ctx, key2)
	Delete(ctx, dest)

	// 添加测试数据
	SAdd(ctx, key1, "a", "b", "c")
	SAdd(ctx, key2, "c", "d", "e")

	// 计算并集并存储
	count, err := SUnionStore(ctx, dest, key1, key2)
	if err != nil {
		t.Errorf("SUnionStore failed: %v", err)
		return
	}

	if count != 5 { // a, b, c, d, e
		t.Errorf("Expected count 5, got %d", count)
	}

	// 验证结果
	card := SCard(ctx, dest)
	if card != 5 {
		t.Errorf("Expected stored set cardinality 5, got %d", card)
	}

	// 清理
	Delete(ctx, key1)
	Delete(ctx, key2)
	Delete(ctx, dest)
	t.Log("SUnionStore test passed")
}
