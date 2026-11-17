package sharding

import (
	"sync"
	"testing"
)

// TestSnowflakeNodeLazyCreateAndCache 验证 snowflakeNode 按需惰性创建、缓存复用、越界返回 error。
func TestSnowflakeNodeLazyCreateAndCache(t *testing.T) {
	s := &Sharding{}

	// 初始不应预建任何节点
	if s.snowflakeNodes != nil {
		t.Fatal("初始时 snowflakeNodes 应为 nil（未预建）")
	}

	n5, err := s.snowflakeNode(5)
	if err != nil {
		t.Fatalf("创建节点 5 失败: %v", err)
	}
	if n5 == nil {
		t.Fatal("节点 5 不应为 nil")
	}
	// 数组按容量 1024 惰性分配，但仅被请求的索引会被填充
	if len(s.snowflakeNodes) != 1024 {
		t.Fatalf("snowflakeNodes 长度应为 1024，实际=%d", len(s.snowflakeNodes))
	}
	if s.snowflakeNodes[6] != nil {
		t.Fatal("未被请求的索引 6 不应被创建")
	}

	// 再次获取应命中缓存（返回同一指针）
	again, err := s.snowflakeNode(5)
	if err != nil {
		t.Fatalf("再次获取节点 5 失败: %v", err)
	}
	if again != n5 {
		t.Fatal("同一索引应返回缓存的同一节点指针")
	}

	// 越界索引返回 error
	if _, err := s.snowflakeNode(1024); err == nil {
		t.Fatal("索引 1024 越界应返回 error")
	}
	if _, err := s.snowflakeNode(-1); err == nil {
		t.Fatal("索引 -1 越界应返回 error")
	}
}

// TestGenSnowflakeKeyLazyNoInitialize 验证无需 Initialize 预建，genSnowflakeKey 即可惰性生成主键并缓存节点。
func TestGenSnowflakeKeyLazyNoInitialize(t *testing.T) {
	s := &Sharding{}

	id := s.genSnowflakeKey(0)
	if id == 0 {
		t.Fatal("genSnowflakeKey 应生成非 0 主键")
	}
	if s.snowflakeNodes[0] == nil {
		t.Fatal("调用后索引 0 的节点应被惰性创建并缓存")
	}
}

// TestSnowflakeNodeConcurrent 并发惰性创建不同索引不应数据竞争（配合 -race）。
func TestSnowflakeNodeConcurrent(t *testing.T) {
	s := &Sharding{}

	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func(idx int64) {
			defer wg.Done()
			if _, err := s.snowflakeNode(idx % 1024); err != nil {
				t.Errorf("并发创建节点出错: %v", err)
			}
		}(int64(i))
	}
	wg.Wait()

	// 并发后所有被请求过的索引都应已缓存
	for i := int64(0); i < 128; i++ {
		if s.snowflakeNodes[i] == nil {
			t.Fatalf("索引 %d 的节点应已被惰性创建", i)
		}
	}
}
