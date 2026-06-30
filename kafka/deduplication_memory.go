package kafka

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// MemoryDeduplicationStore 基于内存的去重存储（适用于单机或测试环境）
type MemoryDeduplicationStore struct {
	mu       sync.RWMutex
	records  map[string]time.Time // key -> 过期时间
	cleaner  *time.Ticker
	stopChan chan struct{}
}

// NewMemoryDeduplicationStore 创建内存去重存储
func NewMemoryDeduplicationStore() *MemoryDeduplicationStore {
	store := &MemoryDeduplicationStore{
		records:  make(map[string]time.Time),
		stopChan: make(chan struct{}),
		cleaner:  time.NewTicker(5 * time.Minute), // 每5分钟清理一次
	}

	// 启动定期清理
	go store.startCleanup()

	return store
}

// IsDuplicate 检查消息是否已处理
func (s *MemoryDeduplicationStore) IsDuplicate(key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	expireTime, exists := s.records[key]
	if !exists {
		return false, nil
	}

	// 检查是否已过期
	if time.Now().After(expireTime) {
		return false, nil
	}

	return true, nil
}

// MarkProcessed 标记消息已处理
func (s *MemoryDeduplicationStore) MarkProcessed(key string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.records[key] = time.Now().Add(ttl)
	return nil
}

// Close 关闭存储
func (s *MemoryDeduplicationStore) Close() error {
	close(s.stopChan)
	s.cleaner.Stop()
	return nil
}

// startCleanup 启动定期清理过期记录
func (s *MemoryDeduplicationStore) startCleanup() {
	for {
		select {
		case <-s.cleaner.C:
			s.cleanup()
		case <-s.stopChan:
			return
		}
	}
}

// cleanup 清理过期记录
func (s *MemoryDeduplicationStore) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	count := 0

	for key, expireTime := range s.records {
		if now.After(expireTime) {
			delete(s.records, key)
			count++
		}
	}

	if count > 0 {
		logrus.Debugf("清理了 %d 条过期的去重记录\n", count)
	}
}

// GetRecordCount 获取当前记录数（用于监控）
func (s *MemoryDeduplicationStore) GetRecordCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records)
}
