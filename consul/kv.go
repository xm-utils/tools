package consul

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"
)

// KVPair KV 键值对
type KVPair struct {
	Key         string
	Value       []byte
	Flags       uint64
	Session     string
	CreateIndex uint64
	ModifyIndex uint64
}

// PutKV 写入 KV 配置
func (c *Client) PutKV(key string, value []byte) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	kv := &api.KVPair{
		Key:   key,
		Value: value,
	}

	_, err := c.consulClient.KV().Put(kv, nil)
	if err != nil {
		return fmt.Errorf("failed to put KV: %w", err)
	}

	return nil
}

// PutKVWithFlags 写入 KV 配置（带标志位）
func (c *Client) PutKVWithFlags(key string, value []byte, flags uint64) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	kv := &api.KVPair{
		Key:   key,
		Value: value,
		Flags: flags,
	}

	_, err := c.consulClient.KV().Put(kv, nil)
	if err != nil {
		return fmt.Errorf("failed to put KV with flags: %w", err)
	}

	return nil
}

// GetKV 获取 KV 配置
// 如果不存在，返回 nil, nil
func (c *Client) GetKV(key string) (*KVPair, error) {
	if key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	pair, _, err := c.consulClient.KV().Get(key, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get KV: %w", err)
	}

	if pair == nil {
		return nil, nil
	}

	return &KVPair{
		Key:         pair.Key,
		Value:       pair.Value,
		Flags:       pair.Flags,
		Session:     pair.Session,
		CreateIndex: pair.CreateIndex,
		ModifyIndex: pair.ModifyIndex,
	}, nil
}

// GetKVString 获取字符串类型的 KV 配置
func (c *Client) GetKVString(key string) (string, error) {
	pair, err := c.GetKV(key)
	if err != nil {
		return "", err
	}

	if pair == nil {
		return "", nil
	}

	return string(pair.Value), nil
}

// GetKVJSON 获取 JSON 类型的 KV 配置并解析到指定结构
func (c *Client) GetKVJSON(key string, result interface{}) error {
	pair, err := c.GetKV(key)
	if err != nil {
		return err
	}

	if pair == nil {
		return fmt.Errorf("key %s not found", key)
	}

	err = json.Unmarshal(pair.Value, result)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return nil
}

// ListKV 列出指定前缀下的所有 KV
// prefix: 前缀，例如: "config/"
// recurse: 是否递归列出所有子键
func (c *Client) ListKV(prefix string, recurse bool) ([]*KVPair, error) {
	if prefix == "" {
		return nil, fmt.Errorf("prefix cannot be empty")
	}

	opts := &api.QueryOptions{}

	pairs, _, err := c.consulClient.KV().List(prefix, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to list KV: %w", err)
	}

	var result []*KVPair
	for _, pair := range pairs {
		result = append(result, &KVPair{
			Key:         pair.Key,
			Value:       pair.Value,
			Flags:       pair.Flags,
			Session:     pair.Session,
			CreateIndex: pair.CreateIndex,
			ModifyIndex: pair.ModifyIndex,
		})
	}

	return result, nil
}

// DeleteKV 删除 KV 配置
func (c *Client) DeleteKV(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	_, err := c.consulClient.KV().Delete(key, nil)
	if err != nil {
		return fmt.Errorf("failed to delete KV: %w", err)
	}

	return nil
}

// DeleteKVRecursive 递归删除指定前缀下的所有 KV
func (c *Client) DeleteKVRecursive(prefix string) error {
	if prefix == "" {
		return fmt.Errorf("prefix cannot be empty")
	}

	_, err := c.consulClient.KV().DeleteTree(prefix, nil)
	if err != nil {
		return fmt.Errorf("failed to delete KV tree: %w", err)
	}

	return nil
}

// CAS (Compare-And-Set) 操作，原子更新
// 只有当 ModifyIndex 匹配时才会更新
func (c *Client) CompareAndSet(key string, value []byte, modifyIndex uint64) (bool, error) {
	if key == "" {
		return false, fmt.Errorf("key cannot be empty")
	}

	kv := &api.KVPair{
		Key:         key,
		Value:       value,
		ModifyIndex: modifyIndex,
	}

	updated, _, err := c.consulClient.KV().CAS(kv, nil)
	if err != nil {
		return false, fmt.Errorf("failed to CAS: %w", err)
	}

	return updated, nil
}

// Acquire 获取分布式锁
// key: 锁的键名
// sessionID: 会话 ID
func (c *Client) AcquireLock(key string, sessionID string) (bool, error) {
	if key == "" {
		return false, fmt.Errorf("key cannot be empty")
	}

	if sessionID == "" {
		return false, fmt.Errorf("session ID cannot be empty")
	}

	kv := &api.KVPair{
		Key:     key,
		Value:   []byte(sessionID),
		Session: sessionID,
	}

	acquired, _, err := c.consulClient.KV().Acquire(kv, nil)
	if err != nil {
		return false, fmt.Errorf("failed to acquire lock: %w", err)
	}

	return acquired, nil
}

// Release 释放分布式锁
func (c *Client) ReleaseLock(key string, sessionID string) (bool, error) {
	if key == "" {
		return false, fmt.Errorf("key cannot be empty")
	}

	if sessionID == "" {
		return false, fmt.Errorf("session ID cannot be empty")
	}

	kv := &api.KVPair{
		Key:     key,
		Value:   []byte(sessionID),
		Session: sessionID,
	}

	released, _, err := c.consulClient.KV().Release(kv, nil)
	if err != nil {
		return false, fmt.Errorf("failed to release lock: %w", err)
	}

	return released, nil
}

// WatchKV 监听 KV 变化（阻塞查询）
// key: 要监听的键
// lastIndex: 上次查询的索引，首次调用传 0
// timeout: 超时时间
// 返回: KV 数据、新的索引、错误
func (c *Client) WatchKV(key string, lastIndex uint64, timeout time.Duration) (*KVPair, uint64, error) {
	if key == "" {
		return nil, 0, fmt.Errorf("key cannot be empty")
	}

	opts := &api.QueryOptions{
		WaitIndex: lastIndex,
		WaitTime:  timeout,
	}

	pair, meta, err := c.consulClient.KV().Get(key, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to watch KV: %w", err)
	}

	if pair == nil {
		return nil, meta.LastIndex, nil
	}

	return &KVPair{
		Key:         pair.Key,
		Value:       pair.Value,
		Flags:       pair.Flags,
		Session:     pair.Session,
		CreateIndex: pair.CreateIndex,
		ModifyIndex: pair.ModifyIndex,
	}, meta.LastIndex, nil
}

// CreateSession 创建会话（用于分布式锁）
func (c *Client) CreateSession(name string, ttl time.Duration) (string, error) {
	session := &api.SessionEntry{
		Name: name,
		TTL:  ttl.String(),
	}

	sessionID, _, err := c.consulClient.Session().Create(session, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create session: %w", err)
	}

	return sessionID, nil
}

// DestroySession 销毁会话
func (c *Client) DestroySession(sessionID string) error {
	if sessionID == "" {
		return fmt.Errorf("session ID cannot be empty")
	}

	_, err := c.consulClient.Session().Destroy(sessionID, nil)
	if err != nil {
		return fmt.Errorf("failed to destroy session: %w", err)
	}

	return nil
}

// RenewSession 续期会话
func (c *Client) RenewSession(sessionID string) (*api.SessionEntry, error) {
	if sessionID == "" {
		return nil, fmt.Errorf("session ID cannot be empty")
	}

	entry, _, err := c.consulClient.Session().Renew(sessionID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to renew session: %w", err)
	}

	return entry, nil
}
