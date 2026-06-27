package consul

import (
	"fmt"

	"github.com/hashicorp/consul/api"
)

// HealthStatus 健康状态类型
type HealthStatus string

const (
	HealthPassing     HealthStatus = "passing"
	HealthWarning     HealthStatus = "warning"
	HealthCritical    HealthStatus = "critical"
	HealthMaintenance HealthStatus = "maintenance"
)

// CheckResult 健康检查结果
type CheckResult struct {
	Node        string
	CheckID     string
	Name        string
	Status      string
	Notes       string
	Output      string
	ServiceID   string
	ServiceName string
}

// GetNodeHealth 获取节点的健康状态
func (c *Client) GetNodeHealth(node string) ([]*CheckResult, error) {
	if node == "" {
		return nil, fmt.Errorf("node name cannot be empty")
	}

	checks, _, err := c.consulClient.Health().Node(node, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get node health: %w", err)
	}

	var results []*CheckResult
	for _, check := range checks {
		results = append(results, &CheckResult{
			Node:        check.Node,
			CheckID:     check.CheckID,
			Name:        check.Name,
			Status:      check.Status,
			Notes:       check.Notes,
			Output:      check.Output,
			ServiceID:   check.ServiceID,
			ServiceName: check.ServiceName,
		})
	}

	return results, nil
}

// GetServiceHealth 获取服务的健康状态
func (c *Client) GetServiceHealth(serviceName string) ([]*CheckResult, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("service name cannot be empty")
	}

	checks, _, err := c.consulClient.Health().Checks(serviceName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get service health: %w", err)
	}

	var results []*CheckResult
	for _, check := range checks {
		results = append(results, &CheckResult{
			Node:        check.Node,
			CheckID:     check.CheckID,
			Name:        check.Name,
			Status:      check.Status,
			Notes:       check.Notes,
			Output:      check.Output,
			ServiceID:   check.ServiceID,
			ServiceName: check.ServiceName,
		})
	}

	return results, nil
}

// GetState 获取指定状态的所有检查
func (c *Client) GetState(status HealthStatus) ([]*CheckResult, error) {
	checks, _, err := c.consulClient.Health().State(string(status), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get state: %w", err)
	}

	var results []*CheckResult
	for _, check := range checks {
		results = append(results, &CheckResult{
			Node:        check.Node,
			CheckID:     check.CheckID,
			Name:        check.Name,
			Status:      check.Status,
			Notes:       check.Notes,
			Output:      check.Output,
			ServiceID:   check.ServiceID,
			ServiceName: check.ServiceName,
		})
	}

	return results, nil
}

// UpdateTTLCheck 更新 TTL 类型的健康检查状态
// checkID: 检查 ID
// status: 状态 (passing, warning, critical)
// note: 备注信息
func (c *Client) UpdateTTLCheck(checkID, status, note string) error {
	if checkID == "" {
		return fmt.Errorf("check ID cannot be empty")
	}

	err := c.consulClient.Agent().UpdateTTL(checkID, note, status)
	if err != nil {
		return fmt.Errorf("failed to update TTL check: %w", err)
	}

	return nil
}

// RegisterCheck 注册自定义健康检查
func (c *Client) RegisterCheck(check *AgentCheck) error {
	if check == nil {
		return fmt.Errorf("check cannot be nil")
	}

	if check.CheckID == "" {
		return fmt.Errorf("check ID cannot be empty")
	}

	if check.Name == "" {
		return fmt.Errorf("check name cannot be empty")
	}

	agentCheck := &api.AgentCheckRegistration{
		ID:    check.CheckID,
		Name:  check.Name,
		Notes: check.Notes,
	}

	// 设置检查类型
	if check.HTTP != "" {
		agentCheck.HTTP = check.HTTP
		agentCheck.Interval = check.Interval
		agentCheck.Timeout = check.Timeout
	} else if check.TCP != "" {
		agentCheck.TCP = check.TCP
		agentCheck.Interval = check.Interval
		agentCheck.Timeout = check.Timeout
	} else if check.TTL != "" {
		agentCheck.TTL = check.TTL
	}

	err := c.consulClient.Agent().CheckRegister(agentCheck)
	if err != nil {
		return fmt.Errorf("failed to register check: %w", err)
	}

	return nil
}

// DeregisterCheck 注销健康检查
func (c *Client) DeregisterCheck(checkID string) error {
	if checkID == "" {
		return fmt.Errorf("check ID cannot be empty")
	}

	err := c.consulClient.Agent().CheckDeregister(checkID)
	if err != nil {
		return fmt.Errorf("failed to deregister check: %w", err)
	}

	return nil
}

// GetLocalChecks 获取本地代理上的所有健康检查
func (c *Client) GetLocalChecks() (map[string]*api.AgentCheck, error) {
	checks, err := c.consulClient.Agent().Checks()
	if err != nil {
		return nil, fmt.Errorf("failed to get local checks: %w", err)
	}

	return checks, nil
}
