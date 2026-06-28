package nacos

import (
	"errors"
	"fmt"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// ConfigClientWrapper Nacos配置客户端包装器（闭包模式）
type ConfigClientWrapper struct {
	client    config_client.IConfigClient
	groupName string
}

// NewConfigClient 创建配置客户端
func NewConfigClient(config *Config) (*ConfigClientWrapper, error) {
	if config == nil {
		return nil, errors.New("nacos config is nil")
	}
	if !config.Enabled {
		return nil, errors.New("nacos is not enabled")
	}

	serverConfigs, clientConfig := config.getConfig()

	// 创建配置客户端
	client, err := clients.NewConfigClient(
		vo.NacosClientParam{
			ClientConfig:  clientConfig,
			ServerConfigs: serverConfigs,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("create config client failed: %w", err)
	}

	wrapper := &ConfigClientWrapper{
		client:    client,
		groupName: config.ClientConfig.GroupName,
	}

	return wrapper, nil
}

// GetConfig 获取配置
func (c *ConfigClientWrapper) GetConfig(dataId string) (string, error) {
	if c.client == nil {
		return "", errors.New("config client not initialized")
	}

	content, err := c.client.GetConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  c.groupName,
	})
	if err != nil {
		return "", fmt.Errorf("get config failed: %w", err)
	}
	return content, nil
}

// ListenConfig 监听配置变化
func (c *ConfigClientWrapper) ListenConfig(dataId string, listener func(namespace, group, dataId, data string)) error {
	if c.client == nil {
		return errors.New("config client not initialized")
	}

	err := c.client.ListenConfig(vo.ConfigParam{
		DataId:   dataId,
		Group:    c.groupName,
		OnChange: listener,
	})
	if err != nil {
		return fmt.Errorf("listen config failed: %w", err)
	}
	return nil
}

// PublishConfig 发布配置
func (c *ConfigClientWrapper) PublishConfig(dataId, content string) (bool, error) {
	if c.client == nil {
		return false, errors.New("config client not initialized")
	}

	success, err := c.client.PublishConfig(vo.ConfigParam{
		DataId:  dataId,
		Group:   c.groupName,
		Content: content,
	})
	if err != nil {
		return false, fmt.Errorf("publish config failed: %w", err)
	}
	return success, nil
}

// DeleteConfig 删除配置
func (c *ConfigClientWrapper) DeleteConfig(dataId string) (bool, error) {
	if c.client == nil {
		return false, errors.New("config client not initialized")
	}

	success, err := c.client.DeleteConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  c.groupName,
	})
	if err != nil {
		return false, fmt.Errorf("delete config failed: %w", err)
	}
	return success, nil
}

// SearchConfig 搜索配置
func (c *ConfigClientWrapper) SearchConfig(search string, pageNo, pageSize uint32) (*model.ConfigPage, error) {
	if c.client == nil {
		return nil, errors.New("config client not initialized")
	}

	configPage, err := c.client.SearchConfig(vo.SearchConfigParam{
		Search:   search,
		PageNo:   int(pageNo),
		PageSize: int(pageSize),
	})
	if err != nil {
		return nil, fmt.Errorf("search config failed: %w", err)
	}
	return configPage, nil
}

// CancelListenConfig 取消监听配置
func (c *ConfigClientWrapper) CancelListenConfig(dataId string) error {
	if c.client == nil {
		return errors.New("config client not initialized")
	}

	err := c.client.CancelListenConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  c.groupName,
	})
	if err != nil {
		return fmt.Errorf("cancel listen config failed: %w", err)
	}
	return nil
}

// GetClient 获取底层配置客户端（用于高级用法）
func (c *ConfigClientWrapper) GetClient() config_client.IConfigClient {
	return c.client
}

// GetGroupName 获取组名
func (c *ConfigClientWrapper) GetGroupName() string {
	return c.groupName
}
