package nacos

import (
	"net"

	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
)

type Config struct {
	Enabled      bool           `yaml:"enabled" json:"enabled"`
	ServerConfig []ServerConfig `yaml:"serverConfig" json:"serverConfig"`
	ClientConfig ClientConfig   `yaml:"clientConfig" json:"clientConfig"`
}

func (c Config) getConfig() ([]constant.ServerConfig, *constant.ClientConfig) {
	if c.ClientConfig.GroupName == "" {
		c.ClientConfig.GroupName = "DEFAULT_GROUP"
	}

	return c.getServerConfig(), c.getClientConfig()
}

func (c Config) getClientConfig() *constant.ClientConfig {
	config := c.ClientConfig
	return &constant.ClientConfig{
		TimeoutMs:            config.TimeoutMs,
		BeatInterval:         config.BeatInterval,
		NamespaceId:          config.NamespaceId, // 命名空间ID，public命名空间填空字符串
		AppName:              config.AppName,
		AppKey:               config.AppKey,
		Endpoint:             config.Endpoint,
		CacheDir:             config.CacheDir,
		DisableUseSnapShot:   config.DisableUseSnapShot,
		UpdateThreadNum:      config.UpdateThreadNum,
		NotLoadCacheAtStart:  config.NotLoadCacheAtStart,
		UpdateCacheWhenEmpty: config.UpdateCacheWhenEmpty,
		Username:             config.Username,
		Password:             config.Password,
		LogDir:               config.LogDir,
		LogLevel:             config.LogLevel,
		ContextPath:          config.ContextPath,
		AppendToStdout:       config.AppendToStdout,
		TLSCfg:               config.getTLSCfg(),
		AsyncUpdateService:   config.AsyncUpdateService,
		EndpointContextPath:  config.EndpointContextPath,
		EndpointQueryParams:  config.EndpointQueryParams,
		ClusterName:          config.ClusterName,
		AppConnLabels:        config.AppConnLabels,
	}
}

func (c Config) getServerConfig() []constant.ServerConfig {

	result := make([]constant.ServerConfig, len(c.ServerConfig))
	for i, config := range c.ServerConfig {
		result[i] = constant.ServerConfig{
			Scheme:      config.Scheme,
			IpAddr:      config.IpAddr,
			Port:        config.Port,
			GrpcPort:    config.GrpcPort,
			ContextPath: config.ContextPath,
		}
	}
	return result
}

type ServerConfig struct {
	Scheme      string `yaml:"scheme" json:"scheme,omitempty"`           // Nacos服务器协议，默认为http，在2.0版本中不是必需的
	IpAddr      string `yaml:"ipAddr" json:"ipAddr,omitempty"`           // Nacos服务器地址
	Port        uint64 `yaml:"port" json:"port,omitempty"`               // Nacos服务器端口
	GrpcPort    uint64 `yaml:"grpcPort" json:"grpcPort,omitempty"`       // Nacos服务器gRPC端口，默认为服务器端口+1000，不是必需的
	ContextPath string `yaml:"contextPath" json:"contextPath,omitempty"` // Nacos服务器上下文路径，默认为/nacos，在2.0版本中不是必需的
}

type ClientConfig struct {
	TimeoutMs            uint64            `yaml:"timeoutMs" json:"timeoutMs,omitempty"`                       // 请求Nacos服务器的超时时间，默认值为10000毫秒
	BeatInterval         int64             `yaml:"beatInterval" json:"beatInterval,omitempty"`                 // 向服务器发送心跳的时间间隔，默认值为5000毫秒
	NamespaceId          string            `yaml:"namespaceId" json:"namespaceId,omitempty"`                   // Nacos的命名空间ID。当命名空间为public时，此处填写空字符串
	GroupName            string            `yaml:"groupName" json:"groupName,omitempty"`                       // Nacos的组名称，默认值为DEFAULT_GROUP
	ContextPath          string            `yaml:"contextPath" json:"contextPath,omitempty"`                   // Nacos服务器上下文路径
	AppName              string            `yaml:"appName" json:"appName,omitempty"`                           // 应用名称
	AppKey               string            `yaml:"appKey" json:"appKey,omitempty"`                             // 客户端身份信息
	Username             string            `yaml:"username" json:"username,omitempty"`                         // Nacos认证用户名
	Password             string            `yaml:"password" json:"password,omitempty"`                         // Nacos认证密码
	CacheDir             string            `yaml:"cacheDir" json:"cacheDir,omitempty"`                         // 持久化Nacos服务信息的目录，默认值为当前路径
	DisableUseSnapShot   bool              `yaml:"disableUseSnapShot" json:"disableUseSnapShot,omitempty"`     // 开关，默认为false，表示当获取远程配置失败时，使用本地缓存文件
	UpdateThreadNum      int               `yaml:"updateThreadNum" json:"updateThreadNum,omitempty"`           // 更新Nacos服务信息的goroutine数量，默认值为20
	NotLoadCacheAtStart  bool              `yaml:"notLoadCacheAtStart" json:"notLoadCacheAtStart,omitempty"`   // 启动时不加载CacheDir中持久化的Nacos服务信息
	UpdateCacheWhenEmpty bool              `yaml:"updateCacheWhenEmpty" json:"updateCacheWhenEmpty,omitempty"` // 当从服务器获取到空的服务实例时也更新缓存
	LogDir               string            `yaml:"logDir" json:"logDir,omitempty"`                             // 日志目录，默认为当前路径
	LogLevel             string            `yaml:"logLevel" json:"logLevel,omitempty"`                         // 日志级别，必须是debug、info、warn、error，默认值为info
	AppendToStdout       bool              `yaml:"appendToStdout" json:"appendToStdout,omitempty"`             // 是否将日志追加到标准输出
	TLSCfg               TLSConfig         `yaml:"TLSCfg" json:"TLSCfg"`                                       // TLS配置
	AsyncUpdateService   bool              `yaml:"asyncUpdateService" json:"asyncUpdateService,omitempty"`     // 开启通过查询异步更新服务
	Endpoint             string            `yaml:"endpoint" json:"endpoint,omitempty"`                         // 用于获取Nacos服务器地址的端点
	EndpointContextPath  string            `yaml:"endpointContextPath" json:"endpointContextPath,omitempty"`   // 地址服务器端点上下文路径
	EndpointQueryParams  string            `yaml:"endpointQueryParams" json:"endpointQueryParams,omitempty"`   // 地址服务器端点查询参数
	ClusterName          string            `yaml:"clusterName" json:"clusterName,omitempty"`                   // 地址服务器集群名称
	AppConnLabels        map[string]string `yaml:"appConnLabels" json:"appConnLabels,omitempty"`               // 应用连接标签
}

func (c ClientConfig) getTLSCfg() constant.TLSConfig {
	return constant.TLSConfig{
		Appointed:          c.TLSCfg.Appointed,
		Enable:             c.TLSCfg.Enable,
		TrustAll:           c.TLSCfg.TrustAll,
		CaFile:             c.TLSCfg.CaFile,
		CertFile:           c.TLSCfg.CertFile,
		KeyFile:            c.TLSCfg.KeyFile,
		ServerNameOverride: c.TLSCfg.ServerNameOverride,
	}
}

type TLSConfig struct {
	Appointed          bool   `yaml:"appointed" json:"appointed,omitempty"`                     // 是否指定，如果为false，将从环境变量获取
	Enable             bool   `yaml:"enable" json:"enable,omitempty"`                           // 启用TLS
	TrustAll           bool   `yaml:"trust_all" json:"trustAll,omitempty"`                      // 信任所有服务器
	CaFile             string `yaml:"ca_file" json:"caFile,omitempty"`                          // 客户端验证服务器证书时使用
	CertFile           string `yaml:"cert_file" json:"certFile,omitempty"`                      // 服务器验证客户端证书时使用
	KeyFile            string `yaml:"key_file" json:"keyFile,omitempty"`                        // 服务器验证客户端证书时使用
	ServerNameOverride string `yaml:"server_name_override" json:"serverNameOverride,omitempty"` // 仅用于测试的服务器名称覆盖
}

// GetLocalIP 获取本地IP地址
func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		// 检查IP地址判断是否是回环地址
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}

	return "", nil
}
