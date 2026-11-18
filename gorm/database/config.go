package database

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/go-querystring/query"
)

// ------------------[mysql]-------------------

type MysqlConfig struct {
	// 连接池配置
	MaxIdleConns    int `yaml:"db_max_idle_conns" json:"maxIdleConns" comment:"最大空闲连接数"`            // 最大空闲连接数
	MaxOpenConns    int `yaml:"db_max_open_conns" json:"maxOpenConns" comment:"最大打开连接数"`            // 最大打开连接数
	ConnMaxLifetime int `yaml:"db_conn_max_lifetime" json:"connMaxLifetime" comment:"连接最大存活时间(秒)"`  // 连接最大存活时间（秒）
	ConnMaxIdleTime int `yaml:"db_conn_max_idle_time" json:"connMaxIdleTime" comment:"连接最大空闲时间(秒)"` // 连接最大空闲时间（秒）

	Debug       string `yaml:"db_debug" json:"debug" comment:"是否调试模式"`
	TablePrefix string `yaml:"db_table_prefix" json:"tablePrefix" comment:"表前缀"`

	// 主库配置
	Master *MysqlNodeConfig `yaml:"db_master" json:"master" comment:"主节点配置"`
	// 一主多从读写分离配置（基于 gorm.io/plugin/dbresolver）
	// 主库即上方 Master 节点；Slaves 为多个读库(从)，每个节点需自行填写完整连接信息
	Slaves []MysqlNodeConfig `yaml:"db_slaves" json:"slaves" comment:"读节点(从)列表，各节点独立配置连接信息；Charset/Location 留空使用默认值"`
}

// MysqlNodeConfig 数据库节点连接配置（主库 Master 与从库 Slaves 元素共用）：
// 每个节点独立提供 Host/Port/User/Password/Database，不继承其他节点配置；
// Charset/Location 留空时 Url 生成 DSN 使用默认值 utf8mb4 / Local
type MysqlNodeConfig struct {
	Host     string `yaml:"host" json:"host" comment:"数据库IP（域名）"`
	Port     string `yaml:"port" json:"port" comment:"数据库端口"`
	User     string `yaml:"user" json:"user" comment:"数据库连接用户名"`
	Password string `yaml:"pwd" json:"password" comment:"数据库连接密码"`
	Database string `yaml:"database" json:"database" comment:"数据库名称"`
	Charset  string `yaml:"charset,omitempty" json:"charset" comment:"字符集类型，留空使用默认值 utf8mb4"`
	Location string `yaml:"location,omitempty" json:"timeLocation" comment:"时区，留空使用默认值 Local"`
}

type LinkParam struct {
	Loc       string `url:"loc"`
	Charset   string `url:"charset"`
	ParseTime bool   `url:"parseTime"`
}

//func (c *MysqlConfig) Url() string { ... } // 旧版扁平配置已移除，DSN 生成改由节点级 MysqlNodeConfig.Url() 负责

// Url 生成本节点的 DSN，完全使用自身字段（不继承其他节点配置）；
// Charset/Location 留空时回落到默认值 utf8mb4 / Local
func (n *MysqlNodeConfig) Url() string {
	user, pwd, database := n.User, n.Password, n.Database
	charset, loc := n.Charset, n.Location

	if charset == "" {
		charset = "utf8mb4"
	}
	if loc == "" {
		loc = "Local"
	}

	values, _ := query.Values(LinkParam{Loc: loc, Charset: charset, ParseTime: true})
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s", user, pwd, n.Host, n.Port, database, values.Encode())
}

// hasReadWriteSplit 判断是否配置了从库（需要启用 DBResolver 读写分离）。
func (c *MysqlConfig) hasReadWriteSplit() bool {
	return len(c.Slaves) > 0
}

// validate 校验单个数据库节点建立连接所需的必填字段，缺失时返回明确 error，避免生成
// 畸形 DSN 后直到连库阶段才暴露晦涩错误。Password 允许为空（部分环境允许空密码），
// 但 User/Host/Port/Database 必须非空。主库与每个从库节点均各自调用本方法独立校验。
func (n *MysqlNodeConfig) validate() error {
	missing := make([]string, 0, 4)
	if n.User == "" {
		missing = append(missing, "User")
	}
	if n.Host == "" {
		missing = append(missing, "Host")
	}
	if n.Port == "" {
		missing = append(missing, "Port")
	}
	if n.Database == "" {
		missing = append(missing, "Database")
	}
	if len(missing) > 0 {
		return fmt.Errorf("database config missing required fields: %s", strings.Join(missing, ", "))
	}

	return nil
}

// poolSettings 返回归一化后的连接池参数，未配置时使用合理默认值。
func (c *MysqlConfig) poolSettings() (maxIdle, maxOpen int, lifetime, idleTime time.Duration) {
	maxIdle, maxOpen = c.MaxIdleConns, c.MaxOpenConns
	if maxIdle <= 0 {
		maxIdle = 10
	}
	if maxOpen <= 0 {
		maxOpen = 100
	}

	lifetimeSec, idleSec := c.ConnMaxLifetime, c.ConnMaxIdleTime
	if lifetimeSec <= 0 {
		lifetimeSec = 3600
	}
	if idleSec <= 0 {
		idleSec = 600
	}
	lifetime = time.Duration(lifetimeSec) * time.Second
	idleTime = time.Duration(idleSec) * time.Second
	return
}

// prefix 为进程级全局表前缀，SetPrefix 写、TableName 读，需加锁保护并发访问。
var (
	prefixMu sync.RWMutex
	prefix   string
)

func SetPrefix(p string) {
	prefixMu.Lock()
	prefix = p
	prefixMu.Unlock()
}

func TableName(tableName string) string {
	prefixMu.RLock()
	defer prefixMu.RUnlock()
	return prefix + tableName
}

type PageParam interface {
	IsValid() bool
	GetLimit() (limit, offset int32)
}
type TimeParam interface {
	IsValid() bool
	GetColumn() string
	GetTime() (start, end time.Time)
}

// ListParam GORM查询参数
type ListParam struct {
	Query map[string]interface{} // 自定义查询条件
	Cond  *Condition             // 自定义查询条件
	Page  PageParam
	Time  TimeParam
	Order []string
}
