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
	Alias       string `yaml:"db_alias" json:"alias" comment:"连接名称"`
	Name        string `yaml:"db_name" json:"name" comment:"数据库名称"`
	User        string `yaml:"db_user" json:"user" comment:"数据库连接用户名"`
	Password    string `yaml:"db_pwd" json:"password" comment:"数据库连接密码"`
	Host        string `yaml:"db_host" json:"host" comment:"数据库IP（域名）"`
	Port        string `yaml:"db_port" json:"port" comment:"数据库端口"`
	Debug       string `yaml:"db_debug" json:"debug" comment:"是否调试模式"`
	TablePrefix string `yaml:"db_table_prefix" json:"tablePrefix" comment:"表前缀"`
	Charset     string `yaml:"db_charset,omitempty" json:"charset" comment:"字符集类型"`
	Location    string `yaml:"db_location,omitempty" json:"timeLocation" comment:"时区"`
	LogDir      string `yaml:"db_log_dir" json:"logDir" comment:"日志目录"`
	// 连接池配置
	MaxIdleConns    int `yaml:"db_max_idle_conns" json:"maxIdleConns" comment:"最大空闲连接数"`            // 最大空闲连接数
	MaxOpenConns    int `yaml:"db_max_open_conns" json:"maxOpenConns" comment:"最大打开连接数"`            // 最大打开连接数
	ConnMaxLifetime int `yaml:"db_conn_max_lifetime" json:"connMaxLifetime" comment:"连接最大存活时间(秒)"`  // 连接最大存活时间（秒）
	ConnMaxIdleTime int `yaml:"db_conn_max_idle_time" json:"connMaxIdleTime" comment:"连接最大空闲时间(秒)"` // 连接最大空闲时间（秒）

	// 一主多从读写分离配置（基于 gorm.io/plugin/dbresolver）
	// 主库使用上方 Host/Port/User/Password/Name；Slaves 为多个读库(从)
	Slaves []MysqlNodeConfig `yaml:"db_slaves" json:"slaves" comment:"读节点(从)列表，User/Password/Database 为空时继承主库配置"`
}

// MysqlNodeConfig 从库节点连接配置（User/Password/Database/Charset/Location 为空时继承主库）
type MysqlNodeConfig struct {
	Host     string `yaml:"host" json:"host" comment:"数据库IP（域名）"`
	Port     string `yaml:"port" json:"port" comment:"数据库端口"`
	User     string `yaml:"user" json:"user" comment:"数据库连接用户名，为空继承主库"`
	Password string `yaml:"pwd" json:"password" comment:"数据库连接密码，为空继承主库"`
	Database string `yaml:"database" json:"database" comment:"数据库名称，为空继承主库"`
	Charset  string `yaml:"charset,omitempty" json:"charset" comment:"字符集类型，为空继承主库"`
	Location string `yaml:"location,omitempty" json:"timeLocation" comment:"时区，为空继承主库"`
}

type LinkParam struct {
	Loc       string `url:"loc"`
	Charset   string `url:"charset"`
	ParseTime bool   `url:"parseTime"`
}

func (c *MysqlConfig) Url() string {
	// 用局部变量取默认值，避免写回接收者 c 造成共享配置的副作用
	location, charset := c.Location, c.Charset
	if location == "" {
		location = "Local"
	}
	if charset == "" {
		charset = "utf8mb4"
	}
	linkParam := LinkParam{
		Loc:       location,
		Charset:   charset,
		ParseTime: true,
	}

	values, _ := query.Values(linkParam)

	path := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s", c.User, c.Password, c.Host, c.Port, c.Name, values.Encode())
	return path
}

// Url 生成从库节点的 DSN，User/Password/Database/Charset/Location 为空时继承主库配置
func (n *MysqlNodeConfig) Url(global *MysqlConfig) string {
	user, pwd, database := n.User, n.Password, n.Database
	charset, loc := n.Charset, n.Location

	if global != nil {
		if user == "" {
			user = global.User
		}
		if pwd == "" {
			pwd = global.Password
		}
		if database == "" {
			database = global.Name
		}
		if charset == "" {
			charset = global.Charset
		}
		if loc == "" {
			loc = global.Location
		}
	}
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

// validate 校验建立主连接所需的必填字段，缺失时返回明确 error，避免生成畸形 DSN 后
// 直到连库阶段才暴露晦涩错误。Password 允许为空（部分环境允许空密码），但
// User/Host/Port/Name 必须非空；若配置了从库，每个从库节点的 Host/Port 也必填。
func (c *MysqlConfig) validate() error {
	missing := make([]string, 0, 4)
	if c.User == "" {
		missing = append(missing, "User")
	}
	if c.Host == "" {
		missing = append(missing, "Host")
	}
	if c.Port == "" {
		missing = append(missing, "Port")
	}
	if c.Name == "" {
		missing = append(missing, "Name")
	}
	if len(missing) > 0 {
		return fmt.Errorf("database config missing required fields: %s", strings.Join(missing, ", "))
	}
	for i := range c.Slaves {
		if c.Slaves[i].Host == "" || c.Slaves[i].Port == "" {
			return fmt.Errorf("database config slaves[%d] missing required fields (host/port)", i)
		}
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
