package database

import (
	"fmt"
	"time"

	"github.com/google/go-querystring/query"
)

// ------------------[mysql]-------------------

type MysqlConfig struct {
	Alias       string `yaml:"db_alias" json:"alias" comment:"连接名称"`
	Name        string `yaml:"db_name" json:"name" comment:"数据库名称"`
	User        string `yaml:"db_user" json:"user" comment:"数据库连接用户名"`
	Password    string `yaml:"db_pwd" json:"password" comment:"数据库连接用户名"`
	Host        string `yaml:"db_host" json:"host" comment:"数据库IP（域名）"`
	Port        string `yaml:"db_port" json:"port" comment:"数据库端口"`
	Debug       string `yaml:"db_debug" json:"debug" comment:"是否调试模式"`
	TablePrefix string `yaml:"db_table_prefix" json:"tablePrefix" comment:"表前缀"`
	Charset     string `yaml:"db_charset,omitempty" json:"charset" comment:"字符集类型"`
	Location    string `yaml:"db_location,omitempty" json:"timeLocation" comment:"时区"`

	// 连接池配置
	MaxIdleConns    int `yaml:"db_max_idle_conns" json:"maxIdleConns" comment:"最大空闲连接数"`            // 最大空闲连接数
	MaxOpenConns    int `yaml:"db_max_open_conns" json:"maxOpenConns" comment:"最大打开连接数"`            // 最大打开连接数
	ConnMaxLifetime int `yaml:"db_conn_max_lifetime" json:"connMaxLifetime" comment:"连接最大存活时间(秒)"`  // 连接最大存活时间（秒）
	ConnMaxIdleTime int `yaml:"db_conn_max_idle_time" json:"connMaxIdleTime" comment:"连接最大空闲时间(秒)"` // 连接最大空闲时间（秒）
}

type LinkParam struct {
	Loc       string `url:"loc"`
	Charset   string `url:"charset"`
	ParseTime bool   `url:"parseTime"`
}

func (c *MysqlConfig) Url() string {

	if c.Location == "" {
		c.Location = "Local"
	}
	if c.Charset == "" {
		c.Charset = "utf8mb4"
	}
	linkParam := LinkParam{
		Loc:       c.Location,
		Charset:   c.Charset,
		ParseTime: true,
	}

	values, _ := query.Values(linkParam)

	path := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s", c.User, c.Password, c.Host, c.Port, c.Name, values.Encode())
	return path
}

var prefix string

func SetPrefix(p string) {
	prefix = p
}

func TableName(tableName string) string {
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
