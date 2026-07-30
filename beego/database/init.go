package database_orm

import (
	"errors"
	"fmt"
	"time"

	"github.com/beego/beego/v2/client/orm"
	"github.com/google/go-querystring/query"
	"github.com/sirupsen/logrus"
)

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
	logrus.Debugf("数据库链接：%s \n", path)
	return path
}
func InitMysql(config *MysqlConfig) error {

	if config == nil {
		return errors.New("init database fail. can not find database config")
	}

	err := orm.RegisterDriver("mysql", orm.DRMySQL)
	if err != nil {
		return err
	}

	if err := orm.RegisterDataBase(config.Alias, "mysql", config.Url()); err != nil {
		return err
	}
	orm.DefaultRowsLimit = -1

	// 设置连接池参数
	setConnectionPool(config)

	//如果是开发模式，则显示命令信息
	if config.Debug == "true" {
		orm.Debug = true
	}
	prefix = config.TablePrefix
	return nil
}

var prefix string

func TableName(tableName string) string {
	return prefix + tableName
}

// setConnectionPool 设置数据库连接池参数
func setConnectionPool(config *MysqlConfig) {
	// 设置默认值
	maxIdleConns := config.MaxIdleConns
	maxOpenConns := config.MaxOpenConns
	connMaxLifetime := config.ConnMaxLifetime
	connMaxIdleTime := config.ConnMaxIdleTime

	// 如果未配置，使用合理的默认值
	if maxIdleConns <= 0 {
		maxIdleConns = 10 // 默认最大空闲连接数
	}
	if maxOpenConns <= 0 {
		maxOpenConns = 100 // 默认最大打开连接数
	}
	if connMaxLifetime <= 0 {
		connMaxLifetime = 3600 // 默认连接最大存活时间 1 小时
	}
	if connMaxIdleTime <= 0 {
		connMaxIdleTime = 600 // 默认连接最大空闲时间 10 分钟
	}

	// 获取底层 sql.DB 对象
	db, err := orm.GetDB(config.Alias)
	if err != nil {
		logrus.Warnf("获取数据库连接失败: %v", err)
		return
	}

	// 应用连接池配置
	db.SetMaxIdleConns(maxIdleConns)
	db.SetMaxOpenConns(maxOpenConns)
	db.SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Second)
	db.SetConnMaxIdleTime(time.Duration(connMaxIdleTime) * time.Second)

	logrus.Infof("数据库连接池配置 - Alias: %s, MaxIdle: %d, MaxOpen: %d, MaxLifetime: %ds, MaxIdleTime: %ds",
		config.Alias, maxIdleConns, maxOpenConns, connMaxLifetime, connMaxIdleTime)
}
