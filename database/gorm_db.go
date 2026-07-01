package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// DBManager GORM数据库管理器
type DBManager struct {
	dbMap map[string]*gorm.DB
}

var gormManager = &DBManager{
	dbMap: make(map[string]*gorm.DB),
}

//var gormTablePrefix string

// GetDB 获取指定别名的数据库实例
func GetDB(alias ...string) *gorm.DB {
	dbAlias := "default"
	if len(alias) > 0 && alias[0] != "" {
		dbAlias = alias[0]
	}

	db, exists := gormManager.dbMap[dbAlias]
	if !exists {
		return nil
	}
	return db
}

// InitGorm 初始化GORM数据库连接
func InitGorm(config *MysqlConfig) error {
	if config == nil {
		return errors.New("init gorm database fail. can not find database config")
	}

	if config.Alias == "" {
		config.Alias = "default"
	}

	dsn := config.Url()
	logrus.Debugf("GORM数据库连接配置: %s \n", dsn)

	// 配置日志级别
	logLevel := logger.Silent
	if config.Debug == "true" {
		logLevel = logger.Info
	}

	// 打开数据库连接
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.New(logrus.StandardLogger(), logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logLevel,
			IgnoreRecordNotFoundError: false,
			Colorful:                  true,
		}),
		SkipDefaultTransaction:                   true, // 跳过默认事务，提升性能
		DisableForeignKeyConstraintWhenMigrating: true, // 禁用外键约束
	})
	if err != nil {
		return fmt.Errorf("failed to connect database: %w", err)
	}

	// 获取底层 sql.DB 对象
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB: %w", err)
	}

	// 设置连接池参数
	setConnectionPool(sqlDB, config)

	// 保存到管理器
	gormManager.dbMap[config.Alias] = db

	// 设置表名前缀
	//gormTablePrefix = config.TablePrefix
	SetPrefix(config.TablePrefix)

	logrus.Debugf("GORM数据库连接池配置 - Alias: %s, MaxIdle: %d, MaxOpen: %d, MaxLifetime: %ds, MaxIdleTime: %d \n",
		config.Alias, config.MaxIdleConns, config.MaxOpenConns, config.ConnMaxLifetime, config.ConnMaxIdleTime)

	return nil
}

// setConnectionPool 设置GORM数据库连接池参数
func setConnectionPool(sqlDB *sql.DB, config *MysqlConfig) {
	maxIdleConns := config.MaxIdleConns
	maxOpenConns := config.MaxOpenConns
	connMaxLifetime := config.ConnMaxLifetime
	connMaxIdleTime := config.ConnMaxIdleTime

	// 如果未配置，使用合理的默认值
	if maxIdleConns <= 0 {
		maxIdleConns = 10
	}
	if maxOpenConns <= 0 {
		maxOpenConns = 100
	}
	if connMaxLifetime <= 0 {
		connMaxLifetime = 3600
	}
	if connMaxIdleTime <= 0 {
		connMaxIdleTime = 600
	}

	sqlDB.SetMaxIdleConns(maxIdleConns)
	sqlDB.SetMaxOpenConns(maxOpenConns)
	sqlDB.SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Second)
	sqlDB.SetConnMaxIdleTime(time.Duration(connMaxIdleTime) * time.Second)
}

// ------------------[GORM 通用工具函数]-------------------

// TxFunc GORM事务函数类型
type TxFunc func(tx *gorm.DB) error

// Tx GORM事务处理器
type Tx struct {
	tx  *gorm.DB
	err error
}

// NewTx 创建新的事务
func NewTx(alias ...string) *Tx {
	db := GetDB(alias...)
	if db == nil {
		return &Tx{
			tx:  nil,
			err: errors.New("database connection not found"),
		}
	}

	tx := db.Begin()
	return &Tx{
		tx:  tx,
		err: tx.Error,
	}
}

// Execute 执行事务
func (tx *Tx) Execute(f TxFunc) error {
	if tx.err != nil {
		return tx.err
	}
	if tx.tx == nil {
		return sql.ErrConnDone
	}

	defer func() {
		if r := recover(); r != nil {
			tx.tx.Rollback()
		}
	}()

	err := f(tx.tx)
	if err != nil {
		tx.tx.Rollback()
		return err
	}

	return tx.tx.Commit().Error
}

// Transaction 简化事务执行（推荐方式）
func Transaction(fn func(tx *gorm.DB) error, alias ...string) error {
	db := GetDB(alias...)
	if db == nil {
		return errors.New("database connection not found")
	}

	return db.Transaction(fn)
}

// ReadOne 根据主键查询单条记录
func ReadOne[T any](t *T, cols ...string) error {
	db := GetDB()
	if db == nil {
		return errors.New("database connection not found")
	}

	if len(cols) > 0 {
		cs := []interface{}{}
		for _, col := range cols {
			cs = append(cs, col)
		}
		db = db.Where(t, cs)
	} else {
		db = db.Where(t)
	}

	return db.First(t).Error
}

// FindOne 根据条件查询单条记录
func FindOne[T any](query interface{}, args ...interface{}) *T {
	var model T
	db := GetDB()
	if db == nil {
		return nil
	}

	err := db.Where(query, args...).First(&model).Error
	if err != nil {
		return nil
	}
	return &model
}

// FindByID 根据ID查询单条记录
func FindByID[T any](id int64) *T {
	var model T
	db := GetDB()
	if db == nil {
		return nil
	}

	err := db.First(&model, id).Error
	if err != nil {
		return nil
	}
	return &model
}

// FindAll 分页查询列表
func FindAll[T any](param ListParam) (list []*T, total int64, err error) {
	db := GetDB()
	if db == nil {
		return nil, 0, errors.New("database connection not found")
	}

	var model T
	query := db.Model(&model)

	// 应用自定义查询条件
	if param.Cond != nil && !param.Cond.IsEmpty() {
		q, v := param.Cond.GormWhere()
		query = query.Where(q, v...)
	}

	if param.Query != nil {
		for key, val := range param.Query {
			query = query.Where(key, val)
		}
	}

	// 应用时间范围查询
	if param.Time != nil && param.Time.IsValid() {
		column := param.Time.GetColumn()
		start, end := param.Time.GetTime()
		query = query.Where(fmt.Sprintf("%s >= ?", column), start).
			Where(fmt.Sprintf("%s < ?", column), end)
	}

	// 获取总数

	if err = query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if total == 0 {
		return make([]*T, 0), 0, nil
	}

	// 应用排序
	if len(param.Order) > 0 {
		orderList := make([]string, len(param.Order))
		for i, order := range param.Order {
			if strings.HasPrefix(order, "-") {
				orderList[i] = order[1:] + " DESC"
			} else {
				orderList[i] = order
			}
		}
		query = query.Order(strings.Join(orderList, ","))
	}

	// 应用分页
	if param.Page != nil && param.Page.IsValid() {
		limit, offset := param.Page.GetLimit()
		query = query.Limit(int(limit)).Offset(int(offset))
	}

	// 查询数据
	err = query.Find(&list).Error
	if err != nil {
		return nil, 0, err
	}

	return list, total, nil
}

// FindList 查询列表（不分页）
func FindList[T any](cond *Condition, page ...PageParam) ([]*T, error) {
	db := GetDB()
	if db == nil {
		return nil, errors.New("database connection not found")
	}

	var model T
	query := db.Model(&model)

	if cond != nil {
		q, v := cond.GormWhere()
		query = query.Where(q, v...)
	}

	// 应用分页
	if len(page) > 0 {
		p := page[0]
		if p.IsValid() {
			limit, offset := p.GetLimit()
			query = query.Limit(int(limit)).Offset(int(offset))
		}
	}

	var list []*T
	err := query.Find(&list).Error
	return list, err
}

// Count 统计数量
func Count[T any](cond *Condition) (int64, error) {
	db := GetDB()
	if db == nil {
		return 0, errors.New("database connection not found")
	}

	var model T
	query := db.Model(&model)

	if cond != nil {
		q, v := cond.GormWhere()
		query = query.Where(q, v...)
	}

	var count int64
	err := query.Count(&count).Error
	return count, err
}

// Update 更新记录
func Update[T any](tx *gorm.DB, data *T, columns ...string) error {
	db := tx
	if db == nil {
		db = GetDB()
		if db == nil {
			return errors.New("database connection not found")
		}
	}

	db = db.Model(data)
	if len(columns) > 0 {
		db = db.Select(columns)
	}

	return db.Updates(data).Error
}

// UpdateByCondition 根据条件更新
func UpdateByCondition[T any](tx *gorm.DB, condition map[string]interface{}, updates map[string]interface{}) error {
	if len(updates) == 0 {
		return errors.New("updates cannot be empty")
	}

	db := tx
	if db == nil {
		db = GetDB()
		if db == nil {
			return errors.New("database connection not found")
		}
	}

	var model T
	query := db.Model(&model)

	if condition != nil {
		for key, val := range condition {
			query = query.Where(key, val)
		}
	}

	return query.Updates(updates).Error
}

// Delete 删除记录
func Delete[T any](tx *gorm.DB, data *T) error {
	db := tx
	if db == nil {
		db = GetDB()
		if db == nil {
			return errors.New("database connection not found")
		}
	}

	return db.Delete(data).Error
}

// DeleteByCondition 根据条件删除
func DeleteByCondition[T any](tx *gorm.DB, condition map[string]interface{}) error {
	db := tx
	if db == nil {
		db = GetDB()
		if db == nil {
			return errors.New("database connection not found")
		}
	}

	var model T
	query := db.Model(&model)

	if condition != nil {
		for key, val := range condition {
			query = query.Where(key, val)
		}
	}

	result := query.Delete(&model)
	return result.Error
}

// Insert 插入记录
func Insert[T any](tx *gorm.DB, data *T) error {
	db := tx
	if db == nil {
		db = GetDB()
		if db == nil {
			return errors.New("database connection not found")
		}
	}

	return db.Create(data).Error
}

// InsertBatch 批量插入
func InsertBatch[T any](tx *gorm.DB, batchSize int, dataList []*T) error {
	if len(dataList) == 0 {
		return nil
	}

	db := tx
	if db == nil {
		db = GetDB()
		if db == nil {
			return errors.New("database connection not found")
		}
	}

	// 分批插入
	total := len(dataList)
	for i := 0; i < total; i += batchSize {
		end := i + batchSize
		if end > total {
			end = total
		}

		batch := dataList[i:end]
		if err := db.CreateInBatches(batch, batchSize).Error; err != nil {
			return err
		}
	}

	return nil
}

// Exec 执行原生SQL
func Exec(sql string, values ...interface{}) error {
	db := GetDB()
	if db == nil {
		return errors.New("database connection not found")
	}

	return db.Exec(sql, values...).Error
}

// Raw 查询原生SQL
func Raw[T any](sql string, values ...interface{}) ([]*T, error) {
	db := GetDB()
	if db == nil {
		return nil, errors.New("database connection not found")
	}

	var results []*T
	err := db.Raw(sql, values...).Scan(&results).Error
	return results, err
}

// WithContext 带上下文的数据库操作
func WithContext(ctx context.Context, alias ...string) *gorm.DB {
	db := GetDB(alias...)
	if db == nil {
		return nil
	}
	return db.WithContext(ctx)
}
