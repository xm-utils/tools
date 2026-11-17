package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/common"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/plugin/dbresolver"
)

var (
	dbOnce sync.Once
	db     *gorm.DB
	dbErr  error
)

// GetDB 获取进程级单例 *gorm.DB；读写分离由 DBResolver 按表名自动路由。
// 未初始化时返回 nil（由调用方判空并返回 error，不再 panic），统一遵循「panic 转 error」约定。
func GetDB() *gorm.DB {
	return db
}

// ReplicaReader 模型实现该接口且 ReadFromReplica() 返回 true 时，
// 其对应表的 SELECT 走从库(Replicas)；其余表读写都走主库（默认全走主库）。
type ReplicaReader interface {
	ReadFromReplica() bool
}

var (
	modelMu       sync.RWMutex
	modelRegistry []any
)

// RegisterModels 注册模型，供 InitGorm 初始化时统一扫描读写分离标记。
// 只有实现 ReplicaReader 且 ReadFromReplica() 为 true 的模型才会被路由到从库。
// 通常在 init() 或启动阶段（InitGorm 之前）调用。
func RegisterModels(models ...any) {
	modelMu.Lock()
	defer modelMu.Unlock()
	modelRegistry = append(modelRegistry, models...)
}

// collectReplicaTables 扫描已注册模型，收集 ReadFromReplica() 为 true 的模型物理表名（去重）。
func collectReplicaTables(instance *gorm.DB) []interface{} {
	modelMu.RLock()
	defer modelMu.RUnlock()

	seen := make(map[string]struct{}, len(modelRegistry))
	tables := make([]interface{}, 0, len(modelRegistry))
	for _, m := range modelRegistry {
		rr, ok := m.(ReplicaReader)
		if !ok || !rr.ReadFromReplica() {
			continue
		}
		// 解析出真实表名（遵循 TableName()/命名策略/全局前缀）
		stmt := &gorm.Statement{DB: instance}
		if err := stmt.Parse(m); err != nil {
			logrus.Warnf("dbresolver 解析模型表名失败: %v", err)
			continue
		}
		if _, dup := seen[stmt.Table]; dup {
			continue
		}
		seen[stmt.Table] = struct{}{}
		tables = append(tables, stmt.Table)
	}
	return tables
}

// InitGorm 初始化 GORM 单例数据库连接，基于 DBResolver 实现一主多从读写分离。
// 使用 sync.Once 保证进程内仅初始化一次，重复调用返回首次初始化的结果。
//
// 读写分离采用「模型声明 + 初始化扫描」：实现 ReplicaReader 接口且 ReadFromReplica()
// 返回 true 的模型，其对应表 SELECT 走从库，其余表读写都走主库（默认全走主库）。
// 模型需先通过 RegisterModels 注册才会被扫描；表名无需写入配置文件。
func InitGorm(config *MysqlConfig) error {
	if config == nil {
		return errors.New("init gorm database fail. can not find database config")
	}

	dbOnce.Do(func() {
		db, dbErr = openDB(config)
	})
	return dbErr
}

// openDB 根据配置建立单例 *gorm.DB：
//   - 以主库节点作为 gorm.Open 的 Dialector；
//   - 若配置了从库且有模型声明读从库，则注册表级 DBResolver；
//   - 统一设置连接池参数与表名前缀。
func openDB(config *MysqlConfig) (*gorm.DB, error) {
	// 先校验必填连接字段，缺失时返回清晰 error，避免生成畸形 DSN 后才在连库阶段暴露晦涩错误
	if err := config.validate(); err != nil {
		return nil, err
	}

	// 配置日志级别
	logLevel := logger.Silent
	if config.Debug == "true" {
		logLevel = logger.Info
	}

	// 主库(写库)作为初始连接，避免明文打印 DSN 中的密码
	logrus.Debugf("GORM数据库连接配置 - Host: %s, Port: %s, DB: %s, 从库数: %d\n",
		config.Host, config.Port, config.Name, len(config.Slaves))

	instance, err := gorm.Open(mysql.Open(config.Url()), &gorm.Config{
		Logger: logger.New(logrus.WithField("module", "GORM"), logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logLevel,
			IgnoreRecordNotFoundError: false,
			Colorful:                  true,
		}),
		SkipDefaultTransaction:                   true, // 跳过默认事务，提升性能
		DisableForeignKeyConstraintWhenMigrating: true, // 禁用外键约束
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect database: %w", err)
	}

	// 先设置表名前缀，确保后续解析模型表名（含 TableName()/前缀）与运行期一致
	SetPrefix(config.TablePrefix)

	// 注册 DBResolver（一主多从读写分离，仅部分声明读从库的模型表）
	if resolver := buildResolver(instance, config); resolver != nil {
		if err := instance.Use(resolver); err != nil {
			return nil, fmt.Errorf("failed to register dbresolver: %w", err)
		}
	}

	// 获取底层 sql.DB 对象并设置主连接池参数
	sqlDB, err := instance.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}
	setConnectionPool(sqlDB, config)

	return instance, nil
}

// buildResolver 构建表级 DBResolver 插件。
// 扫描已注册模型，仅将 ReadFromReplica() 为 true 的表注册为「读走从库」；
// 无从库或无任何模型声明读从库时返回 nil（此时所有表读写都走主库）。
func buildResolver(instance *gorm.DB, config *MysqlConfig) gorm.Plugin {
	if !config.hasReadWriteSplit() {
		return nil
	}

	// 扫描已注册模型，收集「读走从库」的表名
	datas := collectReplicaTables(instance)
	if len(datas) == 0 {
		// 没有任何模型声明读从库：不注册 resolver，所有表读写都走主库
		return nil
	}

	// 仅这些表读走从库；其余表未命中 resolver，读写都回落到主库(gorm.Open 连接)
	resolver := dbresolver.Register(dbresolver.Config{
		Replicas: toDialectors(config.Slaves, config),
		Policy:   dbresolver.RandomPolicy{},
	}, datas...)

	// DBResolver 内部为每个 Replica 单独建连接池，需同步设置池参数
	// （此时插件尚未 Initialize，Setter 会缓存在 compileCallbacks，于 Use 时生效）
	maxIdle, maxOpen, lifetime, idleTime := config.poolSettings()
	resolver = resolver.
		SetMaxIdleConns(maxIdle).
		SetMaxOpenConns(maxOpen).
		SetConnMaxLifetime(lifetime).
		SetConnMaxIdleTime(idleTime)

	return resolver
}

// toDialectors 将节点列表转换为 GORM Dialector 列表。
func toDialectors(nodes []MysqlNodeConfig, config *MysqlConfig) []gorm.Dialector {
	dialectors := make([]gorm.Dialector, 0, len(nodes))
	for i := range nodes {
		dialectors = append(dialectors, mysql.Open(nodes[i].Url(config)))
	}
	return dialectors
}

// setConnectionPool 设置底层 sql.DB 连接池参数
func setConnectionPool(sqlDB *sql.DB, config *MysqlConfig) {
	maxIdle, maxOpen, lifetime, idleTime := config.poolSettings()

	sqlDB.SetMaxIdleConns(maxIdle)
	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetConnMaxLifetime(lifetime)
	sqlDB.SetConnMaxIdleTime(idleTime)

	logrus.Debugf("GORM数据库连接池配置 - MaxIdle: %d, MaxOpen: %d, MaxLifetime: %v, MaxIdleTime: %v\n",
		maxIdle, maxOpen, lifetime, idleTime)
}

// ------------------[GORM 通用工具函数]-------------------

// ReadOne 以入参结构体的非零字段作为条件查询单条记录（强制走主库），返回 *T，未命中返回 nil。
func ReadOne[T any](t T, cols ...string) (result *T) {
	return Primary[T]().ReadOne(t, cols...)
}

// FindOne 根据ID查询单条记录
func FindOne[T any](id int64) *T {
	return Primary[T]().FindOne(id)
}

// FindAll 分页查询列表
func FindAll[T any](param ListParam) (list []*T, total int64, err error) {
	return Primary[T]().FindAll(param)
}

// FindList 查询列表（不分页）
func FindList[T any](cond *Condition, page ...PageParam) ([]*T, error) {
	return Primary[T]().FindList(cond, page...)
}

// Count 统计数量
func Count[T any](cond *Condition) (int64, error) {
	return Primary[T]().Count(cond)
}

// Update 更新记录（仅更新非零字段；指定 columns 时只更新这些列）
func Update[T any](tx *gorm.DB, data *T, columns ...string) (err error) {
	defer recoverToErr(&err)
	db := tx
	if db == nil {
		db = GetDB()
	}
	if db == nil {
		return errors.New("database connection not found")
	}

	db = db.Model(data)
	if len(columns) > 0 {
		db = db.Select(columns)
	}

	return db.Updates(data).Error
}

// UpdateModel 比对 old 与 form，将 form 中非空且发生变化的差异字段更新到数据库（Select 只写这些列）。
//   - 不传 whereCols：按主键更新（与 beego/database 的 UpdateModel 语义一致，复用 Update）；
//   - 传入 whereCols（Go 字段名）：在保留主键条件的基础上，追加 form 中这些字段的值作为额外 WHERE 条件（两者 AND）。
//
// 无差异时直接返回 nil；old 为空指针时返回 gorm.ErrRecordNotFound。
func UpdateModel[T any](tx *gorm.DB, old *T, form T, whereCols ...string) (err error) {
	defer recoverToErr(&err)
	if old == nil {
		return gorm.ErrRecordNotFound
	}

	var fields []string
	old, fields = common.CompareAndAssign(old, form)
	if len(fields) == 0 {
		return nil
	}

	// 无匹配字段：按主键更新（保持原有语义）
	if len(whereCols) == 0 {
		return Update(tx, old, fields...)
	}

	db := tx
	if db == nil {
		db = GetDB()
	}
	if db == nil {
		return errors.New("database connection not found")
	}
	// whereCols 指定字段（值取自 form）作为额外 WHERE 条件；
	// 以 old 作为 Model 目标保留主键条件，最终 WHERE = 主键 AND whereCols 字段。
	cond := pickFields(form, whereCols)
	return db.Model(old).Where(&cond).Select(fields).Updates(old).Error
}

// pickFields 返回 src 的浅拷贝，仅保留 cols 指定的字段（按 Go 字段名匹配），其余字段置零。
func pickFields[T any](src T, cols []string) T {
	var out T
	ov := reflect.ValueOf(&out).Elem()
	sv := reflect.ValueOf(src)
	if sv.Kind() == reflect.Ptr {
		sv = sv.Elem()
	}
	if ov.Kind() != reflect.Struct || sv.Kind() != reflect.Struct {
		return out
	}

	keep := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		keep[c] = struct{}{}
	}
	for i := 0; i < sv.NumField(); i++ {
		if _, ok := keep[sv.Type().Field(i).Name]; !ok {
			continue
		}
		if ov.Field(i).CanSet() && sv.Field(i).CanInterface() {
			ov.Field(i).Set(sv.Field(i))
		}
	}
	return out
}

// UpdateByCondition 根据条件批量更新指定字段
func UpdateByCondition[T any](tx *gorm.DB, cond *Condition, updates map[string]interface{}) (err error) {
	defer recoverToErr(&err)
	if len(updates) == 0 {
		return errors.New("updates cannot be empty")
	}

	db := tx
	if db == nil {
		db = GetDB()
	}
	if db == nil {
		return errors.New("database connection not found")
	}
	if cond == nil || cond.IsEmpty() {
		return errors.New("condition cannot be empty")
	}
	var model T
	query := db.Model(&model)

	q, v := cond.GormWhere()
	query = query.Where(q, v...)

	return query.Updates(updates).Error
}

// Delete 删除记录
func Delete[T any](tx *gorm.DB, data *T) (err error) {
	defer recoverToErr(&err)
	db := tx
	if db == nil {
		db = GetDB()
	}
	if db == nil {
		return errors.New("database connection not found")
	}
	return db.Delete(data).Error
}

// DeleteByCondition 根据条件删除
func DeleteByCondition[T any](tx *gorm.DB, cond *Condition) (err error) {
	defer recoverToErr(&err)
	db := tx
	if db == nil {
		db = GetDB()
	}
	if db == nil {
		return errors.New("database connection not found")
	}
	if cond == nil || cond.IsEmpty() {
		return errors.New("condition cannot be empty")
	}

	var model T
	q, v := cond.GormWhere()
	return db.Model(&model).Where(q, v...).Delete(&model).Error
}

// Insert 插入记录
func Insert[T any](tx *gorm.DB, data *T) (err error) {
	defer recoverToErr(&err)
	db := tx
	if db == nil {
		db = GetDB()
	}
	if db == nil {
		return errors.New("database connection not found")
	}
	return db.Create(data).Error
}

// InsertBatch 批量插入，返回累计影响行数
func InsertBatch[T any](tx *gorm.DB, batchSize int, dataList []*T) (affected int64, err error) {
	defer recoverToErr(&err)
	if len(dataList) == 0 {
		return 0, nil
	}

	db := tx
	if db == nil {
		db = GetDB()
	}
	if db == nil {
		return 0, errors.New("database connection not found")
	}
	if batchSize <= 0 {
		batchSize = len(dataList)
	}

	// 分批插入
	total := len(dataList)
	for i := 0; i < total; i += batchSize {
		end := i + batchSize
		if end > total {
			end = total
		}

		batch := dataList[i:end]
		res := db.CreateInBatches(batch, batchSize)
		if res.Error != nil {
			return affected, res.Error
		}
		affected += res.RowsAffected
	}

	return affected, nil
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
func WithContext(ctx context.Context) *gorm.DB {
	db := GetDB()
	if db == nil {
		return nil
	}
	return db.WithContext(ctx)
}
