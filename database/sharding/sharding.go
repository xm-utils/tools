package sharding

import (
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/longbridgeapp/sqlparser"
	"golang.org/x/exp/slices"
	"gorm.io/gorm"
)

var (
	ErrMissingShardingKey = errors.New("sharding key or id required, and use operator =")
	ErrInvalidID          = errors.New("invalid id format")
	ErrInsertDiffSuffix   = errors.New("can not insert different suffix table in one query ")
)

var (
	ShardingIgnoreStoreKey = "sharding_ignore"
)

type Sharding struct {
	*gorm.DB
	ConnPool       *ConnPool
	configs        map[string]Config
	querys         sync.Map
	snowflakeNodes []*snowflake.Node

	_config Config
	_tables []any

	mutex sync.RWMutex
}

// Config 指定分片的配置
type Config struct {
	// DoubleWrite 启用时，数据会同时写入主表和分片表
	DoubleWrite bool

	// ShardingKey 指定用于分片表行的列名
	// 例如,对于产品订单表,你可能想按 `user_id` 来分割行
	ShardingKey string

	// NumberOfShards 指定要分多少个表
	NumberOfShards uint

	// tableFormat 指定分片表后缀格式
	tableFormat string

	// ShardingType 分片类型: "modulus"(取模) 或 "time_range"(时间范围)
	// 默认为 "modulus"
	ShardingType string

	// TimeRangeFormat 时间范围格式,当 ShardingType 为 "time_range" 时使用
	// 支持: "year"(YYYY), "month"(YYYYMM), "week"(YYYYWW), "day"(YYYYMMDD)
	TimeRangeFormat string

	// TimeColumn 时间字段名,当 ShardingType 为 "time_range" 时使用
	// 默认为 "created_at"
	TimeColumn string

	// ShardingAlgorithm 指定一个函数,通过列值生成分片表的后缀
	// 例如,这个函数实现了一个取模分片算法:
	//
	// 	func(value any) (suffix string, err error) {
	//		if uid, ok := value.(int64);ok {
	//			return fmt.Sprintf("_%02d", user_id % 64), nil
	//		}
	//		return "", errors.New("invalid user_id")
	// 	}
	ShardingAlgorithm func(columnValue any) (suffix string, err error)

	// ShardingSuffixs 指定一个函数,生成所有表的后缀
	// 用于支持 Migrator 和生成主键
	// 例如,这个函数获取一个取模的所有分片后缀:
	//
	// func () (suffixs []string) {
	// 	numberOfShards := 5
	// 	for i := 0; i < numberOfShards; i++ {
	// 		suffixs = append(suffixs, fmt.Sprintf("_%02d", i%numberOfShards))
	// 	}
	// 	return
	// }
	ShardingSuffixs func() (suffixs []string)

	// ShardingAlgorithmByPrimaryKey 指定一个函数,通过主键生成分片表的后缀
	// 在没有指定分片键时使用
	// 例如,这个函数使用 Snowflake 库生成后缀:
	//
	// 	func(id int64) (suffix string) {
	//		return fmt.Sprintf("_%02d", snowflake.ParseInt64(id).Node())
	//	}
	ShardingAlgorithmByPrimaryKey func(id int64) (suffix string)

	// PrimaryKeyGenerator 指定主键生成算法
	// 仅在插入且记录不包含 id 字段时使用
	// 选项有:PKSnowflake、PKPGSequence 和 PKCustom
	// 当使用 PKCustom 时,还需要指定 PrimaryKeyGeneratorFn
	PrimaryKeyGenerator int

	// PrimaryKeyGeneratorFn 指定一个函数来生成主键
	// 当使用自增类生成器时,tableIdx 参数可以忽略
	// 例如,这个函数使用 Snowflake 库生成主键
	// 如果你不想自动填充 `id` 或使用不是名为 `id` 的主键,只需返回 0
	//
	// 	func(tableIdx int64) int64 {
	//		return nodes[tableIdx].Generate().Int64()
	//	}
	PrimaryKeyGeneratorFn func(tableIdx int64) int64
}

func Register(config Config, tables ...any) *Sharding {
	return &Sharding{
		_config: config,
		_tables: tables,
	}
}

func (s *Sharding) compile() error {
	if s.configs == nil {
		s.configs = make(map[string]Config)
	}
	for _, table := range s._tables {
		if t, ok := table.(string); ok {
			s.configs[t] = s._config
		} else {
			stmt := &gorm.Statement{DB: s.DB}
			if err := stmt.Parse(table); err == nil {
				s.configs[stmt.Table] = s._config
			} else {
				return err
			}
		}
	}

	for t, c := range s.configs {
		if c.NumberOfShards > 1024 && c.PrimaryKeyGenerator == PKSnowflake {
			panic("Snowflake NumberOfShards should less than 1024")
		}

		if c.PrimaryKeyGenerator == PKSnowflake {
			c.PrimaryKeyGeneratorFn = s.genSnowflakeKey
		} else if c.PrimaryKeyGenerator == PKPGSequence {

			// Execute SQL to CREATE SEQUENCE for this table if not exist
			err := s.createPostgreSQLSequenceKeyIfNotExist(t)
			if err != nil {
				return err
			}

			c.PrimaryKeyGeneratorFn = func(index int64) int64 {
				return s.genPostgreSQLSequenceKey(t, index)
			}
		} else if c.PrimaryKeyGenerator == PKMySQLSequence {
			err := s.createMySQLSequenceKeyIfNotExist(t)
			if err != nil {
				return err
			}

			c.PrimaryKeyGeneratorFn = func(index int64) int64 {
				return s.genMySQLSequenceKey(t, index)
			}
		} else if c.PrimaryKeyGenerator == PKCustom {
			if c.PrimaryKeyGeneratorFn == nil {
				return errors.New("PrimaryKeyGeneratorFn is required when use PKCustom")
			}
		} else {
			return errors.New("PrimaryKeyGenerator can only be one of PKSnowflake, PKPGSequence, PKMySQLSequence and PKCustom")
		}

		if c.ShardingAlgorithm == nil {
			// 如果是时间范围分片
			if c.ShardingType == "time_range" {
				if c.TimeColumn == "" {
					c.TimeColumn = "created_at"
				}
				if c.TimeRangeFormat == "" {
					c.TimeRangeFormat = "month"
				}

				c.ShardingAlgorithm = func(value any) (suffix string, err error) {
					var t time.Time
					switch v := value.(type) {
					case time.Time:
						t = v
					case *time.Time:
						if v != nil {
							t = *v
						} else {
							return "", errors.New("time column value is nil")
						}
					case string:
						// 尝试解析字符串时间为 time.Time
						layouts := []string{
							"2006-01-02 15:04:05",
							"2006-01-02T15:04:05Z",
							"2006-01-02T15:04:05",
							"2006-01-02",
							time.RFC3339,
						}
						var parseErr error
						for _, layout := range layouts {
							t, parseErr = time.Parse(layout, v)
							if parseErr == nil {
								break
							}
						}
						if parseErr != nil {
							return "", fmt.Errorf("failed to parse time string: %v", parseErr)
						}
					case int64:
						// 假设是 Unix 时间戳
						t = time.Unix(v, 0)
					default:
						return "", fmt.Errorf("time_range sharding only supports time.Time, string, or int64 (timestamp) types")
					}

					// 根据 TimeRangeFormat 生成后缀
					switch c.TimeRangeFormat {
					case "year":
						suffix = "_" + t.Format("2006")
					case "month":
						suffix = "_" + t.Format("200601")
					case "week":
						_, week := t.ISOWeek()
						suffix = fmt.Sprintf("_%dW%02d", t.Year(), week)
					case "day":
						suffix = "_" + t.Format("20060102")
					default:
						return "", fmt.Errorf("unsupported TimeRangeFormat: %s", c.TimeRangeFormat)
					}

					return suffix, nil
				}

				// 为时间范围分片生成默认的后缀函数
				if c.ShardingSuffixs == nil {
					c.ShardingSuffixs = func() (suffixs []string) {
						// 时间范围分片不预先生成所有后缀，返回空列表
						// 表会根据需要动态创建
						return []string{}
					}
				}
			} else {
				// 原有的取模分片逻辑
				if c.NumberOfShards == 0 {
					return errors.New("specify NumberOfShards or ShardingAlgorithm")
				}
				if c.NumberOfShards < 10 {
					c.tableFormat = "_%01d"
				} else if c.NumberOfShards < 100 {
					c.tableFormat = "_%02d"
				} else if c.NumberOfShards < 1000 {
					c.tableFormat = "_%03d"
				} else if c.NumberOfShards < 10000 {
					c.tableFormat = "_%04d"
				}
				c.ShardingAlgorithm = func(value any) (suffix string, err error) {
					id := 0
					switch value := value.(type) {
					case int:
						id = value
					case int64:
						id = int(value)
					case string:
						id, err = strconv.Atoi(value)
						if err != nil {
							id = int(crc32.ChecksumIEEE([]byte(value)))
						}
					default:
						return "", fmt.Errorf("default algorithm only support integer and string column," +
							"if you use other type, specify you own ShardingAlgorithm")
					}

					return fmt.Sprintf(c.tableFormat, id%int(c.NumberOfShards)), nil
				}
			}
		}

		if c.ShardingSuffixs == nil {
			c.ShardingSuffixs = func() (suffixs []string) {
				for i := 0; i < int(c.NumberOfShards); i++ {
					suffix, err := c.ShardingAlgorithm(i)
					if err != nil {
						return nil
					}
					suffixs = append(suffixs, suffix)
				}
				return
			}
		}

		if c.ShardingAlgorithmByPrimaryKey == nil {
			if c.PrimaryKeyGenerator == PKSnowflake {
				c.ShardingAlgorithmByPrimaryKey = func(id int64) (suffix string) {
					return fmt.Sprintf(c.tableFormat, snowflake.ParseInt64(id).Node())
				}
			}
		}
		s.configs[t] = c
	}

	return nil
}

// Name Gorm 插件接口的插件名称
func (s *Sharding) Name() string {
	return "gorm:sharding"
}

// LastQuery 获取最后一次 SQL 查询
func (s *Sharding) LastQuery() string {
	if query, ok := s.querys.Load("last_query"); ok {
		return query.(string)
	}

	return ""
}

// Initialize Gorm 插件接口的实现
func (s *Sharding) Initialize(db *gorm.DB) error {
	db.Dialector = NewShardingDialector(db.Dialector, s)
	s.DB = db
	s.registerCallbacks(db)

	for t, c := range s.configs {
		if c.PrimaryKeyGenerator == PKPGSequence {
			err := s.DB.Exec("CREATE SEQUENCE IF NOT EXISTS " + pgSeqName(t)).Error
			if err != nil {
				return fmt.Errorf("init postgresql sequence error, %w", err)
			}
		}
		if c.PrimaryKeyGenerator == PKMySQLSequence {
			err := s.DB.Exec("CREATE TABLE IF NOT EXISTS " + mySQLSeqName(t) + " (id INT NOT NULL)").Error
			if err != nil {
				return fmt.Errorf("init mysql create sequence error, %w", err)
			}
			err = s.DB.Exec("INSERT INTO " + mySQLSeqName(t) + " VALUES (0)").Error
			if err != nil {
				return fmt.Errorf("init mysql insert sequence error, %w", err)
			}
		}
	}

	s.snowflakeNodes = make([]*snowflake.Node, 1024)
	for i := int64(0); i < 1024; i++ {
		n, err := snowflake.NewNode(i)
		if err != nil {
			return fmt.Errorf("init snowflake node error, %w", err)
		}
		s.snowflakeNodes[i] = n
	}

	return s.compile()
}

func (s *Sharding) registerCallbacks(db *gorm.DB) {
	s.Callback().Create().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Query().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Update().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Delete().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Row().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Raw().Before("*").Register("gorm:sharding", s.switchConn)
}

func (s *Sharding) switchConn(db *gorm.DB) {
	// 在某些情况下支持忽略分片，例如：
	// 当启用 DoubleWrite 时，我们需要在迁移期间通过表名查询数据库模式信息
	if _, ok := db.Get(ShardingIgnoreStoreKey); !ok {
		s.mutex.Lock()
		if db.Statement.ConnPool != nil {
			s.ConnPool = &ConnPool{ConnPool: db.Statement.ConnPool, sharding: s}
			db.Statement.ConnPool = s.ConnPool
		}
		s.mutex.Unlock()
	}
}

// resolve 将旧查询拆分为全表查询和分片表查询
func (s *Sharding) resolve(query string, args ...any) (ftQuery, stQuery, tableName string, err error) {
	ftQuery = query
	stQuery = query
	if len(s.configs) == 0 {
		return
	}

	expr, err := sqlparser.NewParser(strings.NewReader(query)).ParseStatement()
	if err != nil {
		return ftQuery, stQuery, tableName, nil
	}

	var table *sqlparser.TableName
	var condition sqlparser.Expr
	var isInsert bool
	var insertNames []*sqlparser.Ident
	var insertExpressions []*sqlparser.Exprs
	var insertStmt *sqlparser.InsertStatement

	switch stmt := expr.(type) {
	case *sqlparser.SelectStatement:
		tbl, ok := stmt.FromItems.(*sqlparser.TableName)
		if !ok {
			return
		}
		if stmt.Hint != nil && stmt.Hint.Value == "nosharding" {
			return
		}
		table = tbl
		condition = stmt.Condition
	case *sqlparser.InsertStatement:
		table = stmt.TableName
		isInsert = true
		insertNames = stmt.ColumnNames
		insertExpressions = stmt.Expressions
		insertStmt = stmt
	case *sqlparser.UpdateStatement:
		condition = stmt.Condition
		table = stmt.TableName
	case *sqlparser.DeleteStatement:
		condition = stmt.Condition
		table = stmt.TableName
	default:
		return ftQuery, stQuery, "", sqlparser.ErrNotImplemented
	}

	tableName = table.Name.Name
	r, ok := s.configs[tableName]
	if !ok {
		return
	}

	var suffix string
	if isInsert {
		var newTable *sqlparser.TableName
		for _, insertExpression := range insertExpressions {
			var value any
			var id int64
			var keyFind bool
			columnNames := insertNames
			insertValues := insertExpression.Exprs
			value, keyFind, err = s.insertValue(r.ShardingKey, insertNames, insertValues, args...)
			if err != nil {
				return
			}

			var subSuffix string
			subSuffix, err = getSuffix(value, id, keyFind, r)
			if err != nil {
				return
			}

			if suffix != "" && suffix != subSuffix {
				err = ErrInsertDiffSuffix
				return
			}

			suffix = subSuffix

			newTable = &sqlparser.TableName{Name: &sqlparser.Ident{Name: tableName + suffix}}

			fillID := true
			if isInsert {
				for _, name := range insertNames {
					if name.Name == "id" {
						fillID = false
						break
					}
				}
				suffixWord := strings.Replace(suffix, "_", "", 1)
				tblIdx, err := strconv.Atoi(suffixWord)
				if err != nil {
					tblIdx = slices.Index(r.ShardingSuffixs(), suffix)
					if tblIdx == -1 {
						return ftQuery, stQuery, tableName, errors.New("表后缀 '" + suffix + "' 不在 ShardingSuffixs 中。为了生成主键，ShardingSuffixs 应该包含所有表后缀")
					}
				}

				id := r.PrimaryKeyGeneratorFn(int64(tblIdx))
				if id == 0 {
					fillID = false
				}

				if fillID {
					columnNames = append(insertNames, &sqlparser.Ident{Name: "id"})
					insertValues = append(insertValues, &sqlparser.NumberLit{Value: strconv.FormatInt(id, 10)})
				}
			}

			if fillID {
				insertStmt.ColumnNames = columnNames
				insertExpression.Exprs = insertValues
			}
		}

		ftQuery = insertStmt.String()
		insertStmt.TableName = newTable
		stQuery = insertStmt.String()

	} else {
		var value any
		var id int64
		var keyFind bool
		value, id, keyFind, err = s.nonInsertValue(r.ShardingKey, condition, args...)
		if err != nil {
			return
		}

		suffix, err = getSuffix(value, id, keyFind, r)
		if err != nil {
			return
		}

		newTable := &sqlparser.TableName{Name: &sqlparser.Ident{Name: tableName + suffix}}

		switch stmt := expr.(type) {
		case *sqlparser.SelectStatement:
			ftQuery = stmt.String()
			stmt.FromItems = newTable
			stmt.OrderBy = replaceOrderByTableName(stmt.OrderBy, tableName, newTable.Name.Name)
			replaceTableNameInCondition(stmt.Condition, tableName, newTable.Name.Name)
			stQuery = stmt.String()
		case *sqlparser.UpdateStatement:
			ftQuery = stmt.String()
			stmt.TableName = newTable
			replaceTableNameInCondition(stmt.Condition, tableName, newTable.Name.Name)
			stQuery = stmt.String()
		case *sqlparser.DeleteStatement:
			ftQuery = stmt.String()
			stmt.TableName = newTable
			replaceTableNameInCondition(stmt.Condition, tableName, newTable.Name.Name)
			stQuery = stmt.String()
		}
	}

	return
}

func getSuffix(value any, id int64, keyFind bool, r Config) (suffix string, err error) {
	if keyFind {
		suffix, err = r.ShardingAlgorithm(value)
		if err != nil {
			return
		}
	} else {
		if r.ShardingAlgorithmByPrimaryKey == nil {
			err = fmt.Errorf("没有分片键且未配置 ShardingAlgorithmByPrimaryKey")
			return
		}
		suffix = r.ShardingAlgorithmByPrimaryKey(id)
	}

	return
}

func (s *Sharding) insertValue(key string, names []*sqlparser.Ident, exprs []sqlparser.Expr, args ...any) (value any, keyFind bool, err error) {
	if len(names) != len(exprs) {
		return nil, keyFind, errors.New("列名和表达式不匹配")
	}

	for i, name := range names {
		if name.Name == key {
			switch expr := exprs[i].(type) {
			case *sqlparser.BindExpr:
				value = args[expr.Pos]
			case *sqlparser.StringLit:
				value = expr.Value
			case *sqlparser.NumberLit:
				value = expr.Value
			default:
				return nil, keyFind, sqlparser.ErrNotImplemented
			}
			keyFind = true
			break
		}
	}
	if !keyFind {
		return nil, keyFind, ErrMissingShardingKey
	}

	return
}

func (s *Sharding) nonInsertValue(key string, condition sqlparser.Expr, args ...any) (value any, id int64, keyFind bool, err error) {
	err = sqlparser.Walk(sqlparser.VisitFunc(func(node sqlparser.Node) error {
		if n, ok := node.(*sqlparser.BinaryExpr); ok {
			x, ok := n.X.(*sqlparser.Ident)
			if !ok {
				if q, ok2 := n.X.(*sqlparser.QualifiedRef); ok2 {
					x = q.Column
					ok = true
				}
			}
			if ok {
				// 检查是否是分片键或时间字段
				isTimeRangeSharding := false
				if cfg, exists := s.configs["__current_table__"]; exists && cfg.ShardingType == "time_range" {
					isTimeRangeSharding = true
				}

				if x.Name == key && n.Op == sqlparser.EQ {
					keyFind = true
					switch expr := n.Y.(type) {
					case *sqlparser.BindExpr:
						value = args[expr.Pos]
					case *sqlparser.StringLit:
						value = expr.Value
					case *sqlparser.NumberLit:
						value = expr.Value
					default:
						return sqlparser.ErrNotImplemented
					}
					return nil
				} else if isTimeRangeSharding && x.Name == key && (n.Op == sqlparser.GT || n.Op == sqlparser.LT) {
					// 对于时间范围分片，支持范围查询 (>, <)
					keyFind = true
					switch expr := n.Y.(type) {
					case *sqlparser.BindExpr:
						value = args[expr.Pos]
					case *sqlparser.StringLit:
						value = expr.Value
					case *sqlparser.NumberLit:
						value = expr.Value
					default:
						return sqlparser.ErrNotImplemented
					}
					return nil
				} else if x.Name == "id" && n.Op == sqlparser.EQ {
					switch expr := n.Y.(type) {
					case *sqlparser.BindExpr:
						v := args[expr.Pos]
						var ok bool
						if id, ok = v.(int64); !ok {
							return fmt.Errorf("ID 必须是 int64 类型")
						}
					case *sqlparser.NumberLit:
						id, err = strconv.ParseInt(expr.Value, 10, 64)
						if err != nil {
							return err
						}
					default:
						return ErrInvalidID
					}
					return nil
				}
			}
		}
		return nil
	}), condition)
	if err != nil {
		return
	}

	if !keyFind && id == 0 {
		return nil, 0, keyFind, ErrMissingShardingKey
	}

	return
}

func replaceOrderByTableName(orderBy []*sqlparser.OrderingTerm, oldName, newName string) []*sqlparser.OrderingTerm {
	for i, term := range orderBy {
		if x, ok := term.X.(*sqlparser.QualifiedRef); ok {
			if x.Table.Name == oldName {
				x.Table.Name = newName
				orderBy[i].X = x
			}
		}
	}

	return orderBy
}

// replaceTableNameInCondition 遍历 WHERE 表达式树
// 并重命名任何匹配的限定列引用 oldName → newName
func replaceTableNameInCondition(expr sqlparser.Expr, oldName, newName string) {
	if expr == nil {
		return
	}

	_ = sqlparser.Walk(sqlparser.VisitFunc(func(node sqlparser.Node) error {
		if qr, ok := node.(*sqlparser.QualifiedRef); ok && qr.Table.Name == oldName {
			qr.Table.Name = newName
		}

		return nil
	}), expr)
}
