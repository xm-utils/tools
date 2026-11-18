package database

import (
	"context"
	"errors"
	"strings"

	"github.com/xm-utils/tools/common"
	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"
)

// Ctx 发起一组带 context 的链式 CRUD 构建器。所有终端操作都绑定 ctx 生命周期：
// ctx 超时或被取消时，进行中的 SQL 立即中断并返回对应错误（context.Canceled / context.DeadlineExceeded）。
//
// 示例：
//
//	// 读：走主库按 ID 查询
//	user, err := Ctx[testUser](ctx).Primary().FindOne(1)
//	// 读：条件 + 排序 + 分页
//	list, total, err := Ctx[testUser](ctx).Where(Eq("team", "x")).Order("-age").Page(page).FindAll()
//	// 写：在事务中按条件批量更新
//	err := Ctx[testUser](ctx).Tx(tx).Where(Eq("team", "x")).UpdateByCondition(map[string]interface{}{"age": 99})
//
// 链式方法仅记录意图、不触发 SQL；只有终端操作（FindOne/FindAll/Insert/Update/Delete/Exec 等）执行。
// 未初始化数据库且未通过 Tx 指定会话时，终端操作返回 "database connection not found" 错误而非 panic，
// 与包级函数风格一致。超时控制由调用方通过 context.WithTimeout / context.WithCancel 传入。
func Ctx[T any](ctx context.Context) *CtxBuilder[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	return &CtxBuilder[T]{ctx: ctx}
}

// CtxBuilder 带 context 的链式 CRUD 构建器（每终端操作绑定泛型模型 T）。
type CtxBuilder[T any] struct {
	ctx       context.Context
	tx        *gorm.DB             // 显式会话（事务等），优先于进程级单例
	do        dbresolver.Operation // 读写路由提示（Primary/Replica）
	hasDo     bool
	conds     []*Condition           // 链式 WHERE 条件，多个之间 AND
	query     map[string]interface{} // 同 ListParam.Query：key 为 SQL 片段，val 为参数
	timeRange TimeParam              // 时间区间条件（column >= start AND column < end）
	model     *T                     // 显式声明的模型（影响表名解析）
	selects   []string
	orders    []string
	limit     int
	offset    int
	hasPage   bool
}

// ------------------[链式配置方法]-------------------

// WithContext 替换当前 context（传入 nil 时保持不变）。
func (b *CtxBuilder[T]) WithContext(ctx context.Context) *CtxBuilder[T] {
	if ctx != nil {
		b.ctx = ctx
	}
	return b
}

// Tx 指定底层会话（如事务中的 *gorm.DB）；终端操作仍会在其之上包裹 ctx。
func (b *CtxBuilder[T]) Tx(tx *gorm.DB) *CtxBuilder[T] {
	b.tx = tx
	return b
}

// Primary 标记本次构建器的读操作强制走主库（源）。
func (b *CtxBuilder[T]) Primary() *CtxBuilder[T] {
	b.do, b.hasDo = dbresolver.Write, true
	return b
}

// Replica 标记本次构建器的读操作尝试走从库（未注册从库时回落主库）。
func (b *CtxBuilder[T]) Replica() *CtxBuilder[T] {
	b.do, b.hasDo = dbresolver.Read, true
	return b
}

// Model 显式声明模型（用于表名解析）；多数终端操作会自行推断，无需设置。
func (b *CtxBuilder[T]) Model(m *T) *CtxBuilder[T] {
	b.model = m
	return b
}

// Where 追加链式 WHERE 条件，多次调用之间为 AND 关系；nil 或空条件会被跳过。
func (b *CtxBuilder[T]) Where(conds ...*Condition) *CtxBuilder[T] {
	b.conds = append(b.conds, conds...)
	return b
}

// Query 追加 ListParam.Query 等价条件：key 为含占位符的 SQL 片段，val 为其参数。
// 多个条件之间 AND；map 遍历顺序不确定（与旧 FindAll 行为一致）。
func (b *CtxBuilder[T]) Query(query map[string]interface{}) *CtxBuilder[T] {
	if len(query) == 0 {
		return b
	}
	if b.query == nil {
		b.query = make(map[string]interface{}, len(query))
	}
	for key, val := range query {
		b.query[key] = val
	}
	return b
}

// Time 追加时间区间条件：column >= start AND column < end；无效/nil 的 TimeParam 静默忽略。
func (b *CtxBuilder[T]) Time(t TimeParam) *CtxBuilder[T] {
	if t != nil && t.IsValid() {
		b.timeRange = t
	}
	return b
}

// ApplyListParam 一次性应用 ListParam 的全部条件（Cond/Query/Time/Order/Page），
// 便于旧 ListParam 风格 API 透传到链式构建器。
func (b *CtxBuilder[T]) ApplyListParam(param ListParam) *CtxBuilder[T] {
	return b.Where(param.Cond).Query(param.Query).Time(param.Time).Order(param.Order...).Page(param.Page)
}

// Select 指定读写的字段列表。
func (b *CtxBuilder[T]) Select(columns ...string) *CtxBuilder[T] {
	b.selects = append(b.selects, columns...)
	return b
}

// Order 指定排序，"-" 前缀表示 DESC（与 FindAll 的 Order 语义一致）。
func (b *CtxBuilder[T]) Order(orders ...string) *CtxBuilder[T] {
	b.orders = append(b.orders, orders...)
	return b
}

// Page 指定分页参数；p 无效时忽略。
func (b *CtxBuilder[T]) Page(p PageParam) *CtxBuilder[T] {
	if p != nil && p.IsValid() {
		limit, offset := p.GetLimit()
		b.limit, b.offset, b.hasPage = int(limit), int(offset), true
	}
	return b
}

// ------------------[内部会话构建]-------------------

// base 返回包裹 ctx、读写路由、Model、WHERE、Select 后的会话（不含 Order/分页，
// 便于 Count 等场景复用同一构建器而不影响统计）。
func (b *CtxBuilder[T]) base() (*gorm.DB, error) {
	db, err := b.rawDB()
	if err != nil {
		return nil, err
	}
	if b.model != nil {
		db = db.Model(b.model)
	}
	for _, c := range b.conds {
		if c.IsEmpty() {
			continue
		}
		q, v := c.GormWhere()
		db = db.Where(q, v...)
	}
	for key, val := range b.query {
		db = db.Where(key, val)
	}
	if b.timeRange != nil {
		column := b.timeRange.GetColumn()
		start, end := b.timeRange.GetTime()
		db = db.Where(column+" >= ?", start).Where(column+" < ?", end)
	}
	if len(b.selects) > 0 {
		db = db.Select(b.selects)
	}
	return db, nil
}

// rawDB 仅包裹 ctx 与读写路由，不应用链式条件（供 Insert/Raw/Exec 等使用）。
func (b *CtxBuilder[T]) rawDB() (*gorm.DB, error) {
	db := b.tx
	if db == nil {
		db = GetDB()
	}
	if db == nil {
		return nil, errors.New("database connection not found")
	}
	db = db.WithContext(b.ctx)
	if b.hasDo {
		db = db.Clauses(b.do)
	}
	return db, nil
}

// applyOrderPage 在会话上追加 Order 与分页。
func (b *CtxBuilder[T]) applyOrderPage(db *gorm.DB) *gorm.DB {
	if len(b.orders) > 0 {
		db = db.Order(buildOrderClause(b.orders))
	}
	if b.hasPage {
		db = db.Limit(b.limit).Offset(b.offset)
	}
	return db
}

// hasConditions 判断是否存在有效的链式 WHERE 条件。
func (b *CtxBuilder[T]) hasConditions() bool {
	for _, c := range b.conds {
		if !c.IsEmpty() {
			return true
		}
	}
	return false
}

// buildOrderClause 将排序列表转换为 SQL ORDER BY 子句，"-" 前缀转 DESC。
func buildOrderClause(orders []string) string {
	list := make([]string, len(orders))
	for i, order := range orders {
		if strings.HasPrefix(order, "-") {
			list[i] = order[1:] + " DESC"
		} else {
			list[i] = order
		}
	}
	return strings.Join(list, ",")
}

// ------------------[终端操作：读]-------------------

// ReadOne 以入参 t 的结构体非零字段作为条件查询单条记录。
// 与包级 ReadOne 吞错返回 nil 不同：未命中返回 (nil, nil)，真实错误（含 ctx 超时/取消）透传。
func (b *CtxBuilder[T]) ReadOne(t T, cols ...string) (result *T, err error) {
	defer recoverToErr(&err)
	db, err := b.base()
	if err != nil {
		return nil, err
	}
	if len(cols) > 0 {
		cs := make([]any, 0, len(cols))
		for _, col := range cols {
			cs = append(cs, col)
		}
		db = db.Where(t, cs)
	} else {
		db = db.Where(t)
	}
	var out T
	if err := db.First(&out).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &out, nil
}

// FindOne 根据 ID 查询单条记录；未命中返回 (nil, err)，错误透传。
func (b *CtxBuilder[T]) FindOne(id int64) (result *T, err error) {
	defer recoverToErr(&err)
	db, err := b.base()
	if err != nil {
		return nil, err
	}
	var out T
	if err := db.First(&out, id).Error; err != nil {
		return nil, err
	}
	return &out, nil
}

// FindAll 分页查询列表：先按条件 Count，再应用 Order/分页取数。
func (b *CtxBuilder[T]) FindAll() (list []*T, total int64, err error) {
	defer recoverToErr(&err)
	db, err := b.base()
	if err != nil {
		return nil, 0, err
	}
	var model T
	query := db.Model(&model)

	if err = query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return make([]*T, 0), 0, nil
	}

	if err = b.applyOrderPage(query).Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

// FindList 查询列表（应用链式条件与可选 Order/分页）。
func (b *CtxBuilder[T]) FindList() (list []*T, err error) {
	defer recoverToErr(&err)
	db, err := b.base()
	if err != nil {
		return nil, err
	}
	var model T
	err = b.applyOrderPage(db.Model(&model)).Find(&list).Error
	return
}

// Count 统计链式条件下的数量（不受 Order/分页影响）。
func (b *CtxBuilder[T]) Count() (count int64, err error) {
	defer recoverToErr(&err)
	db, err := b.base()
	if err != nil {
		return 0, err
	}
	var model T
	err = db.Model(&model).Count(&count).Error
	return
}

// Raw 执行原生 SQL 查询并扫描结果（不应用链式条件）。
func (b *CtxBuilder[T]) Raw(sql string, values ...interface{}) (list []*T, err error) {
	defer recoverToErr(&err)
	db, err := b.rawDB()
	if err != nil {
		return nil, err
	}
	err = db.Raw(sql, values...).Scan(&list).Error
	return list, err
}

// ------------------[终端操作：写]-------------------

// Insert 插入单条记录。
func (b *CtxBuilder[T]) Insert(data *T) (err error) {
	defer recoverToErr(&err)
	db, err := b.rawDB()
	if err != nil {
		return err
	}
	return db.Create(data).Error
}

// InsertBatch 批量插入，返回累计影响行数；批次间会检查 ctx 状态，
// 超时/取消时中断后续批次并返回已写入行数与 ctx 错误。
func (b *CtxBuilder[T]) InsertBatch(batchSize int, dataList []*T) (affected int64, err error) {
	defer recoverToErr(&err)
	if len(dataList) == 0 {
		return 0, nil
	}
	db, err := b.rawDB()
	if err != nil {
		return 0, err
	}
	if batchSize <= 0 {
		batchSize = len(dataList)
	}

	total := len(dataList)
	for i := 0; i < total; i += batchSize {
		if ctxErr := b.ctx.Err(); ctxErr != nil {
			return affected, ctxErr
		}
		end := i + batchSize
		if end > total {
			end = total
		}
		res := db.CreateInBatches(dataList[i:end], batchSize)
		if res.Error != nil {
			return affected, res.Error
		}
		affected += res.RowsAffected
	}
	return affected, nil
}

// Update 更新 data：默认按主键（与包级 Update 语义一致）；
// 若链式设置了 Where 条件，则与主键条件 AND 组合。仅更新非零字段，指定 columns 时只写这些列。
func (b *CtxBuilder[T]) Update(data *T, columns ...string) (err error) {
	defer recoverToErr(&err)
	db, err := b.base()
	if err != nil {
		return err
	}
	db = db.Model(data)
	if len(columns) > 0 {
		db = db.Select(columns)
	}
	return db.Updates(data).Error
}

// UpdateModel 比对 old 与 form，仅将非空且发生变化的差异字段更新到数据库（Select 只写这些列）。
// 语义与包级 UpdateModel 一致：
//   - 不传 whereCols：按主键更新；
//   - 传入 whereCols（Go 字段名）：在保留主键条件的基础上，追加 form 中这些字段的值作为额外 WHERE 条件（两者 AND）。
//
// 无差异时直接返回 nil；old 为空指针时返回 gorm.ErrRecordNotFound。
func (b *CtxBuilder[T]) UpdateModel(old *T, form T, whereCols ...string) (err error) {
	defer recoverToErr(&err)
	if old == nil {
		return gorm.ErrRecordNotFound
	}

	var fields []string
	old, fields = common.CompareAndAssign(old, form)
	if len(fields) == 0 {
		return nil
	}

	db, err := b.base()
	if err != nil {
		return err
	}
	// 以 old 作为 Model 目标保留主键条件；whereCols 字段作为额外 WHERE（主键 AND whereCols）。
	db = db.Model(old).Select(fields)
	if len(whereCols) > 0 {
		cond := pickFields(form, whereCols)
		db = db.Where(&cond)
	}
	return db.Updates(old).Error
}

// UpdateByCondition 按链式 Where 条件批量更新指定字段；条件为空时拒绝执行，避免全表更新。
func (b *CtxBuilder[T]) UpdateByCondition(updates map[string]interface{}) (err error) {
	defer recoverToErr(&err)
	if len(updates) == 0 {
		return errors.New("updates cannot be empty")
	}
	if !b.hasConditions() {
		return errors.New("condition cannot be empty")
	}
	db, err := b.base()
	if err != nil {
		return err
	}
	var model T
	return db.Model(&model).Updates(updates).Error
}

// Delete 删除 data：默认按主键（与包级 Delete 语义一致）；链式 Where 条件与主键 AND 组合。
func (b *CtxBuilder[T]) Delete(data *T) (err error) {
	defer recoverToErr(&err)
	db, err := b.base()
	if err != nil {
		return err
	}
	return db.Delete(data).Error
}

// DeleteByCondition 按链式 Where 条件删除；条件为空时拒绝执行，避免全表删除。
func (b *CtxBuilder[T]) DeleteByCondition() (err error) {
	defer recoverToErr(&err)
	if !b.hasConditions() {
		return errors.New("condition cannot be empty")
	}
	db, err := b.base()
	if err != nil {
		return err
	}
	var model T
	if b.model == nil {
		db = db.Model(&model)
	}
	return db.Delete(&model).Error
}

// Exec 执行原生 SQL（不应用链式条件），返回影响行数。
func (b *CtxBuilder[T]) Exec(sql string, values ...interface{}) (affected int64, err error) {
	defer recoverToErr(&err)
	db, err := b.rawDB()
	if err != nil {
		return 0, err
	}
	res := db.Exec(sql, values...)
	return res.RowsAffected, res.Error
}
