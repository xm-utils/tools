package database

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"runtime/debug"

	"gorm.io/gorm"
)

// Transaction 简化事务执行（推荐方式）
func Transaction(fn func(tx *gorm.DB) error) (err error) {
	defer recoverToErr(&err)

	gdb := GetDB()
	if gdb == nil {
		return errors.New("database connection not found")
	}
	return gdb.Transaction(fn)
}

// TxFunc GORM事务函数类型
type TxFunc func(tx *gorm.DB) error

// Tx GORM事务处理器
type Tx struct {
	db  *gorm.DB
	err error
}

// NewTx 创建新的事务
func NewTx() *Tx {
	tx := &Tx{}
	defer recoverToErr(&tx.err)

	gdb := GetDB()
	if gdb == nil {
		tx.err = errors.New("database connection not found")
		return tx
	}
	tx.db = gdb.Begin()
	return tx
}

// Execute 执行事务
func (tx *Tx) Execute(f TxFunc) (err error) {
	if tx.err != nil {
		return tx.err
	}
	if tx.db == nil {
		return driver.ErrBadConn
	}

	defer func() {
		if r := recover(); r != nil {
			tx.db.Rollback()
			err = fmt.Errorf("panic recovered: %v\nstack: %s", r, debug.Stack())

		}
	}()

	if err = f(tx.db); err != nil {
		tx.db.Rollback()
		return err
	}

	return tx.db.Commit().Error
}
