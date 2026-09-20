package database

import (
	"database/sql/driver"
	"fmt"
	"runtime/debug"

	"github.com/beego/beego/v2/client/orm"
)

type TxFunc func(o orm.TxOrmer) error

type OrmTx struct {
	o   orm.TxOrmer
	err error
}

func NewOrmTx() (tx *OrmTx) {
	tx = &OrmTx{}
	defer recoverToErr(&tx.err)
	ormer, err := orm.NewOrm().Begin()
	tx.o = ormer
	tx.err = err
	return tx
}

func (tx *OrmTx) Execute(f TxFunc) (err error) {
	if tx.err != nil {
		return tx.err
	}
	if tx.o == nil {
		return driver.ErrBadConn
	}
	// 捕获 beego orm 或业务回调中可能出现的 panic，
	// 并在 panic 时回滚事务，避免连接泄漏。
	defer func() {
		if r := recover(); r != nil {
			_ = tx.o.Rollback()
			err = fmt.Errorf("panic recovered: %v\nstack: %s", r, debug.Stack())
		}
	}()
	err = f(tx.o)
	if err != nil {
		_ = tx.o.Rollback()
		return err
	}

	return tx.o.Commit()
}
