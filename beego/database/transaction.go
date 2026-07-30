package database

import (
	"database/sql/driver"

	"github.com/beego/beego/v2/client/orm"
)

type TxFunc func(o orm.TxOrmer) error

type OrmTx struct {
	o   orm.TxOrmer
	err error
}

func NewOrmTx() *OrmTx {
	ormer, err := orm.NewOrm().Begin()
	return &OrmTx{
		o:   ormer,
		err: err,
	}
}

func (tx *OrmTx) Execute(f TxFunc) error {
	if tx.err != nil {
		return tx.err
	}
	if tx.o == nil {
		return driver.ErrBadConn
	}
	err := f(tx.o)
	if err != nil {
		_ = tx.o.Rollback()
		return err
	}

	return tx.o.Commit()
}
