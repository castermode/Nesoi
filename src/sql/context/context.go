package context

import (
	"github.com/castermode/Nesoi/src/sql/mysql"
)

type Context struct {
	currentDB    string
	affectedRows uint64
	lastInsertID uint64
	status       uint16
	warningCount uint16
}

func NewContext() *Context {
	return &Context{
		currentDB: "Nesoi",
		status:    mysql.ServerStatusAutocommit,
	}
}

func (ctx *Context) AffectedRows() uint64 {
	return ctx.affectedRows
}

func (ctx *Context) LastInsertID() uint64 {
	return ctx.lastInsertID
}

func (ctx *Context) Status() uint16 {
	return ctx.status
}

func (ctx *Context) WarningCount() uint16 {
	return ctx.warningCount
}

func (ctx *Context) GetCurrentDB() string {
	return ctx.currentDB
}

func (ctx *Context) GetTableName(schema string, tname string) string {
	shm := schema
	if shm == "" {
		shm = ctx.GetCurrentDB()
	}

	return shm + "." + tname
}
