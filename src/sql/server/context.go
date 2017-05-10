package server

import (
	"github.com/castermode/Nesoi/src/sql/executor"
	"github.com/castermode/Nesoi/src/sql/result"
)

type Context struct {
	Executor *executor.Executor

	affectedRows  uint64
	lastInsertID  uint64
	status        uint16
	warningCount uint16
}

func (ctx *Context) execute(sql string) ([]result.Result, error) {
	return ctx.Executor.Execute(sql)
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
