package executor

import (
	"github.com/castermode/Nesoi/src/sql/parser"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/go-redis/redis"
)

type Executor struct {
	parser *parser.Parser
	driver *redis.Client
}

func NewExecutor(sd *redis.Client) *Executor {
	return &Executor{
		parser: parser.NewParser(),
		driver:	sd,
	}
}

func (executor *Executor) Execute(sql string) ([]result.Result, error) {
	var rs result.Result
	var rss []result.Result
	stmts, err := executor.parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	
	//@TODO Plan

	for _, stmt := range stmts {
		rs, err = executor.executeStmt(stmt)
		if err != nil {
			return nil, err
		}
		
		if rs != nil {
			rss = append(rss, rs)	
		}
	}

	return rss, nil
}

func (executor *Executor) executeStmt(stmt parser.Statement) (result.Result, error) {
	var result result.Result
	
	switch stmt.StatementType() {
	case parser.DDL:
		result = &DDLExec{stmt: stmt, driver: executor.driver}
		_, err := result.Next()
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	return result, nil
}
