package executor

import (
	"github.com/castermode/Nesoi/src/sql/context"
	"github.com/castermode/Nesoi/src/sql/parser"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/go-redis/redis"
)

type Executor struct {
	parser   *parser.Parser
	analyzer *parser.Analyzer
	driver   *redis.Client
	context  *context.Context
}

func NewExecutor(sd *redis.Client, ctx *context.Context) *Executor {
	return &Executor{
		parser:   parser.NewParser(),
		analyzer: parser.NewAnalyzer(sd, ctx),
		driver:   sd,
		context:  ctx,
	}
}

func (executor *Executor) Execute(sql string) ([]result.Result, error) {
	var rs result.Result
	var rss []result.Result
	stmts, err := executor.parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	
	_, err = executor.analyzer.Analyze(stmts)
	if err != nil {
		return nil, err
	}
	
	//TODO plan
	
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
		result = &DDLExec{stmt: stmt, driver: executor.driver, context: executor.context}
		_, err := result.Next()
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	return result, nil
}
