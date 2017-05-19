package executor

import (
<<<<<<< HEAD
	"errors"

=======
>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
	"github.com/castermode/Nesoi/src/sql/context"
	"github.com/castermode/Nesoi/src/sql/parser"
	"github.com/castermode/Nesoi/src/sql/plan"
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
<<<<<<< HEAD

	var querys []parser.Statement
	querys, err = executor.analyzer.Analyze(stmts)
	if err != nil {
		return nil, err
	}

	var p plan.Plan
	for _, query := range querys {
		switch query.StatementType() {
		case parser.DDL:
			rs, err = executor.executeQuery(query)
			if err != nil {
				return nil, err
			}
		case parser.Rows:
			p, err = plan.Optimize(query)
			if err != nil {
				return nil, err
			}
			rs, err = executor.executePlan(p)
			if err != nil {
				return nil, err
			}
		default:
			return nil, errors.New("unsupport clause!")
		}
=======
	
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

>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
		if rs != nil {
			rss = append(rss, rs)
		}
	}

	return rss, nil
}

func (executor *Executor) executeQuery(query parser.Statement) (result.Result, error) {
	var result result.Result

<<<<<<< HEAD
	result = &DDLExec{stmt: query, driver: executor.driver, context: executor.context}
	_, err := result.Next()
	if err != nil {
		return nil, err
=======
	switch stmt.StatementType() {
	case parser.DDL:
		result = &DDLExec{stmt: stmt, driver: executor.driver, context: executor.context}
		_, err := result.Next()
		if err != nil {
			return nil, err
		}
		return nil, nil
>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
	}
	return nil, nil
}

func (executor *Executor) executePlan(p plan.Plan) (result.Result, error) {
	return nil, nil
}
