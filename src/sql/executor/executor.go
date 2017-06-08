package executor

import (
	"errors"

	"github.com/castermode/Nesoi/src/sql/context"
	"github.com/castermode/Nesoi/src/sql/parser"
	"github.com/castermode/Nesoi/src/sql/plan"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/castermode/Nesoi/src/sql/store"
	"github.com/castermode/Nesoi/src/sql/util"
)

type Executor struct {
	parser   *parser.Parser
	analyzer *parser.Analyzer
	driver   store.Driver
	context  *context.Context
}

func NewExecutor(sd store.Driver, ctx *context.Context) *Executor {
	return &Executor{
		parser:   parser.NewParser(),
		analyzer: parser.NewAnalyzer(sd, ctx),
		driver:   sd,
		context:  ctx,
	}
}

func valueToDatum(v interface{}) (*util.Datum, error) {
	var d *util.Datum = &util.Datum{}
	switch v.(type) {
	case int64:
		d.SetK(util.KindInt64)
		d.SetI(v.(int64))
	case string:
		d.SetK(util.KindString)
		d.SetB(util.ToSlice(v.(string)))
	default:
		return nil, errors.New("unsupport value type!")
	}

	return d, nil
}

func (executor *Executor) Execute(sql string) ([]result.Result, error) {
	var rs result.Result
	var rss []result.Result
	stmts, err := executor.parser.Parse(sql)
	if err != nil {
		return nil, err
	}

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
		case parser.RowsAffected:
			rs, err = executor.executeWrite(query)
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
		if rs != nil {
			rss = append(rss, rs)
		}
	}

	return rss, nil
}

func (executor *Executor) executeQuery(query parser.Statement) (result.Result, error) {
	var result result.Result

	result = &DDLExec{stmt: query, driver: executor.driver, context: executor.context}
	_, err := result.Next()
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (executor *Executor) executeWrite(query parser.Statement) (result.Result, error) {
	var result result.Result
	var p plan.Plan
	var err error

	switch query.(type) {
	case *parser.InsertQuery:
		result = &InsertExec{stmt: query, driver: executor.driver, context: executor.context}
	case *parser.UpdateQuery:
		p, err = plan.Optimize(query)
		if err != nil {
			return nil, err
		}
		result, err = executor.executePlan(p)
		if err != nil {
			return nil, err
		}
	}

	for {
		if result.Done() {
			break
		}
		_, err := result.Next()
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func makePlanExec(p plan.Plan, e *Executor) result.Result {
	switch p.(type) {
	case *plan.Show:
		s := p.(*plan.Show)
		return &ShowExec{operator: s.Operator, driver: e.driver, context: e.context}
	case *plan.Simple:
		s := p.(*plan.Simple)
		return &SimpleExec{fields: s.Fields, context: e.context}
	case *plan.Scan:
		s := p.(*plan.Scan)
		return &ScanExec{scan: s, driver: e.driver, context: e.context}
	case *plan.ScanWithPK:
		s := p.(*plan.ScanWithPK)
		return &ScanWithPKExec{scanpk: s, driver: e.driver, context: e.context}
	case *plan.Selection:
		s := p.(*plan.Selection)
		return NewSelectionExec(s, e)
	case *plan.Projection:
		s := p.(*plan.Projection)
		return NewProjectionExec(s, e)
	case *plan.Limit:
		s := p.(*plan.Limit)
		return NewLimitExec(s, e)
	case *plan.Update:
		s := p.(*plan.Update)
		return NewUpdateExec(s, e)
	}

	return nil
}

func (executor *Executor) executePlan(p plan.Plan) (result.Result, error) {
	return makePlanExec(p, executor), nil
}
