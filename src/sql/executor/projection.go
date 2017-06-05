package executor

import (
	"github.com/castermode/Nesoi/src/sql/plan"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/castermode/Nesoi/src/sql/store"
	"github.com/castermode/Nesoi/src/sql/util"
)

type ProjectionExec struct {
	fieldsNum int
	children  []result.Result
	done      bool
}

func NewProjectionExec(p *plan.Projection, e *Executor) *ProjectionExec {
	pExec := &ProjectionExec{
		fieldsNum: p.FieldsNum,
	}

	for _, n := range p.GetChildren() {
		pExec.children = append(pExec.children, makePlanExec(n, e))
	}

	return pExec
}

func (p *ProjectionExec) Columns() ([]*store.ColumnInfo, error) {
	clms, err := p.children[0].Columns()
	if err != nil {
		return nil, err
	}

	ret := []*store.ColumnInfo{}
	var i int
	for i < p.fieldsNum {
		ret = append(ret, clms[i])
		i++
	}

	return ret, nil
}

func (p *ProjectionExec) Next() (*result.Record, error) {
	if p.done {
		return nil, nil
	}

	record, err := p.children[0].Next()
	if err != nil {
		return nil, err
	}
	if record == nil {
		p.done = true
		return nil, nil
	}

	datums := []*util.Datum{}
	var i int
	for i < p.fieldsNum {
		datums = append(datums, record.Datums[i])
		i++
	}

	return &result.Record{Datums: datums}, nil
}

func (p *ProjectionExec) Done() bool {
	return p.done
}
