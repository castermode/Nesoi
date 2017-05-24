package executor

import (
	"github.com/castermode/Nesoi/src/sql/plan"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/castermode/Nesoi/src/sql/util"
)

type ProjectionExec struct {
	fieldsNum int
	children  []result.Result
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

func (p *ProjectionExec) Columns() ([]string, error) {
	clms, err := p.children[0].Columns()
	if err != nil {
		return nil, err
	}
	var ret []string
	var i int
	for i < p.fieldsNum {
		ret = append(ret, clms[i])
		i++
	}

	return ret, nil
}

func (p *ProjectionExec) Next() (*result.Record, error) {
	record, err := p.children[0].Next()
	if err != nil {
		return nil, err
	}

	datums := []*util.Datum{}
	var i int
	for i < p.fieldsNum {
		datums = append(datums, record.Datums[i])
		i++
	}

	return &result.Record{Datums: datums}, nil

}
