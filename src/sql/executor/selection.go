package executor

import (
	"github.com/castermode/Nesoi/src/sql/plan"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/castermode/Nesoi/src/sql/store"
)

type SelectionExec struct {
	filter   *plan.Qual
	children []result.Result
	done     bool
}

func NewSelectionExec(s *plan.Selection, e *Executor) *SelectionExec {
	selExec := &SelectionExec{
		filter: s.Filter,
	}

	for _, p := range s.GetChildren() {
		selExec.children = append(selExec.children, makePlanExec(p, e))
	}

	return selExec
}

func (s *SelectionExec) Columns() ([]*store.ColumnInfo, error) {
	return s.children[0].Columns()
}

func (s *SelectionExec) Next() (*result.Record, error) {
	if s.done {
		return nil, nil
	}

	v, err := valueToDatum(s.filter.Value)
	if err != nil {
		return nil, err
	}

	for {
		r, err := s.children[0].Next()
		if err != nil {
			return nil, err
		}

		if r == nil {
			s.done = true
			return nil, nil
		}

		if v.Equal(r.Datums[s.filter.Pos-1]) {
			return r, nil
		}
	}
}

func (s *SelectionExec) Done() bool {
	return s.done
}
