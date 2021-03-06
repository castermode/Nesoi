package executor

import (
	"github.com/castermode/Nesoi/src/sql/plan"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/castermode/Nesoi/src/sql/store"
)

type LimitExec struct {
	num      uint64
	cur      uint64
	children []result.Result
	done     bool
}

func NewLimitExec(l *plan.Limit, e *Executor) *LimitExec {
	lmtExec := &LimitExec{
		num: l.Num,
	}

	for _, p := range l.GetChildren() {
		lmtExec.children = append(lmtExec.children, makePlanExec(p, e))
	}

	return lmtExec
}

func (l *LimitExec) Columns() ([]*store.ColumnInfo, error) {
	return l.children[0].Columns()
}

func (l *LimitExec) Next() (*result.Record, error) {
	if l.cur >= l.num {
		l.done = true
		return nil, nil
	}

	r, err := l.children[0].Next()
	if err != nil {
		return nil, err
	}

	l.cur++
	return r, nil
}

func (l *LimitExec) Done() bool {
	return l.done
}
