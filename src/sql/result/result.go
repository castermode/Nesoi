package result

import (
	"github.com/castermode/Nesoi/src/sql/store"
	"github.com/castermode/Nesoi/src/sql/util"
)

type Record struct {
	Datums []*util.Datum
}

type Result interface {
	Columns() ([]*store.ColumnInfo, error)
	Next() (*Record, error)
	Done() bool
}
