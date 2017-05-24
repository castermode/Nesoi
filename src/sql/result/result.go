package result

import (
	"github.com/castermode/Nesoi/src/sql/util"
)

type Record struct {
	Datums []*util.Datum
}

type Result interface {
	Columns() ([]string, error)
	Next() (*Record, error)
}
