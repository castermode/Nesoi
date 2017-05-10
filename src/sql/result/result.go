package result

import (
	"github.com/castermode/Nesoi/src/sql/util"
)

type Record struct {
	datum []util.Datum
}

type Result interface {
	Next() (*Record, error) 
}
