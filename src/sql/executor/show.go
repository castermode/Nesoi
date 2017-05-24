package executor

import (
	"errors"

	"github.com/castermode/Nesoi/src/sql/parser"
	"github.com/castermode/Nesoi/src/sql/result"
)

type ShowExec struct {
	Operator int
	done     bool
}

func (s *ShowExec) Columns() ([]string, error) {
	if s.Operator == parser.SDATABASES {
		return []string{"DATABASES"}, nil
	}

	if s.Operator == parser.STABLES {
		return []string{"TABLES"}, nil
	}

	return nil, errors.New("unsupport clause!")
}

func (s *ShowExec) Next() (*result.Record, error) {
	return nil, nil
}
