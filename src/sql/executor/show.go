package executor

import (
	"errors"

	"github.com/castermode/Nesoi/src/sql/context"
	"github.com/castermode/Nesoi/src/sql/mysql"
	"github.com/castermode/Nesoi/src/sql/parser"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/castermode/Nesoi/src/sql/store"
)

type ShowExec struct {
	context  *context.Context
	Operator int
	done     bool
}

func (s *ShowExec) Columns() ([]*store.ColumnInfo, error) {
	ret := []*store.ColumnInfo{}
	ci := &store.ColumnInfo{
		Schema:   s.context.GetCurrentDB(),
		Table:    "dual",
		OrgTable: "dual",
		Type:     mysql.TypeString,
	}

	if s.Operator == parser.SDATABASES {
		ci.Name = "DATABASES"
		ci.OrgName = "DATABASES"
		ret = append(ret, ci)
		return ret, nil
	}

	if s.Operator == parser.STABLES {
		ci.Name = "TABLES"
		ci.OrgName = "TABLES"
		ret = append(ret, ci)
		return ret, nil
	}

	return nil, errors.New("unsupport clause!")
}

func (s *ShowExec) Next() (*result.Record, error) {
	return nil, nil
}
