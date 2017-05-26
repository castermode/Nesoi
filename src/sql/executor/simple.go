package executor

import (
	"errors"

	"github.com/castermode/Nesoi/src/sql/context"
	"github.com/castermode/Nesoi/src/sql/mysql"
	"github.com/castermode/Nesoi/src/sql/parser"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/castermode/Nesoi/src/sql/store"
	"github.com/castermode/Nesoi/src/sql/util"
)

type SimpleExec struct {
	fields  []*parser.TargetRes
	done    bool
	context *context.Context
}

func (s *SimpleExec) Columns() ([]*store.ColumnInfo, error) {
	ret := []*store.ColumnInfo{}

	for _, f := range s.fields {
		ci := &store.ColumnInfo{
			Schema:   s.context.GetCurrentDB(),
			Table:    "dual",
			OrgTable: "dual",
		}
		switch f.Type {
		case parser.ESYSVAR:
			ci.Name = f.SysVar
			ci.OrgName = f.SysVar
			ci.Type = uint8(mysql.TypeString)
			ret = append(ret, ci)
		case parser.EVALUE:
			ci.Name = "EXPRESSION"
			ci.OrgName = "EXPRESSION"
			switch f.Value.(type) {
			case int64:
				ci.Type = uint8(mysql.TypeLong)
				ci.ColumnLength = 4
			case string:
				ci.Type = mysql.TypeString
			}
			ret = append(ret, ci)
		default:
			return nil, errors.New("caluse error!")
		}
	}

	return ret, nil
}

func (s *SimpleExec) Next() (*result.Record, error) {
	if s.done {
		return nil, nil
	}

	r := &result.Record{}
	for _, f := range s.fields {
		switch f.Type {
		case parser.ESYSVAR:
			sv := context.GetSysVar(f.SysVar)
			if sv == nil {
				return nil, errors.New("unsupport sysvar @@" + f.SysVar)
			}
			d := &util.Datum{}
			d.SetK(util.KindString)
			d.SetB(util.ToSlice(sv.Name))
			r.Datums = append(r.Datums, d)
		case parser.EVALUE:
			d, err := valueToDatum(f.Value)
			if err != nil {
				return nil, err
			}
			r.Datums = append(r.Datums, d)
		default:
			return nil, errors.New("caluse error!")
		}
	}
	s.done = true

	return r, nil
}
