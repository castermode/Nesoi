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

type ShowExec struct {
	context  *context.Context
	driver   store.Driver
	operator int
	keys     []string
	pos      int
	cursor   uint64
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

	if s.operator == parser.SDATABASES {
		ci.Name = "DATABASES"
		ci.OrgName = "DATABASES"
		ret = append(ret, ci)
		return ret, nil
	}

	if s.operator == parser.STABLES {
		ci.Name = "TABLES"
		ci.OrgName = "TABLES"
		ret = append(ret, ci)
		return ret, nil
	}

	return nil, errors.New("unsupport clause!")
}

func (s *ShowExec) nextKey() ([]byte, bool, error) {
	if s.done {
		return nil, false, nil
	}

	var sn string
	if s.operator == parser.SDATABASES {
		sn = store.DBFlag
	} else {
		sn = store.TableFlag + s.context.GetCurrentDB()
	}
	match := store.SystemFlag + sn + "*"
	if s.keys != nil && s.pos < len(s.keys) {
		if s.pos == len(s.keys)-1 && s.cursor == 0 {
			s.done = true
		}
		key := s.keys[s.pos]
		s.pos++
		return util.ToSlice(key), true, nil
	} else {
		var err error
		s.keys, s.cursor, err = s.driver.ScanSysRecords(s.cursor, match, 10)
		if err != nil {
			return nil, true, err
		}
		s.pos = 0
		if s.keys != nil && s.pos < len(s.keys) {
			if s.pos == len(s.keys)-1 && s.cursor == 0 {
				s.done = true
			}
			key := s.keys[s.pos]
			s.pos++
			return util.ToSlice(key), true, nil
		}

		s.done = true
		return nil, false, nil
	}
}

func (s *ShowExec) Next() (*result.Record, error) {
	value, exist, err := s.nextKey()
	if err != nil {
		return nil, err
	}

	if !exist {
		return nil, nil
	}

	var datums []*util.Datum = make([]*util.Datum, 0)
	var p string
	if s.operator == parser.SDATABASES {
		p = store.SystemFlag + store.DBFlag
	} else {
		p = store.SystemFlag + store.TableFlag + s.context.GetCurrentDB() + "."
	}
	l := len(value)
	pl := len(p)
	d := &util.Datum{}
	d.SetK(util.KindString)
	d.SetB(value[pl:l])
	datums = append(datums, d)

	return &result.Record{Datums: datums}, nil
}

func (s *ShowExec) Done() bool {
	return s.done
}
