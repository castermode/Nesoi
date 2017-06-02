package executor

import (
	"errors"
	"strings"

	"github.com/castermode/Nesoi/src/sql/context"
	"github.com/castermode/Nesoi/src/sql/mysql"
	"github.com/castermode/Nesoi/src/sql/parser"
	"github.com/castermode/Nesoi/src/sql/plan"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/castermode/Nesoi/src/sql/store"
	"github.com/castermode/Nesoi/src/sql/util"
	"github.com/go-redis/redis"
)

type ScanExec struct {
	scan    *plan.Scan
	driver  *redis.Client
	context *context.Context
	keys    []string
	pos     int
	cursor  uint64
	done    bool
}

func (s *ScanExec) Columns() ([]*store.ColumnInfo, error) {
	ret := []*store.ColumnInfo{}
	for _, f := range s.scan.Fields {
		ci := &store.ColumnInfo{}
		switch f.Type {
		case parser.ETARGET:
			st := strings.Split(s.scan.From.Name, ".")
			ci.Schema = st[0]
			ci.Table = st[1]
			ci.OrgTable = st[1]
			ci.Name = s.scan.From.ColumnMap[f.FieldID - 1].Name
			ci.OrgName = s.scan.From.ColumnMap[f.FieldID - 1].Name
			switch s.scan.From.ColumnMap[f.FieldID - 1].Type.(type) {
			case *parser.IntType:
				ci.Type = mysql.TypeLong
				ci.ColumnLength = 4
			case *parser.StringType:
				ci.Type = mysql.TypeString
			}
			if s.scan.From.ColumnMap[f.FieldID - 1].Nullable == parser.NotNull {
				ci.Flag |= mysql.NotNullFlag
			}
			if s.scan.From.ColumnMap[f.FieldID - 1].PrimaryKey {
				ci.Flag |= mysql.PriKeyFlag
			}
			if s.scan.From.ColumnMap[f.FieldID - 1].Unique {
				ci.Flag |= mysql.UniqueKeyFlag
			}
			ret = append(ret, ci)
		case parser.ESYSVAR:
			ci.Schema = s.context.GetCurrentDB()
			ci.Table = "dual"
			ci.OrgTable = "dual"
			ci.Name = f.SysVar
			ci.OrgName = f.SysVar
			ci.Type = uint8(mysql.TypeString)
			ret = append(ret, ci)
		case parser.EVALUE:
			ci.Schema = s.context.GetCurrentDB()
			ci.Table = "dual"
			ci.OrgTable = "dual"
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

func (s *ScanExec) nextKey() ([]byte, bool, error) {
	if s.done {
		return nil, false, nil
	}

	if s.keys != nil && s.pos < len(s.keys) {
		if s.pos == len(s.keys)-1 && s.cursor == 0 {
			s.done = true
		}
		key := s.keys[s.pos]
		s.pos++
		return util.ToSlice(key), true, nil
	} else {
		var err error
		match := store.UserFlag + s.scan.From.Name + "/*"
		s.keys, s.cursor, err = s.driver.Scan(s.cursor, match, 10).Result()
		if err != nil {
			return nil, true, err
		}
		s.pos = 0
		if s.keys != nil {
			if s.pos == len(s.keys)-1 && s.cursor == 0 {
				s.done = true
			}
			if len(s.keys) > 0 {
				key := s.keys[0]
				s.pos++
				return util.ToSlice(key), true, nil
			} else {
				return nil, true, nil
			}
		}

		s.done = true
		return nil, false, nil
	}
}

func parseColumnValue(raw string, cm map[int]*parser.ColumnTableDef) (map[int]*util.Datum, error) {
	l := len(cm)
	pos := 0

	dm := make(map[int]*util.Datum)
	for i := 0; i < l; i++ {
		d := &util.Datum{}
		if raw[pos] == '0' {
			pos++
			d.SetK(util.KindNull)
		} else {
			pos++
			switch cm[i].Type.(type) {
			case *parser.IntType:
				i, _, n := util.ParseLengthEncodedInt(util.ToSlice(raw[pos:]))
				d.SetK(util.KindInt64)
				d.SetI(int64(i))
				pos += n
			case *parser.StringType:
				s, _, n, err := util.ParseLengthEncodedBytes(util.ToSlice(raw[pos:]))
				if err != nil {
					return nil, err
				}
				d.SetK(util.KindString)
				d.SetB(s)
				pos += n
			}
		}
		dm[i] = d
	}

	if len(dm) != len(cm) {
		return nil, errors.New("parse column value error!")
	}

	return dm, nil
}

func (s *ScanExec) Next() (*result.Record, error) {
	var key []byte
	var exist bool
	var err error

	for {
		key, exist, err = s.nextKey()
		if err != nil {
			return nil, err
		}
		if !exist {
			return nil, nil
		}

		if key != nil {
			break
		}
	}

	// Get and parse one row
	var raw string
	var dm map[int]*util.Datum
	raw, err = s.driver.Get(util.ToString(key)).Result()
	if err != nil {
		return nil, err
	}
	dm, err = parseColumnValue(raw, s.scan.From.ColumnMap)
	if err != nil {
		return nil, err
	}

	var datums []*util.Datum = make([]*util.Datum, 0)
	var d *util.Datum
	for _, f := range s.scan.Fields {
		switch f.Type {
		case parser.ETARGET:
			var ok bool
			d, ok = dm[f.FieldID - 1]
			if !ok {
				return nil, errors.New("parse column value error!")
			}
		case parser.ESYSVAR:
			d = &util.Datum{}
			sv := context.GetSysVar(f.SysVar)
			if sv == nil {
				return nil, errors.New("unsupport sysvar @@" + f.SysVar)
			}
			d.SetK(util.KindString)
			d.SetB(util.ToSlice(sv.Name))
		case parser.EVALUE:
			var err error
			d, err = valueToDatum(f.Value)
			if err != nil {
				return nil, err
			}
		}
		datums = append(datums, d)
	}

	return &result.Record{Datums: datums}, nil
}

type ScanWithPKExec struct {
	scanpk  *plan.ScanWithPK
	driver  *redis.Client
	context *context.Context
	done    bool
}

func (s *ScanWithPKExec) Columns() ([]*store.ColumnInfo, error) {
	ret := []*store.ColumnInfo{}
	for _, f := range s.scanpk.Fields {
		ci := &store.ColumnInfo{}
		switch f.Type {
		case parser.ETARGET:
			st := strings.Split(s.scanpk.From.Name, ".")
			ci.Schema = st[0]
			ci.Table = st[1]
			ci.OrgTable = st[1]
			ci.Name = s.scanpk.From.ColumnMap[f.FieldID - 1].Name
			ci.OrgName = s.scanpk.From.ColumnMap[f.FieldID - 1].Name
			switch s.scanpk.From.ColumnMap[f.FieldID - 1].Type.(type) {
			case *parser.IntType:
				ci.Type = mysql.TypeLong
				ci.ColumnLength = 4
			case *parser.StringType:
				ci.Type = mysql.TypeString
			}
			if s.scanpk.From.ColumnMap[f.FieldID - 1].Nullable == parser.NotNull {
				ci.Flag |= mysql.NotNullFlag
			}
			if s.scanpk.From.ColumnMap[f.FieldID - 1].PrimaryKey {
				ci.Flag |= mysql.PriKeyFlag
			}
			if s.scanpk.From.ColumnMap[f.FieldID - 1].Unique {
				ci.Flag |= mysql.UniqueKeyFlag
			}
			ret = append(ret, ci)
		case parser.ESYSVAR:
			ci.Schema = s.context.GetCurrentDB()
			ci.Table = "dual"
			ci.OrgTable = "dual"
			ci.Name = f.SysVar
			ci.OrgName = f.SysVar
			ci.Type = uint8(mysql.TypeString)
			ret = append(ret, ci)
		case parser.EVALUE:
			ci.Schema = s.context.GetCurrentDB()
			ci.Table = "dual"
			ci.OrgTable = "dual"
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

func (s *ScanWithPKExec) Next() (*result.Record, error) {
	if s.done {
		return nil, nil
	}

	var pk string
	switch s.scanpk.PK.(type) {
	case *parser.ComparisonQual:
		r := s.scanpk.PK.(*parser.ComparisonQual).Right
		switch r.Value.(type) {
		case int64:
			v := r.Value.(int64)
			pk = util.ToString(util.DumpLengthEncodedInt(uint64(v)))
		case string:
			v := r.Value.(string)
			pk = util.ToString(util.DumpLengthEncodedString(util.ToSlice(v)))
		}
		pk = store.UserFlag + s.scanpk.From.Name + "/" + pk
	default:
		return nil, errors.New("unsupport where clause now!")
	}
	
	// Get and parse one row
	var dm map[int]*util.Datum
	raw, err := s.driver.Get(pk).Result()
	if err != nil {
		return nil, err
	}
	dm, err = parseColumnValue(raw, s.scanpk.From.ColumnMap)
	if err != nil {
		return nil, err
	}
	
	var datums []*util.Datum = make([]*util.Datum, 0)
	for _, f := range s.scanpk.Fields {	
		var d *util.Datum
		switch f.Type {
		case parser.ETARGET:
			var ok bool
			d, ok = dm[f.FieldID -1]
			if !ok {
				return nil, errors.New("parse column value error")
			}
		case parser.ESYSVAR:
			d = &util.Datum{}
			sv := context.GetSysVar(f.SysVar)
			if sv == nil {
				return nil, errors.New("unsupport sysvar @@" + f.SysVar)
			}
			d.SetK(util.KindString)
			d.SetB(util.ToSlice(sv.Name))
		case parser.EVALUE:
			var err error
			d, err = valueToDatum(f.Value)
			if err != nil {
				return nil, err
			}
		}
		datums = append(datums, d)
	}
	s.done = true
	return &result.Record{Datums: datums}, nil
}
