package executor

import (
	"errors"
	"strconv"
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
			ci.Name = s.scan.From.ColumnMap[f.FieldID].Name
			ci.OrgName = s.scan.From.ColumnMap[f.FieldID].Name
			switch s.scan.From.ColumnMap[f.FieldID].Type.(type) {
			case *parser.IntType:
				ci.Type = mysql.TypeLong
				ci.ColumnLength = 4
			case *parser.StringType:
				ci.Type = mysql.TypeString
			}
			if s.scan.From.ColumnMap[f.FieldID].Nullable == parser.NotNull {
				ci.Flag |= mysql.NotNullFlag
			}
			if s.scan.From.ColumnMap[f.FieldID].PrimaryKey {
				ci.Flag |= mysql.PriKeyFlag
			}
			if s.scan.From.ColumnMap[f.FieldID].Unique {
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

	is := strconv.FormatInt(1, 10)
	if s.keys != nil && s.pos < len(s.keys) {
		if s.pos == len(s.keys)-1 && s.cursor == 0 {
			s.done = true
		}
		key := s.keys[s.pos]
		s.pos++
		l := len(key) - len(is)
		return util.ToSlice(key[0:l]), true, nil
	} else {
		var err error
		match := store.UserFlag + s.scan.From.Name + "/*/" + is
		s.keys, s.cursor, err = s.driver.Scan(s.cursor, match, 10).Result()
		if err != nil {
			return nil, true, err
		}
		s.pos = 0
		if s.keys != nil {
			s.pos++
			key := s.keys[0]
			l := len(key) - len(is)
			return util.ToSlice(key[0:l]), true, nil
		}

		s.done = true
		return nil, false, nil
	}
}

func (s *ScanExec) Next() (*result.Record, error) {
	key, exist, err := s.nextKey()
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, nil
	}

	var datums []*util.Datum = make([]*util.Datum, 0)
	for _, f := range s.scan.Fields {
		d := &util.Datum{}
		switch f.Type {
		case parser.ETARGET:
			columnKey := util.ToString(key) + strconv.FormatInt(int64(f.FieldID), 10)
			value, err := s.driver.Get(columnKey).Result()
			if err != nil {
				return nil, errors.New("Get kv storage error!")
			}

			if value[0] == '0' {
				d.SetK(util.KindNull)
			} else {
				parsedValue := value[1:]
				switch s.scan.From.ColumnMap[f.FieldID].Type.(type) {
				case *parser.IntType:
					i, err := strconv.ParseInt(parsedValue, 10, 64)
					if err != nil {
						return nil, err
					}
					d.SetK(util.KindInt64)
					d.SetI(i)
				case *parser.StringType:
					d.SetK(util.KindString)
					d.SetB(util.ToSlice(parsedValue))
				}
			}
		case parser.ESYSVAR:
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
			ci.Name = s.scanpk.From.ColumnMap[f.FieldID].Name
			ci.OrgName = s.scanpk.From.ColumnMap[f.FieldID].Name
			switch s.scanpk.From.ColumnMap[f.FieldID].Type.(type) {
			case *parser.IntType:
				ci.Type = mysql.TypeLong
				ci.ColumnLength = 4
			case *parser.StringType:
				ci.Type = mysql.TypeString
			}
			if s.scanpk.From.ColumnMap[f.FieldID].Nullable == parser.NotNull {
				ci.Flag |= mysql.NotNullFlag
			}
			if s.scanpk.From.ColumnMap[f.FieldID].PrimaryKey {
				ci.Flag |= mysql.PriKeyFlag
			}
			if s.scanpk.From.ColumnMap[f.FieldID].Unique {
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
			pk = strconv.FormatInt(r.Value.(int64), 10)
		case string:
			pk = r.Value.(string)
		}
		pk = store.UserFlag + s.scanpk.From.Name + "/" + pk
	default:
		return nil, errors.New("unsupport where clause now!")
	}
	var datums []*util.Datum = make([]*util.Datum, 0)
	for _, f := range s.scanpk.Fields {
		d := &util.Datum{}
		switch f.Type {
		case parser.ETARGET:
			columnKey := pk + strconv.FormatInt(int64(f.FieldID), 10)
			value, err := s.driver.Get(columnKey).Result()
			if err != nil {
				return nil, errors.New("Get kv storage error!")
			}

			if value[0] == '0' {
				d.SetK(util.KindNull)
			} else {
				parsedValue := value[1:]
				switch s.scanpk.From.ColumnMap[f.FieldID].Type.(type) {
				case *parser.IntType:
					i, err := strconv.ParseInt(parsedValue, 10, 64)
					if err != nil {
						return nil, err
					}
					d.SetK(util.KindInt64)
					d.SetI(i)
				case *parser.StringType:
					d.SetK(util.KindString)
					d.SetB(util.ToSlice(parsedValue))
				}
			}
		case parser.ESYSVAR:
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
