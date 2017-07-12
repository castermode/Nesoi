package executor

import (
	"errors"

	"github.com/castermode/Nesoi/src/sql/context"
	"github.com/castermode/Nesoi/src/sql/parser"
	"github.com/castermode/Nesoi/src/sql/plan"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/castermode/Nesoi/src/sql/store"
	"github.com/castermode/Nesoi/src/sql/util"
)

type InsertExec struct {
	stmt    parser.Statement
	driver  store.Driver
	context *context.Context
	done    bool
}

func (insert *InsertExec) Columns() ([]*store.ColumnInfo, error) {
	return nil, nil
}

func parseIndexFields(raw string) []uint64 {
	num, _, len := util.ParseLengthEncodedInt(util.ToSlice(raw))
	idxFields := make([]uint64, num)
	var i uint64
	for ; i < num; i++ {
		idxField, _, l := util.ParseLengthEncodedInt(util.ToSlice(raw[len:]))
		idxFields[i] = idxField
		len += l
	}
	return idxFields
}

func (insert *InsertExec) Next() (*result.Record, error) {
	if insert.done {
		return nil, nil
	}

	var value string
	stmt := insert.stmt.(*parser.InsertQuery)
	for i := 0; i < stmt.NumColumns; i++ {
		if stmt.Values[i] != nil {
			value += "1"
			switch stmt.Values[i].(type) {
			case int64:
				v := stmt.Values[i].(int64)
				value += util.ToString(util.DumpLengthEncodedInt(uint64(v)))
			case string:
				v := stmt.Values[i].(string)
				value += util.ToString(util.DumpLengthEncodedString(util.ToSlice(v)))
			}
		} else {
			value += "0"
		}
	}

	affectedRows := insert.context.AffectedRows()
	insert.context.SetAffectedRows(affectedRows + 1)
	insert.done = true

	err := insert.driver.SetUserRecord(stmt.PK, value, 0)
	if err != nil {
		return nil, err
	}

	//insert index
	var keys []string
	var cursor uint64
	match := store.SystemFlag + store.TableFlag + store.IndexFlag + stmt.TableName + "*"
	for {
		keys, cursor, err = insert.driver.ScanSysRecords(cursor, match, OnceScanCount)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			var unique bool
			idxName, err := insert.driver.GetSysRecord(key)
			if err != nil {
				return nil, err
			}
			idxFields := parseIndexFields(key[len(match)-1:])
			idxKey := store.UserFlag + idxName + "/"
			for _, idxField := range idxFields {
				switch stmt.Values[int(idxField-1)].(type) {
				case int64:
					v := stmt.Values[int(idxField-1)].(int64)
					idxKey += util.ToString(util.DumpLengthEncodedInt(uint64(v)))
				case string:
					v := stmt.Values[int(idxField-1)].(string)
					idxKey += util.ToString(util.DumpLengthEncodedString(util.ToSlice(v)))
				}
			}
			idxTblKey := store.SystemFlag + store.IndexFlag + store.TableFlag + idxName
			idxTblValue, err := insert.driver.GetSysRecord(idxTblKey)
			if err == nil {
				if idxTblValue[0:1] == "1" {
					unique = true
				} else {
					unique = false
				}
			} else {
				return nil, errors.New("can't get index table system key!")
			}
			err = WriteIndexInfo(insert.driver, unique, idxKey, stmt.PK)
			if err != nil {
				return nil, err
			}
		}
		if cursor == 0 {
			break
		}
	}

	return nil, nil
}

func (insert *InsertExec) Done() bool {
	return insert.done
}

type UpdateExec struct {
	update   *plan.Update
	children []result.Result
	driver   store.Driver
	context  *context.Context
	done     bool
}

func NewUpdateExec(p *plan.Update, e *Executor) *UpdateExec {
	pExec := &UpdateExec{
		update:  p,
		driver:  e.driver,
		context: e.context,
	}

	for _, n := range p.GetChildren() {
		pExec.children = append(pExec.children, makePlanExec(n, e))
	}

	return pExec
}

func (ue *UpdateExec) Columns() ([]*store.ColumnInfo, error) {
	return nil, nil
}

func (ue *UpdateExec) Next() (*result.Record, error) {
	if ue.done {
		return nil, nil
	}

	rc, err := ue.children[0].Next()
	if err != nil {
		return nil, err
	}
	if rc == nil {
		ue.done = true
		return nil, nil
	}

	var newRaw string
	key := store.UserFlag + ue.update.Table.Name + "/"
	for i := 0; i < ue.update.FieldsNum; i++ {
		var raw []byte
		pk := ue.update.Table.ColumnMap[i].PrimaryKey
		c, ok := ue.update.Values[i]
		if !ok {
			raw, err = util.DumpValueToRaw(rc.Datums[i])
			if err != nil {
				return nil, err
			}
			if raw != nil {
				newRaw += "1"
				newRaw += util.ToString(raw)
			} else {
				newRaw += "0"
			}
		} else {
			newRaw += "1"

			switch ue.update.Table.ColumnMap[i].Type.(type) {
			case *parser.IntType:
				v := c.(int64)
				raw = util.DumpLengthEncodedInt(uint64(v))
				newRaw += util.ToString(raw)
			case *parser.StringType:
				v := c.(string)
				raw = util.DumpLengthEncodedString(util.ToSlice(v))
				newRaw += util.ToString(raw)
			}
		}

		if pk {
			key += util.ToString(raw)
		}
	}
	affectedRows := ue.context.AffectedRows()
	ue.context.SetAffectedRows(affectedRows + 1)

	err = ue.driver.SetUserRecord(key, newRaw, 0)
	if err != nil {
		return nil, err
	}

	//update index
	var ks []string
	var cursor uint64
	match := store.SystemFlag + store.TableFlag + store.IndexFlag + ue.update.Table.Name + "*"
	for {
		var shouldReset bool
		ks, cursor, err = ue.driver.ScanSysRecords(cursor, match, OnceScanCount)
		if err != nil {
			return nil, err
		}
		for _, k := range ks {
			var unique bool
			idxName, err := ue.driver.GetSysRecord(k)
			if err != nil {
				return nil, err
			}
			idxFields := parseIndexFields(k[len(match)-1:])
			idxKey := store.UserFlag + idxName + "/"
			for _, idxField := range idxFields {
				c, ok := ue.update.Values[int(idxField-1)]
				if ok {
					shouldReset = true
					switch ue.update.Table.ColumnMap[int(idxField-1)].Type.(type) {
					case *parser.IntType:
						v := c.(int64)
						idxKey += util.ToString(util.DumpLengthEncodedInt(uint64(v)))
					case *parser.StringType:
						v := c.(string)
						idxKey += util.ToString(util.DumpLengthEncodedString(util.ToSlice(v)))
					}
				} else {
					raw, err := util.DumpValueToRaw(rc.Datums[int(idxField-1)])
					if err != nil {
						return nil, err
					}
					idxKey += util.ToString(raw)
				}
			}

			if shouldReset {
				idxTblKey := store.SystemFlag + store.IndexFlag + store.TableFlag + idxName
				idxTblValue, err := ue.driver.GetSysRecord(idxTblKey)
				if err == nil {
					if idxTblValue[0:1] == "1" {
						unique = true
					} else {
						unique = false
					}
				} else {
					return nil, errors.New("can't get index table system key!")
				}
				err = WriteIndexInfo(ue.driver, unique, idxKey, key)
				if err != nil {
					return nil, err
				}
			}
		}
		if cursor == 0 {
			break
		}
	}

	return nil, nil
}

func (ue *UpdateExec) Done() bool {
	return ue.done
}
