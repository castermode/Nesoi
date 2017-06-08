package executor

import (
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
	return nil, err
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
		c, ok := ue.update.Values[i]
		pk := ue.update.Table.ColumnMap[i].PrimaryKey
		var raw []byte
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

	return nil, nil
}

func (ue *UpdateExec) Done() bool {
	return ue.done
}
