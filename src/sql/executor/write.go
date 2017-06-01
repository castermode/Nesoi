package executor

import (
	"github.com/castermode/Nesoi/src/sql/context"
	"github.com/castermode/Nesoi/src/sql/parser"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/castermode/Nesoi/src/sql/store"
	"github.com/castermode/Nesoi/src/sql/util"
	"github.com/go-redis/redis"
)

type InsertExec struct {
	stmt    parser.Statement
	driver  *redis.Client
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

	err := insert.driver.Set(stmt.PK, value, 0).Err()
	return nil, err
}
