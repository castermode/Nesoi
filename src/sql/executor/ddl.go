package executor

import (
	"encoding/json"
	"errors"

	"github.com/castermode/Nesoi/src/sql/context"
	"github.com/castermode/Nesoi/src/sql/parser"
	"github.com/castermode/Nesoi/src/sql/plan"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/castermode/Nesoi/src/sql/store"
	"github.com/castermode/Nesoi/src/sql/util"
)

type DDLExec struct {
	stmt    parser.Statement
	driver  store.Driver
	context *context.Context
	done    bool
}

func (ddl *DDLExec) Columns() ([]*store.ColumnInfo, error) {
	return nil, nil
}

func (ddl *DDLExec) Next() (*result.Record, error) {
	if ddl.done {
		return nil, nil
	}

	var err error
	switch ddl.stmt.(type) {
	case *parser.CreateDatabase:
		err = ddl.executeCreateDatabase()
	case *parser.CreateTable:
		err = ddl.executeCreateTable()
	case *parser.CreateIndexQuery:
		err = ddl.executeCreateIndex()
	case *parser.DropDatabase:
		err = ddl.executeDropDatabase()
	case *parser.DropTable:
		err = ddl.executeDropTable()
	case *parser.UseDB:
		err = ddl.executeUseDB()
	}
	if err != nil {
		return nil, err
	}

	ddl.done = true
	return nil, nil
}

func (ddl *DDLExec) Done() bool {
	return ddl.done
}

func (ddl *DDLExec) executeCreateDatabase() error {
	stmt := ddl.stmt.(*parser.CreateDatabase)
	dbName := store.SystemFlag + store.DBFlag + stmt.DBName

	_, err := ddl.driver.GetSysRecord(dbName)
	if err == nil {
		if stmt.IfNotExists {
			return nil
		}
		return errors.New("database alreary exists!")
	}

	if err != store.Nil {
		return errors.New("get kv storage error!")
	}

	return ddl.driver.SetSysRecord(dbName, "", 0)
}

func (ddl *DDLExec) executeCreateTable() error {
	stmt := ddl.stmt.(*parser.CreateTable)

	tableKey := store.SystemFlag + store.TableFlag + ddl.context.GetTableName(stmt.Table.Schema, stmt.Table.Name)
	_, err := ddl.driver.GetSysRecord(tableKey)
	if err == nil {
		if stmt.IfNotExists {
			return nil
		}
		return errors.New("table alreary exists!")
	}

	if err != store.Nil {
		return errors.New("get kv storage error!")
	}

	cjds := parser.ColumnTableJsonDefs{}
	i := 1
	for _, cd := range stmt.Defs {
		cd.Pos = i
		cjd := &parser.ColumnTableJsonDef{
			Name:       cd.Name,
			Pos:        cd.Pos,
			Nullable:   cd.Nullable,
			PrimaryKey: cd.PrimaryKey,
			Unique:     cd.Unique,
		}
		switch cd.Type.(type) {
		case *parser.IntType:
			cjd.Type = parser.SqlInt
		case *parser.StringType:
			cjd.Type = parser.SqlString
		}
		cjds = append(cjds, cjd)
		i++
	}

	var data []byte
	data, err = json.Marshal(cjds)
	if err != nil {
		return err
	}

	return ddl.driver.SetSysRecord(tableKey, util.ToString(data), 0)
}

func WriteIndexInfo(driver store.Driver, unique bool, key string, value string) error {
	var newValue string
	oldValue, err := driver.GetUserRecord(key)
	if err == nil {
		if unique {
			return errors.New("index repeat!")
		}
		numKeys, _, num := util.ParseLengthEncodedInt(util.ToSlice(value))
		newValue += util.ToString(util.DumpLengthEncodedInt(numKeys + 1))
		newValue += oldValue[num:]
		newValue += util.ToString(util.DumpLengthEncodedString(util.ToSlice(value)))
	} else if err == store.Nil {
		newValue += util.ToString(util.DumpLengthEncodedInt(1))
		newValue += util.ToString(util.DumpLengthEncodedString(util.ToSlice(value)))
	} else {
		return err
	}

	return driver.SetUserRecord(key, newValue, 0)
}

/*
 * create index on table, we storage index info as:
 * system:
 * 		/SYSTEM/INDEX/TABLE/idxName "F + Encoded table name + Encoded Fields Num + Encoded FieldID + ... "
 *      F: unique or not
 * 		/SYSTEM/TABLE/INDEX/+ Encoded table name + Encoded Fields Num + Encoded FieldID + ... idxName
 * user:
 *		/USER/idxName/+ Encoded Field Value + ...	Encoded numKeys + Encoded Primary key + ...
 */
func (ddl *DDLExec) executeCreateIndex() error {
	stmt := ddl.stmt.(*parser.CreateIndexQuery)

	plan := &plan.Scan{
		From:      stmt.TblInfo,
		Fields:    stmt.Fields,
		FieldsNum: stmt.FieldsNum,
	}
	rst := &ScanExec{scan: plan, driver: ddl.driver, context: ddl.context}
	tblName := ddl.context.GetTableName(stmt.Table.Schema, stmt.Table.Name)
	idxName := ddl.context.GetTableName(stmt.Index.Schema, stmt.Index.Name)
	for {
		key, rc, err := rst.nextKV()
		if err != nil {
			return err
		}

		newKey := store.UserFlag + idxName + "/"
		if rc != nil {
			for _, datum := range rc.Datums {
				raw, err := util.DumpValueToRaw(datum)
				if err != nil {
					return err
				}
				newKey += util.ToString(raw)
			}
		} else {
			break
		}
		err = WriteIndexInfo(ddl.driver, stmt.Unique, newKey, key)
		if err != nil {
			return err
		}
	}

	//system table(index->table)
	var sysIndexTableValue string
	sysIndexTableKey := store.SystemFlag + store.IndexFlag + store.TableFlag + idxName
	if stmt.Unique {
		sysIndexTableValue = "1"
	} else {
		sysIndexTableValue = "0"
	}
	sysIndexTableValue += util.ToString(util.DumpLengthEncodedString(util.ToSlice(tblName)))
	encodedFields := util.ToString(util.DumpLengthEncodedInt(uint64(stmt.FieldsNum)))
	for _, fd := range stmt.Fields {
		encodedFields += util.ToString(util.DumpLengthEncodedInt(uint64(fd.FieldID)))
	}
	sysIndexTableValue += encodedFields
	err := ddl.driver.SetSysRecord(sysIndexTableKey, sysIndexTableValue, 0)
	if err != nil {
		return err
	}
	//system table(table->index)
	sysTableIndexKey := store.SystemFlag + store.TableFlag + store.IndexFlag + tblName
	sysTableIndexKey += encodedFields
	sysTableIndexValue := idxName
	return ddl.driver.SetSysRecord(sysTableIndexKey, sysTableIndexValue, 0)
}

func (ddl *DDLExec) executeDropDatabase() error {
	stmt := ddl.stmt.(*parser.DropDatabase)

	dbName := store.SystemFlag + store.DBFlag + stmt.DBName
	_, err := ddl.driver.GetSysRecord(dbName)
	if err == nil {
		return ddl.driver.DelSysRecord(dbName)
	}

	if err != store.Nil {
		return errors.New("get kv storage error!")
	}

	if stmt.IfExists {
		return nil
	}
	return errors.New("database not exists!")
}

func (ddl *DDLExec) executeDropTable() error {
	stmt := ddl.stmt.(*parser.DropTable)

	TblName := store.SystemFlag + store.TableFlag + ddl.context.GetTableName(stmt.TName.Schema, stmt.TName.Name)
	_, err := ddl.driver.GetSysRecord(TblName)
	if err == nil {
		return ddl.driver.DelSysRecord(TblName)
	}

	if err != store.Nil {
		return errors.New("get kv storage error!")
	}

	if stmt.IfExists {
		return nil
	}
	return errors.New("table not exists!")
}

func (ddl *DDLExec) executeUseDB() error {
	stmt := ddl.stmt.(*parser.UseDB)

	dbName := store.SystemFlag + store.DBFlag + stmt.DBName
	_, err := ddl.driver.GetSysRecord(dbName)
	if err == nil {
		ddl.context.SetCurrentDB(stmt.DBName)
	}

	if err != store.Nil {
		return errors.New("get kv storage error!")
	}

	return errors.New("database not exists!")
}
