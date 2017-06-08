package executor

import (
	"encoding/json"
	"errors"

	"github.com/castermode/Nesoi/src/sql/context"
	"github.com/castermode/Nesoi/src/sql/parser"
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
