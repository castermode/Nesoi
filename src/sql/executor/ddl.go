package executor

import (
	"encoding/json"
	"errors"

	"github.com/castermode/Nesoi/src/sql/context"
	"github.com/castermode/Nesoi/src/sql/parser"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/castermode/Nesoi/src/sql/store"
	"github.com/castermode/Nesoi/src/sql/util"
	"github.com/go-redis/redis"
)

type DDLExec struct {
	stmt    parser.Statement
	driver  *redis.Client
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

func (ddl *DDLExec) executeCreateDatabase() error {
	stmt := ddl.stmt.(*parser.CreateDatabase)
	dbName := store.SystemFlag + store.DBFlag + stmt.DBName

	_, err := ddl.driver.Get(dbName).Result()
	if err == nil {
		if stmt.IfNotExists {
			return nil
		}
		return errors.New("database alreary exists!")
	}

	if err != redis.Nil {
		return errors.New("get kv storage error!")
	}

	return ddl.driver.Set(dbName, "", 0).Err()
}

func (ddl *DDLExec) executeCreateTable() error {
	stmt := ddl.stmt.(*parser.CreateTable)

	tableKey := store.SystemFlag + store.TableFlag + ddl.context.GetTableName(stmt.Table.Schema, stmt.Table.Name)
	_, err := ddl.driver.Get(tableKey).Result()
	if err == nil {
		if stmt.IfNotExists {
			return nil
		}
		return errors.New("table alreary exists!")
	}

	if err != redis.Nil {
		return errors.New("get kv storage error!")
	}

	i := 1
	for _, cd := range stmt.Defs {
		cd.Pos = i
		i++
	}

	var data []byte
	data, err = json.Marshal(stmt.Defs)
	if err != nil {
		return err
	}

	return ddl.driver.Set(tableKey, util.ToString(data), 0).Err()
}

func (ddl *DDLExec) executeDropDatabase() error {
	stmt := ddl.stmt.(*parser.DropDatabase)

	dbName := store.SystemFlag + store.DBFlag + stmt.DBName
	_, err := ddl.driver.Get(dbName).Result()
	if err == nil {
		_, err = ddl.driver.Del(dbName).Result()
		return err
	}

	if err != redis.Nil {
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
	_, err := ddl.driver.Get(TblName).Result()
	if err == nil {
		_, err = ddl.driver.Del(TblName).Result()
		return err
	}

	if err != redis.Nil {
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
	_, err := ddl.driver.Get(dbName).Result()
	if err == nil {
		ddl.context.SetCurrentDB(stmt.DBName)
	}

	if err != redis.Nil {
		return errors.New("get kv storage error!")
	}

	return errors.New("database not exists!")
}
