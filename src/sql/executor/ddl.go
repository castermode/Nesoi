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
		return errors.New("Database alreary exists!")
	}

	if err != redis.Nil {
		return errors.New("Get kv storage error!")
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
		return errors.New("Get kv storage error!")
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
