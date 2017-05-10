package executor

import (
	"errors"
	
	"github.com/castermode/Nesoi/src/sql/parser"
	"github.com/castermode/Nesoi/src/sql/store"
	"github.com/castermode/Nesoi/src/sql/result"
	"github.com/go-redis/redis"
)

type DDLExec struct {
	stmt   parser.Statement
	driver *redis.Client
	done   bool
}

func (ddl *DDLExec) Next() (*result.Record, error) {
	if (ddl.done) {
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
	dbname := store.SystemFlag + store.DBFlag + stmt.DBName

	_, err := ddl.driver.Get(dbname).Result()
	if err == nil {
		if stmt.IfNotExists {
			return nil
		}
		return errors.New("Database alreary exists!")
	}
	
	if err != redis.Nil {
		return errors.New("Get kv storage error!")
	}
	
	err = ddl.driver.Set(dbname, "", 0).Err() 
	return err
}

func (ddl *DDLExec) executeCreateTable() error {
	//@TODO
	return nil
}
