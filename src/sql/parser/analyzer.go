package parser

import (
	"encoding/json"
	"errors"
	"strings"
<<<<<<< HEAD

=======
	
>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
	"github.com/castermode/Nesoi/src/sql/context"
	"github.com/castermode/Nesoi/src/sql/store"
	"github.com/castermode/Nesoi/src/sql/util"
	"github.com/go-redis/redis"
)

type Analyzer struct {
<<<<<<< HEAD
	driver  *redis.Client
	context *context.Context
}

func NewAnalyzer(sd *redis.Client, ctx *context.Context) *Analyzer {
	return &Analyzer{
		driver:  sd,
=======
	driver	*redis.Client
	context	*context.Context
}

func NewAnalyzer(sd *redis.Client, ctx *context.Context) *Analyzer {
	return &Analyzer {
		driver: sd,
>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
		context: ctx,
	}
}

func (a *Analyzer) Analyze(stmts []Statement) ([]Statement, error) {
	var querys []Statement
	for _, stmt := range stmts {
		switch stmt.StatementType() {
		case DDL:
			querys = append(querys, stmt)
		case Rows:
			query, err := a.transformStmt(stmt)
			if err != nil {
				return nil, err
			}
			querys = append(querys, query)
		default:
			return nil, errors.New("unsupport statement: " + stmt.String())
		}
	}
<<<<<<< HEAD

=======
	
>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
	return querys, nil
}

func (a *Analyzer) transformStmt(stmt Statement) (Statement, error) {
	switch stmt.(type) {
	case *SelectStmt:
		return a.transformSelectStmt(stmt)
	}
<<<<<<< HEAD

=======
	
>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
	return nil, errors.New("unsupport statement: " + stmt.String())
}

func (a *Analyzer) transformTarget(expr Expr, cds ColumnTableDefs, tgr *TargetRes) error {
	switch expr.(type) {
	case *VariableExpr:
		vtarget := expr.(*VariableExpr)
		if vtarget.Type == ETARGET {
			var num int
			tgr.Type = ETARGET
			for _, cd := range cds {
				num++
				if strings.EqualFold(vtarget.Name, cd.Name) {
					tgr.FieldID = cd.Pos
					break
				}
			}
			if num > len(cds) {
				return errors.New("Invalid target name " + vtarget.Name)
			}
		} else {
			//sysVar
			tgr.Type = ESYSVAR
<<<<<<< HEAD
			tgr.SysVar = vtarget.Name[2:]
=======
			tgr.SysVar = vtarget.Name[2:]		
>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
		}
		return nil
	case *ValueExpr:
		vtarget := expr.(*ValueExpr)
		tgr.Type = EVALUE
		tgr.Value = vtarget.Item
		return nil
	}
<<<<<<< HEAD

=======
	
>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
	return errors.New("unsupport target type: " + expr.String())
}

func (a *Analyzer) transformSelectStmt(stmt Statement) (Statement, error) {
	sstmt := stmt.(*SelectStmt)

<<<<<<< HEAD
	var from *TableInfo
	var cds ColumnTableDefs
	var tblName string
	var tableValue string
	var err error
=======
	var tableValue string 
	var tblName string
    var err error	
>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
	// transform from clause
	if sstmt.From != nil {
		tblName = a.context.GetTableName(sstmt.From.Schema, sstmt.From.Name)
		tableKey := store.SystemFlag + store.TableFlag + tblName
		tableValue, err = a.driver.Get(tableKey).Result()
		if err != nil {
			return nil, err
		}
<<<<<<< HEAD

		cds = ColumnTableDefs{}
		err = json.Unmarshal(util.ToSlice(tableValue), &cds)
		if err != nil {
			return nil, err
		}

		var cm map[int]*ColumnTableDef
		cm = make(map[int]*ColumnTableDef)
		for _, cd := range cds {
			cm[cd.Pos] = cd
		}

		from = &TableInfo{Name: tblName, ColumnMap: cm}
	}

	// transform target clause
=======
	}
	
	// transform target clause
	cds := ColumnTableDefs{}
	err = json.Unmarshal(util.ToSlice(tableValue), &cds)
	if err != nil {
		return nil, err
	}
	
>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
	var num int
	var tgrs []*TargetRes
	i := 1
	for _, target := range sstmt.Target {
		tgr := &TargetRes{TargetID: i}
		err := a.transformTarget(target.Item, cds, tgr)
		if err != nil {
			return nil, err
		}
		tgrs = append(tgrs, tgr)
		i++
	}
	num = i - 1
<<<<<<< HEAD

=======
	
>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
	// transform where clause
	var qual *ComparisonQual
	if sstmt.Where != nil {
		qual = &ComparisonQual{}
		switch sstmt.Where.Cond.(type) {
		case *ComparisonExpr:
			cond := sstmt.Where.Cond.(*ComparisonExpr)
			qual.Operator = cond.Operator
			qual.Left = &TargetRes{TargetID: i}
			i++
			qual.Right = &TargetRes{TargetID: i}
			err := a.transformTarget(cond.Left, cds, qual.Left)
			if err != nil {
				return nil, err
			}
			tgrs = append(tgrs, qual.Left)
<<<<<<< HEAD

=======
		
>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
			err = a.transformTarget(cond.Right, cds, qual.Right)
			if err != nil {
				return nil, err
			}
			tgrs = append(tgrs, qual.Right)
		default:
			return nil, errors.New("unsupport qual target: " + sstmt.Where.Cond.String())
		}
	}
<<<<<<< HEAD

=======
	
>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
	// transform limit clause
	var limitNum uint64
	if sstmt.Limit != nil {
		limitNum = sstmt.Limit.Num
	}
<<<<<<< HEAD

	return &SelectQuery{
		From:      from,
		Fields:    tgrs,
		FieldsNum: num,
		Qual:      qual,
		Limit:     limitNum,
=======
	
	return &SelectQuery{
		TblName: 	tblName,
		Fields:		tgrs,
		FieldsNum:	num,
		Qual:		qual,
		Limit:		limitNum,	
>>>>>>> 57ef05416feb3d1e0142fc3cef7fdcdb2063a76d
	}, nil
}