package parser

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/castermode/Nesoi/src/sql/context"
	"github.com/castermode/Nesoi/src/sql/store"
	"github.com/castermode/Nesoi/src/sql/util"
	"github.com/go-redis/redis"
)

type Analyzer struct {
	driver  *redis.Client
	context *context.Context
}

func NewAnalyzer(sd *redis.Client, ctx *context.Context) *Analyzer {
	return &Analyzer{
		driver:  sd,
		context: ctx,
	}
}

func (a *Analyzer) Analyze(stmts []Statement) ([]Statement, error) {
	var querys []Statement
	for _, stmt := range stmts {
		switch stmt.StatementType() {
		case DDL:
			querys = append(querys, stmt)
		case Rows, RowsAffected:
			query, err := a.transformStmt(stmt)
			if err != nil {
				return nil, err
			}
			querys = append(querys, query)
		default:
			return nil, errors.New("unsupport statement: " + stmt.String())
		}
	}

	return querys, nil
}

func (a *Analyzer) transformStmt(stmt Statement) (Statement, error) {
	switch stmt.(type) {
	case *SelectStmt:
		return a.transformSelectStmt(stmt)
	case *InsertStmt:
		return a.transformInsertStmt(stmt)
	case *ShowDatabases:
		return &Show{Operator: SDATABASES}, nil
	case *ShowTables:
		return &Show{Operator: STABLES}, nil
	}

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
			tgr.SysVar = vtarget.Name[2:]
		}
		return nil
	case *ValueExpr:
		vtarget := expr.(*ValueExpr)
		tgr.Type = EVALUE
		tgr.Value = vtarget.Item
		return nil
	}

	return errors.New("unsupport target type: " + expr.String())
}

func (a *Analyzer) transformSelectStmt(stmt Statement) (Statement, error) {
	sstmt := stmt.(*SelectStmt)

	var from *TableInfo
	var cds ColumnTableDefs
	var tblName string
	var tableValue string
	var err error
	// transform from clause
	if sstmt.From != nil {
		tblName = a.context.GetTableName(sstmt.From.Schema, sstmt.From.Name)
		tableKey := store.SystemFlag + store.TableFlag + tblName
		tableValue, err = a.driver.Get(tableKey).Result()
		if err != nil {
			return nil, err
		}

		cjds := ColumnTableJsonDefs{}
		cds = ColumnTableDefs{}
		err = json.Unmarshal(util.ToSlice(tableValue), &cjds)
		if err != nil {
			return nil, err
		}
		for _, cjd := range cjds {
			cd := &ColumnTableDef{
				Name:       cjd.Name,
				Pos:        cjd.Pos,
				Nullable:   cjd.Nullable,
				PrimaryKey: cjd.PrimaryKey,
				Unique:     cjd.Unique,
			}
			switch cjd.Type {
			case SqlInt:
				cd.Type = &IntType{Name: "INT"}
			case SqlString:
				cd.Type = &StringType{Name: "STRING"}
			}
			cds = append(cds, cd)
		}

		var cm map[int]*ColumnTableDef
		cm = make(map[int]*ColumnTableDef)
		for _, cd := range cds {
			cm[cd.Pos - 1] = cd
		}

		from = &TableInfo{Name: tblName, ColumnMap: cm}
	}

	// transform target clause
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

			err = a.transformTarget(cond.Right, cds, qual.Right)
			if err != nil {
				return nil, err
			}
			tgrs = append(tgrs, qual.Right)
		default:
			return nil, errors.New("unsupport qual target: " + sstmt.Where.Cond.String())
		}
	}

	// transform limit clause
	var limitNum uint64
	if sstmt.Limit != nil {
		limitNum = sstmt.Limit.Num
	}

	return &SelectQuery{
		From:      from,
		Fields:    tgrs,
		FieldsNum: num,
		Qual:      qual,
		Limit:     limitNum,
	}, nil
}

func (a *Analyzer) transformInsertStmt(stmt Statement) (Statement, error) {
	istmt := stmt.(*InsertStmt)

	tblName := a.context.GetTableName(istmt.TName.Schema, istmt.TName.Name)
	tableKey := store.SystemFlag + store.TableFlag + tblName
	tableValue, err := a.driver.Get(tableKey).Result()
	if err != nil {
		return nil, err
	}

	cjds := ColumnTableJsonDefs{}
	cds := ColumnTableDefs{}
	err = json.Unmarshal(util.ToSlice(tableValue), &cjds)
	if err != nil {
		return nil, err
	}
	for _, cjd := range cjds {
		cd := &ColumnTableDef{
			Name:       cjd.Name,
			Pos:        cjd.Pos,
			Nullable:   cjd.Nullable,
			PrimaryKey: cjd.PrimaryKey,
			Unique:     cjd.Unique,
		}
		switch cjd.Type {
		case SqlInt:
			cd.Type = &IntType{Name: "INT"}
		case SqlString:
			cd.Type = &StringType{Name: "STRING"}
		}
		cds = append(cds, cd)
	}

	pks := []int{}
	vm := make(map[int]interface{})
	cm := make(map[string]*ColumnTableDef)
	cm1 := make(map[int]*ColumnTableDef)
	for _, cd := range cds {
		cm[cd.Name] = cd
		cm1[cd.Pos - 1] = cd
		if cd.PrimaryKey {
			pks = append(pks, cd.Pos - 1)
		}
	}

	var e string
	//check column if exists and type
	if istmt.ColumnList == nil {
		l := len(cds)
		istmt.ColumnList = make([]string, 0, l)
		for i := 0; i < l; i++ {
			istmt.ColumnList = append(istmt.ColumnList, cm1[i].Name)
		}
	}

	i := 0
	for _, c := range istmt.ColumnList {
		if _, ok := cm[c]; !ok {
			e = c + " not exists!"
			return nil, errors.New(e)
		}

		// we only support valueexpr now
		ve, ok := istmt.Values[i].(*ValueExpr)
		if !ok {
			return nil, errors.New("we only support value-expr now!")
		}

		switch cm[c].Type.(type) {
		case *IntType:
			if _, ok := ve.Item.(int64); !ok {
				e = "the value of " + cm[c].Name + " isn't int type!"
				return nil, errors.New(e)
			}
		case *StringType:
			if _, ok := ve.Item.(string); !ok {
				e = "the value of " + cm[c].Name + " isn't string type!"
				return nil, errors.New(e)
			}
		}

		vm[cm[c].Pos - 1] = ve.Item
		i++
	}

	//check null
	if len(vm) < len(cds) {
		for _, cd := range cds {
			if _, ok := vm[cd.Pos - 1]; !ok {
				if cd.PrimaryKey {
					e = cd.Name + " is primary key, cann't be null"
					return nil, errors.New(e)
				}

				if cd.Nullable == NotNull {
					e = cd.Name + " is NotNull!"
					return nil, errors.New(e)
				}

				vm[cd.Pos - 1] = nil
			}
		}
	}

	//check primary key
	pkv := store.UserFlag + tblName + "/"
	for _, pk := range pks {
		switch vm[pk].(type) {
		case int64:
			v := vm[pk].(int64)
			pkv += util.ToString(util.DumpLengthEncodedInt(uint64(v)))
		case string:
			v := vm[pk].(string)
			pkv += util.ToString(util.DumpLengthEncodedString(util.ToSlice(v)))
		}
	}
	_, err = a.driver.Get(pkv).Result()
	if err == nil {
		return nil, errors.New("primary key cann't repeat!")
	}

	if err != redis.Nil {
		return nil, err
	}

	return &InsertQuery{NumColumns: len(cds), PK: pkv, Values: vm}, nil

}
