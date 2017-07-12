package parser

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/castermode/Nesoi/src/sql/context"
	"github.com/castermode/Nesoi/src/sql/store"
	"github.com/castermode/Nesoi/src/sql/util"
)

type Analyzer struct {
	driver  store.Driver
	context *context.Context
}

func NewAnalyzer(sd store.Driver, ctx *context.Context) *Analyzer {
	return &Analyzer{
		driver:  sd,
		context: ctx,
	}
}

func (a *Analyzer) Analyze(stmts []Statement) ([]Statement, error) {
	var querys []Statement
	for _, stmt := range stmts {
		var query Statement
		var err error
		switch stmt.StatementType() {
		case DDL:
			if _, ok := stmt.(*CreateIndex); ok {
				query, err = a.transformStmt(stmt)
				if err != nil {
					return nil, err
				}
			} else {
				query = stmt
			}
		case Rows, RowsAffected:
			query, err = a.transformStmt(stmt)
			if err != nil {
				return nil, err
			}
		default:
			return nil, errors.New("unsupport statement: " + stmt.String())
		}
		querys = append(querys, query)
	}

	return querys, nil
}

func (a *Analyzer) getColumnDefs(tname string) (ColumnTableDefs, error) {
	tableKey := store.SystemFlag + store.TableFlag + tname
	tableValue, err := a.driver.GetSysRecord(tableKey)
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

	return cds, nil
}

func (a *Analyzer) transformStmt(stmt Statement) (Statement, error) {
	switch stmt.(type) {
	case *SelectStmt:
		return a.transformSelectStmt(stmt)
	case *InsertStmt:
		return a.transformInsertStmt(stmt)
	case *UpdateStmt:
		return a.transformUpdateStmt(stmt)
	case *CreateIndex:
		return a.transformCreateIndex(stmt)
	case *ShowDatabases:
		return &Show{Operator: SDATABASES}, nil
	case *ShowTables:
		return &Show{Operator: STABLES}, nil
	}

	return nil, errors.New("unsupport statement: " + stmt.String())
}

func (a *Analyzer) transformTarget(expr Expr, cds ColumnTableDefs) ([]*TargetRes, bool, error) {
	if expr == nil {
		return nil, false, errors.New("invalid target value")
	}

	tgrs := []*TargetRes{}
	switch expr.(type) {
	case *VariableExpr:
		var all bool
		vtarget := expr.(*VariableExpr)
		if vtarget.Type == EALLTARGET {
			all = true
			for _, cd := range cds {
				tgr := &TargetRes{Type: ETARGET}
				tgr.TargetID = cd.Pos
				tgr.FieldID = cd.Pos
				tgrs = append(tgrs, tgr)
			}
		} else if vtarget.Type == ETARGET {
			var find bool = false
			tgr := &TargetRes{Type: ETARGET}
			for _, cd := range cds {
				if strings.EqualFold(vtarget.Name, cd.Name) {
					tgr.FieldID = cd.Pos
					find = true
					break
				}
			}
			if !find {
				return nil, false, errors.New("Invalid target name " + vtarget.Name)
			}
			tgrs = append(tgrs, tgr)
		} else {
			//sysVar
			tgr := &TargetRes{Type: ESYSVAR}
			tgr.SysVar = vtarget.Name[2:]
			tgrs = append(tgrs, tgr)
		}
		return tgrs, all, nil
	case *ValueExpr:
		tgr := &TargetRes{Type: EVALUE}
		vtarget := expr.(*ValueExpr)
		tgr.Value = vtarget.Item
		tgrs = append(tgrs, tgr)
		return tgrs, false, nil
	}

	return nil, false, errors.New("unsupport target type: " + expr.String())
}

func (a *Analyzer) transformCreateIndex(stmt Statement) (Statement, error) {
	cistmt := stmt.(*CreateIndex)

	// transform table
	tblName := a.context.GetTableName(cistmt.Table.Schema, cistmt.Table.Name)
	idxName := a.context.GetTableName(cistmt.Index.Schema, cistmt.Table.Name)
	cds, err := a.getColumnDefs(tblName)
	if err != nil {
		return nil, err
	}
	var cm map[int]*ColumnTableDef
	cm = make(map[int]*ColumnTableDef)
	for _, cd := range cds {
		cm[cd.Pos-1] = cd
	}
	table := &TableInfo{Name: tblName, ColumnMap: cm}

	//transform target
	i := 1
	var tgrs []*TargetRes
	for _, target := range cistmt.Targets {
		tgrs1, all, err := a.transformTarget(target.Item, cds)
		if err != nil {
			return nil, err
		}

		if all {
			tgrs = tgrs1
			i = len(tgrs) + 1
			break
		} else {
			tgr := tgrs1[0]
			tgr.TargetID = i
			tgrs = append(tgrs, tgr)
		}
		i++
	}

	//transform index
	sysIndexTableKey := store.SystemFlag + store.IndexFlag + store.TableFlag + idxName
	_, err = a.driver.GetSysRecord(sysIndexTableKey)
	if err == nil {
		return nil, errors.New("index name already exists!")
	}
	if err != store.Nil {
		return nil, err
	}
	fieldsNum := len(tgrs)
	sysTableIndexKey := store.SystemFlag + store.TableFlag + store.IndexFlag
	sysTableIndexKey += util.ToString(util.DumpLengthEncodedString(util.ToSlice(tblName)))
	sysTableIndexKey += util.ToString(util.DumpLengthEncodedInt(uint64(fieldsNum)))
	for _, tgr := range tgrs {
		sysTableIndexKey += util.ToString(util.DumpLengthEncodedInt(uint64(tgr.FieldID)))
	}
	_, err = a.driver.GetSysRecord(sysTableIndexKey)
	if err == nil {
		return nil, errors.New("index column has already exists!")
	}
	if err != store.Nil {
		return nil, err
	}

	return &CreateIndexQuery{
		Index:     cistmt.Index,
		Table:     cistmt.Table,
		TblInfo:   table,
		Unique:    cistmt.Unique,
		Fields:    tgrs,
		FieldsNum: fieldsNum,
	}, nil
}

func (a *Analyzer) transformSelectStmt(stmt Statement) (Statement, error) {
	sstmt := stmt.(*SelectStmt)

	var from *TableInfo
	var cds ColumnTableDefs
	var tblName string
	var err error
	// transform from clause
	if sstmt.From != nil {
		tblName = a.context.GetTableName(sstmt.From.Schema, sstmt.From.Name)
		cds, err = a.getColumnDefs(tblName)
		if err != nil {
			return nil, err
		}
		var cm map[int]*ColumnTableDef
		cm = make(map[int]*ColumnTableDef)
		for _, cd := range cds {
			cm[cd.Pos-1] = cd
		}

		from = &TableInfo{Name: tblName, ColumnMap: cm}
	}

	// transform target clause
	var num int
	var tgrs []*TargetRes
	i := 1
	for _, target := range sstmt.Target {
		tgrs1, all, err := a.transformTarget(target.Item, cds)
		if err != nil {
			return nil, err
		}

		if all {
			tgrs = tgrs1
			i = len(tgrs) + 1
			break
		} else {
			tgr := tgrs1[0]
			tgr.TargetID = i
			tgrs = append(tgrs, tgr)
		}
		i++
	}
	num = len(tgrs)

	// transform where clause
	var qual *ComparisonQual
	if sstmt.Where != nil {
		qual = &ComparisonQual{}
		switch sstmt.Where.Cond.(type) {
		case *ComparisonExpr:
			cond := sstmt.Where.Cond.(*ComparisonExpr)
			qual.Operator = cond.Operator
			tgrs1, _, err := a.transformTarget(cond.Left, cds)
			if err != nil {
				return nil, err
			}
			qual.Left = tgrs1[0]
			qual.Left.TargetID = i
			tgrs = append(tgrs, qual.Left)

			i++
			var tgrs2 []*TargetRes
			tgrs2, _, err = a.transformTarget(cond.Right, cds)
			if err != nil {
				return nil, err
			}
			qual.Right = tgrs2[0]
			qual.Right.TargetID = i
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
	cds, err := a.getColumnDefs(tblName)
	if err != nil {
		return nil, err
	}
	pks := []int{}
	vm := make(map[int]interface{})
	cm := make(map[string]*ColumnTableDef)
	cm1 := make(map[int]*ColumnTableDef)
	for _, cd := range cds {
		cm[cd.Name] = cd
		cm1[cd.Pos-1] = cd
		if cd.PrimaryKey {
			pks = append(pks, cd.Pos-1)
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

		vm[cm[c].Pos-1] = ve.Item
		i++
	}

	//check null
	if len(vm) < len(cds) {
		for _, cd := range cds {
			if _, ok := vm[cd.Pos-1]; !ok {
				if cd.PrimaryKey {
					e = cd.Name + " is primary key, cann't be null"
					return nil, errors.New(e)
				}

				if cd.Nullable == NotNull {
					e = cd.Name + " is NotNull!"
					return nil, errors.New(e)
				}

				vm[cd.Pos-1] = nil
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
	_, err = a.driver.GetUserRecord(pkv)
	if err == nil {
		return nil, errors.New("primary key can't repeat!")
	}

	if err != store.Nil {
		return nil, err
	}

	return &InsertQuery{NumColumns: len(cds), TableName: tblName, PK: pkv, Values: vm}, nil
}

func (a *Analyzer) transformUpdateStmt(stmt Statement) (Statement, error) {
	ustmt := stmt.(*UpdateStmt)

	tblName := a.context.GetTableName(ustmt.TName.Schema, ustmt.TName.Name)
	cds, err := a.getColumnDefs(tblName)
	if err != nil {
		return nil, err
	}
	cm := make(map[int]*ColumnTableDef)
	cm1 := make(map[string]*ColumnTableDef)
	for _, cd := range cds {
		cm[cd.Pos-1] = cd
		cm1[cd.Name] = cd
	}

	table := &TableInfo{Name: tblName, ColumnMap: cm}

	//Get all targetvar
	var tgrs []*TargetRes
	allTarget := &VariableExpr{Type: EALLTARGET}
	tgrs, _, err = a.transformTarget(allTarget, cds)
	if err != nil {
		return nil, err
	}
	num := len(tgrs)

	//check columnset
	vm := make(map[int]interface{})
	for _, cs := range ustmt.ColumnSetList {
		var e string
		var cd *ColumnTableDef
		var ok bool
		cd, ok = cm1[cs.ColumnName]
		if !ok {
			e = cs.ColumnName + " not exists!"
			return nil, errors.New(e)
		}

		v := cs.Value.(*ValueExpr).Item
		switch cd.Type.(type) {
		case *IntType:
			if _, ok := v.(int64); !ok {
				e = "the value of " + cd.Name + " isn't int type!"
				return nil, errors.New(e)
			}
		case *StringType:
			if _, ok := v.(string); !ok {
				e = "the value of " + cd.Name + " isn't string type!"
				return nil, errors.New(e)
			}
		}
		vm[cd.Pos-1] = v
	}

	// transform where clause
	var qual *ComparisonQual
	if ustmt.Where != nil {
		i := num + 1
		qual = &ComparisonQual{}
		switch ustmt.Where.Cond.(type) {
		case *ComparisonExpr:
			cond := ustmt.Where.Cond.(*ComparisonExpr)
			qual.Operator = cond.Operator
			tgrs1, _, err := a.transformTarget(cond.Left, cds)
			if err != nil {
				return nil, err
			}
			qual.Left = tgrs1[0]
			qual.Left.TargetID = i
			tgrs = append(tgrs, qual.Left)

			i++
			var tgrs2 []*TargetRes
			tgrs2, _, err = a.transformTarget(cond.Right, cds)
			if err != nil {
				return nil, err
			}
			qual.Right = tgrs2[0]
			qual.Right.TargetID = i
			tgrs = append(tgrs, qual.Right)
		default:
			return nil, errors.New("unsupport qual target: " + ustmt.Where.Cond.String())
		}
	}

	return &UpdateQuery{
		Table:     table,
		Fields:    tgrs,
		FieldsNum: num,
		Values:    vm,
		Qual:      qual,
	}, nil
}
