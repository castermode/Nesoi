package parser

import (
	"bytes"
	"fmt"
)

type TargetElem struct {
	Item Expr
}

func (node *TargetElem) String() string {
	var buf bytes.Buffer

	if node.Item != nil {
		fmt.Fprintf(&buf, "%s", node.Item)
	}

	return buf.String()
}

type TargetClause []*TargetElem

func (node TargetClause) String() string {
	var buf bytes.Buffer

	for _, t := range node {
		fmt.Fprintf(&buf, "%s, ", t)
	}

	return buf.String()
}

type WhereClause struct {
	Cond Expr
}

func (node *WhereClause) String() string {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "%s ", node.Cond)

	return buf.String()
}

type LimitClause struct {
	Num uint64
}

func (node *LimitClause) String() string {
	return fmt.Sprintf("%v", node.Num)
}

type SelectStmt struct {
	From   *TableName
	Target TargetClause
	Where  *WhereClause
	Limit  *LimitClause
}

func (node *SelectStmt) String() string {
	var buf bytes.Buffer
	buf.WriteString("SELECT ")

	if node.Target != nil {
		fmt.Fprintf(&buf, "%s ", node.Target)
	}

	if node.From != nil {
		fmt.Fprintf(&buf, "FROM %s ", node.From)
	}

	if node.Where != nil {
		fmt.Fprintf(&buf, "WHERE %s ", node.Where)
	}

	if node.Limit != nil {
		fmt.Fprintf(&buf, "LIMIT %s ", node.Limit)
	}

	return buf.String()
}

type InsertStmt struct {
	TName      *TableName
	ColumnList []string
	Values     Exprs
}

func (node * InsertStmt) String() string {
	var buf bytes.Buffer
	buf.WriteString("INSERT INTO")
	
	if node.TName != nil {
		fmt.Fprintf(&buf, " %s", node.TName)
	}
	
	buf.WriteString("(")
	for _, c := range node.ColumnList {
		fmt.Fprintf(&buf, "%s, ", c)
	}
	buf.WriteString(") VALUES (")
	
	for _, v := range node.Values {
		fmt.Fprintf(&buf, "%s, ", v)
	}
	buf.WriteString(")")
	
	return buf.String()
}
