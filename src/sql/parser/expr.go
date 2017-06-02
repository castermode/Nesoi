package parser

import (
	"fmt"
)

type Expr interface {
	fmt.Stringer
}

type Exprs []Expr

const (
	EQ int = iota
)

const (
	ESYSVAR int = iota
	EUSERVAR
	EALLTARGET
	ETARGET
	EVALUE
)

type ComparisonExpr struct {
	Operator    int
	Left, Right Expr
}

func (node *ComparisonExpr) String() string {
	return "Comparison"
}

type VariableExpr struct {
	Type int
	Name string
}

func (node *VariableExpr) String() string {
	return "Variable"
}

type ValueExpr struct {
	Item interface{}
}

func (node *ValueExpr) String() string {
	return "Value"
}
