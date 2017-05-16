package parser

import (
	"fmt"
)

type Expr interface {
	fmt.Stringer
}

const (
	EQ int = iota
)

type ComparisonExpr struct {
	Operator    int
	Left, Right Expr
}

func (node *ComparisonExpr) String() string {
	return "Comparison"
}

type VariableExpr struct {
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
