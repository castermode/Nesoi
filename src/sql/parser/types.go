package parser

import (
	"bytes"
	"fmt"
)

type ColumnType interface {
	fmt.Stringer
	columnType()
}

type IntType struct {
	Name string
	N    int
}

func (node *IntType) String() string {
	var buf bytes.Buffer
	buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(&buf, "(%d)", node.N)
	}
	return buf.String()
}

func (*IntType) columnType() {
}

type StringType struct {
	Name string
	N    int
}

func (node *StringType) String() string {
	var buf bytes.Buffer
	buf.WriteString(node.Name)
	if node.N > 0 {
		fmt.Fprintf(&buf, "(%d)", node.N)
	}
	return buf.String()
}

func (*StringType) columnType() {
}
