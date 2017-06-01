package parser

import (
	"bytes"
	"fmt"
)

// CreateDatabase represents a CREATE DATABASE statement.
type CreateDatabase struct {
	IfNotExists bool
	DBName      string
}

func (node *CreateDatabase) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE DATABASE ")
	if node.IfNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}
	buf.WriteString(node.DBName)
	return buf.String()
}

type TableName struct {
	Schema string
	Name   string
}

func (node *TableName) String() string {
	var buf bytes.Buffer
	if node.Schema != "" {
		fmt.Fprintf(&buf, "%s.", node.Schema)
	}
	fmt.Fprintf(&buf, "%s", node.Name)
	return buf.String()
}

const (
	Null int = iota
	NotNull
)

const (
	SqlInt int = iota
	SqlString
)

type ColumnTableJsonDef struct {
	Name       string
	Pos        int
	Type       int
	Nullable   int
	PrimaryKey bool
	Unique     bool
}

// ColumnTableDef represents a column definition within a CREATE TABLE
// statement.
type ColumnTableDef struct {
	Name       string
	Pos        int
	Type       ColumnType
	Nullable   int
	PrimaryKey bool
	Unique     bool
}

func (node *ColumnTableDef) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s %s", node.Name, node.Type)
	switch node.Nullable {
	case Null:
		buf.WriteString(" NULL")
	case NotNull:
		buf.WriteString(" NOT NULL")
	}
	if node.PrimaryKey {
		buf.WriteString(" PRIMARY KEY")
	} else if node.Unique {
		buf.WriteString(" UNIQUE")
	}

	return buf.String()
}

func newColumnTableDef(name string, typ ColumnType, options []ColumnOption) *ColumnTableDef {
	c := &ColumnTableDef{
		Name: name,
		Type: typ,
	}

	for _, o := range options {
		switch o.(type) {
		case NotNullConstraint:
			c.Nullable = NotNull
		case NullConstraint:
			c.Nullable = Null
		case PrimaryKeyConstraint:
			c.PrimaryKey = true
		case UniqueConstraint:
			c.Unique = true
		default:
			panic(fmt.Sprintf("unexpected column option: %T", c))
		}
	}

	return c
}

type ColumnTableDefs []*ColumnTableDef
type ColumnTableJsonDefs []*ColumnTableJsonDef

func (node ColumnTableDefs) String() string {
	var prefix string
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%s", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// CreateTable represents a CREATE TABLE statement.
type CreateTable struct {
	IfNotExists bool
	Table       *TableName
	Defs        ColumnTableDefs
}

func (node *CreateTable) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE TABLE")
	if node.IfNotExists {
		buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s (%s)", node.Table, node.Defs)
	return buf.String()
}

type DropDatabase struct {
	IfExists bool
	DBName   string
}

func (node *DropDatabase) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP DATABASE")
	if node.IfExists {
		fmt.Fprintf(&buf, " IF EXISTS")
	}
	fmt.Fprintf(&buf, " %s", node.DBName)
	return buf.String()
}

type DropTable struct {
	IfExists bool
	TName    *TableName
}

func (node *DropTable) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP TABLE")
	if node.IfExists {
		fmt.Fprintf(&buf, " IF EXISTS")
	}
	fmt.Fprintf(&buf, " %s", node.TName)
	return buf.String()
}

type UseDB struct {
	DBName string
}

func (node *UseDB) String() string {
	var buf bytes.Buffer
	buf.WriteString("USE")
	fmt.Fprintf(&buf, " %s", node.DBName)
	return buf.String()
}

type ColumnOption interface {
	columnOption()
}

type NotNullConstraint struct {
}

func (NotNullConstraint) columnOption() {
}

type NullConstraint struct {
}

func (NullConstraint) columnOption() {
}

type PrimaryKeyConstraint struct {
}

func (PrimaryKeyConstraint) columnOption() {
}

type UniqueConstraint struct {
}

func (UniqueConstraint) columnOption() {
}
