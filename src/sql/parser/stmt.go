package parser

import (
	"fmt"
)

const (
	// Ack indicates that the statement does not have a meaningful
	// return. Examples include SET, BEGIN, COMMIT.
	Ack int = iota
	// DDL indicates that the statement mutates the database schema.
	DDL
	// RowsAffected indicates that the statement returns the count of
	// affected rows.
	RowsAffected
	// Rows indicates that the statement returns the affected rows after
	// the statement was applied.
	Rows
)

// Statement represents a statement.
type Statement interface {
	fmt.Stringer
	StatementType() int
}

func (*CreateDatabase) StatementType() int {
	return DDL
}

func (*CreateTable) StatementType() int {
	return DDL
}

func (*DropDatabase) StatementType() int {
	return DDL
}

func (*DropTable) StatementType() int {
	return DDL
}

func (*UseDB) StatementType() int {
	return DDL
}

func (*ShowDatabases) StatementType() int {
	return Rows
}

func (*ShowTables) StatementType() int {
	return Rows
}

func (*SelectStmt) StatementType() int {
	return Rows
}

func (*Show) StatementType() int {
	return Rows
}

func (*SelectQuery) StatementType() int {
	return Rows
}
