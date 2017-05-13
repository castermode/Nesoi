package parser

type ShowDatabases struct {
}

func (node *ShowDatabases) String() string {
	return "SHOW DATABASES"
}

type ShowTables struct {
}

func (node *ShowTables) String() string {
	return "SHOW TABLES"
}
