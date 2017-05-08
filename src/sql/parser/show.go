package parser

type ShowDatabases struct {
}

func (node *ShowDatabases) String() string {
	return "SHOW DATABASE"
}
