package parser

type TargetRes struct {
	Type     int
	TargetID int

	// column id
	FieldID int

	// sysvar
	SysVar string

	// value
	Value interface{}
}

type ComparisonQual struct {
	Operator    int
	Left, Right *TargetRes
}

type TableInfo struct {
	Name      string
	ColumnMap map[int]*ColumnTableDef
}

type SelectQuery struct {
	From      *TableInfo
	Fields    []*TargetRes
	FieldsNum int
	Qual      *ComparisonQual
	Limit     uint64
}

func (node *SelectQuery) String() string {
	return "Select Query"
}

func IsPKFilter(qual *ComparisonQual, cm map[int]*ColumnTableDef) bool {
	if qual == nil {
		return false
	}

	if qual.Left == nil || qual.Right == nil {
		return false
	}

	if qual.Left.Type == ETARGET && qual.Right.Type == EVALUE {
		if cm[qual.Left.FieldID-1].PrimaryKey {
			return true
		}
	}

	return false
}

const (
	SDATABASES int = iota
	STABLES
)

type Show struct {
	Operator int
}

func (node *Show) String() string {
	return "SHOW"
}

type InsertQuery struct {
	NumColumns int
	TableName  string
	PK         string
	Values     map[int]interface{}
}

func (node *InsertQuery) String() string {
	return "INSERT QUERY"
}

type UpdateQuery struct {
	Table     *TableInfo
	Fields    []*TargetRes
	FieldsNum int
	Values    map[int]interface{}
	Qual      *ComparisonQual
}

func (node *UpdateQuery) String() string {
	return "UPDATE QUERY"
}

type CreateIndexQuery struct {
	Index     *TableName
	Table     *TableName
	TblInfo   *TableInfo
	Unique    bool
	Fields    []*TargetRes
	FieldsNum int
}

func (node *CreateIndexQuery) String() string {
	return "CREATE INDEX QUERY"
}
