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

func (node *SelectQuery) IsPKFilter() bool {
	if node.Qual == nil {
		return false
	}

	if node.Qual.Left == nil || node.Qual.Right == nil {
		return false
	}

	if node.Qual.Left.Type == ETARGET && node.Qual.Right.Type == EVALUE {
		if node.From.ColumnMap[node.Qual.Left.FieldID].PrimaryKey {
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
