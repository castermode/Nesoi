package parser

type TargetRes struct {
	Type 		int
	TargetID	int
	
	// column id
	FieldID		int
	
	// sysvar
	SysVar  	string
	
	// value
	Value 		interface{}
}

type ComparisonQual struct {
	Operator int
	Left, Right *TargetRes
}

type SelectQuery struct {
	TblName		string
	Fields		[]*TargetRes
	FieldsNum	int
	Qual		*ComparisonQual
	Limit		uint64
}

func (node *SelectQuery) String() string {
	return "Select Query"
}

