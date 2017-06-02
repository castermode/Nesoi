package plan

import (
	"errors"

	"github.com/castermode/Nesoi/src/sql/parser"
)

func Optimize(query parser.Statement) (Plan, error) {
	switch query.(type) {
	case *parser.SelectQuery:
		return doSelectOptimize(query)
	case *parser.Show:
		return doShowOptimize(query)
	default:
		return nil, errors.New("unsupport statement " + query.String())
	}
}

func appendPlan(parent Plan, child Plan) Plan {
	parent.AddChild(child)
	child.AddParent(parent)
	return parent
}

func doSelectOptimize(query parser.Statement) (Plan, error) {
	var plan Plan
	s := query.(*parser.SelectQuery)

	if s.From != nil {
		// Scan with PK?
		var fields []*parser.TargetRes
		var fieldsnum int
		if s.Fields != nil {
			fields = s.Fields
			fieldsnum = s.FieldsNum
		}

		if s.IsPKFilter() {
			plan = &ScanWithPK{
				From:      s.From,
				PK:        s.Qual,
				Fields:    fields,
				FieldsNum: fieldsnum,
			}
		} else {
			plan = &Scan{
				From:      s.From,
				Fields:    fields,
				FieldsNum: fieldsnum,
			}

			if s.Qual != nil {
				qual := &Qual{Pos: s.Qual.Left.FieldID, Value: s.Qual.Right.Value}
				splan := &Selection{Filter: qual}
				plan = appendPlan(splan, plan)
			}
		}

		if fieldsnum != len(fields) {
			pplan := &Projection{FieldsNum: fieldsnum}
			plan = appendPlan(pplan, plan)
		}

		if s.Limit != 0 {
			lplan := &Limit{Num: s.Limit}
			plan = appendPlan(lplan, plan)
		}
	} else {
		plan = &Simple{Fields: s.Fields}
	}

	return plan, nil
}

func doShowOptimize(query parser.Statement) (Plan, error) {
	s := query.(*parser.Show)
	return &Show{Operator: s.Operator}, nil
}
