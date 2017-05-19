package plan

import (
	"errors"

	"github.com/castermode/Nesoi/src/sql/parser"
)

func Optimize(query parser.Statement) (Plan, error) {
	switch query.(type) {
	case *parser.SelectQuery:
		return doSelectOptimize(query)
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

	var isPKFilter bool
	if s.From != nil {
		// Scan with PK?
		if s.IsPKFilter() {
			isPKFilter = true
			plan = &ScanWithPK{
				Table: s.From.Name,
				PK:    s.Qual.Right,
			}
		} else {
			plan = &Scan{
				Table: s.From.Name,
			}
		}
	} else {
		return &Simple{Item: s.Fields}, nil
	}

	if s.Qual != nil {
		if !isPKFilter {
			qual := &Qual{Pos: s.Qual.Left.FieldID, Value: s.Qual.Right.Value}
			splan := &Selection{Filter: qual}
			plan = appendPlan(splan, plan)
		}
	}

	if s.Fields != nil {
		pplan := &Projection{
			Fields:    s.Fields,
			FieldsNum: s.FieldsNum,
		}
		plan = appendPlan(pplan, plan)
	}

	if s.Limit != 0 {
		lplan := &Limit{Num: s.Limit}
		plan = appendPlan(lplan, plan)
	}

	return plan, nil
}
