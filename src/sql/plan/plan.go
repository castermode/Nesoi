package plan

import (
	"github.com/castermode/Nesoi/src/sql/parser"
)

type Plan interface {
	AddParent(parent Plan)
	AddChild(child Plan)
	GetParents() []Plan
	GetChildren() []Plan
}

type Scan struct {
	From      *parser.TableInfo
	Fields    []*parser.TargetRes
	FieldsNum int
	Parents   []Plan
	Children  []Plan
}

func (plan *Scan) AddParent(parent Plan) {
	plan.Parents = append(plan.Parents, parent)
}

func (plan *Scan) AddChild(child Plan) {
	plan.Children = append(plan.Children, child)
}

func (plan *Scan) GetParents() []Plan {
	return plan.Parents
}

func (plan *Scan) GetChildren() []Plan {
	return plan.Children
}

type ScanWithPK struct {
	From      *parser.TableInfo
	Fields    []*parser.TargetRes
	FieldsNum int
	PK        interface{}
	Parents   []Plan
	Children  []Plan
}

func (plan *ScanWithPK) AddParent(parent Plan) {
	plan.Parents = append(plan.Parents, parent)
}

func (plan *ScanWithPK) AddChild(child Plan) {
	plan.Children = append(plan.Children, child)
}

func (plan *ScanWithPK) GetParents() []Plan {
	return plan.Parents
}

func (plan *ScanWithPK) GetChildren() []Plan {
	return plan.Children
}

type Qual struct {
	Pos   int
	Value interface{}
}

type Selection struct {
	Filter   *Qual
	Parents  []Plan
	Children []Plan
}

func (plan *Selection) AddParent(parent Plan) {
	plan.Parents = append(plan.Parents, parent)
}

func (plan *Selection) AddChild(child Plan) {
	plan.Children = append(plan.Children, child)
}

func (plan *Selection) GetParents() []Plan {
	return plan.Parents
}

func (plan *Selection) GetChildren() []Plan {
	return plan.Children
}

type Projection struct {
	FieldsNum int
	Parents   []Plan
	Children  []Plan
}

func (plan *Projection) AddParent(parent Plan) {
	plan.Parents = append(plan.Parents, parent)
}

func (plan *Projection) AddChild(child Plan) {
	plan.Children = append(plan.Children, child)
}

func (plan *Projection) GetParents() []Plan {
	return plan.Parents
}

func (plan *Projection) GetChildren() []Plan {
	return plan.Children
}

type Limit struct {
	Num      uint64
	Parents  []Plan
	Children []Plan
}

func (plan *Limit) AddParent(parent Plan) {
	plan.Parents = append(plan.Parents, parent)
}

func (plan *Limit) AddChild(child Plan) {
	plan.Children = append(plan.Children, child)
}

func (plan *Limit) GetParents() []Plan {
	return plan.Parents
}

func (plan *Limit) GetChildren() []Plan {
	return plan.Children
}

type Simple struct {
	Fields   []*parser.TargetRes
	Parents  []Plan
	Children []Plan
}

func (plan *Simple) AddParent(parent Plan) {
	plan.Parents = append(plan.Parents, parent)
}

func (plan *Simple) AddChild(child Plan) {
	plan.Children = append(plan.Children, child)
}

func (plan *Simple) GetParents() []Plan {
	return plan.Parents
}

func (plan *Simple) GetChildren() []Plan {
	return plan.Children
}

type Show struct {
	Operator int
	Parents  []Plan
	Children []Plan
}

func (plan *Show) AddParent(parent Plan) {
	plan.Parents = append(plan.Parents, parent)
}

func (plan *Show) AddChild(child Plan) {
	plan.Children = append(plan.Children, child)
}

func (plan *Show) GetParents() []Plan {
	return plan.Parents
}

func (plan *Show) GetChildren() []Plan {
	return plan.Children
}

type Update struct {
	Table     *parser.TableInfo
	Values    map[int]interface{}
	FieldsNum int
	Parents   []Plan
	Children  []Plan
}

func (plan *Update) AddParent(parent Plan) {
	plan.Parents = append(plan.Parents, parent)
}

func (plan *Update) AddChild(child Plan) {
	plan.Children = append(plan.Children, child)
}

func (plan *Update) GetParents() []Plan {
	return plan.Parents
}

func (plan *Update) GetChildren() []Plan {
	return plan.Children
}
