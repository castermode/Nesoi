package server

import (
	"github.com/castermode/Nesoi/src/sql/parser"
)

type Session struct {
	parser *parser.Parser
}

func (s *Session) Execute(sql string) (error) {
	return nil
}
