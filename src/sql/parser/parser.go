package parser

import (
	"math"
	"regexp"
	"strconv"
	"unicode"
)

var (
	specCodePattern = regexp.MustCompile(`\/\*!(M?[0-9]{5,6})?([^*]|\*+[^*/])*\*+\/`)
	specCodeStart   = regexp.MustCompile(`^\/\*!(M?[0-9]{5,6} )?[ \t]*`)
	specCodeEnd     = regexp.MustCompile(`[ \t]*\*\/$`)
)

type Parser struct {
	src    string
	lexer  Scanner
	result []Statement

	// the following fields are used by yyParse to reduce allocation.
	cache  []yySymType
	yylval yySymType
	yyVAL  yySymType
}

func (p *Parser) Parse(sql string) ([]Statement, error) {
	p.src = sql
	p.result = p.result[:0]

	var l yyLexer
	p.lexer.reset(sql)
	l = &p.lexer
	yyParse(l, p)

	if len(l.Errors()) != 0 {
		return nil, l.Errors()[0]
	}

	return p.result, nil
}

func toInt(l yyLexer, lval *yySymType, str string) int {
	n, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		e := err.(*strconv.NumError)
		if e.Err == strconv.ErrRange {
			return toDecimal(l, lval, str)
		}
		l.Errorf("integer literal: %v", err)
		return int(unicode.ReplacementChar)
	}

	switch {
	case n < math.MaxInt64:
		lval.item = int64(n)
	default:
		lval.item = uint64(n)
	}
	return intLit
}

func toDecimal(l yyLexer, lval *yySymType, str string) int {
	//@TODO
	lval.item = nil
	return decLit
}

func toFloat(l yyLexer, lval *yySymType, str string) int {
	n, err := strconv.ParseFloat(str, 64)
	if err != nil {
		l.Errorf("float literal: %v", err)
		return int(unicode.ReplacementChar)
	}

	lval.item = float64(n)
	return floatLit
}

// See https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
func toHex(l yyLexer, lval *yySymType, str string) int {
	//@TODO
	lval.item = nil
	return hexLit
}

// See https://dev.mysql.com/doc/refman/5.7/en/bit-type.html
func toBit(l yyLexer, lval *yySymType, str string) int {
	//@TOD
	lval.item = nil
	return bitLit
}

func trimComment(txt string) string {
	txt = specCodeStart.ReplaceAllString(txt, "")
	return specCodeEnd.ReplaceAllString(txt, "")
}
