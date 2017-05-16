// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"bytes"

	"github.com/castermode/Nesoi/src/sql/util"
)

func isLetter(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch rune) bool {
	return (ch >= '0' && ch <= '9')
}

func isIdentChar(ch rune) bool {
	return isLetter(ch) || isDigit(ch) || ch == '_' || ch == '$' || isIdentExtend(ch)
}

func isIdentExtend(ch rune) bool {
	return ch >= 0x80 && ch <= '\uffff'
}

func isIdentFirstChar(ch rune) bool {
	return isLetter(ch) || ch == '_'
}

func isASCII(ch rune) bool {
	return ch >= 0 && ch <= 0177
}

type trieNode struct {
	childs [256]*trieNode
	token  int
	fn     func(s *Scanner) (int, Pos, string)
}

var ruleTable trieNode

func initTokenByte(c byte, tok int) {
	if ruleTable.childs[c] == nil {
		ruleTable.childs[c] = &trieNode{}
	}
	ruleTable.childs[c].token = tok
}

func initTokenString(str string, tok int) {
	node := &ruleTable
	for _, c := range str {
		if node.childs[c] == nil {
			node.childs[c] = &trieNode{}
		}
		node = node.childs[c]
	}
	node.token = tok
}

func initTokenFunc(str string, fn func(s *Scanner) (int, Pos, string)) {
	for i := 0; i < len(str); i++ {
		c := str[i]
		if ruleTable.childs[c] == nil {
			ruleTable.childs[c] = &trieNode{}
		}
		ruleTable.childs[c].fn = fn
	}
	return
}

func init() {
	// invalid is a special token defined in parser.y, when parser meet
	// this token, it will throw an error.
	// set root trie node's token to invalid, so when input match nothing
	// in the trie, invalid will be the default return token.
	ruleTable.token = invalid
	initTokenByte('*', int('*'))
	initTokenByte('/', int('/'))
	initTokenByte('+', int('+'))
	initTokenByte('>', int('>'))
	initTokenByte('<', int('<'))
	initTokenByte('(', int('('))
	initTokenByte(')', int(')'))
	initTokenByte(';', int(';'))
	initTokenByte(',', int(','))
	initTokenByte('&', int('&'))
	initTokenByte('%', int('%'))
	initTokenByte(':', int(':'))
	initTokenByte('|', int('|'))
	initTokenByte('!', int('!'))
	initTokenByte('^', int('^'))
	initTokenByte('~', int('~'))
	initTokenByte('\\', int('\\'))
	initTokenByte('?', placeholder)
	initTokenByte('=', eq)

	initTokenString("||", oror)
	initTokenString("&&", andand)
	initTokenString("&^", andnot)
	initTokenString(":=", assignmentEq)
	initTokenString("<=>", nulleq)
	initTokenString(">=", ge)
	initTokenString("<=", le)
	initTokenString("!=", neq)
	initTokenString("<>", neqSynonym)
	initTokenString("<<", lsh)
	initTokenString(">>", rsh)

	initTokenFunc("/", startWithSlash)
	initTokenFunc("-", startWithDash)
	initTokenFunc("#", startWithSharp)
	initTokenFunc("Xx", startWithXx)
	initTokenFunc("Nn", startWithNn)
	initTokenFunc("Bb", startWithBb)
	initTokenFunc(".", startWithDot)
	initTokenFunc("_$@ACDEFGHIJKLMOPQRSTUVWYZacdefghijklmopqrstuvwyz", scanIdentifier)
	initTokenFunc("`", scanQuotedIdent)
	initTokenFunc("0123456789", startWithNumber)
	initTokenFunc("'\"", startString)
}

var tokenMap = map[string]int{
	"ACTION":             ACTION,
	"ASCII":              ASCII,
	"AUTO_INCREMENT":     AUTO_INCREMENT,
	"AFTER":              AFTER,
	"AT":                 AT,
	"AVG":                AVG,
	"BEGIN":              BEGIN,
	"BIT":                BIT,
	"BOOL":               BOOL,
	"BOOLEAN":            BOOLEAN,
	"BTREE":              BTREE,
	"CHARSET":            CHARSET,
	"COLUMNS":            COLUMNS,
	"COMMIT":             COMMIT,
	"COMPACT":            COMPACT,
	"COMPRESSED":         COMPRESSED,
	"CONSISTENT":         CONSISTENT,
	"DATA":               DATA,
	"DATE":               DATE,
	"DATETIME":           DATETIME,
	"DEALLOCATE":         DEALLOCATE,
	"DO":                 DO,
	"DYNAMIC":            DYNAMIC,
	"END":                END,
	"ENGINE":             ENGINE,
	"ENGINES":            ENGINES,
	"ESCAPE":             ESCAPE,
	"EXECUTE":            EXECUTE,
	"FIELDS":             FIELDS,
	"FIRST":              FIRST,
	"FIXED":              FIXED,
	"FORMAT":             FORMAT,
	"FULL":               FULL,
	"GLOBAL":             GLOBAL,
	"HASH":               HASH,
	"LESS":               LESS,
	"LOCAL":              LOCAL,
	"NAMES":              NAMES,
	"OFFSET":             OFFSET,
	"PASSWORD":           PASSWORD,
	"PREPARE":            PREPARE,
	"QUICK":              QUICK,
	"REDUNDANT":          REDUNDANT,
	"ROLLBACK":           ROLLBACK,
	"SESSION":            SESSION,
	"SIGNED":             SIGNED,
	"SNAPSHOT":           SNAPSHOT,
	"START":              START,
	"STATUS":             STATUS,
	"TABLES":             TABLES,
	"TEXT":               TEXT,
	"THAN":               THAN,
	"TIME":               TIME,
	"TIMESTAMP":          TIMESTAMP,
	"TRANSACTION":        TRANSACTION,
	"TRUNCATE":           TRUNCATE,
	"UNKNOWN":            UNKNOWN,
	"VALUE":              VALUE,
	"WARNINGS":           WARNINGS,
	"YEAR":               YEAR,
	"MODE":               MODE,
	"WEEK":               WEEK,
	"ANY":                ANY,
	"SOME":               SOME,
	"USER":               USER,
	"IDENTIFIED":         IDENTIFIED,
	"COLLATION":          COLLATION,
	"COMMENT":            COMMENT,
	"AVG_ROW_LENGTH":     AVG_ROW_LENGTH,
	"CONNECTION":         CONNECTION,
	"CHECKSUM":           CHECKSUM,
	"COMPRESSION":        COMPRESSION,
	"KEY_BLOCK_SIZE":     KEY_BLOCK_SIZE,
	"MAX_ROWS":           MAX_ROWS,
	"MIN_ROWS":           MIN_ROWS,
	"NATIONAL":           NATIONAL,
	"ROW":                ROW,
	"ROW_FORMAT":         ROW_FORMAT,
	"QUARTER":            QUARTER,
	"GRANTS":             GRANTS,
	"TRIGGERS":           TRIGGERS,
	"DELAY_KEY_WRITE":    DELAY_KEY_WRITE,
	"ISOLATION":          ISOLATION,
	"REPEATABLE":         REPEATABLE,
	"COMMITTED":          COMMITTED,
	"UNCOMMITTED":        UNCOMMITTED,
	"ONLY":               ONLY,
	"SERIALIZABLE":       SERIALIZABLE,
	"LEVEL":              LEVEL,
	"VARIABLES":          VARIABLES,
	"SQL_CACHE":          SQL_CACHE,
	"INDEXES":            INDEXES,
	"PROCESSLIST":        PROCESSLIST,
	"SQL_NO_CACHE":       SQL_NO_CACHE,
	"DISABLE":            DISABLE,
	"ENABLE":             ENABLE,
	"REVERSE":            REVERSE,
	"SPACE":              SPACE,
	"PRIVILEGES":         PRIVILEGES,
	"NO":                 NO,
	"BINLOG":             BINLOG,
	"FUNCTION":           FUNCTION,
	"VIEW":               VIEW,
	"MODIFY":             MODIFY,
	"EVENTS":             EVENTS,
	"PARTITIONS":         PARTITIONS,
	"TIMESTAMPDIFF":      TIMESTAMPDIFF,
	"NONE":               NONE,
	"SUPER":              SUPER,
	"ADD":                ADD,
	"ALL":                ALL,
	"ALTER":              ALTER,
	"ANALYZE":            ANALYZE,
	"AND":                AND,
	"AS":                 AS,
	"ASC":                ASC,
	"BETWEEN":            BETWEEN,
	"BIGINT":             BIGINT,
	"BINARY":             BINARY,
	"BLOB":               BLOB,
	"BOTH":               BOTH,
	"BY":                 BY,
	"CASCADE":            CASCADE,
	"CASE":               CASE,
	"CHANGE":             CHANGE,
	"CHARACTER":          CHARACTER,
	"CHECK":              CHECK,
	"COLLATE":            COLLATE,
	"COLUMN":             COLUMN,
	"CONSTRAINT":         CONSTRAINT,
	"CONVERT":            CONVERT,
	"CREATE":             CREATE,
	"CROSS":              CROSS,
	"CURRENT_DATE":       CURRENT_DATE,
	"CURRENT_TIME":       CURRENT_TIME,
	"CURRENT_TIMESTAMP":  CURRENT_TIMESTAMP,
	"CURRENT_USER":       CURRENT_USER,
	"DATABASE":           DATABASE,
	"DATABASES":          DATABASES,
	"DAY_HOUR":           DAY_HOUR,
	"DAY_MICROSECOND":    DAY_MICROSECOND,
	"DAY_MINUTE":         DAY_MINUTE,
	"DAY_SECOND":         DAY_SECOND,
	"DECIMAL":            DECIMAL,
	"DEFAULT":            DEFAULT,
	"DELETE":             DELETE,
	"DESC":               DESC,
	"DESCRIBE":           DESCRIBE,
	"DISTINCT":           DISTINCT,
	"DIV":                DIV,
	"DOUBLE":             DOUBLE,
	"DROP":               DROP,
	"DUAL":               DUAL,
	"ELSE":               ELSE,
	"ENCLOSED":           ENCLOSED,
	"ESCAPED":            ESCAPED,
	"EXISTS":             EXISTS,
	"EXPLAIN":            EXPLAIN,
	"FALSE":              FALSE,
	"FLOAT":              FLOAT,
	"FOR":                FOR,
	"FORCE":              FORCE,
	"FOREIGN":            FOREIGN,
	"FROM":               FROM,
	"FULLTEXT":           FULLTEXT,
	"GRANT":              GRANT,
	"GROUP":              GROUP,
	"HAVING":             HAVING,
	"HOUR_MICROSECOND":   HOUR_MICROSECOND,
	"HOUR_MINUTE":        HOUR_MINUTE,
	"HOUR_SECOND":        HOUR_SECOND,
	"IF":                 IF,
	"IGNORE":             IGNORE,
	"IN":                 IN,
	"INDEX":              INDEX,
	"INFILE":             INFILE,
	"INNER":              INNER,
	"INSERT":             INSERT,
	"INT":                INT,
	"INTO":               INTO,
	"INTEGER":            INTEGER,
	"INTERVAL":           INTERVAL,
	"IS":                 IS,
	"JOIN":               JOIN,
	"KEY":                KEY,
	"KEYS":               KEYS,
	"KILL":               KILL,
	"LEADING":            LEADING,
	"LEFT":               LEFT,
	"LIKE":               LIKE,
	"LIMIT":              LIMIT,
	"LINES":              LINES,
	"LOAD":               LOAD,
	"LOCALTIME":          LOCALTIME,
	"LOCALTIMESTAMP":     LOCALTIMESTAMP,
	"LOCK":               LOCK,
	"LONGBLOB":           LONGBLOB,
	"LONGTEXT":           LONGTEXT,
	"MAXVALUE":           MAXVALUE,
	"MEDIUMBLOB":         MEDIUMBLOB,
	"MEDIUMINT":          MEDIUMINT,
	"MEDIUMTEXT":         MEDIUMTEXT,
	"MINUTE_MICROSECOND": MINUTE_MICROSECOND,
	"MINUTE_SECOND":      MINUTE_SECOND,
	"MOD":                MOD,
	"NOT":                NOT,
	"NO_WRITE_TO_BINLOG": NO_WRITE_TO_BINLOG,
	"NULL":               NULL,
	"NUMERIC":            NUMERIC,
	"ON":                 ON,
	"OPTION":             OPTION,
	"OR":                 OR,
	"ORDER":              ORDER,
	"OUTER":              OUTER,
	"PARTITION":          PARTITION,
	"PRECISION":          PRECISION,
	"PRIMARY":            PRIMARY,
	"PROCEDURE":          PROCEDURE,
	"RANGE":              RANGE,
	"READ":               READ,
	"REAL":               REAL,
	"REFERENCES":         REFERENCES,
	"REGEXP":             REGEXP,
	"RENAME":             RENAME,
	"REPEAT":             REPEAT,
	"REPLACE":            REPLACE,
	"RESTRICT":           RESTRICT,
	"REVOKE":             REVOKE,
	"RIGHT":              RIGHT,
	"RLIKE":              RLIKE,
	"SCHEMA":             SCHEMA,
	"SCHEMAS":            SCHEMAS,
	"SECOND_MICROSECOND": SECOND_MICROSECOND,
	"SELECT":             SELECT,
	"SET":                SET,
	"SHOW":               SHOW,
	"SMALLINT":           SMALLINT,
	"STARTING":           STARTING,
	"TABLE":              TABLE,
	"TERMINATED":         TERMINATED,
	"THEN":               THEN,
	"TINYBLOB":           TINYBLOB,
	"TINYINT":            TINYINT,
	"TINYTEXT":           TINYTEXT,
	"TO":                 TO,
	"TRAILING":           TRAILING,
	"TRUE":               TRUE,
	"UNION":              UNION,
	"UNIQUE":             UNIQUE,
	"UNLOCK":             UNLOCK,
	"UNSIGNED":           UNSIGNED,
	"UPDATE":             UPDATE,
	"USE":                USE,
	"USING":              USING,
	"UTC_DATE":           UTC_DATE,
	"UTC_TIMESTAMP":      UTC_TIMESTAMP,
	"VALUES":             VALUES,
	"VARBINARY":          VARBINARY,
	"VARCHAR":            VARCHAR,
	"WHEN":               WHEN,
	"WHERE":              WHERE,
	"WRITE":              WRITE,
	"XOR":                XOR,
	"YEAR_MONTH":         YEAR_MONTH,
	"ZEROFILL":           ZEROFILL,
}

func isTokenIdentifier(s string, buf *bytes.Buffer) int {
	buf.Reset()
	buf.Grow(len(s))
	data := buf.Bytes()[:len(s)]
	for i := 0; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'z' {
			data[i] = s[i] + 'A' - 'a'
		} else {
			data[i] = s[i]
		}
	}
	tok := tokenMap[util.ToString(data)]
	return tok
}
