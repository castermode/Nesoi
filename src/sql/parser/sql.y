%{
package parser
%}

%union {
	item 		interface{}
	str		string
	strs		[]string
	stmt		Statement
	stmts		[]Statement
	tname   	*TableName
	tbldef  	*ColumnTableDef
	tbldefs 	ColumnTableDefs
	colType 	ColumnType
	colOption	ColumnOption
	colOptions	[]ColumnOption
}

%type <stmts>	StmtList
%type <stmt>	Stmt
%type <stmt>	CreateDatabaseStmt
%type <stmt>	CreateTableStmt
%type <stmt>	ShowStmt

%type <tname>		TableName
%type <tbldef>		TableElem
%type <tbldefs>		TableElemList
%type <colType>		TypeName NumericType StringType
%type <colOption>	ColumnOptionItem
%type <colOptions>	ColumnOption
%type <str>		Name
%type <str>		UnReservedKeyword ReservedKeyword

%token <item> intLit floatLit decLit hexLit bitLit
%token <str> identifier invalid
%token <str> hintBegin hintEnd underscoreCS stringLit
%token <str> placeholder eq oror andand andnot assignmentEq nulleq ge le neq neqSynonym
%token <str> lsh rsh charset

%token <str> ACTION ASCII AUTO_INCREMENT AFTER AT AVG BEGIN BIT BOOL BOOLEAN BTREE CHARSET
%token <str> COLUMNS COMMIT COMPACT COMPRESSED CONSISTENT DATA DATE DATETIME DEALLOCATE DO
%token <str> DYNAMIC END ENGINE ENGINES ESCAPE EXECUTE FIELDS FIRST FIXED FORMAT FULL GLOBAL
%token <str> HASH LESS LOCAL NAMES OFFSET PASSWORD PREPARE QUICK REDUNDANT
%token <str> ROLLBACK SESSION SIGNED SNAPSHOT START STATUS TABLES TEXT THAN TIME TIMESTAMP
%token <str> TRANSACTION TRUNCATE UNKNOWN VALUE WARNINGS YEAR MODE WEEK ANY SOME USER IDENTIFIED
%token <str> COLLATION COMMENT AVG_ROW_LENGTH CONNECTION CHECKSUM COMPRESSION KEY_BLOCK_SIZE MAX_ROWS
%token <str> MIN_ROWS NATIONAL ROW ROW_FORMAT QUARTER GRANTS TRIGGERS DELAY_KEY_WRITE ISOLATION
%token <str> REPEATABLE COMMITTED UNCOMMITTED ONLY SERIALIZABLE LEVEL VARIABLES SQL_CACHE INDEXES PROCESSLIST
%token <str> SQL_NO_CACHE DISABLE ENABLE REVERSE SPACE PRIVILEGES NO BINLOG FUNCTION VIEW MODIFY EVENTS PARTITIONS
%token <str> TIMESTAMPDIFF NONE SUPER

%token <str> ADD ALL ALTER ANALYZE AND AS ASC BETWEEN BIGINT
%token <str> BINARY BLOB BOTH BY CASCADE CASE CHANGE CHARACTER CHECK COLLATE
%token <str> COLUMN CONSTRAINT CONVERT CREATE CROSS CURRENT_DATE CURRENT_TIME
%token <str> CURRENT_TIMESTAMP CURRENT_USER DATABASE DATABASES DAY_HOUR DAY_MICROSECOND
%token <str> DAY_MINUTE DAY_SECOND DECIMAL DEFAULT DELETE DESC DESCRIBE
%token <str> DISTINCT DIV DOUBLE DROP DUAL ELSE ENCLOSED ESCAPED
%token <str> EXISTS EXPLAIN FALSE FLOAT FOR FORCE FOREIGN FROM
%token <str> FULLTEXT GRANT GROUP HAVING HOUR_MICROSECOND HOUR_MINUTE
%token <str> HOUR_SECOND IF IGNORE IN INDEX INFILE INNER INSERT INT INTO INTEGER
%token <str> INTERVAL IS JOIN KEY KEYS KILL LEADING LEFT LIKE LIMIT LINES LOAD
%token <str> LOCALTIME LOCALTIMESTAMP LOCK LONGBLOB LONGTEXT MAXVALUE MEDIUMBLOB MEDIUMINT MEDIUMTEXT
%token <str> MINUTE_MICROSECOND MINUTE_SECOND MOD NOT NO_WRITE_TO_BINLOG NULL NUMERIC
%token <str> ON OPTION OR ORDER OUTER PARTITION PRECISION PRIMARY PROCEDURE RANGE READ
%token <str> REAL REFERENCES REGEXP RENAME REPEAT REPLACE RESTRICT REVOKE RIGHT RLIKE
%token <str> SCHEMA SCHEMAS SECOND_MICROSECOND SELECT SET SHOW SMALLINT
%token <str> STARTING TABLE TERMINATED THEN TINYBLOB TINYINT TINYTEXT TO
%token <str> TRAILING TRUE UNION UNIQUE UNLOCK UNSIGNED
%token <str> UPDATE USE USING UTC_DATE UTC_TIMESTAMP VALUES VARBINARY VARCHAR
%token <str> WHEN WHERE WRITE XOR YEAR_MONTH ZEROFILL
	
%%

StmtList:
	StmtList ';' Stmt
	{
		if $3 != nil {
			s := $3.(Statement)
			parser.result = append(parser.result, s)
		}
	}
| 	Stmt
	{
		if $1 != nil {
			s := $1.(Statement)
			parser.result = append(parser.result, s)
		}
  	}

Stmt:
	CreateDatabaseStmt
|	CreateTableStmt
|	ShowStmt
| 	/* EMPTY */
	{
		$$ = nil
	}

CreateDatabaseStmt:
	CREATE DATABASE Name
	{
		$$ = &CreateDatabase{DBName: $3}
  	}
|	CREATE DATABASE IF NOT EXISTS Name
	{
		$$ = &CreateDatabase{IfNotExists: true, DBName: $6}
	}

CreateTableStmt:
	CREATE TABLE TableName '(' TableElemList ')'
	{
		$$ = &CreateTable{Table: $3, IfNotExists: false, Defs: $5}
	}
|	CREATE TABLE IF NOT EXISTS TableName '(' TableElemList ')'
	{
		$$ = &CreateTable{Table: $6, IfNotExists: true, Defs: $8}
  	}

ShowStmt:
	SHOW DATABASES
	{
		$$ = &ShowDatabases{}
	}

TableName:
	Name
	{
		$$ = &TableName{Name: $1}	
	}
|	Name '.' Name
	{
		$$ = &TableName{Schema: $1, Name: $3}
	}

TableElemList:
	TableElem
	{
		$$ = ColumnTableDefs{$1}
	}
|	TableElemList ',' TableElem
	{
		$$ = append($1, $3)
	}

TableElem:
	Name TypeName ColumnOption
	{
		$$ = newColumnTableDef($1, $2, $3)
	}

ColumnOption:
	ColumnOption ColumnOptionItem
	{
		$$ = append($1, $2)
	}
| 	/* Empty */
	{
		$$ = nil
	}
	
ColumnOptionItem:
	NOT NULL
	{
		$$ = NotNullConstraint{}
	}
|	NULL
	{
		$$ = NullConstraint{}
	}
|	UNIQUE
	{
    	$$ = UniqueConstraint{}
	}
|	PRIMARY KEY
	{
		$$ = PrimaryKeyConstraint{}
	}

/******************************************Type Begin**********************************************/

TypeName:
	NumericType
	{
		$$ = $1
	}
|	StringType
	{
		$$ = $1
	}
	
NumericType:
	INT
	{
		$$ = &IntType{Name: "INT"}
	}
|	INTEGER
	{
		$$ = &IntType{Name: "INTEGER"}
	}

StringType:
	{
		$$ = &StringType{Name: "STRING"}
	}
/******************************************Type End************************************************/

Name:
	identifier
	{
		$$ = $1
	}
|	UnReservedKeyword
	{
		$$ = $1
	}

UnReservedKeyword:
ACTION | ASCII | AUTO_INCREMENT | AFTER | AT | AVG | BEGIN | BIT | BOOL | BOOLEAN | BTREE | CHARSET
| COLUMNS | COMMIT | COMPACT | COMPRESSED | CONSISTENT | DATA | DATE | DATETIME | DEALLOCATE | DO
| DYNAMIC| END | ENGINE | ENGINES | ESCAPE | EXECUTE | FIELDS | FIRST | FIXED | FORMAT | FULL |GLOBAL
| HASH | LESS | LOCAL | NAMES | OFFSET | PASSWORD | PREPARE | QUICK | REDUNDANT
| ROLLBACK | SESSION | SIGNED | SNAPSHOT | START | STATUS | TABLES | TEXT | THAN | TIME | TIMESTAMP
| TRANSACTION | TRUNCATE | UNKNOWN | VALUE | WARNINGS | YEAR | MODE  | WEEK  | ANY | SOME | USER | IDENTIFIED
| COLLATION | COMMENT | AVG_ROW_LENGTH | CONNECTION | CHECKSUM | COMPRESSION | KEY_BLOCK_SIZE | MAX_ROWS
| MIN_ROWS | NATIONAL | ROW | ROW_FORMAT | QUARTER | GRANTS | TRIGGERS | DELAY_KEY_WRITE | ISOLATION
| REPEATABLE | COMMITTED | UNCOMMITTED | ONLY | SERIALIZABLE | LEVEL | VARIABLES | SQL_CACHE | INDEXES | PROCESSLIST
| SQL_NO_CACHE | DISABLE  | ENABLE | REVERSE | SPACE | PRIVILEGES | NO | BINLOG | FUNCTION | VIEW | MODIFY | EVENTS | PARTITIONS
| TIMESTAMPDIFF | NONE | SUPER

ReservedKeyword:
ADD | ALL | ALTER | ANALYZE | AND | AS | ASC | BETWEEN | BIGINT
| BINARY | BLOB | BOTH | BY | CASCADE | CASE | CHANGE | CHARACTER | CHECK | COLLATE
| COLUMN | CONSTRAINT | CONVERT | CREATE | CROSS | CURRENT_DATE | CURRENT_TIME
| CURRENT_TIMESTAMP | CURRENT_USER | DATABASE | DATABASES | DAY_HOUR | DAY_MICROSECOND
| DAY_MINUTE | DAY_SECOND | DECIMAL | DEFAULT | DELETE | DESC | DESCRIBE
| DISTINCT | DIV | DOUBLE | DROP | DUAL | ELSE | ENCLOSED | ESCAPED
| EXISTS | EXPLAIN | FALSE | FLOAT | FOR | FORCE | FOREIGN | FROM
| FULLTEXT | GRANT | GROUP | HAVING | HOUR_MICROSECOND | HOUR_MINUTE
| HOUR_SECOND | IF | IGNORE | IN | INDEX | INFILE | INNER | INSERT | INT | INTO | INTEGER
| INTERVAL | IS | JOIN | KEY | KEYS | KILL | LEADING | LEFT | LIKE | LIMIT | LINES | LOAD
| LOCALTIME | LOCALTIMESTAMP | LOCK | LONGBLOB | LONGTEXT | MAXVALUE | MEDIUMBLOB | MEDIUMINT | MEDIUMTEXT
| MINUTE_MICROSECOND | MINUTE_SECOND | MOD | NOT | NO_WRITE_TO_BINLOG | NULL | NUMERIC
| ON | OPTION | OR | ORDER | OUTER | PARTITION | PRECISION | PRIMARY | PROCEDURE | RANGE | READ
| REAL | REFERENCES | REGEXP | RENAME | REPEAT | REPLACE | RESTRICT | REVOKE | RIGHT | RLIKE
| SCHEMA | SCHEMAS | SECOND_MICROSECOND | SELECT | SET | SHOW | SMALLINT
| STARTING | TABLE | TERMINATED | THEN | TINYBLOB | TINYINT | TINYTEXT | TO
| TRAILING | TRUE | UNION | UNIQUE | UNLOCK | UNSIGNED
| UPDATE | USE | USING | UTC_DATE | UTC_TIMESTAMP | VALUES | VARBINARY | VARCHAR
| WHEN | WHERE | WRITE | XOR | YEAR_MONTH | ZEROFILL

%%
