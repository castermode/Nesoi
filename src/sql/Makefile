ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

.PHONY: all

all: parser
	go build -o bin/NesoiSQL

goyacc: 
	go build -o bin/goyacc parser/goyacc/main.go

parser: goyacc
	bin/goyacc -o /dev/null -xegen nesoi_temp_sql parser/sql.y
	bin/goyacc -o parser/sql.go -xe nesoi_temp_sql parser/sql.y 2>&1 | egrep "(shift|reduce)/reduce" | awk '{print} END {if (NR > 0) {print "Find conflict in parser.y. Please check y.output for more information."; system("rm -f nesoi_temp_sql"); exit 1;}}'
	rm -f nesoi_temp_sql
	rm -f y.output
