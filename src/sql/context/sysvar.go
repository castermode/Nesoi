package context

import (
	"strings"
)

type SysVar struct {
	Name  string
	Value string
}

var SysVars map[string]*SysVar

func init() {
	SysVars = make(map[string]*SysVar)
	for _, v := range defaultVars {
		SysVars[v.Name] = v
	}
}

func GetSysVar(name string) *SysVar {
	name = strings.ToLower(name)
	return SysVars[name]
}

var defaultVars = []*SysVar{
	{"version_comment", "MySQL Community Server (GPL)"},
}
