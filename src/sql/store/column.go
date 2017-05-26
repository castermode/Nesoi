package store

import (
	"github.com/castermode/Nesoi/src/sql/mysql"
	"github.com/castermode/Nesoi/src/sql/util"
)

type ColumnInfo struct {
	Schema             string
	Table              string
	OrgTable           string
	Name               string
	OrgName            string
	ColumnLength       uint32
	Charset            uint16
	Flag               uint16
	Decimal            uint8
	Type               uint8
	DefaultValueLength uint64
	DefaultValue       []byte
}

// Dump dumps ColumnInfo to bytes.
func (column *ColumnInfo) Dump() []byte {
	l := len(column.Schema) + len(column.Table) + len(column.OrgTable) + len(column.Name) + len(column.OrgName) + len(column.DefaultValue) + 48
	data := make([]byte, 0, l)

	data = append(data, util.DumpLengthEncodedString([]byte("def"))...)

	data = append(data, util.DumpLengthEncodedString([]byte(column.Schema))...)

	data = append(data, util.DumpLengthEncodedString([]byte(column.Table))...)
	data = append(data, util.DumpLengthEncodedString([]byte(column.OrgTable))...)

	data = append(data, util.DumpLengthEncodedString([]byte(column.Name))...)
	data = append(data, util.DumpLengthEncodedString([]byte(column.OrgName))...)

	data = append(data, 0x0c)

	data = append(data, util.DumpUint16(uint16(mysql.CharsetIDs["utf8"]))...)
	data = append(data, util.DumpUint32(0)...)
	data = append(data, column.Type)
	data = append(data, util.DumpUint16(column.Flag)...)
	data = append(data, 0)
	data = append(data, 0, 0)

	if column.DefaultValue != nil {
		data = append(data, util.DumpUint64(uint64(len(column.DefaultValue)))...)
		data = append(data, []byte(column.DefaultValue)...)
	}

	return data
}
