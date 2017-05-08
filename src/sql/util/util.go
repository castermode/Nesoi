package util

import (
	"reflect"
	"unsafe"
)

func dumpUint16(n uint16) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
	}
}

// String converts slice to string without copy.
// Use at your own risk.
func ToString(b []byte) (s string) {
	if len(b) == 0 {
		return ""
	}
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

// Slice converts string to slice without copy.
// Use at your own risk.
func ToSlice(s string) (b []byte) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pbytes.Data = pstring.Data
	pbytes.Len = pstring.Len
	pbytes.Cap = pstring.Len
	return
}
