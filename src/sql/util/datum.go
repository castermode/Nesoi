package util

import (
	"errors"
	"strconv"
)

const (
	KindNull   byte = 0
	KindInt64  byte = 1
	KindString byte = 2
)

type Datum struct {
	k byte
	i int64
	b []byte
}

func (d *Datum) SetK(t byte) {
	d.k = t
}

func (d *Datum) GetK() byte {
	return d.k
}

func (d *Datum) SetI(v int64) {
	d.i = v
}

func (d *Datum) GetI() int64 {
	return d.i
}

func (d *Datum) SetB(v []byte) {
	d.b = v
}

func (d *Datum) GetB() []byte {
	return d.b
}

func (d *Datum) Equal(c *Datum) bool {
	if d.k == KindNull && c.k == KindNull {
		return true
	}
	
	if d.k == KindInt64 && c.k == KindInt64 {
		if d.i == c.i {
			return true
		}
	}
	
	if d.k == KindString && c.k == KindString {
		if ToString(d.b) == ToString(c.b) {
			return true
		}
	}
	
	return false
}

func (d *Datum) IsNull() bool {
	if d.k == KindNull {
		return true
	}

	return false
}

func DumpValueToText(v *Datum) ([]byte, error) {
	switch v.k {
	case KindInt64:
		return strconv.AppendInt(nil, v.i, 10), nil
	case KindString:
		return v.b, nil
	default:
		return nil, errors.New("invalid type!")
	}
}
