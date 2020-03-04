package resp

import (
	"fmt"
	"strconv"
)

// Unmarshaler can unmarshal from a RESP value
type Unmarshaler interface {
	UnmarshalRESP(Value) error
}

// Appender appends a RESP value to a buffer
type Appender interface {
	AppendRESP(buf []byte) []byte
}

// BulkStringArray is RESP array containing non null bulk strings
type BulkStringArray []string

var _ Unmarshaler = (*BulkStringArray)(nil)

// UnmarshalRESP implements Unmarshaler interface
func (a *BulkStringArray) UnmarshalRESP(v Value) error {
	if h := v.hint(); h != nil {
		switch h.typ {
		case TypeArray:
			return v.reply.decodeBulkStringSlice((*[]string)(a), h)
		case TypeError:
			return Error(v.reply.str(h))
		default:
			return fmt.Errorf("Invalid RESP value %s", h.typ)
		}
	}
	return fmt.Errorf("Invalid value %v", v.Any())
}

// Get treats the array as a map of consecutive key/value pairs
func (a BulkStringArray) Get(key string) (string, bool) {
	var k, v string
	for len(a) >= 2 {
		k, v, a = a[0], a[1], a[2:]
		if k == key {
			return v, true
		}
	}
	return "", false
}

// EachKV calls fn for each key/value pair in an array
func (a BulkStringArray) EachKV(fn func(k, v string) error) (err error) {
	if len(a)%2 != 0 {
		return fmt.Errorf("Invalid array size %d", len(a))
	}
	var k, v string
	for len(a) >= 2 {
		k, v, a = a[0], a[1], a[2:]
		if err = fn(k, v); err != nil {
			return
		}
	}
	return
}

// AppendRESP implements Appender interface
func (a BulkStringArray) AppendRESP(buf []byte) []byte {
	if a == nil {
		return append(buf, byte(TypeArray), '-', '1', '\r', '\n')
	}
	buf = AppendArray(buf, int64(len(a)))
	bulk := BulkString{
		Valid: true,
	}
	for _, s := range a {
		bulk.String = s
		buf = bulk.AppendRESP(buf)
	}
	return buf

}

// func (a BulkStringArray) Fields() (fields Fields) {
// 	fields = make([]FieldArg, 0, len(a)/2)
// 	var k, v string
// 	for len(a) >= 2 {
// 		k, v, a = a[0], a[1], a[2:]
// 		fields = append(fields, Field(k, String(v)))
// 	}
// 	return
// }

// func (a BulkStringArray) KVs() (kvs KVs) {
// 	kvs = make([]KVArg, 0, len(a)/2)
// 	var k, v string
// 	for len(a) >= 2 {
// 		k, v, a = a[0], a[1], a[2:]
// 		kvs = append(kvs, KV(k, String(v)))
// 	}
// 	return
// }

// BulkStringMap is RESP map containing non null bulk strings
type BulkStringMap map[string]string

var _ Unmarshaler = (*BulkStringMap)(nil)

// UnmarshalRESP implements Unmarshaler interface
func (m *BulkStringMap) UnmarshalRESP(v Value) error {
	if h := v.hint(); h != nil {
		switch h.typ {
		case TypeArray:
			values, err := v.reply.decodeBulkStringMap(h)
			if err != nil {
				return err
			}
			*m = values
			return nil
		case TypeError:
			return Error(v.reply.str(h))
		default:
			return fmt.Errorf("Invalid RESP value %s", h.typ)
		}
	}
	return fmt.Errorf("Invalid value %v", v.Any())
}

// AppendRESP implements Appender interface
func (m BulkStringMap) AppendRESP(buf []byte) []byte {
	if m == nil {
		return BulkStringArray(nil).AppendRESP(buf)
	}
	buf = AppendArray(buf, int64(len(m)*2))
	bulk := BulkString{Valid: true}
	for k, v := range m {
		bulk.String = k
		buf = bulk.AppendRESP(buf)
		bulk.String = v
		buf = bulk.AppendRESP(buf)
	}
	return buf
}

// BulkStringBytes is a buffer of a bulk string
type BulkStringBytes []byte

// UnmarshalRESP implements Unmarshaler interface
func (raw *BulkStringBytes) UnmarshalRESP(v Value) error {
	if h := v.hint(); h != nil {
		switch h.typ {
		case TypeBulkString:
			if h.null {
				*raw = nil
			} else {
				*raw = append((*raw)[:0], v.reply.str(h)...)
			}
			return nil
		case TypeError:
			return Error(v.reply.str(h))
		default:
			return fmt.Errorf("Invalid RESP value %s", h.typ)
		}
	}
	return fmt.Errorf("Invalid value %v", v.Any())
}

// AppendRESP implements Appender interface
func (raw BulkStringBytes) AppendRESP(buf []byte) []byte {
	if raw == nil {
		return append(buf, byte(TypeBulkString), '-', '1', '\r', '\n')
	}
	buf = append(buf, byte(TypeBulkString))
	buf = strconv.AppendInt(buf, int64(len(raw)), 10)
	buf = append(buf, CRLF...)
	buf = append(buf, raw...)
	return append(buf, CRLF...)

}

type DecodeError struct {
	Reason error
	Source Any
	Dest   interface{}
}

func (d *DecodeError) Unwrap() error {
	return d.Reason
}
func (d *DecodeError) Error() string {
	return fmt.Sprintf("RESP decode %s -> %v failed: %s", d.Source.Type(), d.Dest, d.Reason)
}
