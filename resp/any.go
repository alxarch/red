package resp

import (
	"bufio"
	"database/sql"
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/alxarch/red/resp/internal"
)

// Any is the interface of all RESP values
type Any interface {
	Type() Type
	Decode(x interface{}) error
	Appender
	resp() // restrict implementations of Value to resp package
}

// SimpleString is a RESP simple string value
type SimpleString string

var _ Any = SimpleString("")

// Type implements Value interface
func (SimpleString) Type() Type { return TypeSimpleString }
func (SimpleString) resp()      {}

var _ Unmarshaler = (*SimpleString)(nil)

func (s SimpleString) Check() error {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\r', '\n':
			return fmt.Errorf("Unsafe characer %c at position %d", s[i], i)
		}
	}
	return nil
}

// AppendRESP implements Appender interface
func (s SimpleString) AppendRESP(buf []byte) []byte {
	buf = append(buf, byte(TypeSimpleString))
	buf = append(buf, s...)
	return append(buf, CRLF...)
}

// UnmarshalRESP implements Unmarshaler interface
func (s *SimpleString) UnmarshalRESP(v Value) error {
	if h := v.hint(); h != nil {
		switch h.typ {
		case TypeSimpleString:
			*s = SimpleString(v.msg.str(h))
			return nil
		case TypeError:
			return Error(v.msg.str(h))
		default:
			return fmt.Errorf("Invalid RESP value %s", h.typ)
		}
	}
	return fmt.Errorf("Invalid RESP value %v", v.Any())
}

func reflectAssign(dst, src interface{}) bool {
	dv := reflect.Indirect(reflect.ValueOf(dst))
	sv := reflect.ValueOf(src)
	if sv.Type().AssignableTo(dv.Type()) {
		dv.Set(sv)
		return true
	}
	return false
}

// Decode implements Value interface
func (s SimpleString) Decode(x interface{}) error {
	switch dest := x.(type) {
	case *string:
		*dest = string(s)
		return nil
	case *interface{}:
		*dest = string(s)
		return nil
	default:
		if reflectAssign(x, s) {
			return nil
		}
		return fmt.Errorf("Invalid SimpleString target %v", x)
	}
}

// Error is a RESP error value
type Error string

var _ Any = Error("")

// Type implements Value interface
func (Error) Type() Type { return TypeError }
func (Error) resp()      {}

// AppendRESP implements Appender interface
func (e Error) AppendRESP(buf []byte) []byte {
	buf = append(buf, byte(TypeError))
	buf = append(buf, e...)
	return append(buf, CRLF...)
}

var _ Unmarshaler = (*Error)(nil)

// UnmarshalRESP implements Unmarshaler interface
func (e *Error) UnmarshalRESP(v Value) error {
	if h := v.hint(); h != nil {
		switch h.typ {
		case TypeError:
			*e = Error(v.msg.str(h))
			return nil
		default:
			return fmt.Errorf("Invalid RESP value %s", h.typ)
		}
	}
	return fmt.Errorf("Invalid RESP value %v", v.Any())
}

// Decode implements Value interface
func (e Error) Decode(x interface{}) error {
	switch x := x.(type) {
	case *error:
		*x = e
		return nil
	case *interface{}:
		*x = (error)(e)
		return nil
	default:
		if reflectAssign(x, (error)(e)) {
			return nil
		}
		return e
	}
}

func (e Error) Error() string { return string(e) }

// BulkString is a RESP bulk string value
type BulkString struct {
	String string
	Valid  bool
}

var _ Any = (*BulkString)(nil)

// Type implements Value interface
func (*BulkString) Type() Type { return TypeBulkString }
func (*BulkString) resp()      {}

func (s *BulkString) AppendRESP(buf []byte) []byte {
	if s.Valid {
		buf = append(buf, byte(TypeBulkString))
		buf = strconv.AppendInt(buf, int64(len(s.String)), 10)
		buf = append(buf, CRLF...)
		buf = append(buf, s.String...)
		return append(buf, CRLF...)
	}
	return append(buf, byte(TypeBulkString), '-', '1', '\r', '\n')
}

var _ Unmarshaler = (*BulkString)(nil)

// UnmarshalRESP implements Unmarshaler interface
func (s *BulkString) UnmarshalRESP(v Value) error {
	if h := v.hint(); h != nil {
		switch h.typ {
		case TypeBulkString:
			if h.null {
				*s = BulkString{}
			} else {
				*s = BulkString{
					String: v.msg.str(h),
					Valid:  true,
				}
			}
			return nil
		case TypeError:
			return Error(v.msg.str(h))
		default:
			return fmt.Errorf("Invalid RESP value %s", h.typ)
		}
	}
	return fmt.Errorf("Invalid RESP value %v", v.Any())
}

// Len returns the bulk string length
func (s *BulkString) Len() int {
	if s.Valid {
		return len(s.String)
	}
	return -1
}

// Null checks if the bulk string is null
func (s *BulkString) Null() bool {
	return !s.Valid
}

// Decode implements Value interface
func (s *BulkString) Decode(x interface{}) error {
	switch x := x.(type) {
	case *string:
		if s.Null() {
			return ErrNull
		}
		*x = s.String
		return nil
	case *float64:
		if s.Null() {
			*x = math.NaN()
			return nil
		}
		f, err := strconv.ParseFloat(s.String, 64)
		if err != nil {
			return err
		}
		*x = f
		return nil
	case *float32:
		if s.Null() {
			*x = float32(math.NaN())
			return nil
		}
		f, err := strconv.ParseFloat(s.String, 32)
		if err != nil {
			return err
		}
		*x = float32(f)
		return nil
	case *int64:
		if s.Valid {
			n, err := strconv.ParseInt(s.String, 10, 64)
			if err != nil {
				return err
			}
			*x = int64(n)
			return nil
		}
	case *int:
		if s.Valid {
			n, err := strconv.ParseUint(s.String, 10, bits.UintSize)
			if err != nil {
				return err
			}
			*x = int(n)
			return nil
		}
	case *int32:
		if s.Valid {
			n, err := strconv.ParseInt(s.String, 10, 32)
			if err != nil {
				return err
			}
			*x = int32(n)
			return nil
		}
	case *int16:
		if s.Valid {
			n, err := strconv.ParseInt(s.String, 10, 16)
			if err != nil {
				return err
			}
			*x = int16(n)
			return nil
		}
	case *int8:
		if s.Valid {
			n, err := strconv.ParseInt(s.String, 10, 8)
			if err != nil {
				return err
			}
			*x = int8(n)
			return nil
		}
	case *uint:
		if s.Valid {
			n, err := strconv.ParseUint(s.String, 10, bits.UintSize)
			if err != nil {
				return err
			}
			*x = uint(n)
			return nil
		}
	case *uint64:
		if s.Valid {
			n, err := strconv.ParseUint(s.String, 10, 64)
			if err != nil {
				return err
			}
			*x = uint64(n)
			return nil
		}
	case *uint32:
		if s.Valid {
			n, err := strconv.ParseUint(s.String, 10, 32)
			if err != nil {
				return err
			}
			*x = uint32(n)
			return nil
		}
	case *uint16:
		if s.Valid {
			n, err := strconv.ParseUint(s.String, 10, 16)
			if err != nil {
				return err
			}
			*x = uint16(n)
			return nil
		}
	case *uint8:
		if s.Valid {
			n, err := strconv.ParseUint(s.String, 10, 8)
			if err != nil {
				return err
			}
			*x = uint8(n)
			return nil
		}
	case *sql.NullString:
		*x = sql.NullString{
			String: s.String,
			Valid:  s.Valid,
		}
		return nil
	case *interface{}:
		if s.Valid {
			*x = s.String
		} else {
			*x = nil
		}
		return nil
	case *time.Time:
		if s.Valid {
			tm, err := time.Parse(time.RFC3339Nano, s.String)
			if err != nil {
				return err
			}
			*x = tm
		} else {
			*x = time.Time{}
		}
		return nil
	case sql.Scanner:
		if s.Valid {
			return x.Scan(s.String)
		}
		return x.Scan(nil)
	default:
		if s.Valid && reflectAssign(x, s.String) {
			return nil
		}
	}
	return fmt.Errorf("Invalid bulk string target %v", x)
}

// Integer is a RESP integer value
type Integer int64

var _ Any = Integer(0)

// Type implements Value interface
func (i Integer) Type() Type { return TypeInteger }
func (i Integer) resp()      {}

// AppendRESP implements Appender interface
func (i Integer) AppendRESP(buf []byte) []byte {
	buf = append(buf, byte(TypeInteger))
	buf = strconv.AppendInt(buf, int64(i), 10)
	return append(buf, CRLF...)
}

var _ Unmarshaler = (*Integer)(nil)

// UnmarshalRESP implements Unmarshaler interface
func (i *Integer) UnmarshalRESP(v Value) error {
	if h := v.hint(); h != nil {
		switch h.typ {
		case TypeInteger:
			*i = Integer(h.int())
			return nil
		case TypeError:
			return Error(v.msg.str(h))
		default:
			return fmt.Errorf("Invalid RESP value %s", h.typ)
		}
	}
	return fmt.Errorf("Invalid RESP value %v", v.Any())
}

// Decode implements Value interface
func (i Integer) Decode(x interface{}) error {
	switch x := x.(type) {
	case *int64:
		*x = int64(i)
		return nil
	case *interface{}:
		*x = int64(i)
		return nil
	case *float64:
		*x = float64(i)
		return nil
	default:
		if v := reflect.ValueOf(x); v.Kind() == reflect.Ptr {
			if el := v.Elem(); reflect.TypeOf(int64(i)).AssignableTo(el.Type()) {
				el.Set(reflect.ValueOf(i))
				return nil
			}
		}
		return fmt.Errorf("Invalid Integer target %v", x)
	}

}

// Array is a RESP array value
type Array []Any

var _ Any = (Array)(nil)

// Type implements Value interface
func (Array) Type() Type { return TypeArray }
func (Array) resp()      {}

// AppendRESP implements Appender interface
func (a Array) AppendRESP(buf []byte) []byte {
	if a.Null() {
		return ((BulkStringArray)(nil)).AppendRESP(buf)
	}
	buf = appendArray(buf, int64(len(a)))
	for _, v := range a {
		buf = v.AppendRESP(buf)
	}
	return buf
}

func (a Array) decodeArray(dst []interface{}) error {
	if len(dst) != len(a) {
		return fmt.Errorf("Invalid target size %d", len(dst))
	}
	for i := range a {
		if err := a[i].Decode(&dst[i]); err != nil {
			return err
		}
	}
	return nil
}

func (a Array) Decode(x interface{}) error {
	switch dest := x.(type) {
	case *[]string:
		values, err := a.BulkStringArray()
		if err != nil {
			return err
		}
		*dest = values
		return nil
	case *map[string]string:
		values, err := a.BulkStringMap()
		if err != nil {
			return err
		}
		*dest = values
		return nil
	case *[]Any:
		*dest = a
		return nil
	case *interface{}:
		if a == nil {
			*dest = nil
			return nil
		}
		arr := make([]interface{}, len(a))
		if err := a.decodeArray(arr); err != nil {
			return err
		}
		*dest = arr
		return nil
	case []interface{}:
		if a == nil {
			return ErrNull
		}
		return a.decodeArray(dest)
	case *[]interface{}:
		if a == nil {
			*dest = nil
			return nil
		}
		arr := internal.MakeSliceInterface(*dest, len(a))
		if err := a.decodeArray(arr); err != nil {
			return err
		}
		*dest = arr
		return nil
	default:
		return a.deflect(reflect.ValueOf(dest))
	}
}

func (a Array) deflect(dv reflect.Value) error {
	switch k := dv.Kind(); k {
	case reflect.Ptr:
		switch v := dv.Elem(); v.Kind() {
		case reflect.Slice:
			if a == nil {
				dv.Set(reflect.Zero(dv.Type()))
				return nil
			}
			typ := dv.Type().Elem()
			v.Set(reuseSlice(v, typ, len(a)))
			return a.deflect(v)
		case reflect.Map:
			return a.deflect(v)
		case reflect.Array:
			if a == nil {
				return ErrNull
			}
			return a.deflect(v)
		default:
			return fmt.Errorf("Invalid target %v", dv)
		}
	case reflect.Slice, reflect.Array:
		if len(a) != dv.Len() {
			return fmt.Errorf("Invalid target size %d", dv.Len())
		}
		// FIXME: handle interface slice properly
		typ := dv.Type().Elem()
		el := reflect.New(typ)
		for i := range a {
			if err := a[i].Decode(el.Interface()); err != nil {
				return err
			}
			dv.Index(i).Elem().Set(el.Elem())
		}
		return nil
	case reflect.Map:
		if len(a)%2 != 0 {
			return fmt.Errorf("Invalid array size %d", dv.Len())
		}
		typ := dv.Type()
		// Reset map to empty
		dv.Set(reflect.MakeMap(typ))
		key := reflect.New(typ.Key())
		val := reflect.New(typ.Elem())
		for i := 0; i < len(a); i += 2 {
			k, v := a[i], a[i+1]
			if err := k.Decode(key.Interface()); err != nil {
				return fmt.Errorf("Invalid key element %v: %s", k, err)
			}
			if err := v.Decode(val.Interface()); err != nil {
				return fmt.Errorf("Invalid value element %v: %s", dv, err)
			}
			dv.SetMapIndex(key.Elem(), val.Elem())
		}
		return nil
	default:
		return fmt.Errorf("Invalid target %v", dv)
	}
}

// Null checks if the array is null
func (a Array) Null() bool {
	return a == nil
}

// Len returns the array length
func (a Array) Len() int {
	if a.Null() {
		return -1
	}
	return len(a)
}

var _ Unmarshaler = (*Array)(nil)

// UnmarshalRESP implements Unmarshaler interface
func (a *Array) UnmarshalRESP(v Value) error {
	if h := v.hint(); h != nil && h.typ == TypeArray {
		if h.null {
			*a = nil
			return nil
		}
		arr := *a
		offset, size := h.offset, h.size
		if arr == nil {
			arr = make([]Any, size)
		} else if size <= uint32(len(arr)) {
			var drop []Any
			arr, drop = arr[:size], arr[size:]
			for i := range drop {
				drop[i] = nil
			}
		} else if size <= uint32(cap(arr)) {
			arr = arr[:size]
		} else {
			arr = make([]Any, size)
		}
		v.index = offset
		for i := range arr {
			arr[i] = v.Any()
			v.index++
		}
		*a = arr

		return nil
	}
	return fmt.Errorf("Invalid RESP value %v", v.Any())
}

// BulkStringArray converts a RESP array to a slice of strings
func (a Array) BulkStringArray() (BulkStringArray, error) {
	if a.Null() {
		return nil, nil
	}
	values := make([]string, len(a))
	for i, v := range a {
		b, ok := v.(*BulkString)
		if !ok {
			return nil, fmt.Errorf("Invalid element %d %v", i, v)
		}
		if !b.Valid {
			return nil, fmt.Errorf("Invalid element %d %e", i, ErrNull)
		}
		values[i] = b.String
	}
	return values, nil
}

// BulkStringMap converts a RESP array of consecutive non null bulk string pairs to a map
func (a Array) BulkStringMap() (BulkStringMap, error) {
	if a.Null() {
		return nil, nil
	}
	if len(a)%2 != 0 {
		return nil, fmt.Errorf("Invalid array size %d", len(a))
	}
	m := make(map[string]string, len(a)/2)
	for i := 0; i < len(a); i += 2 {
		key, val := a[i], a[i+1]
		k, ok := key.(*BulkString)
		if !ok || !k.Valid {
			return nil, fmt.Errorf("Invalid key %d %v", i, key)
		}
		v, ok := val.(*BulkString)
		if !ok || !v.Valid {
			return nil, fmt.Errorf("Invalid %q value %d %v", k.String, i+1, val)
		}
		m[k.String] = v.String
	}
	return m, nil
}

// ReadAny read a RESP Value from a buffered reader
func ReadAny(r *bufio.Reader) (Any, error) {
	typ, line, err := readNext(r)
	if err != nil {
		return nil, err
	}
	switch typ {
	case TypeBulkString:
		n, ok := internal.ParseInt(line)
		if !ok || n <= -1 {
			return nil, errInvalidSize
		}
		if n == -1 {
			return &BulkString{}, nil
		}
		b := BulkString{Valid: true}
		if n > 0 {
			s := strings.Builder{}
			// err is always a read error because strings.Builder cannot fail in writes
			if _, _, err := internal.CopyN(r, &s, n); err != nil {
				return nil, err
			}
			b.String = s.String()
		}
		if _, err := r.Discard(len(CRLF)); err != nil {
			return nil, err
		}
		return &b, nil
	case TypeInteger:
		if n, ok := internal.ParseInt(line); ok {
			return Integer(n), nil
		}
		return nil, errInvalidInteger
	case TypeSimpleString:
		return SimpleString(line), nil
	case TypeError:
		return Error(line), nil
	case TypeArray:
		n, ok := internal.ParseInt(line)
		if !ok || n <= -1 {
			return nil, errInvalidSize
		}
		if n == -1 {
			return Array(nil), nil
		}
		values := make([]Any, n)
		for i := range values {
			v, err := ReadAny(r)
			if err != nil {
				return nil, err
			}
			values[i] = v
		}
		return Array(values), nil
	default:
		r.UnreadByte()
		return nil, errInvalidType
	}
}
