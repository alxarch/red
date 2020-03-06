package resp

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/alxarch/red/resp/internal"
)

// Value is handle to a value in a RESP reply.
type Value struct {
	reply *Message
	index uint32
}

// Decode decodes a RESP value to x
func (v Value) Decode(x interface{}) error {
	switch dest := x.(type) {
	case *Any:
		if dest != nil {
			*dest = v.Any()
			return nil
		}
		x = nil
	case Unmarshaler:
		if dest != nil {
			return dest.UnmarshalRESP(v)
		}
		x = nil
	}
	if x == nil {
		return fmt.Errorf("Nil target")
	}
	h := v.hint()
	if h == nil {
		return fmt.Errorf("Invalid RESP value %v", v.Any())
	}
	switch h.typ {
	case TypeBulkString:
		s := BulkString{
			Valid:  !h.null,
			String: v.reply.str(h),
		}
		return s.Decode(x)
	case TypeSimpleString:
		s := v.reply.str(h)
		return SimpleString(s).Decode(x)
	case TypeInteger:
		return Integer(h.int()).Decode(x)
	case TypeError:
		s := v.reply.str(h)
		return Error(s).Decode(x)
	case TypeArray:
		switch dest := x.(type) {
		case *[]string:
			return v.reply.decodeBulkStringSlice(dest, h)
		case *map[string]string:
			m, err := v.reply.decodeBulkStringMap(h)
			if err != nil {
				return err
			}
			*dest = m
			return nil
		case *interface{}:
			if h.null {
				*dest = nil
				return nil
			}
			arr := make([]interface{}, h.size)
			v.index = h.offset
			for i := range arr {
				if err := v.Decode(&arr[i]); err != nil {
					return err
				}
				v.index++
			}
			*dest = arr
			return nil
		case *[]interface{}:
			if h.null {
				*dest = nil
				return nil
			}
			arr := internal.MakeSliceInterface(*dest, int(h.size))
			v.index = h.offset
			for i := range arr {
				if err := v.Decode(&arr[i]); err != nil {
					return err
				}
				v.index++
			}
			*dest = arr
			return nil
		case []interface{}:
			if h.null {
				return ErrNull
			}
			if uint32(len(dest)) != h.size {
				return fmt.Errorf("Invalid target size %d", h.size)
			}
			v.index = h.offset
			for i := range dest {
				y := reflect.ValueOf(dest[i])
				if err := v.Decode(y.Interface()); err != nil {
					return err
				}
				v.index++
			}
			return nil
		default:
			// Revert to reflection-based decode
			return v.reply.deflectArray(reflect.ValueOf(dest), h)
		}
	default:
		return fmt.Errorf("Invalid node %s", h.typ)
	}
}

func (v Value) hint() *hint {
	if v.reply != nil && v.index < uint32(len(v.reply.hints)) {
		return &v.reply.hints[v.index]
	}
	return nil
}

// Err returns an error if the value is a RESP error value.
func (v Value) Err() error {
	if h := v.hint(); h != nil && h.typ == TypeError {
		return errors.New(v.reply.str(h))
	}
	return nil
}

// Type returns the type of the value.
func (v Value) Type() Type {
	if h := v.hint(); h != nil {
		return h.typ
	}
	return 0
}

// Integer retuns the reply as int.
func (v Value) Integer() (int64, bool) {
	if h := v.hint(); h != nil && h.typ == TypeInteger {
		return h.int(), true
	}
	return 0, false
}

// SimpleString returns a RESP simple string value
func (v Value) SimpleString() (string, bool) {
	if h := v.hint(); h != nil && h.typ == TypeSimpleString {
		return v.reply.str(h), true
	}
	return "", false
}

// BulkString returns a RESP bulk string value
func (v Value) BulkString() (BulkString, bool) {
	if h := v.hint(); h != nil && h.typ == TypeBulkString {
		if h.null {
			return BulkString{}, true
		}
		return BulkString{
			String: v.reply.str(h),
			Valid:  true,
		}, true
	}
	return BulkString{}, false
}

// IsZero checks if a RESP value is the zero value
func (v Value) IsZero() bool {
	return v == Value{}
}

// NullArray checks if a value is a null array
func (v Value) NullArray() bool {
	h := v.hint()
	return h != nil && h.typ == TypeArray && h.null
}

// Null checks if a value is a null value.
func (v Value) Null() bool {
	if h := v.hint(); h != nil {
		switch h.typ {
		case TypeBulkString, TypeArray:
			return h.null
		}
	}
	return false
}

// NullBulkString checks if a value is a null bulk string
func (v Value) NullBulkString() bool {
	h := v.hint()
	return h != nil && h.typ == TypeBulkString && h.null
}

// Len returns the number of an array value's elements.
func (v Value) Len() int64 {
	if h := v.hint(); h != nil && h.typ == TypeArray {
		return int64(h.size)
	}
	return -1
}

// Iter returns an iterator over RESP values
func (v Value) Iter() Iter {
	if h := v.hint(); h != nil && h.typ == TypeArray && !h.null {
		return Iter{
			offset: h.offset,
			n:      h.size,
			reply:  v.reply,
		}
	}
	return Iter{}
}

// Any returns a Any for a RESP value
func (v Value) Any() Any {
	if h := v.hint(); h != nil {
		switch h.typ {
		case TypeBulkString:
			if h.null {
				return &BulkString{}
			}
			return &BulkString{
				String: v.reply.str(h),
				Valid:  true,
			}
		case TypeSimpleString:
			return SimpleString(v.reply.str(h))
		case TypeError:
			return Error(v.reply.str(h))
		case TypeInteger:
			return Integer(h.int())
		case TypeArray:
			if h.null {
				return Array(nil)
			}
			arr := Array(make([]Any, h.size))
			if h.size > 0 {
				v.index = h.offset
				for i := range arr {
					arr[i] = v.Any()
					v.index++
				}
			}
			return arr
		}
	}
	return nil
}

// AppendRESP implements Appender interface
func (v Value) AppendRESP(buf []byte) []byte {
	if h := v.hint(); h != nil {
		switch h.typ {
		case TypeBulkString:
			if h.null {
				s := BulkString{}
				return s.AppendRESP(buf)
			}
			s := BulkString{
				String: v.reply.str(h),
				Valid:  true,
			}
			return s.AppendRESP(buf)
		case TypeSimpleString:
			return SimpleString(v.reply.str(h)).AppendRESP(buf)
		case TypeError:
			return Error(v.reply.str(h)).AppendRESP(buf)
		case TypeInteger:
			return Integer(h.int()).AppendRESP(buf)
		case TypeArray:
			if h.null {
				return Array(nil).AppendRESP(buf)
			}
			buf = AppendArray(buf, int64(h.size))
			if h.size > 0 {
				end := h.offset + h.size
				for v.index = h.offset; v.index < end; v.index++ {
					buf = v.AppendRESP(buf)
				}
			}
			return buf
		}
	}
	return buf
}

func (v Value) Each(fn func(v string) error) error {
	offset, size, err := v.nonNullArray()
	if err != nil {
		return err
	}
	end := offset + size
	var val BulkString
	for v.index = offset; v.index < end; v.index++ {
		if err := val.UnmarshalRESP(v); err != nil {
			return err
		}
		if !val.Valid {
			return ErrNull
		}
		if err := fn(val.String); err != nil {
			return err
		}
	}
	return nil
}
func (v Value) nonNullArray() (offset, size uint32, err error) {
	if h := v.hint(); h != nil {
		switch h.typ {
		case TypeArray:
			if h.null {
				err = ErrNull
			} else {
				offset, size = h.offset, h.size
			}
			return
		case TypeError:
			err = Error(v.reply.str(h))
			return
		}
	}
	err = fmt.Errorf("Invalid RESP value %v", v.Any())
	return
}

// EachKV calls fn for each key/value pair in an array
func (v Value) EachKV(fn func(k, v string) error) error {
	offset, size, err := v.nonNullArray()
	if err != nil {
		return err
	}
	if size%2 != 0 {
		return fmt.Errorf("Invalid array size %d", size)
	}
	v.index = offset
	end := offset + size
	var key, val BulkString
	for v.index = offset; v.index < end; v.index++ {
		if err := key.UnmarshalRESP(v); err != nil {
			return err
		}
		if !key.Valid {
			return ErrNull
		}
		v.index++
		if err := val.UnmarshalRESP(v); err != nil {
			return err
		}
		if !val.Valid {
			return ErrNull
		}
		if err := fn(key.String, val.String); err != nil {
			return err
		}
	}
	return nil
}

// Iter iterates over an array of RESP values
type Iter struct {
	offset uint32
	n      uint32
	index  uint32
	reply  *Message
}

// // Len returns the total size of an iterator
// func (iter *ArrayIter) Len() int {
// 	return int(iter.n)
// }

// More checks if the iterator has more elements
func (iter *Iter) More() bool {
	if iter.index < iter.n {
		return true
	}
	return false
}

// Close ends the iterator and releases the reply buffer to avoid memory leaks
func (iter *Iter) Close() {
	iter.reply = nil
	iter.index = iter.n
}

// Value returns the current iterator value
func (iter *Iter) Value() Value {
	return Value{
		index: iter.offset + iter.index,
		reply: iter.reply,
	}
}

// Next advances the iteration
func (iter *Iter) Next() {
	iter.index++
}

// var errInvalidScanTarget = errors.New("Invalid scan target")
// var errInvalidScanValue = errors.New("Invalid scan value")

// // ScanError describes a failure of Scan
// type ScanError struct {
// 	Value  Value
// 	Target reflect.Type
// 	Reason error
// }

// // NewScanError creates a new ScanError
// func NewScanError(v Value, target interface{}, reason error) *ScanError {
// 	return &ScanError{
// 		Value:  v,
// 		Target: reflect.TypeOf(target),
// 		Reason: reason,
// 	}
// }

// func (e *ScanError) Error() string {
// 	typ := reflect.TypeOf(e.Target)
// 	if e.Reason != nil {
// 		return fmt.Sprintf("Scan failed %s -> %s: %s", e.Value.Type(), typ, e.Reason)
// 	}
// 	return fmt.Sprintf("Scan failed %s -> %s", e.Value.Type(), typ)
// }

// func (i Integer) scan(x interface{}) error {
// 	switch x := x.(type) {
// 	case *int64:
// 		*x = int64(i)
// 		return nil
// 	case *interface{}:
// 		*x = int64(i)
// 		return nil
// 	case *float64:
// 		*x = float64(i)
// 		return nil
// 	case *string:
// 		*x = strconv.FormatInt(int64(i), 10)
// 		return nil
// 	default:
// 		return errInvalidScanTarget
// 	}
// }

// func (a Array) scan(x interface{}) error {
// 	switch x := x.(type) {
// 	case *[]interface{}:
// 		if a == nil {
// 			*x = nil
// 			return nil
// 		}
// 		y := make([]interface{}, len(a))
// 		if err := a.scan(&y); err != nil {
// 			return err
// 		}
// 		*x = y
// 		return nil
// 	case *interface{}:
// 		if a == nil {
// 			*x = nil
// 			return nil
// 		}
// 		y := make([]interface{}, len(a))
// 		if err := a.scan(&y); err != nil {
// 			return err
// 		}
// 		*x = y
// 		return nil
// 	case *[]string:
// 		if a == nil {
// 			*x = nil
// 			return nil
// 		}
// 		values := make([]string, len(a))
// 		for i := range values {
// 			if err := Scan(a[i], &values[i]); err != nil {
// 				return err
// 			}
// 		}
// 		*x = values
// 		return nil
// 	case *map[string]string:
// 		if a == nil {
// 			*x = nil
// 			return nil
// 		}
// 		if len(a)%2 != 0 {
// 			return fmt.Errorf("Invalid array length %d", len(a))
// 		}
// 		m := make(map[string]string, len(a)/2)
// 		var key, val string
// 		for i, el := range a {
// 			if i%2 == 0 {
// 				if err := Scan(el, &key); err != nil {
// 					return err
// 				}
// 			} else {
// 				if err := Scan(el, &val); err != nil {
// 					return err
// 				}
// 				m[key] = val
// 			}
// 		}
// 		*x = m
// 		return nil
// 	case *map[string]float64:
// 		if a == nil {
// 			*x = nil
// 			return nil
// 		}
// 		if len(a)%2 != 0 {
// 			return fmt.Errorf("Invalid array length %d", len(a))
// 		}
// 		m := make(map[string]float64, len(a)/2)
// 		var key string
// 		var val float64
// 		for i, el := range a {
// 			if i%2 == 0 {
// 				if err := Scan(el, &key); err != nil {
// 					return err
// 				}
// 			} else {
// 				if err := Scan(el, &val); err != nil {
// 					return err
// 				}
// 				m[key] = val
// 			}
// 		}
// 		*x = m
// 		return nil
// 	case []interface{}:
// 		if len(x) == len(a) {
// 			for i := range a {
// 				if err := Scan(a[i], x[i]); err != nil {
// 					return fmt.Errorf("Invalid scan element %d: %s", i, err)
// 				}
// 			}
// 		}
// 		return nil
// 	default:
// 		return a.scanV(reflect.ValueOf(x))
// 	}
// }

// // Scan implements the Scanner inteface
// func (b *BulkStringMap) Scan(v Value) error {
// 	if arr, ok := v.(Array); ok {
// 		m, err := arr.BulkStringMap()
// 		if err != nil {
// 			return err
// 		}
// 		*b = m
// 	}
// 	return errInvalidScanValue
// }

// func (s *BulkString) scan(x interface{}) error {
// 	switch x := x.(type) {
// 	case *string:
// 		if s.Valid {
// 			*x = s.String
// 			return nil
// 		}
// 		return ErrNull
// 	case *interface{}:
// 		if s.Valid {
// 			*x = s.String
// 		} else {
// 			*x = nil
// 		}
// 		return nil
// 	case *int64:
// 		if s.Valid {
// 			n, err := strconv.ParseInt(s.String, 10, 64)
// 			if err != nil {
// 				return err
// 			}
// 			*x = n
// 		}
// 		return ErrNull
// 	case *uint64:
// 		if s.Valid {
// 			u, err := strconv.ParseUint(s.String, 10, 64)
// 			if err != nil {
// 				return err
// 			}
// 			*x = u
// 		}
// 		return ErrNull
// 	case *float64:
// 		if s.Valid {
// 			f, err := strconv.ParseFloat(s.String, 64)
// 			if err != nil {
// 				return err
// 			}
// 			*x = f
// 		}
// 		return ErrNull
// 	default:
// 		return errInvalidScanTarget
// 	}
// }

// func (e Error) scan(x interface{}) error {
// 	switch x := x.(type) {
// 	case *error:
// 		*x = e
// 		return nil
// 	case *interface{}:
// 		*x = (error)(e)
// 		return nil
// 	default:
// 		return e
// 	}
// }

// func (s SimpleString) scan(x interface{}) error {
// 	switch v := x.(type) {
// 	case *string:
// 		*v = string(s)
// 		return nil
// 	case *interface{}:
// 		*v = string(s)
// 		return nil
// 	default:
// 		return errInvalidScanTarget
// 	}
// }
