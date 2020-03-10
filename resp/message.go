package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/alxarch/red/resp/internal"
)

// Message is a reply for a redis command.
type Message struct {
	buffer string
	hints  []hint
}

// ReadFrom reads a reply from a redis stream.
func (msg *Message) ReadFrom(r *bufio.Reader) (Value, error) {
	p := parser{
		hints: msg.hints[:0],
	}

	p.reserve(1)
	if err := p.parse(r, &p.hints[0]); err != nil {
		return Value{}, err
	}
	*msg = Message{
		buffer: p.buffer.String(),
		hints:  p.hints,
	}
	return Value{msg: msg}, nil
}

// Parse parses a RESP value from a buffer
func (msg *Message) Parse(buf []byte) (Value, error) {
	r := bytes.NewReader(buf)
	b := bufio.NewReader(r)
	return msg.ReadFrom(b)
}

// ParseString parses a RESP value from a string
func (msg *Message) ParseString(str string) (Value, error) {
	r := strings.NewReader(str)
	b := bufio.NewReader(r)
	return msg.ReadFrom(b)
}

// Reset resets the reply buffer
func (msg *Message) Reset() {
	*msg = Message{
		hints: msg.hints[:0],
	}
}

// Value returns the root Value node
func (msg *Message) Value() Value {
	if len(msg.hints) > 0 {
		return Value{msg: msg}
	}
	return Value{}
}

// Any returns the reply RESP value
func (msg *Message) Any() Any {
	return msg.Value().Any()
}

// AppendRESP implements Appender interface
func (msg *Message) AppendRESP(buf []byte) []byte {
	return msg.Value().AppendRESP(buf)
}

func (msg *Message) str(h *hint) string {
	return h.str(msg.buffer)
}

func zeroSlize(v reflect.Value) {
	zero := reflect.Zero(v.Type().Elem())
	for i := 0; i < v.Len(); i++ {
		v.Index(i).Elem().Set(zero)
	}
}

func reuseSlice(v reflect.Value, typ reflect.Type, size int) reflect.Value {
	if v.IsNil() {
		return reflect.MakeSlice(typ, size, size)
	}
	if size <= v.Len() {
		zeroSlize(v.Slice(size, v.Len()))
		return v.Slice(0, size)
	}
	if size <= v.Cap() {
		return v.Slice(0, size)
	}
	return reflect.MakeSlice(typ, size, size)

}
func (msg *Message) deflectArray(target reflect.Value, h *hint) error {
	switch k := target.Kind(); k {
	case reflect.Ptr:
		switch v := target.Elem(); v.Kind() {
		case reflect.Array:
			if h.null {
				v.Set(reflect.Zero(v.Type()))
				return nil
			}
			return msg.deflectArray(v, h)
		case reflect.Slice:
			if h.null {
				v.Set(reflect.Zero(v.Type()))
				return nil
			}

			typ := target.Elem().Type()
			v.Set(reuseSlice(v, typ, int(h.size)))
			return msg.deflectArray(v, h)
		case reflect.Map:
			if h.null {
				v.Set(reflect.Zero(v.Type()))
				return nil
			}
			return msg.deflectArray(v, h)
		default:
			k := v.Kind()
			return fmt.Errorf("Invalid target %s %v", k, target.Interface())
		}
	case reflect.Slice, reflect.Array:
		if h.null {
			return ErrNull
		}
		if uint32(target.Len()) != h.size {
			return fmt.Errorf("Invalid target size %d", target.Len())
		}
		node := Value{index: h.offset, msg: msg}
		v := reflect.New(target.Type().Elem())
		for i := 0; i < target.Len(); i++ {
			if err := node.Decode(v.Interface()); err != nil {
				return fmt.Errorf("Invalid element %d: %s", i, err)
			}
			target.Index(i).Set(v.Elem())
			node.index++
		}
		return nil
	case reflect.Map:
		if h.size%2 != 0 {
			return fmt.Errorf("Invalid array size %d", h.size)
		}
		typ := target.Type()
		target.Set(reflect.MakeMap(typ))
		key := reflect.New(typ.Key())
		val := reflect.New(typ.Elem())
		node := Value{index: h.offset, msg: msg}
		for i := uint32(0); i < h.size; i += 2 {
			if err := node.Decode(key.Interface()); err != nil {
				return fmt.Errorf("Invalid key %d: %s", i, err)
			}
			node.index++
			if err := node.Decode(val.Interface()); err != nil {
				return fmt.Errorf("Invalid element %d: %s", i, err)
			}
			target.SetMapIndex(key.Elem(), val.Elem())
			node.index++
		}
		return nil
	default:
		return fmt.Errorf("Invalid target %v", target.Interface())
	}

}

func (msg *Message) decodeBulkStringSlice(a *[]string, h *hint) error {
	if h.null {
		*a = nil
		return nil
	}
	arr := internal.MakeSliceString(*a, h.size)
	var s BulkString
	node := Value{index: h.offset, msg: msg}
	for i := range arr {
		if err := s.UnmarshalRESP(node); err != nil {
			return fmt.Errorf("Invalid element %d: %s", i, err)
		}
		if s.Null() {
			return fmt.Errorf("Invalid element %d: %s", i, ErrNull)
		}
		arr[i] = s.String
		node.index++
	}
	*a = arr
	return nil
}

func (msg *Message) decodeBulkStringMap(h *hint) (map[string]string, error) {
	if h.null {
		return nil, nil
	}
	if h.size%2 != 0 {
		return nil, fmt.Errorf("Invalid array size %d", h.size)
	}
	m := make(map[string]string, h.size/2)
	var key, val BulkString
	node := Value{index: h.offset, msg: msg}
	for i := uint32(0); i < h.size; i += 2 {
		if err := key.UnmarshalRESP(node); err != nil || key.Null() {
			return nil, fmt.Errorf("Invalid pair key %s", err)
		}
		node.index++
		if err := val.UnmarshalRESP(node); err != nil || val.Null() {
			return nil, fmt.Errorf("Invalid pair value %s", err)
		}
		node.index++
		m[key.String] = val.String
	}
	return m, nil
}
