package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
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

type parser struct {
	buffer strings.Builder
	hints  []hint
}

type hint struct {
	typ    Type
	null   bool
	offset uint32
	size   uint32
}

func (h *hint) int() int64 {
	return int64(uint64(h.offset)<<32 | uint64(h.size))
}

func (h *hint) str(s string) string {
	if h.offset <= uint32(len(s)) {
		s = s[h.offset:]
		if h.size <= uint32(len(s)) {
			return s[:h.size]
		}
	}
	return ""
}

func (p *parser) copyBulkString(r *bufio.Reader, size int64) (offset uint32, err error) {
	offset = uint32(p.buffer.Len())
	if _, _, err = internal.CopyN(r, &p.buffer, size); err == nil {
		_, err = r.Discard(len(CRLF))
	}
	return
}

func (p *parser) copy(line []byte) (offset uint32) {
	offset = uint32(p.buffer.Len())
	p.buffer.Write(line)
	return
}

func (p *parser) parse(r *bufio.Reader, h *hint) (err error) {
	line, isPrefix, err := r.ReadLine()
	if err != nil {
		return
	}
	if isPrefix {
		if line, err = internal.ReadLine(nil, line, r); err != nil {
			return
		}
	}
	var typ Type
	if len(line) > 0 {
		typ, line = Type(line[0]), line[1:]
	}
	switch typ {
	case TypeSimpleString, TypeError:
		n := len(line)
		if n == 0 {
			*h = hint{typ: typ}
			return
		}
		*h = hint{
			typ:    typ,
			offset: p.copy(line),
			size:   uint32(len(line)),
		}
		return
	case TypeInteger:
		if n, ok := internal.ParseInt(line); ok {
			u := uint64(n)
			*h = hint{
				typ:    TypeInteger,
				offset: uint32(u >> 32),
				size:   uint32(u),
			}
			return
		}
		return errInvalidInteger
	case TypeBulkString:
		if n, ok := internal.ParseInt(line); ok {
			if 0 < n && n <= math.MaxUint32 {
				if buffered := r.Buffered(); int64(buffered) < n {
					// String is longer than buffered data
					var offset uint32
					offset, err = p.copyBulkString(r, n)
					*h = hint{
						typ:    TypeBulkString,
						offset: offset,
						size:   uint32(n),
					}
					return
				}
				// Read from buffered data
				peek, _ := r.Peek(int(n))
				// if len(peek) <= maxTinyStringSize {
				// 	*h = tinyString(TypeBulkString, peek)
				// } else {
				*h = hint{
					typ:    TypeBulkString,
					offset: p.copy(peek),
					size:   uint32(len(peek)),
				}
				// }
				_, err = r.Discard(len(peek) + len(CRLF))
				return
			}
			if n == -1 {
				*h = hint{
					typ:  TypeBulkString,
					null: true,
				}
				return
			}
			if n == 0 {
				*h = hint{
					typ: TypeBulkString,
				}
				_, err = r.Discard(len(CRLF))
				return
			}
		}
		return errInvalidSize
	case TypeArray:
		if n, ok := internal.ParseInt(line); ok {
			if 0 < n && n < math.MaxUint32 {

				// WARNING: order of statements is important
				offset := uint32(len(p.hints))
				// Assign to h BEFORE reserve() so the pointer is still valid
				*h = hint{
					typ:    TypeArray,
					offset: offset,
					size:   uint32(n),
				}
				// Order is important see above
				p.reserve(n)

				// WARNING END: order of statements is important

				for n > 0 {
					if err = p.parse(r, &p.hints[offset]); err != nil {
						return
					}
					offset++
					n--
				}
				return
			}
			if n == -1 {
				*h = hint{
					typ:  TypeArray,
					null: true,
				}
				return
			}
			if n == 0 {
				*h = hint{
					typ: TypeArray,
				}
				return
			}
		}
		return errInvalidSize
	default:
		return errInvalidType
	}
}

func (p *parser) reserve(n int64) (index uint32) {
	size := n + int64(len(p.hints))
	if 0 <= size && size < int64(cap(p.hints)) {
		index, p.hints = uint32(len(p.hints)), p.hints[:size]
		return
	}
	tmp := make([]hint, size, 2*int64(len(p.hints))+n)
	copy(tmp, p.hints)
	index, p.hints = uint32(len(p.hints)), tmp[:size]
	return
}
