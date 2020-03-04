package resp

import (
	"bufio"
	"math"
	"strings"

	"github.com/alxarch/red/resp/internal"
)

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
