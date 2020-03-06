package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"

	"github.com/alxarch/red/resp/internal"
)

type Stream struct {
	r     *bufio.Reader
	reply Message
	err   error
}

func NewStream(r io.Reader) *Stream {
	return &Stream{
		r: bufio.NewReaderSize(r, defaultBufferSize),
	}
}
func NewStreamSize(r io.Reader, size int) *Stream {
	if size < minBufferSize {
		size = minBufferSize
	}
	return &Stream{
		r: bufio.NewReaderSize(r, size),
	}
}

// func (r *Decoder) SetMaxBulkStringSize(n int64) {
// 	r.maxBulkSize = n
// }

func (s *Stream) Reset(r *bufio.Reader) {
	s.r = r
	s.reply.Reset()
	s.err = nil
}

var (
	ErrNull = errors.New("Null")
)

func (s *Stream) CopyBulkString(w io.Writer) (n int64, err error) {
	if err = s.err; err != nil {
		return
	}
	t, err := s.r.ReadByte()
	if err != nil {
		s.err = err
		return
	}
	if typ := Type(t); typ != TypeBulkString {
		s.r.UnreadByte()
		return 0, fmt.Errorf("Invalid type %s", typ)
	}

	line, isPrefix, err := s.r.ReadLine()
	if err != nil {
		s.err = err
		return
	}
	if isPrefix {
		if line, err = internal.ReadLine(nil, line, s.r); err != nil {
			s.err = err
			return
		}
	}
	size, ok := internal.ParseInt(line)
	if !ok || size <= -1 {
		err = errInvalidSize
		s.err = err
		return
	}

	if size == -1 {
		return 0, ErrNull
	}
	n, isRead, err := internal.CopyN(s.r, w, size)
	if err != nil {
		if isRead {
			s.err = err
		} else {
			// err is a write error, discard the remaining bulk string
			s.discardN(size - n + int64(len(CRLF)))
		}
		return
	}
	if _, err = s.r.Discard(len(CRLF)); err != nil {
		s.err = err
	}

	return
}

func (s *Stream) Err() error {
	if s.err != nil {
		return s.err
	}
	return nil
}

func (s *Stream) discardN(n int64) error {
	if _, err := s.r.Discard(int(n)); err != nil {
		s.err = err
		return err
	}
	return nil
}

func (s *Stream) Skip() error {
	if s.err != nil {
		return s.err
	}
	if err := discardNext(s.r); err != nil {
		s.err = err
		return err
	}
	return nil

}
func (s *Stream) Next() (Value, error) {
	if s.err != nil {
		return Value{}, s.err
	}
	v, err := s.reply.ReadFrom(s.r)
	if err != nil {
		s.err = err
		return Value{}, err
	}
	return v, nil
}

// func (s *Stream) Decode(x interface{}) error {
// 	if x == nil {
// 		s.reply.Reset()
// 		if err := discardNext(s.r); err != nil {
// 			s.err = err
// 			return err
// 		}
// 		return nil
// 	}
// 	v, err := s.reply.ReadFrom(s.b)
// 	if err != nil {
// 		s.err = err
// 		return err
// 	}
// 	if err := v.Decode(x); err != nil {
// 		return &DecodeError{
// 			Reason: err,
// 			Source: v.Any(),
// 			Dest:   x,
// 		}
// 	}
// 	return nil
// }

// discardNext discards a value from a reader
func discardNext(r *bufio.Reader) (err error) {
	typ, line, err := readNext(r)
	if err != nil {
		return
	}
	switch typ {
	case TypeSimpleString, TypeError, TypeInteger:
		return
	case TypeBulkString:
		if n, ok := internal.ParseInt(line); ok {
			if n >= 0 {
				_, err = r.Discard(int(n + 2))
				return
			} else if n == -1 {
				return
			}
		}
		return errInvalidSize
	case TypeArray:
		if n, ok := internal.ParseInt(line); ok && n >= -1 {
			for ; n > 0; n-- {
				if err = discardNext(r); err != nil {
					return
				}
			}
			return nil
		}
		return errInvalidSize
	default:
		return errInvalidType
	}
}

func readNext(r *bufio.Reader) (typ Type, line []byte, err error) {
	line, isPrefix, err := r.ReadLine()
	if err != nil {
		return
	}
	if isPrefix {
		if line, err = internal.ReadLine(nil, line, r); err != nil {
			return
		}
	}
	if len(line) > 0 {
		typ, line = Type(line[0]), line[1:]
	}
	return
}
