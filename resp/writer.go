package resp

import (
	"errors"
	"io"
	"strconv"
)

// Writer is a buffered writer for the RESP protocol.
//
// It provides helper methods to write RESP messages with minimum copying and allocations.
// All writes are buffered up to a specified size.
// An explicit call to `Flush()` is required to write all data to the underlying `io.Writer`.
// The writer acts like `bufio.Writer` but avoids duplicate buffering during RESP protocol serialization.
type Writer struct {
	dest    io.Writer // The underlying writer
	buffer  []byte    // The buffer of pending writes
	err     error     // Sticky error, once an error has occured during Flush the writer always returns this error
	scratch []byte    // Reusable buffer used for numeric conversions on args
}

// MaxBulkStringSize is the maximum bulk string size specifed in the RESP Protocol
const MaxBulkStringSize = 512 * 1024 * 1024 // 512 mb

const (
	defaultBufferSize       = 4096
	minBufferSize           = 512
	maxBulkStringHeaderSize = len("$") + len("536870912") + len(CRLF)
	maxIntEncodedSize       = len(":") + len("-9223372036854775808") + len(CRLF)
)

// NewWriter creates a new writer using the default buffer size (4096 bytes)
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		dest:   w,
		buffer: make([]byte, 0, defaultBufferSize),
	}
}

// NewWriterSize creates a new writer using the specified buffer size.
// A minimum buffer size of 512 bytes is enforced to allow enough space
// for RESP serialization without exceeding the buffer's capacity.
func NewWriterSize(w io.Writer, size int) *Writer {
	if size < minBufferSize {
		size = minBufferSize
	}
	return &Writer{
		dest:   w,
		buffer: make([]byte, 0, size),
	}
}

// Reset resets the underlying `io.Writer`.
// This discards any buffered data and retains the buffer's capacity
func (w *Writer) Reset(dest io.Writer) {
	if cap(w.buffer) < minBufferSize {
		w.buffer = make([]byte, defaultBufferSize)
	}
	*w = Writer{
		dest:    dest,
		buffer:  w.buffer[:0],
		scratch: w.scratch[:0],
	}
}

// Buffered returns the size of buffered data in bytes
func (w *Writer) Buffered() int {
	return len(w.buffer)
}

// Err returns an error of the writer
func (w *Writer) Err() error {
	return w.err
}

// Size returns the size of the buffer in bytes
func (w *Writer) Size() int {
	return cap(w.buffer)
}

// Available returns the size available before data is flushed to the underlying `io.Writer`
func (w *Writer) Available() int {
	return cap(w.buffer) - len(w.buffer)
}

// WriteBulkString writes `s` as a RESP bulk string
func (w *Writer) WriteBulkString(s string) error {
	if len(s) > MaxBulkStringSize {
		return errors.New("Invalid bulk string size")
	}
	if w.err != nil {
		return w.err
	}
	if maxBulkStringHeaderSize+len(s)+len(CRLF) <= w.Available() {
		w.buffer = appendBulkString(w.buffer, s)
		return nil
	}
	return w.writeBulkStringBig("", s)
}

// WriteBulkStringPrefix writes `s` prefixed by `prefix` as a RESP bulk string
func (w *Writer) WriteBulkStringPrefix(prefix, s string) error {
	size := len(prefix) + len(s)
	if size > MaxBulkStringSize {
		return errors.New("Invalid bulk string size")
	}
	if w.err != nil {
		return w.err
	}
	if maxBulkStringHeaderSize+size+len(CRLF) <= w.Available() {
		w.buffer = appendBulkStringHeader(w.buffer, size)
		w.buffer = append(w.buffer, prefix...)
		w.buffer = append(w.buffer, s...)
		w.buffer = append(w.buffer, CRLF...)
		return nil
	}
	return w.writeBulkStringBig(prefix, s)
}

func (w *Writer) writeBulkStringBig(prefix, s string) error {
	if err := w.Flush(); err != nil {
		return err
	}
	w.buffer = appendBulkStringHeader(w.buffer, len(s)+len(prefix))
	w.buffer = append(w.buffer, prefix...)
	w.buffer, s = fillString(w.buffer, s)
	for len(s) > 0 {
		if err := w.Flush(); err != nil {
			return err
		}
		w.buffer, s = fillString(w.buffer, s)
	}
	if len(CRLF) <= w.Available() {
		w.buffer = append(w.buffer, CRLF...)
		return nil
	}
	if err := w.Flush(); err != nil {
		return err
	}
	w.buffer = append(w.buffer, CRLF...)
	return nil
}

const nullBulkString = "$-1\r\n"

// WriteBulkStringFloat writes `f` as a RESP bulk string
func (w *Writer) WriteBulkStringFloat(f float64) error {
	w.scratch = strconv.AppendFloat(w.scratch[:0], f, 'f', -1, 64)
	return w.WriteBulkStringBytes(w.scratch[:])
}

// WriteBulkStringInt writes `n` as a RESP bulk string
func (w *Writer) WriteBulkStringInt(n int64) error {
	w.scratch = strconv.AppendInt(w.scratch[:0], n, 10)
	return w.WriteBulkStringBytes(w.scratch[:])
}

// WriteBulkStringUint writes `n` as a RESP bulk string
func (w *Writer) WriteBulkStringUint(n uint64) error {
	w.scratch = strconv.AppendUint(w.scratch[:0], n, 10)
	return w.WriteBulkStringBytes(w.scratch[:])
}

// WriteBulkStringNull writes a null RESP bulk string
func (w *Writer) WriteBulkStringNull() error {
	if w.err != nil {
		return w.err
	}
	if len(nullBulkString) > w.Available() {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	w.buffer = append(w.buffer, nullBulkString...)
	return nil
}

// WriteBulkStringBytes writes `s` as a RESP bulk string
func (w *Writer) WriteBulkStringBytes(s []byte) error {
	if s == nil {
		return w.WriteBulkStringNull()
	}

	if len(s) > MaxBulkStringSize {
		return errors.New("Invalid bulk string size")
	}
	if w.err != nil {
		return w.err
	}
	if maxBulkStringHeaderSize+len(s)+len(CRLF) <= w.Available() {
		w.buffer = appendBulkStringHeader(w.buffer, len(s))
		w.buffer = append(w.buffer, s...)
		w.buffer = append(w.buffer, CRLF...)
		return nil
	}
	// Size of write > available bytes (Slow path)
	if err := w.Flush(); err != nil {
		return err
	}
	w.buffer = appendBulkStringHeader(w.buffer, len(s))
	w.buffer, s = fillBytes(w.buffer, s)
	for len(s) > 0 {
		if err := w.Flush(); err != nil {
			return err
		}
		w.buffer, s = fillBytes(w.buffer, s)
	}
	if len(w.buffer)+len(CRLF) <= cap(w.buffer) {
		w.buffer = append(w.buffer, CRLF...)
		return nil
	}
	if err := w.Flush(); err != nil {
		return err
	}
	w.buffer = append(w.buffer, CRLF...)
	return nil
}

func fillString(b []byte, s string) ([]byte, string) {
	out := b[:cap(b)]
	n := copy(out[len(b):], s)
	return out[:len(b)+n], s[n:]
}
func fillBytes(b []byte, s []byte) ([]byte, []byte) {
	out := b[:cap(b)]
	n := copy(out[len(b):], s)
	return out[:len(b)+n], s[n:]
}

// WriteError writes `s` as a RESP error string
func (w *Writer) WriteError(s string) error {
	if err := SimpleString(s).Check(); err != nil {
		return err
	}
	if w.err != nil {
		return w.err
	}
	return w.writeSafeString(byte(TypeError), s)
}

// WriteSimpleString writes `s` as a RESP simple string
func (w *Writer) WriteSimpleString(s string) error {
	if err := SimpleString(s).Check(); err != nil {
		return err
	}
	if w.err != nil {
		return w.err
	}
	return w.writeSafeString(byte(TypeSimpleString), s)
}

func (w *Writer) writeSafeString(typ byte, s string) error {
	n := len("+") + len(s) + len(CRLF)
	if w.Available() < n {
		if cap(w.buffer) < n {
			return io.ErrShortBuffer
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}
	w.buffer = append(w.buffer, typ)
	w.buffer = append(w.buffer, s+CRLF...)
	return nil
}

// WriteInteger writes `i` as a RESP integer
func (w *Writer) WriteInteger(i int64) error {
	if w.err != nil {
		return w.err
	}
	return w.writeInteger(TypeInteger, i)
}

// WriteArray writes a RESP array with size `i`
func (w *Writer) WriteArray(i int64) error {
	if w.err != nil {
		return w.err
	}
	return w.writeInteger(TypeArray, i)
}

func (w *Writer) writeInteger(typ Type, i int64) error {
	if w.Available() < maxIntEncodedSize {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	w.buffer = append(w.buffer, byte(typ))
	w.buffer = strconv.AppendInt(w.buffer, int64(i), 10)
	w.buffer = append(w.buffer, CRLF...)
	return nil
}

// Flush writes all buffered data to the underlying `io.Writer` and empties the buffer.
func (w *Writer) Flush() error {
	if w.err != nil {
		return w.err
	}
	for len(w.buffer) > 0 {
		n, err := w.dest.Write(w.buffer)
		w.buffer = shiftL(w.buffer, n)
		if err != nil {
			w.err = err
			return err
		}
	}

	return nil
}

func shiftL(b []byte, n int) []byte {
	if 0 <= n && n < len(b) {
		return b[:copy(b, b[n:])]
	}
	return b[:0]
}

func appendBulkStringHeader(buf []byte, size int) []byte {
	buf = append(buf, byte(TypeBulkString))
	buf = strconv.AppendInt(buf, int64(size), 10)
	return append(buf, CRLF...)
}

func appendBulkString(buf []byte, s string) []byte {
	buf = append(buf, byte(TypeBulkString))
	buf = strconv.AppendInt(buf, int64(len(s)), 10)
	buf = append(buf, CRLF...)
	buf = append(buf, s...)
	return append(buf, CRLF...)
}

func appendArray(buf []byte, n int64) []byte {
	buf = append(buf, byte(TypeArray))
	buf = strconv.AppendInt(buf, n, 10)
	return append(buf, CRLF...)
}
