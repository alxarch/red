package resp

import (
	"errors"
	"io"
	"strconv"
)

// Writer is a buffered writer for the RESP protocol.
//
// All writes are buffered up to a specified size.
// An explicit call to `Flush()` is required to write all data to the underlying `io.Writer`.
// The writer acts like `bufio.Writer` but avoids duplicate buffering during RESP protocol serialization.
type Writer struct {
	buffer []byte    // The buffer of pending writes
	err    error     // Sticky error, once an error has occured during Flush the writer always returns this error
	dest   io.Writer // The underlying writer
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
		dest:   dest,
		buffer: w.buffer[:0],
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

func appendBulkStringHeader(buf []byte, size int) []byte {
	buf = append(buf, byte(TypeBulkString))
	buf = strconv.AppendInt(buf, int64(size), 10)
	return append(buf, CRLF...)
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
	b := w.buffer
	if len(b)+maxBulkStringHeaderSize+size+len(CRLF) <= cap(b) {
		b = append(b, byte(TypeBulkString))
		b = strconv.AppendInt(b, int64(size), 10)
		b = append(b, CRLF...)
		b = append(b, prefix...)
		b = append(b, s...)
		w.buffer = append(b, CRLF...)
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

// WriteBulkString writes `s` as a RESP bulk string
func (w *Writer) WriteBulkString(s string) error {
	if len(s) > MaxBulkStringSize {
		return errors.New("Invalid bulk string size")
	}
	if w.err != nil {
		return w.err
	}
	b := w.buffer
	if len(b)+maxBulkStringHeaderSize+len(s)+len(CRLF) <= cap(b) {
		b = append(b, byte(TypeBulkString))
		b = strconv.AppendInt(b, int64(len(s)), 10)
		b = append(b, CRLF...)
		b = append(b, s...)
		w.buffer = append(b, CRLF...)
		return nil
	}
	return w.writeBulkStringBig("", s)
}

const nullBulkString = "$-1\r\n"

// WriteBulkStringNull writes a RESP null bulk string
func (w *Writer) WriteBulkStringNull() error {
	if w.err != nil {
		return w.err
	}
	if len(w.buffer)+len(nullBulkString) > cap(w.buffer) {
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
	b := w.buffer
	if maxSize := len(b) + maxBulkStringHeaderSize + len(s) + len(CRLF); maxSize <= cap(b) {
		b = appendBulkStringHeader(b, len(s))
		b = append(b, s...)
		w.buffer = append(b, CRLF...)
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
	if n > cap(w.buffer) {
		return io.ErrShortBuffer
	}
	if len(w.buffer)+n > cap(w.buffer) {
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
	if len(w.buffer)+maxIntEncodedSize > cap(w.buffer) {
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
