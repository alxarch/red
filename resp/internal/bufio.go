package internal

import (
	"bufio"
	"io"
)

func CopyN(r *bufio.Reader, w io.Writer, size int64) (n int64, isRead bool, err error) {
	var (
		maxReadSize = int64(r.Size())
		readSize    int64
		buf         []byte
		nn          int
	)
	// Pre-allocate size for copying if possible
	type grower interface {
		Grow(int)
	}
	if g, ok := w.(grower); ok {
		g.Grow(int(size))
	}
	// Loop condition ensures Peek cannot return ErrNegativeCount
	for size > 0 {
		// Ensure Peek(readSize) cannot return ErrBufferFull
		readSize = maxReadSize
		if size < readSize {
			readSize = size
		}
		buf, err = r.Peek(int(readSize))
		if err != nil {
			// err is an error of the underlying reader
			isRead = true
			return
		}
		if _, err = w.Write(buf); err != nil {
			return
		}
		size -= int64(len(buf))
		// Discard cannot fail since readSize < maxReadSize
		nn, err = r.Discard(len(buf))
		n += int64(nn)
		if err != nil {
			return
		}

	}
	return
}

func ReadLine(dst []byte, prefix []byte, r *bufio.Reader) ([]byte, error) {
	dst = append(dst, prefix...)
	for {
		prefix, isPrefix, err := r.ReadLine()
		if err != nil {
			return dst, err
		}
		dst = append(dst, prefix...)
		if !isPrefix {
			return dst, nil
		}
	}
}

func ParseInt(s []byte) (n int64, ok bool) {
	if len(s) == 0 {
		return
	}
	var c byte
	var negative bool
	if len(s) > 0 {
		c, s = s[0], s[1:]
		if ok = c == '0'; ok {
			return
		}
		if c == '-' {
			negative = true
			if len(s) > 0 {
				c, s = s[0], s[1:]
			} else {
				return
			}
		}
		if '0' <= c && c <= '9' {
			n = int64(c - '0')
		} else {
			return
		}
	}
	for len(s) > 0 {
		c, s = s[0], s[1:]
		if '0' <= c && c <= '9' {
			d := n*10 + int64(c-'0')
			if d < n {
				// Overflow
				return
			}
			n = d
		} else {
			return
		}
	}
	if negative {
		return -n, true
	}
	return n, true
}
