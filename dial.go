package red

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/alxarch/red/resp"
)

// Dial opens a connection to a redis server
func Dial(addr string, options *ConnOptions) (*Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return WrapConn(conn, options)
}

// ParseURL parses a URL to PoolOptions
func ParseURL(redisURL string) (*Pool, error) {
	pool := Pool{}
	u, err := url.Parse(redisURL)
	if err != nil {
		return nil, err
	}
	pool.Dial, err = dialURL(u)
	if err != nil {
		return nil, err
	}
	q := u.Query()

	if v, ok := q["clock-interval"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			pool.ClockInterval = d
		}
	}

	if v, ok := q["max-idle-time"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			pool.MaxIdleTime = d
		}
	}
	if v, ok := q["max-connections"]; ok && len(v) > 0 {
		if size, _ := strconv.Atoi(v[0]); size > 0 {
			pool.MaxConnections = size
		}
	}
	if v, ok := q["min-connections"]; ok && len(v) > 0 {
		if size, _ := strconv.Atoi(v[0]); size > 0 {
			pool.MinConnections = size
		}
	}

	return &pool, nil
}

// DialFunc dials a red.Conn
type DialFunc func() (*Conn, error)

func dialURL(u *url.URL) (DialFunc, error) {
	options := ConnOptions{}
	if u.Scheme != "redis" {
		return nil, fmt.Errorf(`Invalid URL scheme %q`, u.Scheme)
	}

	if path := strings.Trim(u.Path, "/"); path != "" {
		n, err := strconv.ParseInt(path, 10, 32)
		if err != nil || n < 0 {
			return nil, fmt.Errorf(`Invalid URL path %q`, u.Path)
		}
		options.DB = int(n)
	}
	host, port := u.Hostname(), u.Port()
	if port == "" {
		port = "6379"
	}
	addr := host + ":" + port

	q := u.Query()
	if v, ok := q["read-timeout"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			options.ReadTimeout = d
		}
	}
	if v, ok := q["write-timeout"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			options.WriteTimeout = d
		}
	}
	if v, ok := q["read-buffer-size"]; ok && len(v) > 0 {
		if size, _ := strconv.Atoi(v[0]); size > 0 {
			options.ReadBufferSize = size
		}
	}
	if v, ok := q["write-buffer-size"]; ok && len(v) > 0 {
		if size, _ := strconv.Atoi(v[0]); size > 0 {
			options.WriteBufferSize = size
		}
	}
	if v, ok := q["key-prefix"]; ok && len(v) > 0 {
		options.KeyPrefix = v[0]
	}
	return func() (*Conn, error) {
		return Dial(addr, &options)
	}, nil

}

const minBufferSize = 4096

// WrapConn wraps a net.Conn in a redis connection
func WrapConn(conn net.Conn, options *ConnOptions) (*Conn, error) {
	if options == nil {
		options = new(ConnOptions)
	}
	now := time.Now()
	sizeR := options.ReadBufferSize
	if sizeR < minBufferSize {
		sizeR = minBufferSize
	}
	sizeW := options.WriteBufferSize
	if sizeW < minBufferSize {
		sizeW = minBufferSize
	}
	w := timeoutWriter(conn, options.WriteTimeout)
	c := Conn{
		conn:    conn,
		options: *options,
		r:       *resp.NewStreamSize(conn, sizeR),
		w: PipelineWriter{
			KeyPrefix: options.KeyPrefix,
			dest:      bufio.NewWriterSize(w, sizeW),
		},
		createdAt:  now,
		lastUsedAt: now,
		scripts:    make(map[Arg]string),
	}

	if pass := options.Auth; pass != "" {
		if err := c.Auth(pass); err != nil {
			conn.Close()
			return nil, err
		}
	}

	if db := options.DB; DBIndexValid(db) {
		if err := c.injectCommand("SELECT", Int(db)); err != nil {
			conn.Close()
			return nil, err
		}
	}

	if options.WriteOnly {
		type closeReader interface {
			CloseRead() error
		}
		if cr, ok := conn.(closeReader); ok {
			if err := cr.CloseRead(); err != nil {
				conn.Close()
				return nil, err
			}
		}
		if err := c.WriteCommand("CLIENT", String("REPLY"), String("OFF")); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return &c, nil
}

type funcWriter func([]byte) (int, error)

var _ io.Writer = (funcWriter)(nil)

func (f funcWriter) Write(p []byte) (int, error) {
	return f(p)
}

func timeoutWriter(conn net.Conn, timeout time.Duration) io.Writer {
	if timeout > 0 {
		return funcWriter(func(p []byte) (int, error) {
			deadline := time.Now().Add(timeout)
			if err := conn.SetWriteDeadline(deadline); err != nil {
				return 0, err
			}
			return conn.Write(p)
		})
	}
	return conn
}
