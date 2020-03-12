package red

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/alxarch/red/internal/pipeline"
	"github.com/alxarch/red/resp"
)

// Conn is a redis client connection
type Conn struct {
	noCopy noCopy //nolint:unused,structcheck

	conn net.Conn
	// err      error
	w       PipelineWriter
	r       resp.Stream
	options ConnOptions

	managed bool
	state   pipeline.State
	scripts map[Arg]string // Loaded scripts

	// Pool fields
	createdAt  time.Time
	lastUsedAt time.Time
	pool       *Pool
}

// WriteCommand writes a redis command to the pipeline buffer updating the state
func (conn *Conn) WriteCommand(name string, args ...Arg) error {
	if conn.managed {
		return errConnManaged
	}
	if !conn.options.Debug {
		name, args = conn.rewriteCommand(name, args)
	}

	switch name {
	case "CLIENT":
		return fmt.Errorf("CLIENT commands not allowed")
	case "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE":
		return fmt.Errorf("Subscribe commands not allowed")
	}

	if err := conn.w.WriteCommand(name, args...); err != nil {
		_ = conn.Close()
		return err
	}
	conn.updatePipeline(name, args...)
	return nil
}

// DoCommand executes a redis command
func (conn *Conn) DoCommand(dest interface{}, name string, args ...Arg) error {
	if err := conn.Err(); err != nil {
		return err
	}
	if n := conn.state.CountReplies(); n > 0 {
		return fmt.Errorf("Pending %d replies", n)
	}
	if err := conn.WriteCommand(name, args...); err != nil {
		return err
	}
	if err := conn.Scan(dest); err != nil {
		return err
	}
	return nil
}

// func (conn *Conn) clientScanValue(skip bool) (pipeline.Entry, resp.Value, error) {
// 	if err := conn.flush(); err != nil {
// 		return pipeline.Entry{}, resp.Value{}, err
// 	}
// 	for {
// 		entry, ok := conn.state.Pop()
// 		if !ok {
// 			return pipeline.Entry{}, resp.Value{}, ErrNoReplies
// 		}
// 		if entry.Skip() {
// 			continue
// 		}
// 		var v resp.Value
// 		var err error
// 		if skip {
// 			err = conn.discardValue(entry)
// 		} else {
// 			v, err = conn.readValue(entry)
// 		}
// 		if err != nil {
// 			conn.closeWithError(err)
// 		}
// 		return entry, v, err
// 	}
// }

type replyExec struct {
	dest []interface{}
	err  error
}

func (r *replyExec) UnmarshalRESP(v resp.Value) error {
	switch {
	case v.NullArray():
		return fmt.Errorf("MULTI/EXEC transaction WATCH failed")
	case v.Type() == resp.TypeArray:
		iter := v.Iter()
		for i, x := range r.dest {
			if !iter.More() {
				return fmt.Errorf("Invalid multi size %d > %d", len(r.dest), v.Len())
			}
			if x != nil {
				if err := iter.Value().Decode(x); err != nil {
					r.dest[i] = err
				} else {
					r.dest[i] = nil
				}
			}
			iter.Next()
		}
		if iter.More() {
			return fmt.Errorf("Invalid target size %d < %d", len(r.dest), v.Len())
		}
		for _, x := range r.dest {
			if err, ok := x.(error); ok && err != nil {
				return err
			}
		}
		return nil
	case v.Err() != nil:
		return fmt.Errorf("MULTI/EXEC transaction aborted %s", v.Err())
	default:
		return fmt.Errorf("Invalid exec reply %v", v.Any())
	}
}

// ScanMulti scans the results of a MULTI/EXEC transaction
func (conn *Conn) ScanMulti(dest ...interface{}) error {
	if err := conn.Err(); err != nil {
		return err
	}
	if conn.managed {
		return errConnManaged
	}
	if err := conn.flush(); err != nil {
		return err
	}
	if conn.options.WriteOnly {
		return errConnWriteOnly
	}
	entry := conn.state.Peek()
	if !entry.Multi() {
		return fmt.Errorf("Non multi entry ahead %v", entry)
	}
	for {
		entry, ok := conn.state.Pop()
		if !ok {
			return ErrNoReplies
		}
		switch {
		case entry.Skip():
			continue
		case entry.Multi():
			var isOK AssertOK
			if err := conn.scanValue(&isOK, entry); err != nil {
				return fmt.Errorf("MULTI failed: %s", err)
			}
		case entry.Discard():
			return fmt.Errorf("MULTI/EXEC transaction discarded")
		case entry.Exec():
			exec := replyExec{
				dest: dest,
			}
			return conn.scanValue(&exec, entry)
		case entry.Queued():
			if err := conn.scanValue(nil, entry); err != nil {
				return err
			}
		default:
			return fmt.Errorf("Invalid MULTI/EXEC entry %v", entry)
		}
	}
}

// Scan decodes a reply to dest
// If so keep the deadline(?) so an appropriate timeout is set next time
// XXX (see below) Other solution: return an error on write if a blocking command skips the reply
// Edit: This is nuts, the timeout is on the server. Since we are writing
// the command in a pipeline we cannot know when the server will set the timeout
// It's plain and simple to only allow blocking commands on a `clean` connection
// and handle the timeouts appropriately there.
// XXX (see below) OTOH it's plausible that a blocking command could be the last step in
// a MULTI/EXEC transaction
// Edit: From testing via redis-cli it seems that the server does *NOT*
// respect the timeout when a blocking command is inside MULTI/EXEC block
// It executes the pop immediately if a value is available or return a nil response
// Edit: This is also the case for client reply skip followed by BLPOP...
// Edit: CLIENT REPLY SKIP and blocking commands don't mix well
// If an error occurs because the command was wrong nothing is returned
// Otherwise the SKIP is ignored and does *NOT* carry over to the next command
// Because of all these intricacies the best thing to do is to
// a) disallow client reply subcommand entirely and only use it internally - DONE
// b) ignore the timeout of blocking commands when queued - DONE
// c) maybe change the way timeout is stored in an `cmd.Entry`
//    so that it stores the deadline when it is written to the pipeline
//    This is not urgent as setting a lax deadline is not so harmful if
//    the connection is healthy
func (conn *Conn) Scan(dest interface{}) error {
	if err := conn.Err(); err != nil {
		return err
	}
	if conn.managed {
		return errConnManaged
	}
	if conn.options.WriteOnly {
		return errConnWriteOnly
	}
	if err := conn.flush(); err != nil {
		return err
	}
	for {
		entry, ok := conn.state.Pop()
		if !ok {
			return ErrNoReplies
		}
		if !entry.Skip() {
			return conn.scanValue(dest, entry)
		}
	}

}

// WriteQuick is a convenience wrapper for WriteCommand
func (conn *Conn) WriteQuick(name, key string, args ...string) error {
	return conn.WriteCommand(name, QuickArgs(key, args...)...)
}

// ConnOptions holds connection options
type ConnOptions struct {
	ReadBufferSize  int           // Size of the read buffer
	WriteBufferSize int           // Size of the write buffer
	ReadTimeout     time.Duration // If > 0 all reads will fail if exceeded
	WriteTimeout    time.Duration // If > 0 all writes will fail if exceeded
	WriteOnly       bool          // WriteOnly connections return no replies
	DB              int           // Redis DB index
	KeyPrefix       string        // Prefix all keys
	Auth            string        // Redis auth
	Debug           bool          // Disables script injection
}

var (
	// ErrNoReplies is returned when no more replies are expected on Scan
	ErrNoReplies     = errors.New("No more replies")
	errConnClosed    = errors.New("Connection closed")
	errConnManaged   = errors.New("Connection managed by client")
	errConnWriteOnly = errors.New("Connection write only")
)

// // Managed checks if a connection is managed by a client
// func (conn *Conn) Managed() bool {
// 	return conn.managed
// }

// Dirty checkd if a connection has pending replies to scan
func (conn *Conn) Dirty() bool {
	return conn.state.Dirty()
}

// Err checks if the connection has an error
func (conn *Conn) Err() error {
	if conn != nil && conn.conn != nil {
		return nil
	}
	return errConnClosed
}

// Close closes a redis connection
func (conn *Conn) Close() error {
	if conn.pool != nil {
		err := conn.pool.put(conn)
		return err
	}
	if cn := conn.conn; conn != nil {
		conn.conn = nil
		return cn.Close()
	}
	return errConnClosed
}

// Reset resets the connection to a state as defined by the options
func (conn *Conn) Reset(options *ConnOptions) error {
	if err := conn.Err(); err != nil {
		return err
	}
	if conn.managed {
		return errConnManaged
	}
	if options == nil {
		options = &conn.options
	} else {
		conn.options = *options
	}
	state := &conn.state
	if state.IsMulti() {
		_ = conn.WriteCommand("DISCARD")
	} else if state.IsWatch() {
		_ = conn.WriteCommand("UNWATCH")
	}
	if options.WriteOnly {
		_ = conn.WriteCommand("CLIENT", String("REPLY"), String("OFF"))
	} else if state.IsReplyOFF() {
		_ = conn.WriteCommand("CLIENT", String("REPLY"), String("ON"))
	} else if state.IsReplySkip() {
		_ = conn.WriteCommand("PING")
	}
	if DBIndexValid(options.DB) && int(state.DB()) != options.DB {
		_ = conn.injectCommand("SELECT", Int(options.DB))
	}
	return conn.clear()
}

func (conn *Conn) clear() error {
	if conn.options.WriteOnly {
		_ = conn.WriteCommand("CLIENT", String("REPLY"), String("OFF"))
	} else {
		_ = conn.flush()
		_ = conn.drain()
	}
	return conn.Err()
}

func (conn *Conn) rewriteCommand(name string, args []Arg) (string, []Arg) {
	name = strings.ToUpper(name)
	switch name {
	case "EVAL":
		// Inject scripts
		if len(args) > 0 {
			arg := args[0]
			if sha1, ok := conn.scripts[arg]; ok {
				args[0] = String(sha1)
				return "EVALSHA", args
			}
			if script, ok := arg.Value().(string); ok {
				sha1 := sha1Sum(script)
				conn.scripts[arg] = sha1
				conn.injectCommand("SCRIPT", String("LOAD"), String(script))
				args[0] = String(sha1)
				return "EVALSHA", args
			}
		}
	}
	return name, args
}

// writeCommandSkipReply writes a redis command skipping the reply
func (conn *Conn) injectCommand(name string, args ...Arg) error {
	switch {
	case conn.state.IsMulti():
		return fmt.Errorf("Connection is in MULTI/EXEC transaction")
	case conn.state.IsReplyOFF():
		return conn.WriteCommand(name, args...)
	case conn.options.WriteOnly:
		return conn.WriteCommand(name, args...)
	case conn.state.IsReplySkip():
		return fmt.Errorf("Connection is already on CLIENT REPLY SKIP")
	default:
		// NOTE: any write error in conn.cmd is sticky so it will be returned
		// by the conn.WriteCommand call at the end of the function

		_ = conn.w.WriteCommand("CLIENT", String("REPLY"), String("SKIP"))
		conn.updatePipeline("CLIENT", String("REPLY"), String("SKIP"))
		return conn.WriteCommand(name, args...)
	}
}

// flush flushes the pipeline buffer
func (conn *Conn) flush() error {
	if err := conn.w.Flush(); err != nil {
		_ = conn.Close()
		return err
	}
	return nil
}

func (conn *Conn) drain() error {
	for {
		entry, ok := conn.state.Pop()
		if !ok {
			return ErrNoReplies
		}
		if entry.Skip() {
			continue
		}
		if err := conn.scanValue(nil, entry); err != nil {
			_ = conn.Close()
			return err
		}
		// if !conn.state.Dirty() {
		// 	return nil
		// }
		// if conn.state.Flush(&conn.replies) == 0 {
		// 	return nil
		// }
	}
}

func sha1Sum(s string) string {
	sum := sha1.Sum([]byte(s))
	var dst [2 * sha1.Size]byte
	hex.Encode(dst[:], sum[:])
	return string(dst[:])
}

func (conn *Conn) resetTimeout(entry pipeline.Entry) error {
	// Setup timeout
	timeout := conn.options.ReadTimeout
	if timeout < 0 {
		timeout = 0
	}
	// Only manage blocking timeouts when the command is not part of MULTI/EXEC
	if t, block := entry.Block(); block && !entry.Queued() {
		if t > 0 {
			timeout += t
		} else {
			timeout = -1
		}
	}
	if timeout == 0 {
		return nil
	}
	if timeout > 0 {
		deadline := time.Now().Add(timeout)
		return conn.conn.SetReadDeadline(deadline)
	}
	return conn.conn.SetReadDeadline(time.Time{})
}
func isDecodeError(err error) bool {
	_, ok := err.(*resp.DecodeError)
	return ok
}

func (conn *Conn) scanValue(dest interface{}, entry pipeline.Entry) error {
	if err := conn.resetTimeout(entry); err != nil {
		_ = conn.Close()
		return err
	}
	if err := conn.r.Decode(dest); err != nil {
		if !isDecodeError(err) {
			_ = conn.Close()
		}
		return err
	}
	return nil
}

// func (conn *Conn) manage() {
// 	conn.managed = true
// }

// func (conn *Conn) unmanage() {
// 	conn.managed = false
// }

// func (conn *Conn) getClient() *Client {
// 	if conn.pool != nil {
// 		return conn.pool.getClient()
// 	}
// 	return new(Client)
// }
// func (conn *Conn) putClient(client *Client) {
// 	if conn.pool != nil {
// 		conn.pool.putClient(client)
// 	}
// }

// Auth authenticates a connection
func (conn *Conn) Auth(password string) error {
	var ok AssertOK
	if err := conn.DoCommand(&ok, "AUTH", String(password)); err != nil {
		return fmt.Errorf("Authentication failed: %s", err)
	}
	return nil
}

func (conn *Conn) updatePipeline(name string, args ...Arg) {
	switch name {
	case "SELECT":
		index := selectArg(args)
		if 0 <= index && index < MaxDBIndex {
			conn.state.Select(index)
		} else {
			conn.state.Command()
		}
	case "MULTI":
		conn.state.Multi()
	case "EXEC":
		conn.state.Exec()
	case "DISCARD":
		conn.state.Discard()
	case "WATCH":
		conn.state.Watch(len(args))
	case "UNWATCH":
		conn.state.Unwatch()
	case "CLIENT":
		switch clientReplyArg(args) {
		case "OFF":
			conn.state.ReplyOFF()
		case "ON":
			conn.state.ReplyON()
		case "SKIP":
			conn.state.ReplySkip()
		default:
			conn.state.Command()
		}
	case "BLPOP", "BRPOP", "BRPOPLPUSH", "BZPOPMIN", "BZPOPMAX":
		timeout := lastArgTimeout(args)
		conn.state.Block(timeout)
	default:
		conn.state.Command()
	}
}

func selectArg(args []Arg) int64 {
	if len(args) > 0 {
		// TODO: force arg to int64
		if index, ok := args[0].Value().(int64); ok {
			return index
		}
	}
	return -1
}
func clientReplyArg(args []Arg) string {
	if len(args) == 2 {
		arg0, arg1 := args[0], args[1]
		if s, ok := arg0.Value().(string); ok && strings.ToUpper(s) == "REPLY" {
			if s, ok := arg1.Value().(string); ok {
				return strings.ToUpper(s)
			}
		}
	}
	return ""
}
func lastArgTimeout(args []Arg) time.Duration {
	if last := len(args) - 1; 1 <= last && last < len(args) {
		arg := &args[last]
		switch v := arg.Value().(type) {
		case int64:
			return time.Duration(v) * time.Millisecond
		case string:
			n, _ := strconv.ParseInt(v, 10, 64)
			return time.Duration(n) * time.Millisecond
		}
	}
	return 0
}

type managedConn struct {
	*Conn
}

func (m *managedConn) Close() error {
	if conn := m.Conn; m.conn != nil {
		m.Conn = nil
		conn.managed = false
		return nil
	}
	return errConnClosed
}
