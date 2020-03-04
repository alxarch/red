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

	conn    net.Conn
	err     error
	w       Writer
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

	if name == "CLIENT" {
		return fmt.Errorf("CLIENT commands not allowed")
	}

	if err := conn.w.WriteCommand(conn.options.KeyPrefix, name, args...); err != nil {
		conn.closeWithError(err)
		return err
	}
	conn.updatePipeline(name, args...)
	return nil
}

// DoCommand executes a redis command
func (conn *Conn) DoCommand(dest interface{}, name string, args ...Arg) error {
	if conn.err != nil {
		return conn.err
	}
	if conn.Dirty() {
		return fmt.Errorf("Pending replies")
	}
	if err := conn.WriteCommand(name, args...); err != nil {
		return err
	}
	if err := conn.Scan(dest); err != nil {
		return err
	}
	return nil
}

func (conn *Conn) clientReadValue() (pipeline.Entry, resp.Value, error) {
	if err := conn.flush(); err != nil {
		return pipeline.Entry{}, resp.Value{}, err
	}
	for {
		entry, ok := conn.state.Pop()
		if !ok {
			return pipeline.Entry{}, resp.Value{}, ErrNoReplies
		}
		if entry.Skip() {
			continue
		}
		v, err := conn.readValue(entry)
		if err != nil {
			conn.closeWithError(err)
		}
		return entry, v, err
	}
}

// Scan decodes a reply to x
func (conn *Conn) Scan(x interface{}) error {
	if conn.err != nil {
		return conn.err
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
		if entry.Skip() {
			continue
		}
		var v resp.Value
		var err error
		if x == nil {
			err = conn.discardValue(entry)
		} else {
			v, err = conn.readValue(entry)
		}
		if err != nil {
			conn.closeWithError(err)
			return err
		}
		if x != nil {
			return v.Decode(x)
		}
		return nil

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

// Managed checks if a connection is managed by a client
func (conn *Conn) Managed() bool {
	return conn.managed
}

// Dirty checkd if a connection has pending replies to scan
func (conn *Conn) Dirty() bool {
	return conn.state.Dirty()
}

// Err checks if the connection has an error
func (conn *Conn) Err() error {
	return conn.err
}

// Close closes a redis connection
func (conn *Conn) Close() error {
	if conn.pool != nil {
		err := conn.pool.put(conn)
		return err
	}
	if conn.err == nil {
		conn.closeWithError(errConnClosed)
		return nil
	}
	return conn.err
}

func (conn *Conn) closeWithError(err error) {
	if conn.err == nil {
		var c net.Conn
		c, conn.conn, conn.err = conn.conn, nil, err
		_ = c.Close()
	}
}

// Reset resets the connection to a state as defined by the options
func (conn *Conn) Reset(options *ConnOptions) error {
	if conn.err != nil {
		return conn.err
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

		_ = conn.w.WriteCommand(conn.options.KeyPrefix, "CLIENT", String("REPLY"), String("SKIP"))
		conn.updatePipeline("CLIENT", String("REPLY"), String("SKIP"))
		return conn.WriteCommand(name, args...)
	}
}

// flush flushes the pipeline buffer
func (conn *Conn) flush() error {
	// if conn.err != nil {
	// 	return conn.err
	// }
	// state := &conn.state
	// if state.Len() == 0 {
	// 	return nil
	// }
	// if state.IsMulti() {
	// 	return fmt.Errorf("Cannot flush during MULTI/EXEC transaction")
	// }

	// // Pad leftover skip commands with "PING"
	// if state.Skip() {
	// 	if err := conn.WriteCommand("PING"); err != nil {
	// 		return err
	// 	}
	// }

	// _ = state.Flush(&conn.replies)

	if err := conn.w.Flush(); err != nil {
		conn.closeWithError(err)
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
		if err := conn.discardValue(entry); err != nil {
			conn.closeWithError(err)
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

func (conn *Conn) discardValue(entry pipeline.Entry) error {
	if err := conn.resetTimeout(entry); err != nil {
		return err
	}
	return conn.r.Skip()
}
func (conn *Conn) readValue(entry pipeline.Entry) (resp.Value, error) {
	if err := conn.resetTimeout(entry); err != nil {
		return resp.Value{}, err
	}
	return conn.r.Next()
}

func (conn *Conn) manage() {
	conn.managed = true
}

func (conn *Conn) unmanage() {
	conn.managed = false
}

func (conn *Conn) getClient() *Client {
	if conn.pool != nil {
		return conn.pool.getClient()
	}
	return new(Client)
}
func (conn *Conn) putClient(client *Client) {
	if conn.pool != nil {
		conn.pool.putClient(client)
	}
}

// Auth authenticates a connection
func (conn *Conn) Auth(password string) error {
	var ok AssertOK
	if err := conn.DoCommand(&ok, "AUTH", String(password)); err != nil {
		return fmt.Errorf("Authentication failed: %s", err)
	}
	return nil
}

// Client handles over the connection to be managed by a red.Client
func (conn *Conn) Client() (*Client, error) {
	if conn.err != nil {
		return nil, conn.err
	}
	if conn.managed {
		return nil, fmt.Errorf("Connection already managed")
	}
	if conn.Dirty() {
		return nil, fmt.Errorf("Connection pending replies")
	}
	client := conn.getClient()
	conn.manage()
	client.conn = conn
	return client, nil
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
