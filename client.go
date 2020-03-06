package red

import (
	"errors"
	"fmt"

	"github.com/alxarch/red/resp"
)

// Client manages a red.Conn mapping redis commands to methods
type Client struct {
	conn    *Conn
	args    ArgBuilder
	replies []*clientReply
}

// Do writes a redis command and binds the reply to dest
func (c *Client) Do(dest interface{}, cmd CommandBuilder) {
	reply := clientReply{
		dest: dest,
	}
	c.do(cmd.BuildCommand(&c.args), &reply)
}

// Close closes the client releasing the managed red.Conn
func (c *Client) Close() error {
	var conn *Conn
	conn, c.conn = c.conn, conn
	if conn != nil {
		conn.unmanage()
		conn.putClient(c)
		return conn.Close()
	}
	c.clear()
	return nil
}

type clientQueued struct {
	reply *clientReply
}

func (q *clientQueued) UnmarshalRESP(v resp.Value) error {
	var queued resp.SimpleString
	if err := queued.UnmarshalRESP(v); err != nil {
		q.reply.reject(err)
	}
	if queued != StatusQueued {
		return fmt.Errorf("Invalid queued status %q", queued)
	}
	return nil
}

type clientExec struct {
	queued []*clientReply
	reply  *clientReply
}

func (exec *clientExec) reject(err error) {
	exec.reply.reject(err)
	for _, q := range exec.queued {
		q.reject(err)
	}
}
func (exec *clientExec) UnmarshalRESP(v resp.Value) error {
	var execAbort resp.Error
	if v.NullArray() {
		exec.reject(resp.ErrNull)
		// exec.reject(fmt.Errorf("MULTI/EXEC aborted by WATCH lock"))
	} else if v.Len() >= 0 {
		iter := v.Iter()
		defer iter.Close()
		for _, reply := range exec.queued {
			if !iter.More() {
				err := fmt.Errorf("Invalid result size %d", v.Len())
				exec.reject(err)
				return err
			}
			v := iter.Value()
			reply.reply(v)
			iter.Next()
		}
	} else if execAbort.UnmarshalRESP(v) == nil {
		exec.reject(execAbort)
	} else {
		err := fmt.Errorf("Invalid EXEC reply %v", v.Any())
		exec.reject(err)
		return err
	}
	return nil
}

// Sync flushes all pending commands and reads all pending replies
func (c *Client) Sync() error {
	if c.conn == nil {
		return fmt.Errorf("Client closed")
	}
	c.conn.unmanage()
	defer c.conn.manage()
	defer c.clear()
	var multi []*clientReply
	for i := range c.replies {
		r := c.replies[i]
		entry, value, err := c.conn.clientReadValue()
		switch {
		case err != nil:
		// case entry.Skip():
		// 	continue
		case entry.Multi():
			err = r.reply(value)
		case entry.Exec():
			exec := clientExec{
				queued: multi,
				reply:  r,
			}
			err = exec.UnmarshalRESP(value)
			multi = multi[:0]
		case entry.Queued():
			multi = append(multi, r)
			q := clientQueued{
				reply: r,
			}
			err = q.UnmarshalRESP(value)
		case entry.Discard():
			multi = multi[:0]
			fallthrough
		default:
			err = r.reply(value)
		}
		if err != nil {
			tail := c.replies[i:]
			for i := range tail {
				r := tail[i]
				r.reject(err)
			}
			return err
		}
	}
	return nil

}

// ErrReplyPending is the error of a reply until a `Client.Sync` is called
var ErrReplyPending = errors.New("Reply pending")

func (c *Client) do(cmd string, reply *clientReply) {
	reply.cmd = cmd
	reply.err = ErrReplyPending
	c.conn.unmanage()
	defer c.conn.manage()
	err := c.conn.WriteCommand(cmd, c.args.Args()...)
	c.args.Reset()
	if err != nil {
		reply.reject(err)
		return
	}
	c.replies = append(c.replies, reply)
}

func (c *Client) doBulkStringArray(cmd string) *ReplyBulkStringArray {
	reply := ReplyBulkStringArray{}
	reply.Bind(&reply.values)
	c.do(cmd, &reply.clientReply)
	return &reply
}
func (c *Client) doFloat(cmd string) *ReplyFloat {
	reply := ReplyFloat{}
	reply.Bind(&reply.f)
	c.do(cmd, &reply.clientReply)
	return &reply
}
func (c *Client) doBool(cmd string) *ReplyBool {
	reply := ReplyBool{}
	reply.Bind(&reply.n)
	c.do(cmd, &reply.clientReply)
	return &reply

}
func (c *Client) doSimpleString(cmd string) *ReplySimpleString {
	reply := ReplySimpleString{}
	reply.Bind(&reply.status)
	c.do(cmd, &reply.clientReply)
	return &reply
}

func (c *Client) doBulkString(cmd string) *ReplyBulkString {
	reply := ReplyBulkString{}
	reply.Bind(&reply.str)
	c.do(cmd, &reply.clientReply)
	return &reply
}

func (c *Client) doSimpleStringOK(cmd string, mode Mode) *ReplyOK {
	reply := ReplyOK{ok: AssertOK{Mode: mode}}
	reply.Bind(&reply.ok)
	c.do(cmd, &reply.clientReply)
	return &reply
}

func (c *Client) doInteger(cmd string) *ReplyInteger {
	reply := ReplyInteger{}
	reply.Bind(&reply.n)
	c.do(cmd, &reply.clientReply)
	return &reply
}

func (c *Client) clear() {
	for i := range c.replies {
		c.replies[i] = nil
	}
	c.args.Clear()
	c.replies = c.replies[:0]
}
