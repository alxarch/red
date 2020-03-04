package red

import (
	"fmt"

	"github.com/alxarch/red/resp"
)

type Client struct {
	conn    *Conn
	args    ArgBuilder
	replies []clientReply
}

func (c *Client) Do(dest interface{}, cmd CommandBuilder) {
	var reply batchReply
	if r, ok := dest.(batchReply); ok {
		reply = r
	} else {
		r := replyBase{}
		r.Tee(dest)
		reply = &r
	}
	c.do(cmd.BuildCommand(&c.args), reply)
}

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

// func (c *Client) Reset(conn *Conn) {
// 	c.conn, conn = conn, c.conn
// 	if conn != nil {
// 		_ = conn.Close()
// 	}
// }

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
	queued []batchReply
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

func (c *Client) Sync() error {
	if c.conn == nil {
		return fmt.Errorf("Client closed")
	}
	c.conn.unmanage()
	defer c.conn.manage()
	defer c.clear()
	var multi []batchReply
	for i := range c.replies {
		r := &c.replies[i]
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
			multi = append(multi, r.batchReply)
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
				r := &tail[i]
				r.reject(err)
			}
			return err
		}
	}
	return nil

}

func (c *Client) do(cmd string, reply batchReply) {
	c.conn.unmanage()
	defer c.conn.manage()
	err := c.conn.WriteCommand(cmd, c.args.Args()...)
	c.args.Reset()
	if err != nil {
		reply.reject(err)
		return
	}
	c.replies = append(c.replies, clientReply{cmd, reply})
}

func (c *Client) clear() {
	for i := range c.replies {
		c.replies[i] = clientReply{}
	}
	c.args.Clear()
	c.replies = c.replies[:0]
}

type clientReply struct {
	cmd string
	batchReply
}

func (r *clientReply) UnmarshalRESP(v resp.Value) error {
	if r.batchReply == nil {
		return fmt.Errorf("BUG: Invalid client reply")
	}
	return r.batchReply.reply(v)
}
