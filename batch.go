package red

import (
	"errors"
	"fmt"
	"sync"

	"github.com/alxarch/red/resp"
)

// Batch is a batch of redis commands bound to replies
type Batch struct {
	batchAPI
}

// Pipeline is a connection manager for easy pipelining
type Pipeline struct {
	Batch
	mc managedConn
}

var pipelinePool = sync.Pool{
	New: func() interface{} {
		return new(Pipeline)
	},
}

func getPipeline() *Pipeline {
	return pipelinePool.Get().(*Pipeline)
}
func putPipeline(p *Pipeline) {
	if p == nil {
		return
	}
	_ = p.Close()
	p.Reset()
	pipelinePool.Put(p)
}

// Pipeline manages a connection for easier usage
func (c *Conn) Pipeline() (*Pipeline, error) {
	if err := c.Err(); err != nil {
		return nil, err
	}
	if c.managed {
		return nil, errConnManaged
	}
	if c.state.CountReplies() > 0 {
		return nil, ErrReplyPending
	}
	c.managed = true
	p := Pipeline{
		mc: managedConn{
			Conn: c,
		},
	}
	return &p, nil
}

// Sync syncs all queued commands in the pipeline
func (p *Pipeline) Sync() error {
	if err := p.mc.Err(); err != nil {
		return err
	}
	p.mc.Conn.managed = false
	if err := p.mc.doBatch(&p.batchAPI); err != nil {
		_ = p.Close()
		return err
	}
	p.mc.Conn.managed = true
	return nil
}

// Close closes a pipeline releasing the managed connection
func (p *Pipeline) Close() error {
	if p == nil {
		return nil
	}
	err := p.mc.Close()
	return err
}

// Tx is a MULTI/EXEC transaction block
type Tx struct {
	batchAPI
}

// Multi queues a MULTI/EXEC transaction
func (b *Batch) Multi(tx *Tx) *ReplyTX {
	reply := ReplyTX{
		batchReply: batchReply{
			dest: tx.replies,
		},
	}
	tx.replies = nil
	_ = b.w.WriteCommand("MULTI")
	b.replies = append(b.replies, &reply.batchReply)
	_ = tx.w.WriteTo(&b.w)
	_ = b.w.WriteCommand("EXEC")
	tx.reset()
	return &reply
}

var batchPool = sync.Pool{
	New: func() interface{} {
		return new(Batch)
	},
}

// AcquireBatch gets an empty Batch from a package-wide pool
func AcquireBatch() *Batch {
	return batchPool.Get().(*Batch)
}

// ReleaseBatch releases a Batch to a package-wide pool
func ReleaseBatch(b *Batch) {
	if b == nil {
		return
	}
	b.Reset()
	batchPool.Put(b)
}

type batchAPI struct {
	args    ArgBuilder
	w       batchWriter
	replies []*batchReply
}

func (b *batchAPI) Do(cmd string, args ...Arg) *ReplyAny {
	b.args.Append(args...)
	return b.doAny(cmd)
}

// Reset clears all pending commands
func (b *Batch) Reset() {
	b.batchAPI.reset()
}

func (b *batchAPI) reset() {
	b.args.Clear()
	b.w.Reset()
	for i := range b.replies {
		b.replies[i] = nil
	}
	*b = batchAPI{
		args:    b.args,
		w:       b.w,
		replies: b.replies[:0],
	}
}

func (b *batchAPI) do(cmd string, reply *batchReply) {
	_ = b.w.WriteCommand(cmd, b.args.Args()...)
	b.args.Reset()
	b.replies = append(b.replies, reply)
}

func (b *batchAPI) doInteger(cmd string) *ReplyInteger {
	reply := ReplyInteger{}
	reply.Bind(&reply.n)
	b.do(cmd, &reply.batchReply)
	return &reply
}
func (b *batchAPI) doAny(cmd string) *ReplyAny {
	reply := ReplyAny{}
	reply.Bind(&reply.value)
	b.do(cmd, &reply.batchReply)
	return &reply
}

func (b *batchAPI) doSimpleStringOK(cmd string, mode Mode) *ReplyOK {
	reply := ReplyOK{ok: AssertOK{Mode: mode}}
	reply.Bind(&reply.ok)
	b.do(cmd, &reply.batchReply)
	return &reply
}
func (b *batchAPI) doBulkString(cmd string) *ReplyBulkString {
	reply := ReplyBulkString{}
	reply.Bind(&reply.str)
	b.do(cmd, &reply.batchReply)
	return &reply
}

func (b *batchAPI) doSimpleString(cmd string) *ReplySimpleString {
	reply := ReplySimpleString{}
	reply.Bind(&reply.status)
	b.do(cmd, &reply.batchReply)
	return &reply
}

func (b *batchAPI) doBulkStringArray(cmd string) *ReplyBulkStringArray {
	reply := ReplyBulkStringArray{}
	reply.Bind(&reply.values)
	b.do(cmd, &reply.batchReply)
	return &reply
}
func (b *batchAPI) doFloat(cmd string) *ReplyFloat {
	reply := ReplyFloat{}
	reply.Bind(&reply.f)
	b.do(cmd, &reply.batchReply)
	return &reply
}
func (b *batchAPI) doBool(cmd string) *ReplyBool {
	reply := ReplyBool{}
	reply.Bind(&reply.n)
	b.do(cmd, &reply.batchReply)
	return &reply

}

// DoBatch executes a batch
func (c *Conn) DoBatch(b *Batch) error {
	return c.doBatch(&b.batchAPI)
}

// ErrReplyPending is the error of a reply until a `Client.Sync` is called
var ErrReplyPending = errors.New("Reply pending")

// ErrDiscarded is the error of a reply if it's part of a transaction that got discarded
var ErrDiscarded = errors.New("MULTI/EXEC Transaction discarded")

func (c *Conn) doBatch(b *batchAPI) error {
	if err := c.Err(); err != nil {
		return err
	}
	if c.state.CountReplies() > 0 {
		return ErrReplyPending
	}
	defer b.reset()
	if err := b.w.WriteTo(c); err != nil {
		return err
	}
	if c.state.IsMulti() {
		if err := c.WriteCommand("EXEC"); err != nil {
			return err
		}
	}
	return c.scanBatch(b.replies)
}

type batchExec []*batchReply

func (tx *batchExec) UnmarshalRESP(value resp.Value) (err error) {
	queued := *tx
	var execAbort resp.Error
	switch {
	case value.Len() == int64(len(queued)):
		iter := value.Iter()
		defer iter.Close()
		for _, reply := range queued {
			v := iter.Value()
			if reply.dest != nil {
				reply.err = v.Decode(reply.dest)
			}
			iter.Next()
		}
	case value.NullArray():
		err = resp.ErrNull
	case execAbort.UnmarshalRESP(value) == nil:
		err = execAbort
	default:
		err = fmt.Errorf("Invalid EXEC reply %v", value.Any())
	}
	if err != nil {
		for _, reply := range queued {
			reply.reject(err)
		}
	}
	return
}

func unwrapDecodeError(err error) error {
	if err, ok := err.(*resp.DecodeError); ok {
		return err.Reason
	}
	return err
}
func (c *Conn) scanBatch(replies []*batchReply) error {
	if len(replies) == 0 {
		return nil
	}
	if err := c.flush(); err != nil {
		for _, reply := range replies {
			reply.reject(err)
		}
		return err
	}
	// var tx batchTx
	for i := 0; 0 <= i && i < len(replies); i++ {
		reply := replies[i]
		if queued, ok := reply.dest.([]*batchReply); ok {
			ok := AssertOK{}
			if err := c.Scan(&ok); err != nil {
				err = unwrapDecodeError(err)
				for i := range queued {
					queued[i].reject(err)
				}
				reply.reject(err)
				continue
			}
			for _, reply := range queued {
				q := assertQueued{}
				if err := c.Scan(&q); err != nil {
					err = unwrapDecodeError(err)
					reply.reject(err)
				}
			}
			tx := batchExec(queued)
			reply.err = unwrapDecodeError(c.Scan(&tx))
		} else {
			reply.err = unwrapDecodeError(c.Scan(reply.dest))
		}
		if err := c.Err(); err != nil {
			for _, reply := range replies[i:] {
				reply.reject(err)
			}
			return err
		}
	}
	return nil

}

type batchWriter struct {
	args     []Arg
	commands []batchCmd
}

func (w *batchWriter) WriteCommand(name string, args ...Arg) error {
	argv := len(w.args)
	w.args = append(w.args, args...)
	w.commands = append(w.commands, batchCmd{
		name: name,
		argv: uint32(argv),
		argc: uint32(len(args)),
	})
	return nil
}

func (w *batchWriter) Reset() {
	for i := range w.args {
		w.args[i] = Arg{}
	}
	for i := range w.commands {
		w.commands[i] = batchCmd{}
	}
	*w = batchWriter{
		args:     w.args[:0],
		commands: w.commands[:0],
	}
}

func (w *batchWriter) WriteTo(dest CommandWriter) error {
	for i := range w.commands {
		cmd := &w.commands[i]
		args := cmd.Args(w.args)
		if err := dest.WriteCommand(cmd.name, args...); err != nil {
			return err
		}
	}
	return nil
}

type batchCmd struct {
	name       string
	argv, argc uint32
}

func (b *batchCmd) Args(args []Arg) []Arg {
	if b.argv < uint32(len(args)) {
		args = args[b.argv:]
		if b.argc <= uint32(len(args)) {
			return args[:b.argc]
		}
	}
	return nil
}

type batchReply struct {
	dest interface{}
	err  error
}

func (r *batchReply) reject(err error) {
	if r.err == nil {
		r.err = err
	}
}

func (r *batchReply) Err() error {
	return r.err
}
func (r *batchReply) Bind(dest interface{}) {
	r.dest = dest
}
