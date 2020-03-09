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

func (b *batchAPI) Reset() {
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
func (conn *Conn) DoBatch(b *Batch) error {
	return conn.doBatch(&b.batchAPI)
}

// ErrReplyPending is the error of a reply until a `Client.Sync` is called
var ErrReplyPending = errors.New("Reply pending")

// ErrDiscarded is the error of a reply if it's part of a transaction that got discarded
var ErrDiscarded = errors.New("MULTI/EXEC Transaction discarded")

func (conn *Conn) doBatch(b *batchAPI) error {
	if err := conn.Err(); err != nil {
		return err
	}
	if conn.state.CountReplies() > 0 {
		return ErrReplyPending
	}
	defer b.Reset()
	if err := b.w.WriteTo(conn); err != nil {
		return err
	}
	if conn.state.IsMulti() {
		if err := conn.WriteCommand("EXEC"); err != nil {
			return err
		}
	}
	return conn.scanBatch(b.replies)
}

func (conn *Conn) scanBatch(replies []*batchReply) error {
	if len(replies) == 0 {
		return nil
	}
	var tx batchTx
	for i := 0; 0 <= i && i < len(replies); i++ {
		reply := replies[i]
		noReply := reply.dest == nil
		entry, value, err := conn.clientReadValue(noReply)
		if err != nil {
			for _, reply := range replies[i:] {
				reply.reject(err)
			}
			return err
		}
		switch {
		case entry.Exec():
			tx.Exec(value, reply)
		case entry.Queued():
			_ = tx.Queued(value, reply)
		case entry.Discard():
			tx.Discard(value, reply)
		case noReply:
			// nada
		default:
			reply.err = value.Decode(reply.dest)
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

type batchTx struct {
	queued []*batchReply
}

func (tx *batchTx) Discard(value resp.Value, reply *batchReply) {
	if reply.dest != nil {
		reply.err = value.Decode(reply.dest)
	}
	var ok AssertOK
	err := ok.UnmarshalRESP(value)
	if err == nil {
		err = ErrDiscarded
	}
	tx.Reject(err)
}

func (tx *batchTx) Exec(value resp.Value, reply *batchReply) {
	var err error
	var execAbort resp.Error
	switch {
	case value.Len() == int64(len(tx.queued)):
		iter := value.Iter()
		defer iter.Close()
		for _, reply := range tx.queued {
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
		tx.Reject(err)
		reply.reject(err)
	}
}

func (tx *batchTx) Queued(value resp.Value, reply *batchReply) error {
	var status resp.SimpleString
	err := status.UnmarshalRESP(value)
	if err != nil {
		reply.reject(err)
		tx.queued = append(tx.queued, nil)
		return nil
	}
	if status != StatusQueued {
		return fmt.Errorf("Invalid QUEUED status %q", status)
	}
	tx.queued = append(tx.queued, reply)
	return nil
}
func (tx *batchTx) Reset() {
	for i := range tx.queued {
		tx.queued[i] = nil
	}
	tx.queued = tx.queued[:0]
}
func (tx *batchTx) Reject(err error) {
	defer tx.Reset()
	for _, reply := range tx.queued {
		reply.reject(err)
	}
}
