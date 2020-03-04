package pipeline

import (
	"time"
)

type entryType uint8

// Status enum
const (
	_ entryType = iota
	entryMulti
	entryExec
	entryDiscard
	entryUnwatch
	entryWatch
)

type Entry struct {
	typ     entryType
	queued  bool
	skip    bool
	block   bool
	timeout time.Duration
}

func (e *Entry) Discard() bool {
	return e.typ == entryDiscard
}
func (e *Entry) Multi() bool {
	return e.typ == entryMulti
}
func (e *Entry) Exec() bool {
	return e.typ == entryExec
}
func (e *Entry) Queued() bool {
	return e.queued
}
func (e *Entry) Skip() bool {
	return e.skip
}
func (e *Entry) Block() (time.Duration, bool) {
	return e.timeout, e.block
}

func (s *State) Pop() (Entry, bool) {
	if last := len(s.stack) - 1; 0 <= last && last < len(s.stack) {
		var entry Entry
		entry, s.stack = s.stack[last], s.stack[:last]
		return entry, true
	}
	if len(s.queue) > 0 {
		entry, tail := s.queue[0], s.queue[1:]
		s.dirty = len(tail) == 0
		for i := len(tail) - 1; i >= 0; i-- {
			s.stack = append(s.stack, tail[i])
		}
		s.queue = s.queue[:0]
		return entry, true
	} else {
		s.dirty = false
	}
	return Entry{}, false
}

type State struct {
	dirty     bool
	multi     bool
	replyOFF  bool
	replySkip bool
	watch     int
	queued    int
	db        int64
	queue     []Entry
	stack     []Entry
}

func (q *State) skip() bool {
	if q.multi {
		return false
	}
	skip := q.replySkip || q.replyOFF
	q.replySkip = false
	return skip
}

func (q *State) push(typ entryType) *Entry {
	index := len(q.queue)
	switch typ {
	case entryMulti:
		q.multi = true
	case entryExec, entryDiscard:
		q.multi = false
		q.watch = 0
		q.queued = 0
	case entryUnwatch:
		q.watch = 0
	}

	skip := q.skip()
	q.dirty = q.dirty || !skip
	q.queue = append(q.queue, Entry{
		typ:    typ,
		queued: q.multi,
		skip:   skip,
	})
	return &q.queue[index]
}

func (q *State) Peek() Entry {
	if last := len(q.stack) - 1; 0 <= last && last < len(q.stack) {
		return q.stack[last]
	}
	if len(q.queue) > 0 {
		return q.queue[0]
	}
	return Entry{}
}

func (q *State) Multi() (ok bool) {
	ok = !q.multi
	q.multi = true
	_ = q.push(entryMulti)
	return
}

func (q *State) Exec() (n int) {
	if q.multi {
		n = q.queued
	}
	_ = q.push(entryExec)
	return
}

func (q *State) Discard() (ok bool) {
	ok, q.multi = q.multi, ok
	_ = q.push(entryDiscard)
	return
}
func (q *State) Watch(n int) int {
	if n > 0 {
		q.watch += n
	}
	_ = q.push(entryWatch)
	return q.watch
}
func (q *State) Unwatch() (n int) {
	n, q.watch = q.watch, n
	_ = q.push(entryWatch)
	return
}
func (q *State) ReplySkip() {
	q.replySkip = true
	_ = q.push(0)
	q.replySkip = true
}

func (q *State) DB() int64 {
	return q.db
}
func (q *State) Select(db int64) {
	if 0 <= db && db < 16 {
		q.db = db
	}
	_ = q.push(0)
}
func (q *State) IsReplySkip() bool {
	return q.replySkip
}
func (q *State) IsReplyOFF() bool {
	return q.replyOFF
}
func (q *State) IsMulti() bool {
	return q.multi
}
func (q *State) Queued() int {
	return q.queued
}
func (q *State) ReplyON() {
	q.replyOFF = false
	_ = q.push(0)
}
func (q *State) ReplyOFF() {
	q.replyOFF = true
	_ = q.push(0)
}

func (q *State) Command() {
	_ = q.push(0)
}

func (q *State) Block(timeout time.Duration) {
	e := q.push(0)
	e.block = true
	e.timeout = timeout
}
func (q *State) Len() int {
	return len(q.queue)
}

func (q *State) CountReplies() (n int) {
	for i := range q.stack {
		e := &q.stack[i]
		if e.skip {
			continue
		}
		n++
	}
	for i := range q.queue {
		e := &q.queue[i]
		if e.skip {
			continue
		}
		n++
	}
	return
}

func (state *State) Dirty() bool {
	return state.dirty
}

func (state *State) IsWatch() bool {
	return state.watch > 0
}

// func QueueEntry(q *Queue, name string, args ...Arg) (status Status, timeout time.Duration) {
// 	switch name {
// 	case "SELECT":
// 		if len(args) > 0 {
// 			// TODO: force arg to int64
// 			if index, ok := args[0].Value().(int64); ok && 0 <= index && index < red.MaxDBIndex {
// 				q.DB = int(index)
// 			}
// 		}
// 	case "MULTI":
// 		status |= Multi
// 	case "EXEC":
// 		status |= Exec
// 	case "DISCARD":
// 		q.Unwatch()
// 		q.Discard()
// 	case "WATCH":
// 		q.Watch(len(args))
// 	case "UNWATCH":
// 		q.Unwatch()
// 	case "CLIENT":
// 		if len(args) == 2 {
// 			arg0, arg1 := args[0], args[1]
// 			if s, ok := arg0.Value().(string); ok && strings.ToUpper(s) != "REPLY" {
// 				break
// 			}
// 			switch s, _ := arg1.Value().(string); strings.ToUpper(s) {
// 			case "OFF":
// 				q.ReplyOFF()
// 			case "ON":
// 				q.ReplyOFF()
// 			case "SKIP":
// 				status |= q.ReplySkip()
// 			}
// 		}
// 	case "BLPOP", "BRPOP", "BRPOPLPUSH", "BZPOPMIN", "BZPOPMAX":
// 		status |= Block
// 		timeout = lastArgTimeout(args)
// 	}
// 	return
// 	// q.dirty = status&ReplySkip == 0
// 	// q.queue = append(q.queue, Entry{
// 	// 	Status:  status,
// 	// 	Timeout: timeout,
// 	// })

// }

// func (q *Queue) Push(status Status, timeout time.Duration) {
// 	if q.Skip() {
// 		status |= ReplySkip
// 		q.Status &^= ReplySkip
// 	}
// 	if q.Status.Multi() {
// 		if status.Exec() {
// 			q.Status &^= Multi
// 			q.Unwatch()

// 		}
// 		status |= Queued
// 		status &^= (ReplySkip | Multi)
// 	}
// 	if q.Status.Multi() {
// 		} else {
// 			status |= Queued
// 		}
// 	}
// 	switch {
// 	case status.Multi():
// 		if q.Status.Multi() {
// 			status &^= Multi
// 		} else {
// 			q.Status |= Multi
// 		}
// 		status &^= (Exec | Queued)
// 	case status.Exec():
// 		q.Status &^= Multi
// 		status &^= (Queued | Multi)
// 	case status.Discard():
// 		q.Status &^= Multi
// 	case status.ReplyOFF():
// 		q.Status |= ReplyOFF
// 	case "CLIENT":
// 		if len(args) == 2 {
// 			arg0, arg1 := args[0], args[1]
// 			if s, ok := arg0.Value().(string); ok && strings.ToUpper(s) != "REPLY" {
// 				break
// 			}
// 			switch s, _ := arg1.Value().(string); strings.ToUpper(s) {
// 			case "OFF":
// 				q.skipAll = true
// 				status |= ReplySkip
// 			case "ON":
// 				q.skipAll = false
// 				status &^= ReplySkip
// 			case "SKIP":
// 				status |= ReplySkip
// 				q.skipReply = true
// 			}
// 		}
// 	case "BLPOP", "BRPOP", "BRPOPLPUSH", "BZPOPMIN", "BZPOPMAX":
// 		status |= Block
// 		timeout = lastArgTimeout(args)
// 	}
// 	q.dirty = !status.Skip()
// 	q.queue = append(q.queue, Entry{
// 		Status:  status,
// 		Timeout: timeout,
// 	})
// 	return
// }

// func lastArgTimeout(args []Arg) time.Duration {
// 	// if last := len(args) - 1; 1 <= last && last < len(args) {
// 	// 	arg := &args[last]
// 	// 	if arg.typ == argInt {
// 	// 		return time.Duration(arg.num)
// 	// 	}
// 	// }
// 	return 0
// }
