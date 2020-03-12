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
	entryCommand
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
		s.dirty = len(s.stack) != 0
		return entry, true
	}
	if len(s.queue) > 0 {
		entry, tail := s.queue[0], s.queue[1:]
		for i := len(tail) - 1; i >= 0; i-- {
			s.stack = append(s.stack, tail[i])
		}
		s.queue = s.queue[:0]
		s.dirty = len(tail) != 0
		return entry, true
	}
	s.dirty = false
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
	default:
		typ = entryCommand
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

func (q *State) Peek() (entry Entry) {
	for i := len(q.stack) - 1; 0 <= i && i < len(q.stack); i-- {
		entry = q.stack[i]
		if !entry.Skip() {
			return
		}
	}
	for _, entry = range q.queue {
		if !entry.Skip() {
			return
		}
	}
	return Entry{}
}

func (q *State) Last() Entry {
	if last := len(q.queue) - 1; 0 <= last && last < len(q.queue) {
		return q.queue[last]
	}
	if len(q.stack) > 0 {
		return q.stack[0]
	}
	return Entry{}
}

func (q *State) Multi() (ok bool) {
	ok = !q.multi
	_ = q.push(entryMulti)
	q.multi = true
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
	q.replySkip = !q.multi
	_ = q.push(entryCommand)
	q.replySkip = !q.multi
}

func (q *State) DB() int64 {
	return q.db
}
func (q *State) Select(db int64) {
	if 0 <= db && db < 16 {
		q.db = db
	}
	_ = q.push(entryCommand)
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
	_ = q.push(entryCommand)
}
func (q *State) ReplyOFF() {
	q.replyOFF = true
	_ = q.push(entryCommand)
}

func (q *State) Command() {
	_ = q.push(entryCommand)
}

func (q *State) Block(timeout time.Duration) {
	e := q.push(entryCommand)
	if !e.queued {
		// Blocking commands inside MULTI/EXEC have zero timeout
		e.block = true
		e.timeout = timeout
	}
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
