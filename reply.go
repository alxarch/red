package red

import (
	"fmt"
	"strconv"

	"github.com/alxarch/red/resp"
	"github.com/alxarch/red/resputil"
)

type batchReply interface {
	reject(err error)
	reply(v resp.Value) error
}

type replyBase struct {
	ready bool
	bind  resp.Unmarshaler
	err   error
}

func (r *replyBase) reject(err error) {
	r.ready = true
	r.err = err
}

func (r *replyBase) reply(v resp.Value) error {
	r.tee(v)
	return nil
}

func (r *replyBase) tee(v resp.Value) {
	if r.bind != nil {
		_ = r.bind.UnmarshalRESP(v)
	}
}

func (r *replyBase) Tee(dest interface{}) {
	if dest == nil {
		return
	}
	u, _ := dest.(resp.Unmarshaler)
	if u == nil {
		u = resputil.Once(dest)
	}
	if r.bind == nil {
		r.bind = u
		return
	}
	if tee, ok := r.bind.(resputil.Tee); ok {
		r.bind = tee.Concat(u)
		return
	}
	r.bind = resputil.Tee{r.bind, u}
}

// ReplyOK is a redis "OK" status reply
type ReplyOK struct {
	ok bool
	replyBase
}

var _ batchReply = (*ReplyOK)(nil)

// Reply returns if the status was OK
func (r *ReplyOK) Reply() (bool, error) {
	return r.ok, r.err
}

func (r *ReplyOK) reply(v resp.Value) error {
	var status resp.SimpleString
	r.err = status.UnmarshalRESP(v)
	if r.err == nil {
		r.ok = string(status) == "OK"
		if !r.ok {
			r.err = fmt.Errorf("Invalid ok reply %q", status)
		}
	}
	r.tee(v)
	return nil
}

// ReplyInteger is a redis integer reply
type ReplyInteger struct {
	n resp.Integer
	replyBase
}

var _ batchReply = (*ReplyInteger)(nil)

// Reply returns the integer value
func (r *ReplyInteger) Reply() (int64, error) {
	return int64(r.n), r.err
}

func (r *ReplyInteger) reply(v resp.Value) error {
	r.ready = true
	r.err = r.n.UnmarshalRESP(v)
	r.tee(v)
	return nil
}

// ReplySimpleString is a redis status reply
type ReplySimpleString struct {
	status resp.SimpleString
	replyBase
}

var _ batchReply = (*ReplySimpleString)(nil)

// Reply returns the status reply
func (r *ReplySimpleString) Reply() (string, error) {
	return string(r.status), r.err
}
func (r *ReplySimpleString) reply(v resp.Value) error {
	r.err = r.status.UnmarshalRESP(v)
	r.tee(v)
	return nil
}

// ReplyBool is a redis integer reply with values 1 or 0
type ReplyBool struct {
	n resp.Integer
	replyBase
}

var _ batchReply = (*ReplyBool)(nil)

// Reply returns the boolean reply
func (r *ReplyBool) Reply() (bool, error) {
	return r.n == 1, r.err
}

func (r *ReplyBool) reply(v resp.Value) error {
	r.err = r.n.UnmarshalRESP(v)
	if r.err == nil {
		switch r.n {
		case 0, 1:
		default:
			r.err = fmt.Errorf("Invalid bool %d", r.n)
		}
	}
	r.tee(v)
	return nil
}

// ReplyBulkStringArray is a redis array reply with non-null bulk string elements
type ReplyBulkStringArray struct {
	resp.BulkStringArray
	replyBase
}

var _ batchReply = (*ReplyBulkStringArray)(nil)

// Reply returns the strings reply
func (r *ReplyBulkStringArray) Reply() ([]string, error) {
	return r.BulkStringArray, r.err
}

func (r *ReplyBulkStringArray) reply(v resp.Value) error {
	r.err = v.Decode(&r.BulkStringArray)
	r.tee(v)
	return nil
}

// ReplyFloat is a redis bulk string reply that is parsed as a float
type ReplyFloat struct {
	f float64
	replyBase
}

var _ batchReply = (*ReplyFloat)(nil)

// Reply returns the float value
func (r *ReplyFloat) Reply() (float64, error) {
	return r.f, r.err
}

func (r *ReplyFloat) reply(v resp.Value) error {
	var s resp.BulkString
	r.err = s.UnmarshalRESP(v)
	if r.err == nil {
		if s.Valid {
			r.f, r.err = strconv.ParseFloat(s.String, 64)
		} else {
			r.err = resp.ErrNull
		}
	}
	r.tee(v)
	return nil
}

// ReplyAny is a redis reply of any kind
type ReplyAny struct {
	value resp.Any
	replyBase
}

var _ batchReply = (*ReplyAny)(nil)

func (r *ReplyAny) reply(v resp.Value) error {
	r.value = v.Any()
	r.value.Decode(&r.err)
	r.tee(v)
	return nil
}

// Reply returns the reply as a resp.Any value
func (r *ReplyAny) Reply() (resp.Any, error) {
	return r.value, r.err
}

// ReplyBulkString is a single bulk string reply
type ReplyBulkString struct {
	str resp.BulkString
	replyBase
}

var _ batchReply = (*ReplyBulkString)(nil)

// Reply returns the bulk string
func (r *ReplyBulkString) Reply() (string, error) {
	if r.err != nil {
		return "", r.err
	}
	if r.str.Valid {
		return r.str.String, nil
	}
	return "", resp.ErrNull
}

func (r *ReplyBulkString) reply(v resp.Value) error {
	r.err = r.str.UnmarshalRESP(v)
	r.tee(v)
	return nil
}

// AssertOK is a convenience target for `conn.Scan` to check an `OK` reply
type AssertOK struct{}

// UnmarshalRESP implements the resp.Unmarshal interface
func (*AssertOK) UnmarshalRESP(v resp.Value) error {
	var s resp.SimpleString
	if err := s.UnmarshalRESP(v); err != nil {
		return err
	}
	if s != StatusOK {
		return fmt.Errorf("Invalid status %q", s)
	}
	return nil
}