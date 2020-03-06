package red

import (
	"fmt"

	"github.com/alxarch/red/resp"
)

type batchReply interface {
	reject(err error)
	reply(v resp.Value) error
}

type clientReply struct {
	cmd  string
	dest interface{}
	err  error
}

func (r *clientReply) reject(err error) {
	r.dest = nil
	r.err = err
}

func (r *clientReply) reply(v resp.Value) error {
	if r.dest != nil {
		r.err = v.Decode(r.dest)
	}
	return nil
}

func (r *clientReply) Bind(dest interface{}) {
	dest, r.dest = r.dest, dest
}

// ReplyOK is a redis "OK" status reply
type ReplyOK struct {
	ok AssertOK
	clientReply
}

var _ batchReply = (*ReplyOK)(nil)

// Reply returns if the status was OK
func (r *ReplyOK) Reply() (ok bool, err error) {
	err = r.err
	ok = err == nil
	return
}

// ReplyInteger is a redis integer reply
type ReplyInteger struct {
	n resp.Integer
	clientReply
}

// var _ batchReply = (*ReplyInteger)(nil)

// Reply returns the integer value
func (r *ReplyInteger) Reply() (int64, error) {
	return int64(r.n), r.err
}

// ReplySimpleString is a redis status reply
type ReplySimpleString struct {
	status resp.SimpleString
	clientReply
}

// var _ batchReply = (*ReplySimpleString)(nil)

// Reply returns the status reply
func (r *ReplySimpleString) Reply() (string, error) {
	return string(r.status), r.err
}

// ReplyBool is a redis integer reply with values 1 or 0
type ReplyBool struct {
	n resp.Integer
	clientReply
}

// var _ batchReply = (*ReplyBool)(nil)

// Reply returns the boolean reply
func (r *ReplyBool) Reply() (bool, error) {
	return r.n == 1, r.err
}

// ReplyBulkStringArray is a redis array reply with non-null bulk string elements
type ReplyBulkStringArray struct {
	values resp.BulkStringArray
	clientReply
}

// var _ batchReply = (*ReplyBulkStringArray)(nil)

// Reply returns the strings reply
func (r *ReplyBulkStringArray) Reply() ([]string, error) {
	return r.values, r.err
}

// ReplyFloat is a redis bulk string reply that is parsed as a float
type ReplyFloat struct {
	f float64
	clientReply
}

// Reply returns the float value
func (r *ReplyFloat) Reply() (float64, error) {
	return r.f, r.err
}

func (r *ReplyFloat) reply() *clientReply {
	r.clientReply.Bind(&r.f)
	return &r.clientReply
}

// ReplyAny is a redis reply of any kind
type ReplyAny struct {
	value resp.Any
	clientReply
}

// Reply returns the reply as a resp.Any value
func (r *ReplyAny) Reply() (resp.Any, error) {
	return r.value, r.err
}

// ReplyBulkString is a single bulk string reply
type ReplyBulkString struct {
	str resp.BulkString
	clientReply
}

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

// AssertOK is a convenience target for `conn.Scan` to check an `OK` reply
type AssertOK struct {
	Mode Mode
}

// UnmarshalRESP implements the resp.Unmarshal interface
func (ok *AssertOK) UnmarshalRESP(v resp.Value) error {
	switch ok.Mode {
	case NX, XX:
		if v.NullBulkString() {
			return fmt.Errorf("SET %q  failed", ok.Mode)
		}
	}
	var status resp.SimpleString
	if err := status.UnmarshalRESP(v); err != nil {
		return err
	}
	if status != StatusOK {
		return fmt.Errorf("Invalid status %q", status)
	}
	return nil
}
