package red

import (
	"fmt"
	"math"
	"time"

	"github.com/alxarch/red/resp"
)

// Append appends a string to a value of a key
func (c *Client) Append(key, value string) *ReplyBulkString {
	reply := ReplyBulkString{}
	c.args.Key(key)
	c.args.String(value)
	c.do("APPEND", &reply)
	return &reply
}

// Decr decrements key by 1
func (c *Client) Decr(key string) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Key(key)
	c.do("DECR", &reply)
	return &reply

}

// DecrBy decrements key by n
func (c *Client) DecrBy(key string, d int64) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Key(key)
	c.args.Int(int64(d))

	c.do("DECRBY", &reply)
	return &reply
}

// Get returns the string value of a key
func (c *Client) Get(key string) *ReplyBulkString {
	reply := ReplyBulkString{}
	c.args.Key(key)

	c.do("GET", &reply)
	return &reply
}

// GetRange gets a part of a string
func (c *Client) GetRange(key string, start, end int64) *ReplyBulkString {
	reply := ReplyBulkString{}
	c.args.Key(key)
	c.args.Int(start)
	c.args.Int(end)

	c.do("GETRANGE", &reply)
	return &reply
}

// GetSet atomicaly replaces a value returning the old value
func (c *Client) GetSet(key, value string) *ReplyBulkString {
	reply := ReplyBulkString{}
	c.args.Key(key)
	c.args.String(value)

	c.do("GETSET", &reply)
	return &reply
}

// Incr increments key by 1
func (c *Client) Incr(key string) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Key(key)

	c.do("INCR", &reply)
	return &reply

}

// IncrBy incremments the value at a key by an integer amount
func (c *Client) IncrBy(key string, n int64) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Key(key)
	c.args.Int(n)

	c.do("INCRBY", &reply)
	return &reply

}

// IncrByFloat incremments the value at a key by a float amount
func (c *Client) IncrByFloat(key string, incr float64) *ReplyFloat {
	reply := ReplyFloat{}
	c.args.Key(key)
	c.args.Float(incr)

	c.do("INCRBYFLOAT", &reply)
	return &reply

}

// MGet gets multiple key values
func (c *Client) MGet(key string, keys ...string) *ReplyBulkStringArray {
	reply := ReplyBulkStringArray{}
	c.args.Key(key)
	c.args.Keys(keys...)
	c.do("MGET", &reply)
	return &reply
}

// MSet sets multiple keys
func (c *Client) MSet(values ...string) *ReplyOK {
	reply := ReplyOK{}

	var k, v string
	for len(values) >= 2 {
		k, v, values = values[0], values[1], values[2:]
		c.args.Key(k)
		c.args.String(v)
	}

	c.do("MSET", &reply)
	return &reply
}

// MSetArg sets multiple keys
func (c *Client) MSetArg(values map[string]Arg) *ReplyOK {
	reply := ReplyOK{}

	for k, v := range values {
		c.args.Key(k)
		c.args.Arg(v)
	}

	c.do("MSET", &reply)
	return &reply
}

// MSetNX sets multiple keys if they do not exist
func (c *Client) MSetNX(values ...string) *ReplyBool {
	reply := ReplyBool{}
	var k, v string
	for len(values) >= 2 {
		k, v, values = values[0], values[1], values[2:]
		c.args.Key(k)
		c.args.String(v)
	}

	c.do("MSETNX", &reply)
	return &reply
}

type ReplySet struct {
	mode Mode
	ok   bool
	replyBase
}

// Reply returns the reply of the SET command
func (r *ReplySet) Reply() (bool, error) {
	return r.ok, r.err
}

// UnmarshalRESP implements resp.Unmarshaler inteface
func (r *ReplySet) reply(v resp.Value) error {
	var status resp.SimpleString
	switch r.mode {
	case NX, XX:
		if v.Null() {
			r.err = fmt.Errorf("SET %q  failed", r.mode)
		} else {
			r.err = status.UnmarshalRESP(v)
		}
	default:
		r.err = status.UnmarshalRESP(v)
	}
	r.ok = string(status) == "OK"
	r.tee(v)
	return nil
}

func (c *Client) setArgs(mode Mode, k, v string, ttl time.Duration) {
	c.args.Key(k)
	c.args.String(v)
	const KeepTTL time.Duration = math.MinInt64
	if ttl > 0 {
		if ex := ttl.Truncate(time.Second); ex == ttl {
			c.args.String("EX")
			c.args.Arg(Seconds(ttl))
		} else {
			c.args.String("PX")
			c.args.Arg(Milliseconds(ttl))
		}
	}
	switch mode {
	case NX:
		c.args.String("NX")
	case XX:
		c.args.String("XX")
	}
	if ttl == KeepTTL {
		c.args.String("KEEPTTL")
	}
}

// SetXX resets a key value if it exists
func (c *Client) SetXX(key, value string, ttl time.Duration) *ReplySet {
	r := ReplySet{mode: XX}
	c.setArgs(XX, key, value, ttl)
	c.do("SET", &r)
	return &r
}

// Set sets a key to value
func (c *Client) Set(key, value string, ttl time.Duration) *ReplySet {
	r := ReplySet{}
	c.setArgs(0, key, value, ttl)
	c.do("SET", &r)
	return &r
}

// SetNX sets a new key value
func (c *Client) SetNX(key, value string, ttl time.Duration) *ReplySet {
	r := ReplySet{mode: NX}
	c.setArgs(NX, key, value, ttl)
	c.do("SET", &r)
	return &r
}

// SetEX sets a key if it already exists
func (c *Client) SetEX(key, value string, ttl time.Duration) *ReplyOK {
	reply := ReplyOK{}
	c.args.Key(key)
	c.args.Arg(Seconds(ttl))
	c.args.String(value)
	c.do("SETEX", &reply)
	return &reply
}

// SetRange sets a part of a string
func (c *Client) SetRange(key string, offset int64, value string) *ReplyBulkString {
	reply := ReplyBulkString{}
	c.args.Key(key)
	c.args.Int(offset)
	c.args.String(value)
	c.do("SETRANGE", &reply)
	return &reply
}

// StrLen return the length of a string value
func (c *Client) StrLen(key string) *ReplyBulkString {
	reply := ReplyBulkString{}
	c.args.Key(key)

	c.do("STRLEN", &reply)
	return &reply
}
