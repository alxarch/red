package red

import (
	"math"
	"time"
)

// Append appends a string to a value of a key
func (c *Client) Append(key, value string) *ReplyBulkString {
	c.args.Key(key)
	c.args.String(value)
	return c.doBulkString("APPEND")
}

// Decr decrements key by 1
func (c *Client) Decr(key string) *ReplyInteger {
	c.args.Key(key)
	return c.doInteger("DECR")
}

// DecrBy decrements key by n
func (c *Client) DecrBy(key string, d int64) *ReplyInteger {
	c.args.Key(key)
	c.args.Int(int64(d))
	return c.doInteger("DECRBY")
}

// Get returns the string value of a key
func (c *Client) Get(key string) *ReplyBulkString {
	c.args.Key(key)
	return c.doBulkString("GET")
}

// GetRange gets a part of a string
func (c *Client) GetRange(key string, start, end int64) *ReplyBulkString {
	c.args.Key(key)
	c.args.Int(start)
	c.args.Int(end)
	return c.doBulkString("GETRANGE")
}

// GetSet atomicaly replaces a value returning the old value
func (c *Client) GetSet(key, value string) *ReplyBulkString {
	c.args.Key(key)
	c.args.String(value)
	return c.doBulkString("GETSET")
}

// Incr increments key by 1
func (c *Client) Incr(key string) *ReplyInteger {
	c.args.Key(key)
	return c.doInteger("INCR")

}

// IncrBy incremments the value at a key by an integer amount
func (c *Client) IncrBy(key string, n int64) *ReplyInteger {
	c.args.Key(key)
	c.args.Int(n)
	return c.doInteger("INCRBY")

}

// IncrByFloat incremments the value at a key by a float amount
func (c *Client) IncrByFloat(key string, incr float64) *ReplyFloat {
	c.args.Key(key)
	c.args.Float(incr)
	return c.doFloat("INCRBYFLOAT")
}

// MGet gets multiple key values
func (c *Client) MGet(key string, keys ...string) *ReplyBulkStringArray {
	c.args.Key(key)
	c.args.Keys(keys...)
	return c.doBulkStringArray("MGET")
}

// MSet sets multiple keys
func (c *Client) MSet(values ...string) *ReplyOK {
	var k, v string
	for len(values) >= 2 {
		k, v, values = values[0], values[1], values[2:]
		c.args.Key(k)
		c.args.String(v)
	}
	return c.doSimpleStringOK("MSET", 0)
}

// MSetArg sets multiple keys
func (c *Client) MSetArg(values map[string]Arg) *ReplyOK {
	for k, v := range values {
		c.args.Key(k)
		c.args.Arg(v)
	}
	return c.doSimpleStringOK("MSET", 0)
}

// MSetNX sets multiple keys if they do not exist
func (c *Client) MSetNX(values ...string) *ReplyBool {
	var k, v string
	for len(values) >= 2 {
		k, v, values = values[0], values[1], values[2:]
		c.args.Key(k)
		c.args.String(v)
	}
	return c.doBool("MSETNX")
}

func (c *Client) doSet(mode Mode, k, v string, ttl time.Duration) *ReplyOK {
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
	return c.doSimpleStringOK("SET", mode)
}

// SetXX resets a key value if it exists
func (c *Client) SetXX(key, value string, ttl time.Duration) *ReplyOK {
	return c.doSet(XX, key, value, ttl)
}

// Set sets a key to value
func (c *Client) Set(key, value string, ttl time.Duration) *ReplyOK {
	return c.doSet(0, key, value, ttl)
}

// SetNX sets a new key value
func (c *Client) SetNX(key, value string, ttl time.Duration) *ReplyOK {
	return c.doSet(NX, key, value, ttl)
}

// SetEX sets a key with a ttl
func (c *Client) SetEX(key, value string, ttl time.Duration) *ReplyOK {
	return c.doSet(0, key, value, ttl)
}

// SetRange sets a part of a string
func (c *Client) SetRange(key string, offset int64, value string) *ReplyBulkString {
	c.args.Key(key)
	c.args.Int(offset)
	c.args.String(value)
	return c.doBulkString("SETRANGE")
}

// StrLen return the length of a string value
func (c *Client) StrLen(key string) *ReplyBulkString {
	c.args.Key(key)
	return c.doBulkString("STRLEN")
}
