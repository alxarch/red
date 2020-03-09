package red

import (
	"math"
	"time"
)

// Append appends a string to a value of a key
func (b *batchAPI) Append(key, value string) *ReplyBulkString {
	b.args.Key(key)
	b.args.String(value)
	return b.doBulkString("APPEND")
}

// Decr decrements key by 1
func (b *batchAPI) Decr(key string) *ReplyInteger {
	b.args.Key(key)
	return b.doInteger("DECR")
}

// DecrBy decrements key by n
func (b *batchAPI) DecrBy(key string, d int64) *ReplyInteger {
	b.args.Key(key)
	b.args.Int(int64(d))
	return b.doInteger("DECRBY")
}

// Get returns the string value of a key
func (b *batchAPI) Get(key string) *ReplyBulkString {
	b.args.Key(key)
	return b.doBulkString("GET")
}

// GetRange gets a part of a string
func (b *batchAPI) GetRange(key string, start, end int64) *ReplyBulkString {
	b.args.Key(key)
	b.args.Int(start)
	b.args.Int(end)
	return b.doBulkString("GETRANGE")
}

// GetSet atomicaly replaces a value returning the old value
func (b *batchAPI) GetSet(key, value string) *ReplyBulkString {
	b.args.Key(key)
	b.args.String(value)
	return b.doBulkString("GETSET")
}

// Incr increments key by 1
func (b *batchAPI) Incr(key string) *ReplyInteger {
	b.args.Key(key)
	return b.doInteger("INCR")

}

// IncrBy incremments the value at a key by an integer amount
func (b *batchAPI) IncrBy(key string, n int64) *ReplyInteger {
	b.args.Key(key)
	b.args.Int(n)
	return b.doInteger("INCRBY")

}

// IncrByFloat incremments the value at a key by a float amount
func (b *batchAPI) IncrByFloat(key string, incr float64) *ReplyFloat {
	b.args.Key(key)
	b.args.Float(incr)
	return b.doFloat("INCRBYFLOAT")
}

// MGet gets multiple key values
func (b *batchAPI) MGet(key string, keys ...string) *ReplyBulkStringArray {
	b.args.Key(key)
	b.args.Keys(keys...)
	return b.doBulkStringArray("MGET")
}

// MSet sets multiple keys
func (b *batchAPI) MSet(values ...string) *ReplyOK {
	var k, v string
	for len(values) >= 2 {
		k, v, values = values[0], values[1], values[2:]
		b.args.Key(k)
		b.args.String(v)
	}
	return b.doSimpleStringOK("MSET", 0)
}

// MSetArg sets multiple keys
func (b *batchAPI) MSetArg(values map[string]Arg) *ReplyOK {
	for k, v := range values {
		b.args.Key(k)
		b.args.Arg(v)
	}
	return b.doSimpleStringOK("MSET", 0)
}

// MSetNX sets multiple keys if they do not exist
func (b *batchAPI) MSetNX(values ...string) *ReplyBool {
	var k, v string
	for len(values) >= 2 {
		k, v, values = values[0], values[1], values[2:]
		b.args.Key(k)
		b.args.String(v)
	}
	return b.doBool("MSETNX")
}

func (b *batchAPI) doSet(mode Mode, k, v string, ttl time.Duration) *ReplyOK {
	b.args.Key(k)
	b.args.String(v)
	const KeepTTL time.Duration = math.MinInt64
	if ttl > 0 {
		if ex := ttl.Truncate(time.Second); ex == ttl {
			b.args.String("EX")
			b.args.Arg(Seconds(ttl))
		} else {
			b.args.String("PX")
			b.args.Arg(Milliseconds(ttl))
		}
	}
	switch mode {
	case NX:
		b.args.String("NX")
	case XX:
		b.args.String("XX")
	}
	if ttl == KeepTTL {
		b.args.String("KEEPTTL")
	}
	return b.doSimpleStringOK("SET", mode)
}

// SetXX resets a key value if it exists
func (b *batchAPI) SetXX(key, value string, ttl time.Duration) *ReplyOK {
	return b.doSet(XX, key, value, ttl)
}

// Set sets a key to value
func (b *batchAPI) Set(key, value string, ttl time.Duration) *ReplyOK {
	return b.doSet(0, key, value, ttl)
}

// SetNX sets a new key value
func (b *batchAPI) SetNX(key, value string, ttl time.Duration) *ReplyOK {
	return b.doSet(NX, key, value, ttl)
}

// SetEX sets a key with a ttl
func (b *batchAPI) SetEX(key, value string, ttl time.Duration) *ReplyOK {
	return b.doSet(0, key, value, ttl)
}

// SetRange sets a part of a string
func (b *batchAPI) SetRange(key string, offset int64, value string) *ReplyBulkString {
	b.args.Key(key)
	b.args.Int(offset)
	b.args.String(value)
	return b.doBulkString("SETRANGE")
}

// StrLen return the length of a string value
func (b *batchAPI) StrLen(key string) *ReplyBulkString {
	b.args.Key(key)
	return b.doBulkString("STRLEN")
}
