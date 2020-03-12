package red

import (
	"time"

	"github.com/alxarch/red/resp"
)

type Pop struct {
	Key   string
	Value string
}

func (p *Pop) UnmarshalRESP(v resp.Value) error {
	return v.Decode([]interface{}{
		&p.Key,
		&p.Value,
	})
}

// BLPop is the blocking variant of LPop
func (c *Conn) BLPop(timeout time.Duration, key string, keys ...string) (pop Pop, err error) {
	args := ArgBuilder{}
	args.KeysUnique(key, keys...)
	args.Milliseconds(timeout)
	err = c.DoCommand(&pop, "BLPOP", args.Args()...)
	return
}

// BRPop is the blocking variant of RPop
func (c *Conn) BRPop(timeout time.Duration, key string, keys ...string) (pop Pop, err error) {
	args := ArgBuilder{}
	args.KeysUnique(key, keys...)
	args.Milliseconds(timeout)
	err = c.DoCommand(&pop, "BRPOP", args.Args()...)
	return
}

// BRPopLPush is the blocking variant of RPopLPush
func (c *Conn) BRPopLPush(src, dst string, timeout time.Duration) (el string, err error) {
	err = c.DoCommand(&el, "BRPOPLPUSH", Key(src), Key(dst), Milliseconds(timeout))
	return
}

// LIndex returns the element at index index in the list stored at key.
func (c *batchAPI) LIndex(key string, index int64) *ReplyInteger {
	c.args.Key(key)
	c.args.Int(index)
	return c.doInteger("LINDEX")
}

// LInsertAfter inserts element in the list stored at key after the reference value pivot.
// LINSERT key AFTER pivot element
func (c *batchAPI) LInsertAfter(key string, pivot int64, value Arg) *ReplyInteger {
	c.args.Key(key)
	c.args.String("AFTER")
	c.args.Int(pivot)
	c.args.Arg(value)
	return c.doInteger("LINSERT")
}

// LInsertBefore inserts element in the list stored at key before the reference value pivot.
// LINSERT key BEFORE pivot element
func (c *batchAPI) LInsertBefore(key string, pivot int64, value Arg) *ReplyInteger {
	c.args.Key(key)
	c.args.String("BEFORE")
	c.args.Int(pivot)
	c.args.Arg(value)
	return c.doInteger("LINSERT")
}

// LLen returns the length of the list stored at key.
func (c *batchAPI) LLen(key string) *ReplyInteger {
	c.args.Key(key)
	return c.doInteger("LLEN")
}

// LPop removes and returns the first element of the list stored at key.
func (c *batchAPI) LPop(key string) *ReplyBulkString {
	c.args.Key(key)
	return c.doBulkString("LPOP")
}

// LPush inserts specified values at the head of the list stored at key.
func (c *batchAPI) LPush(key string, values ...Arg) *ReplyInteger {
	c.args.Key(key)
	c.args.Append(values...)
	return c.doInteger("LPUSH")
}

// LPushX inserts specified values at the head of the list stored at key, only if key already exists and holds a list.
func (c *batchAPI) LPushX(key string, values ...Arg) *ReplyInteger {
	c.args.Key(key)
	c.args.Append(values...)
	return c.doInteger("LPUSHX")
}

// LRange returns the specified elements of the list stored at key.
// LRANGE key start stop
func (c *batchAPI) LRange(key string, start, stop int64) *ReplyBulkStringArray {
	c.args.Key(key)
	c.args.Int(start)
	c.args.Int(stop)
	return c.doBulkStringArray("LRANGE")
}

// LRem removes the first count occurrences of elements equal to element from the list stored at key.
func (c *batchAPI) LRem(key string, count int64, element Arg) *ReplyInteger {
	c.args.Key(key)
	c.args.Int(count)
	c.args.Arg(element)
	return c.doInteger("LREM")
}

// LSet sets list element at index to element.
func (c *batchAPI) LSet(key string, index int64, element Arg) *ReplyOK {
	c.args.Key(key)
	c.args.Int(index)
	return c.doSimpleStringOK("LSET", 0)
}

// LTrim trims an existing list so that it will contain only the specified range of elements specified.
func (c *batchAPI) LTrim(key string, start, stop int64) *ReplyInteger {
	c.args.Key(key)
	c.args.Int(start)
	c.args.Int(stop)
	return c.doInteger("LTRIM")
}

// RPop removes and returns the last element of the list stored at key.
func (c *batchAPI) RPop(key string) *ReplyBulkString {
	c.args.Key(key)
	return c.doBulkString("RPOP")
}

// RPopLPush atomically returns and removes the last element (tail) of the list stored at source, and pushes the element at the first element (head) of the list stored at destination.
func (c *batchAPI) RPopLPush(src, dest string, timeout time.Duration) *ReplyBulkString {
	c.args.Key(src)
	c.args.Key(dest)
	c.args.Milliseconds(timeout)
	return c.doBulkString("RPOPLPUSH")
}

// RPush inserts specified values at the tail of the list stored at key.
func (c *batchAPI) RPush(key string, elements ...Arg) *ReplyInteger {
	c.args.Key(key)
	c.args.Append(elements...)
	return c.doInteger("RPUSH")
}

// RPushX Inserts specified values at the tail of the list stored at key, only if key already exists and holds a list.
func (c *batchAPI) RPushX(key string, elements ...Arg) *ReplyInteger {
	c.args.Key(key)
	c.args.Append(elements...)
	return c.doInteger("RPUSHX")
}
