package red

import (
	"time"
)

// Keys

// Del adds a DEL command to the pipeline
func (c *Client) Del(key string, keys ...string) *ReplyInteger {
	c.args.Key(key)
	c.args.Keys(keys...)
	return c.doInteger("DEL")
}

// Dump adds a DUMP command
func (c *Client) Dump(key string) *ReplyBulkString {
	c.args.Key(key)
	return c.doBulkString("DUMP")
}

// Exists is redis EXISTS command
func (c *Client) Exists(keys ...string) *ReplyInteger {
	c.args.Keys(keys...)
	return c.doInteger("EXISTS")
}

// Expire is redis EXPIRE command
func (c *Client) Expire(key string, ttl time.Duration) *ReplyInteger {
	c.args.Key(key)
	c.args.Arg(Seconds(ttl))
	return c.doInteger("EXPIRE")
}

// ExpireAt is redis EXPIREAT command
func (c *Client) ExpireAt(key string, tm time.Time) *ReplyInteger {
	c.args.Key(key)
	c.args.Arg(UnixSeconds(tm))
	return c.doInteger("EXPIREAT")
}

// Keys returns all keys matching a pattern
func (c *Client) Keys(pattern string) *ReplyBulkStringArray {
	if pattern == "" {
		pattern = "*"
	}
	c.args.String("MATCH")
	c.args.String(pattern)
	return c.doBulkStringArray("KEYS")
}

// Migrate moves data across servers
type Migrate struct {
	Host          string
	Port          int
	DestinasionDB int
	Timeout       time.Duration
	Copy          bool
	Replace       bool
	Auth          string
	Keys          []string
}

// Migrate moves data across servers
func (c *Client) Migrate(m Migrate) *ReplyOK {
	c.args.String(m.Host)
	c.args.Int(int64(m.Port))
	c.args.String("")
	c.args.Int(int64(m.DestinasionDB))
	c.args.Arg(Milliseconds(m.Timeout))
	c.args.Flag("COPY", m.Copy)
	c.args.Flag("REPLACE", m.Replace)
	c.args.Option("AUTH", m.Auth)
	c.args.String("KEYS")
	c.args.Keys(m.Keys...)
	return c.doSimpleStringOK("MIGRATE", 0)
}

// Move moves a key to a different DB index
func (c *Client) Move(key string, db int) *ReplyInteger {
	c.args.Key(key)
	c.args.Int(int64(db))
	return c.doInteger("MOVE")
}

// ObjectRefCount is the redis' OBJECT REFCOUNT command
func (c *Client) ObjectRefCount(key string) *ReplyInteger {
	c.args.String("REFCOUNT")
	c.args.Key(key)
	return c.doInteger("OBJECT")
}

// ObjectEncoding is the redis' OBJECT ENCODING command
func (c *Client) ObjectEncoding(key string) *ReplyBulkString {
	c.args.String("ENCODING")
	c.args.Key(key)
	return c.doBulkString("OBJECT")
}

// ObjectIdleTime is the redis' OBJECT IDLETIME command
func (c *Client) ObjectIdleTime(key string) *ReplyInteger {
	c.args.String("IDLETIME")
	c.args.Key(key)
	return c.doInteger("OBJECT")
}

// ObjectFreq is the redis' OBJECT FREQ command
func (c *Client) ObjectFreq(key string) *ReplyInteger {
	c.args.String("FREQ")
	c.args.Key(key)
	return c.doInteger("OBJECT")
}

// ObjectHelp is the redis' OBJECT HELP command
func (c *Client) ObjectHelp(key string) *ReplyBulkString {
	c.args.String("HELP")
	c.args.Key(key)
	return c.doBulkString("OBJECT")
}

// Persist removes any TTL from a key
func (c *Client) Persist(key string) *ReplyInteger {
	c.args.Key(key)
	return c.doInteger("PERSIST")
}

// PExpire adds a TTL to a key in milliseconds
func (c *Client) PExpire(key string, ttl time.Duration) *ReplyInteger {
	c.args.Key(key)
	c.args.Arg(Milliseconds(ttl))
	return c.doInteger("PEXPIRE")
}

// PExpireAt is redis PEXPIREAT command
func (c *Client) PExpireAt(key string, tm time.Time) *ReplyInteger {
	c.args.Key(key)
	c.args.Arg(UnixSeconds(tm))
	return c.doInteger("PEXPIREAT")
}

// PTTL gets the TTL of a key in milliseconds
func (c *Client) PTTL(key string) *ReplyInteger {
	c.args.Key(key)
	return c.doInteger("PTTL")
}

// RandomKey returns a random key
func (c *Client) RandomKey() *ReplyBulkString {
	return c.doBulkString("RANDOMKEY")
}

// Rename renames a key
func (c *Client) Rename(key, newkey string) *ReplyOK {
	c.args.Key(key)
	c.args.Key(newkey)
	return c.doSimpleStringOK("RENAME", 0)
}

// RenameNX renames a key if the new name does not exist
func (c *Client) RenameNX(key, newkey string) *ReplyOK {
	c.args.Key(key)
	c.args.Key(newkey)
	return c.doSimpleStringOK("RENAMENX", NX)
}

// Restore restores a key value from a string
type Restore struct {
	Key         string
	TTL         time.Duration
	Value       string
	Replace     bool
	AbsoluteTTL bool
	IdleTime    time.Duration
	Frequency   int64
}

// Restore restores a key value from a string
func (c *Client) Restore(r Restore) *ReplyOK {
	args := &c.args
	args.Key(r.Key)
	args.Arg(Milliseconds(r.TTL))
	args.String(r.Value)
	if r.Replace {
		args.String("REPLACE")
	}
	if r.AbsoluteTTL {
		args.String("ABSTTL")
	}
	if r.IdleTime > time.Second {
		args.Append(String("IDLETIIME"), Seconds(r.IdleTime))
	}
	if r.Frequency > 0 {
		args.Append(String("FREQ"), Int64(r.Frequency))
	}
	return c.doSimpleStringOK("RESTORE", 0)
}

// Sort sorts keys
type Sort struct {
	By           string
	Offset       int64
	Count        int64
	Get          []string
	Order        SortOrder
	Alphanumeric bool
}

// SortOrder defines sorting order
type SortOrder int

// Sort orders ASC=1, DESC=2
const (
	_ SortOrder = iota
	SortAscending
	SortDescending
)

func (o SortOrder) String() string {
	switch o {
	case SortAscending:
		return "ASC"
	case SortDescending:
		return "DESC"
	default:
		return ""
	}

}

// Sort sorts a key's values
func (c *Client) Sort(key string, sort Sort) *ReplyBulkStringArray {
	args := &c.args
	args.Key(key)
	args.Key(key)
	if sort.By != "" {
		args.String("BY")
		args.String(sort.By)
	}
	if sort.Count > 0 {
		args.String("LIMIT")
		args.Int(sort.Offset)
		args.Int(sort.Count)
	}
	for i := range sort.Get {
		args.String("GET")
		args.String(sort.Get[i])
	}
	if ord := sort.Order.String(); ord != "" {
		args.String(ord)
	}
	if sort.Alphanumeric {
		args.String("ALPHA")
	}
	return c.doBulkStringArray("SORT")
}

// SortStore sorts a `key`'s value storing the result in `dest`
func (c *Client) SortStore(dest, key string, sort Sort) *ReplyInteger {
	args := &c.args
	args.Key(key)
	args.Key(key)
	if sort.By != "" {
		args.String("BY")
		args.String(sort.By)
	}
	if sort.Count > 0 {
		args.String("LIMIT")
		args.Int(sort.Offset)
		args.Int(sort.Count)
	}
	for i := range sort.Get {
		args.String("GET")
		args.String(sort.Get[i])
	}
	if ord := sort.Order.String(); ord != "" {
		args.String(ord)
	}
	if sort.Alphanumeric {
		args.String("ALPHA")
	}
	args.String("STORE")
	args.Key(dest)
	return c.doInteger("SORT")
}

// Touch alters the last access time of a key(s).
func (c *Client) Touch(keys ...string) *ReplyInteger {
	c.args.Keys(keys...)
	return c.doInteger("TOUCH")
}

// TTL returns the remaining lifetime of a key in seconds
func (c *Client) TTL(key string) *ReplyInteger {
	c.args.Key(key)
	return c.doInteger("TTL")
}

// Type returns the type of the value of a key
func (c *Client) Type(key string) *ReplySimpleString {
	c.args.Key(key)
	return c.doSimpleString("TYPE")
}

// Unlink drops keys
func (c *Client) Unlink(keys ...string) *ReplyInteger {
	c.args.Keys(keys...)
	return c.doInteger("UNLINK")
}

// Wait blocks until a number of replicas have stored the data or timeout occured
func (c *Client) Wait(numReplicas int, timeout time.Duration) *ReplyInteger {
	c.args.Int(int64(numReplicas))
	c.args.Arg(Milliseconds(timeout))
	return c.doInteger("WAIT")
}
