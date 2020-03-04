package red

import (
	"fmt"
	"time"

	"github.com/alxarch/red/resp"
)

// Keys

// Del adds a DEL command to the pipeline
func (c *Client) Del(key string, keys ...string) *ReplyInteger {
	c.args.Key(key)
	c.args.Keys(keys...)
	reply := ReplyInteger{}
	c.do("DEL", &reply)
	return &reply
}

// Dump adds a DUMP command
func (c *Client) Dump(key string) *ReplyBulkString {
	reply := ReplyBulkString{}
	c.args.Key(key)
	c.do("DUMP", &reply)
	return &reply
}

// Exists is redis EXISTS command
func (c *Client) Exists(keys ...string) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Keys(keys...)
	c.do("EXISTS", &reply)
	return &reply
}

// Expire is redis EXPIRE command
func (c *Client) Expire(key string, ttl time.Duration) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Key(key)
	c.args.Arg(Seconds(ttl))
	c.do("EXPIRE", &reply)
	return &reply
}

// ExpireAt is redis EXPIREAT command
func (c *Client) ExpireAt(key string, tm time.Time) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Key(key)
	c.args.Arg(UnixSeconds(tm))
	c.do("EXPIREAT", &reply)
	return &reply
}

// Keys returns all keys matching a pattern
func (c *Client) Keys(pattern string) *ReplyBulkStringArray {
	reply := ReplyBulkStringArray{}
	if pattern == "" {
		pattern = "*"
	}
	c.args.String("MATCH")
	c.args.String(pattern)
	c.do("KEYS", &reply)
	return &reply
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
	reply := ReplyOK{}
	c.do("MIGRATE", &reply)
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
	return &reply
}

// Move moves a key to a different DB index
func (c *Client) Move(key string, db int) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Key(key)
	c.args.Int(int64(db))
	c.do("MOVE", &reply)
	return &reply
}

// ObjectRefCount is the redis' OBJECT REFCOUNT command
func (c *Client) ObjectRefCount(key string) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.String("REFCOUNT")
	c.args.Key(key)
	c.do("OBJECT", &reply)
	return &reply
}

// ObjectEncoding is the redis' OBJECT ENCODING command
func (c *Client) ObjectEncoding(key string) *ReplyBulkString {
	reply := ReplyBulkString{}
	c.args.String("ENCODING")
	c.args.Key(key)
	c.do("OBJECT", &reply)
	return &reply
}

// ObjectIdleTime is the redis' OBJECT IDLETIME command
func (c *Client) ObjectIdleTime(key string) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.String("IDLETIME")
	c.args.Key(key)
	c.do("OBJECT", &reply)
	return &reply
}

// ObjectFreq is the redis' OBJECT FREQ command
func (c *Client) ObjectFreq(key string) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.String("FREQ")
	c.args.Key(key)
	c.do("OBJECT", &reply)
	return &reply
}

// ObjectHelp is the redis' OBJECT HELP command
func (c *Client) ObjectHelp(key string) *ReplyBulkString {
	reply := ReplyBulkString{}
	c.args.String("HELP")
	c.args.Key(key)
	c.do("OBJECT", &reply)
	return &reply
}

// Persist removes any TTL from a key
func (c *Client) Persist(key string) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Key(key)
	c.do("PERSIST", &reply)
	return &reply
}

// PExpire adds a TTL to a key in milliseconds
func (c *Client) PExpire(key string, ttl time.Duration) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Key(key)
	c.args.Arg(Milliseconds(ttl))
	c.do("PEXPIRE", &reply)
	return &reply
}

// PExpireAt is redis PEXPIREAT command
func (c *Client) PExpireAt(key string, tm time.Time) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Key(key)
	c.args.Arg(UnixSeconds(tm))
	c.do("PEXPIREAT", &reply)
	return &reply
}

// PTTL gets the TTL of a key in milliseconds
func (c *Client) PTTL(key string) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Key(key)
	c.do("PTTL", &reply)
	return &reply
}

// RandomKey returns a random key
func (c *Client) RandomKey() *ReplyBulkString {
	reply := ReplyBulkString{}
	c.do("RANDOMKEY", &reply)
	return &reply
}

// Rename renames a key
func (c *Client) Rename(key, newkey string) *ReplyOK {
	reply := ReplyOK{}
	c.args.Key(key)
	c.args.Key(newkey)
	c.do("RENAME", &reply)
	return &reply
}

// RenameNX renames a key if the new name does not exist
func (c *Client) RenameNX(key, newkey string) *ReplyOK {
	reply := ReplyOK{}
	c.args.Key(key)
	c.args.Key(newkey)
	c.do("RENAMENX", &reply)
	return &reply
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
	reply := ReplyOK{}
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
	c.do("RESTORE", &reply)
	return &reply
}

// Sort sorts keys
type Sort struct {
	By           string
	Offset       int64
	Count        int64
	Get          []string
	Order        SortOrder
	Alphanumeric bool
	Store        string
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

// ReplySort is the reply of redis' SORT command
type ReplySort struct {
	Sort
	sorted resp.BulkStringArray
	stored resp.Integer
	replyBase
}

// Reply returns the SORT reply
func (r *ReplySort) Reply() ([]string, int64, error) {
	return r.sorted, int64(r.stored), r.err
}

// UnmarshalRESP implements resp.Unmarshaler interface
func (r *ReplySort) reply(v resp.Value) error {
	defer r.tee(v)
	switch v.Type() {
	case resp.TypeArray:
		if r.Store == "" {
			r.err = v.Decode(&r.sorted)
			return nil
		}
	case resp.TypeInteger:
		if r.Store != "" {
			r.err = v.Decode(&r.stored)
			return nil
		}
	case resp.TypeError:
		r.err = v.Err()
		return nil
	}
	r.err = fmt.Errorf("Invalid sort reply %v", v)
	return nil
}

// Sort sorts a key's values
func (c *Client) Sort(key string, sort Sort) *ReplySort {
	reply := ReplySort{
		Sort: sort,
	}
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
	if sort.Store != "" {
		args.String("STORE")
		args.Key(sort.Store)
	}
	c.do("SORT", &reply)
	return &reply
}

// Touch alters the last access time of a key(s).
func (c *Client) Touch(keys ...string) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Keys(keys...)
	c.do("TOUCH", &reply)
	return &reply
}

// TTL returns the remaining lifetime of a key in seconds
func (c *Client) TTL(key string) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Key(key)
	c.do("TTL", &reply)
	return &reply
}

// Type returns the type of the value of a key
func (c *Client) Type(key string) *ReplySimpleString {
	reply := ReplySimpleString{}
	c.args.Key(key)
	c.do("TYPE", &reply)
	return &reply
}

// Unlink drops keys
func (c *Client) Unlink(keys ...string) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Keys(keys...)
	c.do("UNLINK", &reply)
	return &reply
}

// Wait blocks until a number of replicas have stored the data or timeout occured
func (c *Client) Wait(numReplicas int, timeout time.Duration) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Int(int64(numReplicas))
	c.args.Arg(Milliseconds(timeout))
	c.do("WAIT", &reply)
	return &reply
}
