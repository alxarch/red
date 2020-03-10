package red

import (
	"time"
)

// Keys

// Del adds a DEL command to the pipeline
func (b *batchAPI) Del(key string, keys ...string) *ReplyInteger {
	b.args.Key(key)
	b.args.Keys(keys...)
	return b.doInteger("DEL")
}

// Dump adds a DUMP command
func (b *batchAPI) Dump(key string) *ReplyBulkString {
	b.args.Key(key)
	return b.doBulkString("DUMP")
}

// Exists is redis EXISTS command
func (b *batchAPI) Exists(keys ...string) *ReplyInteger {
	b.args.Keys(keys...)
	return b.doInteger("EXISTS")
}

// Expire is redis EXPIRE command
func (b *batchAPI) Expire(key string, ttl time.Duration) *ReplyInteger {
	b.args.Key(key)
	b.args.Seconds(ttl)
	return b.doInteger("EXPIRE")
}

// ExpireAt is redis EXPIREAT command
func (b *batchAPI) ExpireAt(key string, tm time.Time) *ReplyInteger {
	b.args.Key(key)
	b.args.Arg(UnixSeconds(tm))
	return b.doInteger("EXPIREAT")
}

// Keys returns all keys matching a pattern
func (b *batchAPI) Keys(pattern string) *ReplyBulkStringArray {
	if pattern == "" {
		pattern = "*"
	}
	b.args.String("MATCH")
	b.args.String(pattern)
	return b.doBulkStringArray("KEYS")
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
func (b *batchAPI) Migrate(m Migrate) *ReplyOK {
	b.args.String(m.Host)
	b.args.Int(int64(m.Port))
	b.args.String("")
	b.args.Int(int64(m.DestinasionDB))
	b.args.Arg(Milliseconds(m.Timeout))
	b.args.Flag("COPY", m.Copy)
	b.args.Flag("REPLACE", m.Replace)
	b.args.Option("AUTH", m.Auth)
	b.args.String("KEYS")
	b.args.Keys(m.Keys...)
	return b.doSimpleStringOK("MIGRATE", 0)
}

// Move moves a key to a different DB index
func (b *batchAPI) Move(key string, db int) *ReplyInteger {
	b.args.Key(key)
	b.args.Int(int64(db))
	return b.doInteger("MOVE")
}

// ObjectRefCount is the redis' OBJECT REFCOUNT command
func (b *batchAPI) ObjectRefCount(key string) *ReplyInteger {
	b.args.String("REFCOUNT")
	b.args.Key(key)
	return b.doInteger("OBJECT")
}

// ObjectEncoding is the redis' OBJECT ENCODING command
func (b *batchAPI) ObjectEncoding(key string) *ReplyBulkString {
	b.args.String("ENCODING")
	b.args.Key(key)
	return b.doBulkString("OBJECT")
}

// ObjectIdleTime is the redis' OBJECT IDLETIME command
func (b *batchAPI) ObjectIdleTime(key string) *ReplyInteger {
	b.args.String("IDLETIME")
	b.args.Key(key)
	return b.doInteger("OBJECT")
}

// ObjectFreq is the redis' OBJECT FREQ command
func (b *batchAPI) ObjectFreq(key string) *ReplyInteger {
	b.args.String("FREQ")
	b.args.Key(key)
	return b.doInteger("OBJECT")
}

// ObjectHelp is the redis' OBJECT HELP command
func (b *batchAPI) ObjectHelp(key string) *ReplyBulkString {
	b.args.String("HELP")
	b.args.Key(key)
	return b.doBulkString("OBJECT")
}

// Persist removes any TTL from a key
func (b *batchAPI) Persist(key string) *ReplyInteger {
	b.args.Key(key)
	return b.doInteger("PERSIST")
}

// PExpire adds a TTL to a key in milliseconds
func (b *batchAPI) PExpire(key string, ttl time.Duration) *ReplyInteger {
	b.args.Key(key)
	b.args.Arg(Milliseconds(ttl))
	return b.doInteger("PEXPIRE")
}

// PExpireAt is redis PEXPIREAT command
func (b *batchAPI) PExpireAt(key string, tm time.Time) *ReplyInteger {
	b.args.Key(key)
	b.args.Arg(UnixSeconds(tm))
	return b.doInteger("PEXPIREAT")
}

// PTTL gets the TTL of a key in milliseconds
func (b *batchAPI) PTTL(key string) *ReplyInteger {
	b.args.Key(key)
	return b.doInteger("PTTL")
}

// RandomKey returns a random key
func (b *batchAPI) RandomKey() *ReplyBulkString {
	return b.doBulkString("RANDOMKEY")
}

// Rename renames a key
func (b *batchAPI) Rename(key, newkey string) *ReplyOK {
	b.args.Key(key)
	b.args.Key(newkey)
	return b.doSimpleStringOK("RENAME", 0)
}

// RenameNX renames a key if the new name does not exist
func (b *batchAPI) RenameNX(key, newkey string) *ReplyOK {
	b.args.Key(key)
	b.args.Key(newkey)
	return b.doSimpleStringOK("RENAMENX", NX)
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
func (b *batchAPI) Restore(r Restore) *ReplyOK {
	b.args.Key(r.Key)
	b.args.Arg(Milliseconds(r.TTL))
	b.args.String(r.Value)
	if r.Replace {
		b.args.String("REPLACE")
	}
	if r.AbsoluteTTL {
		b.args.String("ABSTTL")
	}
	if r.IdleTime > time.Second {
		b.args.Append(String("IDLETIIME"), Seconds(r.IdleTime))
	}
	if r.Frequency > 0 {
		b.args.Append(String("FREQ"), Int64(r.Frequency))
	}
	return b.doSimpleStringOK("RESTORE", 0)
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
func (b *batchAPI) Sort(key string, sort Sort) *ReplyBulkStringArray {
	b.args.Key(key)
	b.args.Key(key)
	if sort.By != "" {
		b.args.String("BY")
		b.args.String(sort.By)
	}
	if sort.Count > 0 {
		b.args.String("LIMIT")
		b.args.Int(sort.Offset)
		b.args.Int(sort.Count)
	}
	for i := range sort.Get {
		b.args.String("GET")
		b.args.String(sort.Get[i])
	}
	if ord := sort.Order.String(); ord != "" {
		b.args.String(ord)
	}
	if sort.Alphanumeric {
		b.args.String("ALPHA")
	}
	return b.doBulkStringArray("SORT")
}

// SortStore sorts a `key`'s value storing the result in `dest`
func (b *batchAPI) SortStore(dest, key string, sort Sort) *ReplyInteger {
	b.args.Key(key)
	b.args.Key(key)
	if sort.By != "" {
		b.args.String("BY")
		b.args.String(sort.By)
	}
	if sort.Count > 0 {
		b.args.String("LIMIT")
		b.args.Int(sort.Offset)
		b.args.Int(sort.Count)
	}
	for i := range sort.Get {
		b.args.String("GET")
		b.args.String(sort.Get[i])
	}
	if ord := sort.Order.String(); ord != "" {
		b.args.String(ord)
	}
	if sort.Alphanumeric {
		b.args.String("ALPHA")
	}
	b.args.String("STORE")
	b.args.Key(dest)
	return b.doInteger("SORT")
}

// Touch alters the last access time of a key(s).
func (b *batchAPI) Touch(keys ...string) *ReplyInteger {
	b.args.Keys(keys...)
	return b.doInteger("TOUCH")
}

// TTL returns the remaining lifetime of a key in seconds
func (b *batchAPI) TTL(key string) *ReplyInteger {
	b.args.Key(key)
	return b.doInteger("TTL")
}

// Type returns the type of the value of a key
func (b *batchAPI) Type(key string) *ReplySimpleString {
	b.args.Key(key)
	return b.doSimpleString("TYPE")
}

// Unlink drops keys
func (b *batchAPI) Unlink(keys ...string) *ReplyInteger {
	b.args.Keys(keys...)
	return b.doInteger("UNLINK")
}

// Wait blocks until a number of replicas have stored the data or timeout occured
func (b *batchAPI) Wait(numReplicas int, timeout time.Duration) *ReplyInteger {
	b.args.Int(int64(numReplicas))
	b.args.Arg(Milliseconds(timeout))
	return b.doInteger("WAIT")
}
