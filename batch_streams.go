package red

import (
	"time"

	"github.com/alxarch/red/resp"
)

type XAck struct {
	Key   string
	Group string
	IDs   []string
	// replyInteger
}

// Command implements Commander interface
func (b *batchAPI) XAck(key, group string, ids ...string) *ReplyInteger {
	b.args.Key(key)
	b.args.String(group)
	b.args.Strings(ids...)
	return b.doInteger("XACK")
}

type XAdd struct {
	Key    string
	MaxLen int64
	ID     string
	Fields []HArg
	// replyBulkString
}

// Command implements Commander interface
func (b *batchAPI) XAdd(key string, maxLen int64, id string, fields ...HArg) *ReplyBulkString {
	b.args.Key(key)
	if maxLen > 0 {
		b.args.String("MAXLEN")
		b.args.String("~")
		b.args.Int(maxLen)
	}
	if id == "" {
		id = "*"
	}
	b.args.String(id)
	for i := range fields {
		field := &fields[i]
		b.args.String(field.Field)
		b.args.Arg(field.Value)
	}
	return b.doBulkString("XADD")
}

type XClaim struct {
	Key         string
	Group       string
	Consumer    string
	MinIdleTime time.Duration
	IDs         []string
	Idle        time.Duration
	RetryCount  int
	Force       bool
	Time        time.Time
	JustID      bool
}

// BuildCommand implements CommandBuilder interface
func (cmd *XClaim) BuildCommand(args *ArgBuilder) string {
	//  XCLAIM key group consumer min-idle-time ID [ID ...] [IDLE ms] [TIME ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID]
	args.Key(cmd.Key)
	args.String(cmd.Group)
	args.String(cmd.Consumer)
	args.Milliseconds(cmd.MinIdleTime)
	args.String("ID")
	args.Strings(cmd.IDs...)
	if idle := cmd.Idle.Truncate(time.Millisecond); idle > 0 {
		args.String("IDLE")
		args.Milliseconds(cmd.Idle)
	} else if !cmd.Time.IsZero() {
		args.String("TIME")
		args.Arg(UnixMilliseconds(cmd.Time))
	}
	if cmd.RetryCount > 0 {
		args.String("RETRYCOUNT")
		args.Int(int64(cmd.RetryCount))
	}
	args.Flag("FORCE", cmd.Force)
	args.Flag("JUSTID", cmd.JustID)
	return "XCLAIM"
}

// XDel deletes entries from a stream
func (b *batchAPI) XDel(key string, ids ...string) *ReplyInteger {
	b.args.Key(key)
	b.args.Strings(ids...)
	return b.doInteger("XDEL")
}

// XGroupCreate adds an XGROUP CREATE redis command
func (b *batchAPI) XGroupCreate(key, group, id string, makeStream bool) *ReplyOK {
	b.args.String("CREATE")
	b.args.Key(key)
	b.args.String(group)
	if id == "" {
		id = "$"
	}
	b.args.String(id)
	b.args.Flag("MKSTREAM", makeStream)
	return b.doSimpleStringOK("XGROUP", 0)
}

// Command implements Commander interface
func (b *batchAPI) XGroupSetID(key, group, id string) *ReplyOK {
	b.args.String("SETID")
	b.args.Key(key)
	b.args.String(group)
	if id == "" {
		id = "$"
	}
	b.args.String(id)
	return b.doSimpleStringOK("XGROUP", 0)
}

// Command implements Commander interface
func (b *batchAPI) XGroupDestroy(key, group string) *ReplyInteger {
	b.args.String("DESTROY")
	b.args.Key(key)
	b.args.String(group)
	return b.doInteger("XGROUP")
}

// Command implements Commander interface
func (b *batchAPI) XGroupDelConsumer(key, group, consumer string) *ReplyInteger {
	b.args.String("DELCONSUMER")
	b.args.Key(key)
	b.args.String(group)
	b.args.String(consumer)
	return b.doInteger("XGROUP")
}

func (b *batchAPI) XInfoConsumers(key, group string) *ReplyAny {
	b.args.String("CONSUMERS")
	b.args.Key(key)
	b.args.String(group)
	return b.doAny("XINFO")

}
func (b *batchAPI) XInfoGroups(key string) *ReplyAny {
	b.args.String("GROUPS")
	b.args.Key(key)
	return b.doAny("XINFO")
}

func (b *batchAPI) XInfoStream(key string) *ReplyAny {
	b.args.String("STREAM")
	b.args.Key(key)
	return b.doAny("XINFO")
}
func (b *batchAPI) XInfoHelp() *ReplyAny {
	b.args.String("HELP")
	return b.doAny("XINFO")
}

func (b *batchAPI) XLen(key string) *ReplyInteger {
	b.args.Key(key)
	return b.doInteger("XLEN")
}

type StreamRecord struct {
	ID     string
	Record []HArg
}

// UnmarshalRESP implements resp.Unmarshaler interface
func (s *StreamRecord) UnmarshalRESP(v resp.Value) error {
	return v.Decode([]interface{}{
		&s.ID,
		(*HSet)(&s.Record),
	})
}

type ReplyXRange struct {
	records []StreamRecord
	batchReply
}

func (r *ReplyXRange) Reply() ([]StreamRecord, error) {
	return r.records, r.Err()
}

// Command implements Commander interface
func (b *batchAPI) XRange(key, group, start, end string, count int64) *ReplyXRange {
	if start == "" {
		start = "-"
	}
	if end == "" {
		end = "+"
	}
	b.args.Key(key)
	b.args.String(start)
	b.args.String(end)
	if count > 0 {
		b.args.String("COUNT")
		b.args.Int(count)
	}
	reply := ReplyXRange{}
	reply.Bind(&reply.records)
	b.do("XRANGE", &reply.batchReply)
	return &reply
}

func (b *batchAPI) XRevRange(key, group, end, start string, count int64) *ReplyXRange {
	if start == "" {
		start = "-"
	}
	if end == "" {
		end = "+"
	}
	b.args.Key(key)
	b.args.String(start)
	b.args.String(end)
	if count > 0 {
		b.args.String("COUNT")
		b.args.Int(count)
	}
	reply := ReplyXRange{}
	reply.Bind(&reply.records)
	b.do("XREVRANGE", &reply.batchReply)
	return &reply
}

type XPending struct {
	Key        string
	Group      string
	Consumer   string
	Start, End string
	Count      int64
}

// BuildCommand implements CommandBuilder interface
func (cmd *XPending) BuildCommand(args *ArgBuilder) string {

	//  XPENDING key group [start end count] [consumer]
	args.Key(cmd.Key)
	args.String(cmd.Group)
	if cmd.Count > 0 {
		start := cmd.Start
		if start == "" {
			start = "-"
		}
		end := cmd.End
		if end == "" {
			end = "+"
		}
		args.String(start)
		args.String(end)
		args.Int(cmd.Count)
	}
	if cmd.Consumer != "" {
		args.String(cmd.Consumer)
	}
	return "XPENDING"
}

type XReadStream struct {
	Key    string
	LastID string
}

func Stream(key, id string) XReadStream {
	return XReadStream{Key: key, LastID: id}
}

type XRead struct {
	Count    int64
	Group    string
	Consumer string
	NoACK    bool
	Block    time.Duration
	Streams  []XReadStream
}

// BuildCommand implements CommandBuilder interface
func (cmd *XRead) BuildCommand(args *ArgBuilder) string {

	if cmd.Group != "" {
		args.String("GROUP")
		args.String(cmd.Group)
		args.String(cmd.Consumer)
	}
	if cmd.Count > 0 {
		args.String("COUNT")
		args.Int(cmd.Count)
	}
	if cmd.Block > time.Millisecond {
		args.String("BLOCK")
		args.Milliseconds(cmd.Block)
	}
	args.Flag("NOACK", cmd.Group != "" && cmd.NoACK)
	args.String("STREAMS")
	for i := range cmd.Streams {
		s := &cmd.Streams[i]
		id := s.LastID
		if id == "" {
			id = "$"
		}
		args.Key(s.Key)
		args.String(id)
	}
	if cmd.Group == "" {
		return "XREAD"
	}
	return "XREADGROUP"

}
