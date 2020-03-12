package red

import (
	"time"

	"github.com/alxarch/red/resp"
)

// XAck removes one or multiple messages from the pending entries list (PEL) of a stream consumer group.
// XACK key group ID [ID ...]
func (b *batchAPI) XAck(key, group string, id string, ids ...string) *ReplyInteger {
	b.args.Key(key)
	b.args.String(group)
	b.args.Unique(id, ids...)
	return b.doInteger("XACK")
}

// XAdd appends the specified stream entry to the stream at the specified key.
//
//     XADD key ID field value [field value ...]
//
// If the key does not exist, as a side effect of running this command,
// the key is created with a stream value.
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

// XClaim claims a pending message in a consumer group.
//
//   XCLAIM key group consumer min-idle-time ID [ID ...] [IDLE ms] [TIME ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID]
//
//   Available since 5.0.0.
//
//   Time complexity: O(log N) with N being the number of messages in the PEL of the consumer group.
//
// In the context of a stream consumer group, this command changes the ownership of a pending message,
// so that the new owner is the consumer specified as the command argument. Normally this is what happens:
//
// 1. There is a stream with an associated consumer group.
// 2. Some consumer A reads a message via XREADGROUP from a stream, in the context of that consumer group.
// 3. As a side effect a pending message entry is created in the pending entries list (PEL)
//    of the consumer group: it means the message was delivered to a given consumer,
//    but it was not yet acknowledged via XACK.
// 4. Then suddenly that consumer fails forever.
// 5. Other consumers may inspect the list of pending messages, that are stale for quite some time,
//    using the XPENDING command. In order to continue processing such messages, they use XCLAIM
//    to acquire the ownership of the message and continue.
//
// This dynamic is clearly explained in the [Stream intro documentation](https://redis.io/topics/streams-intro).
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

// XGroupCreate creates a new consumer group associated with a stream.
// XGROUP [CREATE key groupname id-or-$]
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

// XGroupSetID sets the consumer group last delivered ID to something else.
// XGROUP [SETID key groupname id-or-$]
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

// XGroupDestroy destroys a consumer group
func (b *batchAPI) XGroupDestroy(key, group string) *ReplyInteger {
	b.args.String("DESTROY")
	b.args.Key(key)
	b.args.String(group)
	return b.doInteger("XGROUP")
}

// XGroupDelConsumer removes a specific consumer from a consumer group
func (b *batchAPI) XGroupDelConsumer(key, group, consumer string) *ReplyInteger {
	b.args.String("DELCONSUMER")
	b.args.Key(key)
	b.args.String(group)
	b.args.String(consumer)
	return b.doInteger("XGROUP")
}

type XInfoStream struct {
	Length          int64
	RadixTreeKeys   int64
	RadixTreeNodes  int64
	Groups          int64
	LastGeneratedID resp.BulkString
	FirstEntry      XInfoEntry
	LastEntry       XInfoEntry
}
type ReplyXInfoStream struct {
	stream XInfoStream
	batchReply
}

func (r *ReplyXInfoStream) Reply() (XInfoStream, error) {
	return r.stream, r.err
}

func (info *XInfoStream) UnmarshalRESP(v resp.Value) error {
	var values resp.SimpleStringRecord
	if err := values.UnmarshalRESP(v); err != nil {
		return err
	}
	s := XInfoStream{}
	if n, ok := values["length"].(resp.Integer); ok {
		s.Length = int64(n)
	}
	if n, ok := values["radix-tree-keys"].(resp.Integer); ok {
		s.RadixTreeKeys = int64(n)
	}
	if n, ok := values["radix-tree-nodes"].(resp.Integer); ok {
		s.RadixTreeNodes = int64(n)
	}
	if n, ok := values["groups"].(resp.Integer); ok {
		s.Groups = int64(n)
	}
	if id, ok := values["last-generated-id"].(*resp.BulkString); ok {
		s.LastGeneratedID = *id
	}

	if entry, ok := values["last-entry"].(resp.Array); ok {
		if err := entry.Decode([]interface{}{
			&s.LastEntry.ID,
			&s.LastEntry.Values,
		}); err != nil {
			return err
		}
	}

	if entry, ok := values["first-entry"].(resp.Array); ok {
		if err := entry.Decode([]interface{}{
			&s.FirstEntry.ID,
			&s.FirstEntry.Values,
		}); err != nil {
			return err
		}
	}
	return nil
}

type XInfoEntry struct {
	ID     string
	Values map[string]string
}

func (info *XInfoEntry) UnmarshalRESP(v resp.Value) error {
	return v.Decode([]interface{}{
		&info.ID,
		&info.Values,
	})
}

type XInfoGroup struct {
	Name      string
	Consumers int64
	Pending   int64
}

func (info *XInfoGroup) UnmarshalRESP(v resp.Value) error {
	var values resp.SimpleStringRecord
	if err := values.UnmarshalRESP(v); err != nil {
		return err
	}
	var c XInfoGroup
	if name, ok := values["name"].(*resp.BulkString); ok && name.Valid {
		c.Name = name.String
	}
	if pending, ok := values["pending"].(resp.Integer); ok {
		c.Pending = int64(pending)
	}
	if consumers, ok := values["consumers"].(resp.Integer); ok {
		c.Consumers = int64(consumers)
	}
	*info = c
	return nil
}

type XInfoConsumer struct {
	Name    string
	Pending int64
	Idle    time.Duration
}

func (info *XInfoConsumer) UnmarshalRESP(v resp.Value) error {
	var values resp.SimpleStringRecord
	if err := values.UnmarshalRESP(v); err != nil {
		return err
	}
	var c XInfoConsumer
	if name, ok := values["name"].(*resp.BulkString); ok && name.Valid {
		c.Name = name.String
	}
	if pending, ok := values["pending"].(resp.Integer); ok {
		c.Pending = int64(pending)
	}
	if idle, ok := values["idle"].(resp.Integer); ok {
		c.Idle = time.Duration(idle) * time.Millisecond
	}
	*info = c
	return nil
}

// XInfoConsumers returns the list of every consumer in a specific consumer group
func (b *batchAPI) XInfoConsumers(key, group string) *ReplyXInfoConsumers {
	b.args.String("CONSUMERS")
	b.args.Key(key)
	b.args.String(group)
	reply := ReplyXInfoConsumers{}
	reply.Bind(&reply.consumers)
	b.do("XINFO", &reply.batchReply)
	return &reply
}

// XInfoGroups returns all the consumer groups associated with the stream.
func (b *batchAPI) XInfoGroups(key string) *ReplyXInfoGroups {
	b.args.String("GROUPS")
	b.args.Key(key)
	reply := ReplyXInfoGroups{}
	reply.Bind(&reply.groups)
	b.do("XINFO", &reply.batchReply)
	return &reply
}

type ReplyXInfoGroups struct {
	groups []XInfoGroup
	batchReply
}

func (r *ReplyXInfoGroups) Reply() ([]XInfoGroup, error) {
	return r.groups, r.err
}

type ReplyXInfoConsumers struct {
	consumers []XInfoConsumer
	batchReply
}

func (r *ReplyXInfoConsumers) Reply() ([]XInfoConsumer, error) {
	return r.consumers, r.err
}

// XInfoStream returns general information about the stream stored at the specified key.
func (b *batchAPI) XInfoStream(key string) *ReplyXInfoStream {
	b.args.String("STREAM")
	b.args.Key(key)
	reply := ReplyXInfoStream{}
	reply.Bind(&reply.stream)
	b.do("XINFO", &reply.batchReply)
	return &reply
}

// XInfoHelp gets help for XINFO command
func (b *batchAPI) XInfoHelp() *ReplyBulkStringArray {
	b.args.String("HELP")
	return b.doBulkStringArray("XINFO")
}

// XLen returns the number of entries inside a stream.
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

func (b *batchAPI) XPending(key, group string) *ReplyXPendingSummary {
	b.args.Key(key)
	b.args.String(group)
	reply := ReplyXPendingSummary{}
	reply.Bind(&reply.summary)
	b.do("XPENDING", &reply.batchReply)
	return &reply
}

type XRange struct {
	Start string
	End   string
	Count int64
}

func (b *batchAPI) XPendingGroup(args XPending) *ReplyXPending {
	b.args.Key(args.Key)
	b.args.String(args.Group)
	start := args.Start
	if start == "" {
		start = "-"
	}
	b.args.String(start)
	end := args.End
	if end == "" {
		start = "+"
	}
	b.args.String(end)
	reply := ReplyXPending{}
	reply.Bind(&reply.entries)
	b.do("XPENDING", &reply.batchReply)
	return &reply
}
func (b *batchAPI) XPendingConsumer(args XPending) *ReplyXPending {
	b.args.Key(args.Key)
	b.args.String(args.Group)
	start := args.Start
	if start == "" {
		start = "-"
	}
	b.args.String(start)
	end := args.End
	if end == "" {
		start = "+"
	}
	b.args.String(end)
	b.args.String(args.Consumer)
	reply := ReplyXPending{}
	reply.Bind(&reply.entries)
	b.do("XPENDING", &reply.batchReply)
	return &reply
}

type XPending struct {
	Key        string
	Group      string
	Consumer   string
	Start, End string
	Count      int64
}

type XPendingSummary struct {
	Pending   int64
	MinID     string
	MaxID     string
	Consumers []XPendingConsumer
}

func (x *XPendingSummary) UnmarshalRESP(v resp.Value) error {
	return v.Decode([]interface{}{
		&x.Pending,
		&x.MinID,
		&x.MaxID,
		&x.Consumers,
	})
}

type XPendingConsumer struct {
	Name    string
	Pending int64
}

func (x *XPendingConsumer) UnmarshalRESP(v resp.Value) error {
	return v.Decode([]interface{}{
		&x.Name,
		&x.Pending,
	})
}

type ReplyXPendingSummary struct {
	summary XPendingSummary
	batchReply
}

func (r *ReplyXPendingSummary) Reply() (XPendingSummary, error) {
	return r.summary, r.Err()
}

type ReplyXPending struct {
	entries []XPendingEntry
	batchReply
}

func (r *ReplyXPending) Reply() ([]XPendingEntry, error) {
	return r.entries, r.Err()
}

type XPendingEntry struct {
	ID       string
	Consumer string
	age      int64
	Retries  int64
}

func (x *XPendingEntry) Age() time.Duration {
	return time.Duration(x.age) * time.Millisecond
}
func (x *XPendingEntry) UnmarshalRESP(v resp.Value) error {
	return v.Decode([]interface{}{
		&x.ID,
		&x.Consumer,
		&x.age,
		&x.Retries,
	})
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
