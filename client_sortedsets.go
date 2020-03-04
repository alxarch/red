package red

import (
	"strconv"
	"time"

	"github.com/alxarch/red/resp"
)

// ZEntry is the entry of a sorted set
type ZEntry struct {
	Member string
	Score  float64
}

type ReplyZRange struct {
	members []ZEntry
	replyBase
}

func (r *ReplyZRange) Reply() ([]ZEntry, error) {
	return r.members, r.err
}

func (r *ReplyZRange) reply(v resp.Value) error {
	members := (*zResults)(&r.members)
	members.Reset()
	r.err = members.UnmarshalRESP(v)
	r.tee(v)
	return nil
}

type ReplyZPop struct {
	Key    string
	Member ZEntry
	replyBase
}

var _ batchReply = (*ReplyZPop)(nil)

func (reply *ReplyZPop) Reply() error {
	return reply.err
}

// UnmarshalRESP implements resp.Unmarshaler interface
func (r *ReplyZPop) reply(v resp.Value) error {
	r.err = v.Decode([]interface{}{
		&r.Key, &r.Member.Member, &r.Member.Score,
	})
	r.tee(v)
	return nil
}

type zResults []ZEntry

func (z *zResults) Reset() {
	entries := *z
	for i := range entries {
		entries[i] = ZEntry{}
	}
	*z = entries[:0]
}

func (z *zResults) UnmarshalRESP(v resp.Value) error {
	entries := (*z)[:0]
	if err := v.EachKV(func(k, v string) error {
		score, err := strconv.ParseFloat(k, 64)
		if err != nil {
			return err
		}
		entries = append(entries, ZEntry{
			Member: v,
			Score:  score,
		})
		return nil
	}); err != nil {
		return err
	}
	*z = entries
	return nil

}

// BZPopMin is the blocking version of ZPOPMIN
func (c *Client) BZPopMin(timeout time.Duration, keys ...string) *ReplyZPop {
	c.args.Keys(keys...)
	c.args.Arg(Milliseconds(timeout))
	reply := ReplyZPop{}
	c.do("BZPOPMIN", &reply)
	return &reply
}

// BZPopMax is the blocking version of ZPopMax
func (c *Client) BZPopMax(timeout time.Duration, keys ...string) *ReplyZPop {
	c.args.Keys(keys...)
	c.args.Arg(Milliseconds(timeout))
	reply := ReplyZPop{}
	c.do("BZPOPMAX", &reply)
	return &reply
}

// ZAdd adds or modifies the a member of a sorted set
func (c *Client) ZAdd(key string, mode Mode, members ...ZEntry) *ReplyInteger {
	c.args.Key(key)
	c.argZAdd(mode &^ INCR)
	for i := range members {
		m := &members[i]
		c.args.Float(m.Score)
		c.args.String(m.Member)
	}
	reply := ReplyInteger{}
	c.do("ZADD", &reply)
	return &reply
}

// ZAddIncr increments the score of a sorted set member by d
func (c *Client) ZAddIncr(key string, mode Mode, member string, d float64) *ReplyFloat {
	c.args.Key(key)
	c.argZAdd(mode | INCR)
	c.args.Float(d)
	c.args.String(member)
	reply := ReplyFloat{}
	c.do("ZADD", &reply)
	return &reply
}

// ZCard returns the cardinality of a sorted set
func (c *Client) ZCard(key string) *ReplyInteger {
	c.args.Key(key)
	reply := ReplyInteger{}
	c.do("ZCARD", &reply)
	return &reply
}

func (c *Client) argZAdd(mode Mode) {
	if mode.NX() {
		c.args.String("NX")
	} else if mode.XX() {
		c.args.String("XX")
	}
	if mode.CH() {
		c.args.String("CH")
	}
	if mode.INCR() {
		c.args.String("INCR")
	}
}

// ZCount returns the number of elements in the sorted set at key with a score between min and max.
func (c *Client) ZCount(key string, min, max Arg) *ReplyInteger {
	c.args.Key(key)
	c.args.Append(min, max)
	reply := ReplyInteger{}
	c.do("ZCOUNT", &reply)
	return &reply
}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
func (c *Client) ZIncrBy(key string, incr float64, member string) *ReplyFloat {
	c.args.Key(key)
	c.args.Float(incr)
	c.args.String(member)
	reply := ReplyFloat{}
	c.do("ZINCRBY", &reply)
	return &reply
}

// ZLexCount returns the number of elements in the sorted set at key with a value between min and max.
func (c *Client) ZLexCount(key string, min, max Arg) *ReplyInteger {
	c.args.Key(key)
	c.args.Append(min, max)
	reply := ReplyInteger{}
	c.do("ZLEXCOUNT", &reply)
	return &reply
}

// ZPopMax removes and returns up to count members with the highest scores in the sorted set stored at key.
func (c *Client) ZPopMax(key string, count int64) *ReplyInteger {

	c.args.Key(key)
	if count > 0 {
		c.args.Int(count)
	}
	reply := ReplyInteger{}
	c.do("ZPOPMAX", &reply)
	return &reply
}

// ZPopMin removes and returns up to count members with the lowest scores in the sorted set stored at key.
func (c *Client) ZPopMin(key string, count int64) *ReplyInteger {

	c.args.Key(key)
	if count > 0 {
		c.args.Int(count)
	}
	reply := ReplyInteger{}
	c.do("ZPOPMIN", &reply)
	return &reply
}

// ZRange returns the specified range of elements in the sorted set stored at key.
func (c *Client) ZRange(key string, min, max int64) *ReplyZRange {
	c.args.Key(key)
	c.args.Int(min)
	c.args.Int(max)
	reply := ReplyZRange{}
	c.do("ZRANGE", &reply)
	return &reply
}

// ZRangeWithScores returns the specified range of elements in the sorted set stored at key.
func (c *Client) ZRangeWithScores(key string, min, max int64) *ReplyBulkStringArray {
	c.args.Key(key)
	c.args.Int(min)
	c.args.Int(max)
	c.args.String("WITHSCORES")
	reply := ReplyBulkStringArray{}
	c.do("ZRANGE", &reply)
	return &reply
}

func CheckLex(lex string) bool {
	if len(lex) > 1 {
		switch lex[0] {
		case '(', '[':
			return true
		}
	}
	return false
}

// ZRangeByLex when all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returnWhen all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returns all the elements in the sorted set at key with a value between min and max.s all the elements in the sorted set at key with a value between min and max.
func (c *Client) ZRangeByLex(key string, min, max string, args ZRange) *ReplyInteger {
	c.args.Key(key)
	args.argsLex(&c.args, min, max, false)
	reply := ReplyInteger{}
	c.do("ZRANGEBYLEX", &reply)
	return &reply
}

// ZRemRangeByLex removes all elements in the sorted set stored at key between the lexicographical range specified by min and max.
func (c *Client) ZRemRangeByLex(key string, min, max Arg) *ReplyInteger {
	c.args.Key(key)
	c.args.Append(min, max)
	reply := ReplyInteger{}
	c.do("ZREMRANGEBYLEX", &reply)
	return &reply
}

// ZRemRangeByRank removes all elements in the sorted set stored at key with rank between start and stop.
func (c *Client) ZRemRangeByRank(key string, min, max int64) *ReplyInteger {
	c.args.Key(key)
	c.args.Int(min)
	c.args.Int(max)
	reply := ReplyInteger{}
	c.do("ZREMRANGEBYRANK", &reply)
	return &reply
}

func (c *Client) ZRevRangeByScore(key string, args ZRange) *ReplyBulkStringArray {
	c.args.Key(key)
	args.argsScore(&c.args, true)
	reply := ReplyBulkStringArray{}
	c.do("ZREVRANGEBYSCORE", &reply)
	return &reply
}

type ZRange struct {
	Min, Max      Arg
	Offset, Count int64
	WithScores    bool
}

func (z *ZRange) argsLex(args *ArgBuilder, min, max string, reverse bool) {
	minArg, maxArg := z.Min, z.Max

	if minArg == (Arg{}) {
		if min == "" {
			minArg = String("-")
		} else if CheckLex(min) {
			minArg = String(min)
		} else {
			minArg = Lex(min, true)
		}
	}
	if maxArg == (Arg{}) {
		if max == "" {
			maxArg = String("+")
		} else if CheckLex(max) {
			maxArg = String(max)
		} else {
			maxArg = Lex(max, true)
		}
	}

	if reverse {
		args.Append(maxArg, minArg)
	} else {
		args.Append(minArg, maxArg)
	}

	args.Flag("WITHSCORES", z.WithScores)

	if z.Count != 0 {
		args.String("LIMIT")
		args.Int(z.Offset)
		args.Int(z.Count)
	}
}

func (z *ZRange) argsScore(args *ArgBuilder, reverse bool) {
	if reverse {
		args.Append(z.Max, z.Min)
	} else {
		args.Append(z.Min, z.Max)
	}
	if z.Count != 0 {
		args.String("LIMIT")
		args.Int(z.Offset)
		args.Int(z.Count)
	}
	args.Flag("WITHSCORES", z.WithScores)
}

// ZRangeByScore returns all the elements in the sorted set at key with a score between min and max
func (c *Client) ZRangeByScore(key string, args ZRange) *ReplyBulkStringArray {
	c.args.Key(key)
	args.argsScore(&c.args, false)
	reply := ReplyBulkStringArray{}
	c.do("ZRANGEBYSCORE", &reply)
	return &reply
}

// ZRemRangeByScore removes all elements in the sorted set stored at key with a score between min and max (inclusive).
func (c *Client) ZRemRangeByScore(key string, min, max Arg) *ReplyInteger {
	c.args.Key(key)
	c.args.Append(min, max)
	reply := ReplyInteger{}
	c.do("ZREMRANGEBYSCORE", &reply)
	return &reply
}

// ZRank gets the rank of a member of a sorted set ordered from low to high
func (c *Client) ZRank(key, member string) *ReplyInteger {
	c.args.Key(key)
	c.args.String(member)
	reply := ReplyInteger{}
	c.do("ZRANK", &reply)
	return &reply
}

// ZRevRank gets the rank of a member of a sorted set ordered from high to low
func (c *Client) ZRevRank(key, member string) *ReplyInteger {
	c.args.Key(key)
	c.args.String(member)
	reply := ReplyInteger{}
	c.do("ZREVRANK", &reply)
	return &reply
}

// ZRem removes member(s) from a sorted set
func (c *Client) ZRem(key string, members ...string) *ReplyInteger {
	c.args.Key(key)
	c.args.Strings(members...)
	reply := ReplyInteger{}
	c.do("ZREM", &reply)
	return &reply
}

// ZScore returns the score of a member
func (c *Client) ZScore(key, member string) *ReplyFloat {
	c.args.Key(key)
	c.args.String(member)
	reply := ReplyFloat{}
	c.do("ZSCORE", &reply)
	return &reply
}

// Aggregate is an aggregate method
type Aggregate int

// Aggregate methods
const (
	_ Aggregate = iota
	AggregateSum
	AggregateMin
	AggregateMax
)

func (a Aggregate) String() string {
	switch a {
	case AggregateMax:
		return "MAX"
	case AggregateMin:
		return "MIN"
	case AggregateSum:
		return "SUM"
	default:
		return ""
	}
}

type ZStore struct {
	Destination string
	Keys        []string
	Weights     map[string]float64
	Aggregate   Aggregate
}

func (z *ZStore) args(args *ArgBuilder) {
	args.Key(z.Destination)
	args.Int(int64(len(z.Keys)))
	args.Keys(z.Keys...)
	if len(z.Weights) > 0 {
		args.String("WEIGHTS")
		for _, key := range z.Keys {
			weight, ok := z.Weights[key]
			if !ok {
				weight = 1.0
			}
			args.Float(weight)
		}
	}
	if agg := z.Aggregate.String(); agg != "" {
		args.String("AGGREGATE")
		args.String(agg)
	}
}

// ZInterStore computes the intersection of numkeys sorted sets given by the specified keys, and stores the result in destination.
func (c *Client) ZInterStore(args ZStore) *ReplyInteger {
	args.args(&c.args)
	reply := ReplyInteger{}
	c.do("ZINTERSTORE", &reply)
	return &reply
}

// ZUnionStore computes the union of numkeys sorted sets given by the specified keys, and stores the result in destination.
func (c *Client) ZUnionStore(args ZStore) *ReplyInteger {
	args.args(&c.args)
	reply := ReplyInteger{}
	c.do("ZUNIONSTORE", &reply)
	return &reply
}
