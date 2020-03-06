package red

import (
	"strconv"
	"time"

	"github.com/alxarch/red/resp"
)

// BZPopMin is the blocking version of ZPOPMIN
func (c *Client) BZPopMin(timeout time.Duration, keys ...string) *ReplyZPop {
	c.args.Keys(keys...)
	c.args.Arg(Milliseconds(timeout))
	return c.doZPop("BZPOPMIN")
}

// BZPopMax is the blocking version of ZPopMax
func (c *Client) BZPopMax(timeout time.Duration, keys ...string) *ReplyZPop {
	c.args.Keys(keys...)
	c.args.Arg(Milliseconds(timeout))
	return c.doZPop("BZPOPMAX")
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
	return c.doInteger("ZADD")
}

// ZAddIncr increments the score of a sorted set member by d
func (c *Client) ZAddIncr(key string, mode Mode, member string, d float64) *ReplyFloat {
	c.args.Key(key)
	c.argZAdd(mode | INCR)
	c.args.Float(d)
	c.args.String(member)
	return c.doFloat("ZADD")
}

// ZCard returns the cardinality of a sorted set
func (c *Client) ZCard(key string) *ReplyInteger {
	c.args.Key(key)
	return c.doInteger("ZCARD")
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
	return c.doInteger("ZCOUNT")
}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
func (c *Client) ZIncrBy(key string, incr float64, member string) *ReplyFloat {
	c.args.Key(key)
	c.args.Float(incr)
	c.args.String(member)
	return c.doFloat("ZINCRBY")
}

// ZLexCount returns the number of elements in the sorted set at key with a value between min and max.
func (c *Client) ZLexCount(key string, min, max Arg) *ReplyInteger {
	c.args.Key(key)
	c.args.Arg(min)
	c.args.Arg(max)
	return c.doInteger("ZLEXCOUNT")
}

// ZPopMax removes and returns up to count members with the highest scores in the sorted set stored at key.
func (c *Client) ZPopMax(key string, count int64) *ReplyZRange {
	c.args.Key(key)
	if count > 0 {
		c.args.Int(count)
	}
	return c.doZRange("ZPOPMAX")
}

// ZPopMin removes and returns up to count members with the lowest scores in the sorted set stored at key.
func (c *Client) ZPopMin(key string, count int64) *ReplyZRange {
	c.args.Key(key)
	if count > 0 {
		c.args.Int(count)
	}
	return c.doZRange("ZPOPMIN")
}

// func CheckLex(lex string) bool {
// 	if len(lex) > 1 {
// 		switch lex[0] {
// 		case '(', '[':
// 			return true
// 		}
// 	}
// 	return false
// }

// func (z *ZRange) argsLex(args *ArgBuilder, min, max string, reverse bool) {
// 	var minArg, maxArg Arg

// 	if minArg == (Arg{}) {
// 		if min == "" {
// 			minArg = String("-")
// 		} else if CheckLex(min) {
// 			minArg = String(min)
// 		} else {
// 			minArg = Lex(min, true)
// 		}
// 	}
// 	if maxArg == (Arg{}) {
// 		if max == "" {
// 			maxArg = String("+")
// 		} else if CheckLex(max) {
// 			maxArg = String(max)
// 		} else {
// 			maxArg = Lex(max, true)
// 		}
// 	}

// 	if reverse {
// 		args.Append(maxArg, minArg)
// 	} else {
// 		args.Append(minArg, maxArg)
// 	}

// 	// args.Flag("WITHSCORES", z.withScores)

// 	if z.Count != 0 {
// 		args.String("LIMIT")
// 		args.Int(z.Offset)
// 		args.Int(z.Count)
// 	}
// }

// func (z *ZRange) argsScore(args *ArgBuilder, min, max Arg, withScores bool) {
// 	args.Append(min, max)
// 	if z.Count != 0 {
// 		args.String("LIMIT")
// 		args.Int(z.Offset)
// 		args.Int(z.Count)
// 	}
// 	args.Flag("WITHSCORES", withScores)
// }

// ZRank gets the rank of a member of a sorted set ordered from low to high
func (c *Client) ZRank(key, member string) *ReplyInteger {
	c.args.Key(key)
	c.args.String(member)
	return c.doInteger("ZRANK")
}

// ZRevRank gets the rank of a member of a sorted set ordered from high to low
func (c *Client) ZRevRank(key, member string) *ReplyInteger {
	c.args.Key(key)
	c.args.String(member)
	return c.doInteger("ZREVRANK")
}

// ZRem removes member(s) from a sorted set
func (c *Client) ZRem(key string, members ...string) *ReplyInteger {
	c.args.Key(key)
	c.args.Strings(members...)
	return c.doInteger("ZREM")
}

// ZScore returns the score of a member
func (c *Client) ZScore(key, member string) *ReplyFloat {
	c.args.Key(key)
	c.args.String(member)
	return c.doFloat("ZSCORE")
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

// ZStore holds arguments for ZUNIONSTORE and ZINTERSTORE commands
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
	return c.doInteger("ZINTERSTORE")
}

// ZUnionStore computes the union of numkeys sorted sets given by the specified keys, and stores the result in destination.
func (c *Client) ZUnionStore(args ZStore) *ReplyInteger {
	args.args(&c.args)
	return c.doInteger("ZUNIONSTORE")
}

// ZRange returns the specified range of elements in the sorted set stored at key.
func (c *Client) ZRange(key string, start, stop int64) *ReplyBulkStringArray {
	c.args.Key(key)
	c.args.Int(start)
	c.args.Int(stop)
	return c.doBulkStringArray("ZRANGE")
}

// ZRevRange returns the specified range of elements in the sorted set stored at key.
func (c *Client) ZRevRange(key string, start, stop int64) *ReplyBulkStringArray {
	c.args.Key(key)
	c.args.Int(start)
	c.args.Int(stop)
	return c.doBulkStringArray("ZREVRANGE")
}

// ZRemRangeByRank removes all elements in the sorted set stored at key with rank between start and stop.
func (c *Client) ZRemRangeByRank(key string, start, stop int64) *ReplyInteger {
	c.args.Key(key)
	c.args.Int(start)
	c.args.Int(stop)
	return c.doInteger("ZREMRANGEBYRANK")
}

// ZRangeWithScores returns the specified range of elements in the sorted set stored at key.
func (c *Client) ZRangeWithScores(key string, start, stop int64) *ReplyZRange {
	c.args.Key(key)
	c.args.Int(start)
	c.args.Int(stop)
	c.args.String("WITHSCORES")
	return c.doZRange("ZRANGE")
}

// ZRevRangeWithScores returns the specified range of elements in the sorted set stored at key.
func (c *Client) ZRevRangeWithScores(key string, start, stop int64) *ReplyZRange {
	c.args.Key(key)
	c.args.Int(start)
	c.args.Int(stop)
	c.args.String("WITHSCORES")
	return c.doZRange("ZREVRANGE")
}

// ZRangeByScore returns all the elements in the sorted set at key with a score between min and max
func (c *Client) ZRangeByScore(key string, min, max Arg, offset, count int64) *ReplyBulkStringArray {
	c.args.Key(key)
	c.args.Arg(min)
	c.args.Arg(max)
	if count != 0 {
		c.args.String("LIMIT")
		c.args.Int(offset)
		c.args.Int(count)
	}
	return c.doBulkStringArray("ZRANGEBYSCORE")
}

// ZRevRangeByScore returns all the elements in the sorted set at key with a score between min and max
func (c *Client) ZRevRangeByScore(key string, max, min Arg, offset, count int64) *ReplyBulkStringArray {
	c.args.Key(key)
	c.args.Arg(max)
	c.args.Arg(min)
	if count != 0 {
		c.args.String("LIMIT")
		c.args.Int(offset)
		c.args.Int(count)
	}
	return c.doBulkStringArray("ZREVRANGEBYSCORE")
}

// ZRemRangeByScore removes all elements in the sorted set stored at key with a score between min and max (inclusive).
func (c *Client) ZRemRangeByScore(key string, min, max Arg) *ReplyInteger {
	c.args.Key(key)
	c.args.Append(min, max)
	return c.doInteger("ZREMRANGEBYSCORE")
}

// ZRangeByScoreWithScores returns all the elements in the sorted set at key with a score between min and max
func (c *Client) ZRangeByScoreWithScores(key string, min, max Arg, offset, count int64) *ReplyZRange {
	c.args.Key(key)
	c.args.Arg(min)
	c.args.Arg(max)
	c.args.String("WITHSCORES")
	if count != 0 {
		c.args.String("LIMIT")
		c.args.Int(offset)
		c.args.Int(count)
	}
	return c.doZRange("ZRANGEBYSCORE")
}

// ZRevRangeByScoreWithScores returns all the elements in the sorted set at key with a score between min and max
func (c *Client) ZRevRangeByScoreWithScores(key string, max, min Arg, offset, count int64) *ReplyZRange {
	c.args.Key(key)
	c.args.Arg(max)
	c.args.Arg(min)
	c.args.String("WITHSCORES")
	if count != 0 {
		c.args.String("LIMIT")
		c.args.Int(offset)
		c.args.Int(count)
	}
	return c.doZRange("ZREVRANGEBYSCORE")
}

// ZRangeByLex when all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returnWhen all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returns all the elements in the sorted set at key with a value between min and max.s all the elements in the sorted set at key with a value between min and max.
func (c *Client) ZRangeByLex(key string, min, max Arg, offset, count int64) *ReplyBulkStringArray {
	c.args.Key(key)
	c.args.Arg(min)
	c.args.Arg(max)
	if count != 0 {
		c.args.String("LIMIT")
		c.args.Int(offset)
		c.args.Int(count)
	}
	return c.doBulkStringArray("ZRANGEBYLEX")
}

// ZRevRangeByLex when all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returnWhen all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returns all the elements in the sorted set at key with a value between min and max.s all the elements in the sorted set at key with a value between min and max.
func (c *Client) ZRevRangeByLex(key string, max, min Arg, offset, count int64) *ReplyBulkStringArray {
	c.args.Key(key)
	c.args.Arg(max)
	c.args.Arg(min)
	if count != 0 {
		c.args.String("LIMIT")
		c.args.Int(offset)
		c.args.Int(count)
	}
	return c.doBulkStringArray("ZREVRANGEBYLEX")
}

// ZRemRangeByLex removes all elements in the sorted set stored at key between the lexicographical range specified by min and max.
func (c *Client) ZRemRangeByLex(key string, min, max Arg) *ReplyInteger {
	c.args.Key(key)
	c.args.Append(min, max)
	return c.doInteger("ZREMRANGEBYLEX")
}

// ZEntry is the entry of a sorted set
type ZEntry struct {
	Member string
	Score  float64
}

// Z creates a ZEntry
func Z(member string, score float64) ZEntry {
	return ZEntry{
		Member: member,
		Score:  score,
	}
}

// ReplyZRange is a map of members with scores
type ReplyZRange struct {
	members []ZEntry
	clientReply
}

func (c *Client) doZRange(cmd string) *ReplyZRange {
	reply := ReplyZRange{}
	reply.Bind((*zEntriesWithScores)(&reply.members))
	c.do(cmd, &reply.clientReply)
	return &reply
}

// Reply return the reply
func (r *ReplyZRange) Reply() ([]ZEntry, error) {
	return r.members, r.err
}

// ReplyZPop is the reply of a BZPOPMIN/BZPOPMAX command
type ReplyZPop struct {
	key    string
	member string
	score  float64
	clientReply
}

var _ batchReply = (*ReplyZPop)(nil)

// Reply returns the BZPOPMIN/BZPOPMAX reply
func (reply *ReplyZPop) Reply() (key, member string, score float64, err error) {
	return reply.key, reply.member, reply.score, reply.err
}

// type zEntries []ZEntry
// func (z *zEntries) UnmarshalRESP(v resp.Value) error {
// 	entries := *z
// 	for i := range entries {
// 		entries[i] = ZEntry{}
// 	}
// 	entries = entries[:0]
// 	for iter := v.Iter(); iter.More(); iter.Next() {

// 	}
// 	if err := v.Each(func(v string) error {
// 		entries = append(entries, ZEntry{
// 			Member: v,
// 			Score:  1.0,
// 		})
// 		return nil
// 	}); err != nil {
// 		return err
// 	}
// 	*z = entries
// 	return nil
// }
type zEntriesWithScores []ZEntry

func (z *zEntriesWithScores) UnmarshalRESP(v resp.Value) error {
	entries := *z
	for i := range entries {
		entries[i] = ZEntry{}
	}
	entries = entries[:0]
	if err := v.EachKV(func(k, v string) error {
		score, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return err
		}
		entries = append(entries, ZEntry{
			Member: k,
			Score:  score,
		})
		return nil
	}); err != nil {
		return err
	}
	*z = entries
	return nil
}

func (c *Client) doZPop(cmd string) *ReplyZPop {
	reply := ReplyZPop{}
	reply.Bind([]interface{}{
		&reply.key,
		&reply.member,
		&reply.score,
	})
	c.do(cmd, &reply.clientReply)
	return &reply

}
