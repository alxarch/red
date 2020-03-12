package red

import (
	"strconv"
	"time"

	"github.com/alxarch/red/resp"
)

// BZPopMax is the blocking version of ZPopMax
func (conn *Conn) BZPopMax(timeout time.Duration, key string, keys ...string) (z ZPop, err error) {
	args := ArgBuilder{
		args: make([]Arg, 0, len(keys)+2),
	}
	args.KeysUnique(key, keys...)
	args.Milliseconds(timeout)
	err = conn.DoCommand(&z, "BZPOPMAX", args.Args()...)
	return
}

// BZPopMin is the blocking version of ZPopMin
func (conn *Conn) BZPopMin(timeout time.Duration, key string, keys ...string) (z ZPop, err error) {
	args := ArgBuilder{
		args: make([]Arg, 0, len(keys)+2),
	}
	args.KeysUnique(key, keys...)
	args.Milliseconds(timeout)
	err = conn.DoCommand(&z, "BZPOPMIN", args.Args()...)
	return
}

// ZAdd adds or modifies the a member of a sorted set
func (b *batchAPI) ZAdd(key string, mode Mode, members ...ZEntry) *ReplyInteger {
	b.args.Key(key)
	b.argZAdd(mode &^ INCR)
	for i := range members {
		m := &members[i]
		b.args.Float(m.Score)
		b.args.String(m.Member)
	}
	return b.doInteger("ZADD")
}

// ZAddIncr increments the score of a sorted set member by d
func (b *batchAPI) ZAddIncr(key string, mode Mode, member string, d float64) *ReplyFloat {
	b.args.Key(key)
	b.argZAdd(mode | INCR)
	b.args.Float(d)
	b.args.String(member)
	return b.doFloat("ZADD")
}

// ZCard returns the cardinality of a sorted set
func (b *batchAPI) ZCard(key string) *ReplyInteger {
	b.args.Key(key)
	return b.doInteger("ZCARD")
}

func (b *batchAPI) argZAdd(mode Mode) {
	if mode.NX() {
		b.args.String("NX")
	} else if mode.XX() {
		b.args.String("XX")
	}
	if mode.CH() {
		b.args.String("CH")
	}
	if mode.INCR() {
		b.args.String("INCR")
	}
}

// ZCount returns the number of elements in the sorted set at key with a score between min and max.
func (b *batchAPI) ZCount(key string, min, max Arg) *ReplyInteger {
	b.args.Key(key)
	b.args.Append(min, max)
	return b.doInteger("ZCOUNT")
}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
func (b *batchAPI) ZIncrBy(key string, incr float64, member string) *ReplyFloat {
	b.args.Key(key)
	b.args.Float(incr)
	b.args.String(member)
	return b.doFloat("ZINCRBY")
}

// ZLexCount returns the number of elements in the sorted set at key with a value between min and max.
func (b *batchAPI) ZLexCount(key string, min, max Arg) *ReplyInteger {
	b.args.Key(key)
	b.args.Arg(min)
	b.args.Arg(max)
	return b.doInteger("ZLEXCOUNT")
}

// ZPopMax removes and returns up to count members with the highest scores in the sorted set stored at key.
func (b *batchAPI) ZPopMax(key string, count int64) *ReplyZRange {
	b.args.Key(key)
	if count > 0 {
		b.args.Int(count)
	}
	return b.doZRange("ZPOPMAX")
}

// ZPopMin removes and returns up to count members with the lowest scores in the sorted set stored at key.
func (b *batchAPI) ZPopMin(key string, count int64) *ReplyZRange {
	b.args.Key(key)
	if count > 0 {
		b.args.Int(count)
	}
	return b.doZRange("ZPOPMIN")
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
func (b *batchAPI) ZRank(key, member string) *ReplyInteger {
	b.args.Key(key)
	b.args.String(member)
	return b.doInteger("ZRANK")
}

// ZRevRank gets the rank of a member of a sorted set ordered from high to low
func (b *batchAPI) ZRevRank(key, member string) *ReplyInteger {
	b.args.Key(key)
	b.args.String(member)
	return b.doInteger("ZREVRANK")
}

// ZRem removes member(s) from a sorted set
func (b *batchAPI) ZRem(key string, members ...string) *ReplyInteger {
	b.args.Key(key)
	b.args.Strings(members...)
	return b.doInteger("ZREM")
}

// ZScore returns the score of a member
func (b *batchAPI) ZScore(key, member string) *ReplyFloat {
	b.args.Key(key)
	b.args.String(member)
	return b.doFloat("ZSCORE")
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
func (b *batchAPI) ZInterStore(args ZStore) *ReplyInteger {
	args.args(&b.args)
	return b.doInteger("ZINTERSTORE")
}

// ZUnionStore computes the union of numkeys sorted sets given by the specified keys, and stores the result in destination.
func (b *batchAPI) ZUnionStore(args ZStore) *ReplyInteger {
	args.args(&b.args)
	return b.doInteger("ZUNIONSTORE")
}

// ZRange returns the specified range of elements in the sorted set stored at key.
func (b *batchAPI) ZRange(key string, start, stop int64) *ReplyBulkStringArray {
	b.args.Key(key)
	b.args.Int(start)
	b.args.Int(stop)
	return b.doBulkStringArray("ZRANGE")
}

// ZRevRange returns the specified range of elements in the sorted set stored at key.
func (b *batchAPI) ZRevRange(key string, start, stop int64) *ReplyBulkStringArray {
	b.args.Key(key)
	b.args.Int(start)
	b.args.Int(stop)
	return b.doBulkStringArray("ZREVRANGE")
}

// ZRemRangeByRank removes all elements in the sorted set stored at key with rank between start and stop.
func (b *batchAPI) ZRemRangeByRank(key string, start, stop int64) *ReplyInteger {
	b.args.Key(key)
	b.args.Int(start)
	b.args.Int(stop)
	return b.doInteger("ZREMRANGEBYRANK")
}

// ZRangeWithScores returns the specified range of elements in the sorted set stored at key.
func (b *batchAPI) ZRangeWithScores(key string, start, stop int64) *ReplyZRange {
	b.args.Key(key)
	b.args.Int(start)
	b.args.Int(stop)
	b.args.String("WITHSCORES")
	return b.doZRange("ZRANGE")
}

// ZRevRangeWithScores returns the specified range of elements in the sorted set stored at key.
func (b *batchAPI) ZRevRangeWithScores(key string, start, stop int64) *ReplyZRange {
	b.args.Key(key)
	b.args.Int(start)
	b.args.Int(stop)
	b.args.String("WITHSCORES")
	return b.doZRange("ZREVRANGE")
}

// ZRangeByScore returns all the elements in the sorted set at key with a score between min and max
func (b *batchAPI) ZRangeByScore(key string, min, max Arg, offset, count int64) *ReplyBulkStringArray {
	b.args.Key(key)
	b.args.Arg(min)
	b.args.Arg(max)
	if count != 0 {
		b.args.String("LIMIT")
		b.args.Int(offset)
		b.args.Int(count)
	}
	return b.doBulkStringArray("ZRANGEBYSCORE")
}

// ZRevRangeByScore returns all the elements in the sorted set at key with a score between min and max
func (b *batchAPI) ZRevRangeByScore(key string, max, min Arg, offset, count int64) *ReplyBulkStringArray {
	b.args.Key(key)
	b.args.Arg(max)
	b.args.Arg(min)
	if count != 0 {
		b.args.String("LIMIT")
		b.args.Int(offset)
		b.args.Int(count)
	}
	return b.doBulkStringArray("ZREVRANGEBYSCORE")
}

// ZRemRangeByScore removes all elements in the sorted set stored at key with a score between min and max (inclusive).
func (b *batchAPI) ZRemRangeByScore(key string, min, max Arg) *ReplyInteger {
	b.args.Key(key)
	b.args.Append(min, max)
	return b.doInteger("ZREMRANGEBYSCORE")
}

// ZRangeByScoreWithScores returns all the elements in the sorted set at key with a score between min and max
func (b *batchAPI) ZRangeByScoreWithScores(key string, min, max Arg, offset, count int64) *ReplyZRange {
	b.args.Key(key)
	b.args.Arg(min)
	b.args.Arg(max)
	b.args.String("WITHSCORES")
	if count != 0 {
		b.args.String("LIMIT")
		b.args.Int(offset)
		b.args.Int(count)
	}
	return b.doZRange("ZRANGEBYSCORE")
}

// ZRevRangeByScoreWithScores returns all the elements in the sorted set at key with a score between min and max
func (b *batchAPI) ZRevRangeByScoreWithScores(key string, max, min Arg, offset, count int64) *ReplyZRange {
	b.args.Key(key)
	b.args.Arg(max)
	b.args.Arg(min)
	b.args.String("WITHSCORES")
	if count != 0 {
		b.args.String("LIMIT")
		b.args.Int(offset)
		b.args.Int(count)
	}
	return b.doZRange("ZREVRANGEBYSCORE")
}

// ZRangeByLex when all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returnWhen all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returns all the elements in the sorted set at key with a value between min and max.s all the elements in the sorted set at key with a value between min and max.
func (b *batchAPI) ZRangeByLex(key string, min, max Arg, offset, count int64) *ReplyBulkStringArray {
	b.args.Key(key)
	b.args.Arg(min)
	b.args.Arg(max)
	if count != 0 {
		b.args.String("LIMIT")
		b.args.Int(offset)
		b.args.Int(count)
	}
	return b.doBulkStringArray("ZRANGEBYLEX")
}

// ZRevRangeByLex when all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returnWhen all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returns all the elements in the sorted set at key with a value between min and max.s all the elements in the sorted set at key with a value between min and max.
func (b *batchAPI) ZRevRangeByLex(key string, max, min Arg, offset, count int64) *ReplyBulkStringArray {
	b.args.Key(key)
	b.args.Arg(max)
	b.args.Arg(min)
	if count != 0 {
		b.args.String("LIMIT")
		b.args.Int(offset)
		b.args.Int(count)
	}
	return b.doBulkStringArray("ZREVRANGEBYLEX")
}

// ZRemRangeByLex removes all elements in the sorted set stored at key between the lexicographical range specified by min and max.
func (b *batchAPI) ZRemRangeByLex(key string, min, max Arg) *ReplyInteger {
	b.args.Key(key)
	b.args.Append(min, max)
	return b.doInteger("ZREMRANGEBYLEX")
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
	batchReply
}

func (b *batchAPI) doZRange(cmd string) *ReplyZRange {
	reply := ReplyZRange{}
	reply.Bind((*zEntriesWithScores)(&reply.members))
	b.do(cmd, &reply.batchReply)
	return &reply
}

// Reply return the reply
func (r *ReplyZRange) Reply() ([]ZEntry, error) {
	return r.members, r.err
}

type ZPop struct {
	Key    string
	Member string
	Score  float64
}

func (z *ZPop) UnmarshalRESP(v resp.Value) error {
	return v.Decode([]interface{}{
		&z.Key,
		&z.Member,
		&z.Score,
	})
}

// ReplyZPop is the reply of a BZPOPMIN/BZPOPMAX command
type ReplyZPop struct {
	zpop ZPop
	batchReply
}

// var _ batchReply = (*ReplyZPop)(nil)

// Reply returns the BZPOPMIN/BZPOPMAX reply
func (reply *ReplyZPop) Reply() (ZPop, error) {
	return reply.zpop, reply.err
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

func (b *batchAPI) doZPop(cmd string) *ReplyZPop {
	reply := ReplyZPop{}
	reply.Bind(&reply.zpop)
	b.do(cmd, &reply.batchReply)
	return &reply

}
