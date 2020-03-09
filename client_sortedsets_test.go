package red_test

import (
	"reflect"
	"testing"

	"github.com/alxarch/red"
)

func TestClient_SortedSets(t *testing.T) {
	dial := dialer()
	conn, err := dial()
	if err != nil {
		t.Fatalf("Dial failed %s", err)
	}
	p := new(red.Batch)
	defer conn.DoBatch(p)
	defer p.FlushDB(false)

	zadd := p.ZAdd("foo", 0, red.Z("foo", 1.0), red.Z("bar", 2.0), red.Z("baz", 3.0))
	defer func() {
		n, err := zadd.Reply()
		if err != nil {
			t.Errorf("ZADD failed %s", err)
		}
		if n != 3 {
			t.Errorf("ZADD failed %s %d", err, n)
		}
	}()
	zincr := p.ZAddIncr("foo", red.XX, "foo", 3.0)
	defer func() {
		score, err := zincr.Reply()
		if err != nil {
			t.Errorf("ZADD INCR failed %s", err)
		}
		if score != 4.0 {
			t.Errorf("ZADD INCR failed %s %f", err, score)
		}
	}()

	zcard := p.ZCard("foo")
	defer func() {
		n, err := zcard.Reply()
		if err != nil {
			t.Errorf("ZCARD failed %s", err)
		}
		if n != 3 {
			t.Errorf("ZCARD failed %s %d", err, n)
		}
	}()

	zcount := p.ZCount("foo", red.MinScore(), red.Score(3.0, false))
	defer func() {
		n, err := zcount.Reply()
		if err != nil {
			t.Errorf("ZCOUNT failed %s", err)
		}
		if n != 1 {
			t.Errorf("ZCOUNT failed %s %d", err, n)
		}
	}()
	zrange := p.ZRange("foo", 0, -1)
	defer func() {
		members, err := zrange.Reply()
		if err != nil {
			t.Errorf("ZRANGE failed %s", err)
		}
		if !reflect.DeepEqual(members, []string{"bar", "baz", "foo"}) {
			t.Errorf("ZRANGE failed %v", members)
		}

	}()
	zrevrange := p.ZRevRange("foo", 0, -1)
	defer func() {
		members, err := zrevrange.Reply()
		if err != nil {
			t.Errorf("ZREVRANGE failed %s", err)
		}
		if !reflect.DeepEqual(members, []string{"foo", "baz", "bar"}) {
			t.Errorf("ZREVRANGE failed %v", members)
		}

	}()
	zrangew := p.ZRangeWithScores("foo", 0, -1)
	defer func() {
		members, err := zrangew.Reply()
		if err != nil {
			t.Errorf("ZRANGE failed %s", err)
		}
		if !reflect.DeepEqual(members, []red.ZEntry{
			{"bar", 2.0},
			{"baz", 3.0},
			{"foo", 4.0},
		}) {
			t.Errorf("ZRANGE failed %v", members)
		}

	}()
	zrevrangew := p.ZRevRangeWithScores("foo", 0, -1)
	defer func() {
		members, err := zrevrangew.Reply()
		if err != nil {
			t.Errorf("ZREVRANGE failed %s", err)
		}
		if !reflect.DeepEqual(members, []red.ZEntry{
			{"foo", 4.0},
			{"baz", 3.0},
			{"bar", 2.0},
		}) {
			t.Errorf("ZREVRANGE failed %v", members)
		}

	}()

	zincrby := p.ZIncrBy("foo", 1.0, "baz")
	defer func() {
		n, err := zincrby.Reply()
		if err != nil {
			t.Errorf("ZINCRBY failed %s", err)
		}
		if n != 4.0 {
			t.Errorf("ZINCRBY failed %s %f", err, n)
		}
	}()

	zlexcount := p.ZLexCount("foo", red.Lex("bar", false), red.MaxLex())
	defer func() {
		n, err := zlexcount.Reply()
		if err != nil {
			t.Errorf("ZLEXCOUNT failed %s", err)
		}
		if n != 2 {
			t.Errorf("ZLEXCOUNT failed %s %d", err, n)
		}
	}()
	zrank := p.ZRank("foo", "foo")
	defer func() {
		n, err := zrank.Reply()
		if err != nil {
			t.Errorf("ZRANK failed %s", err)
		}
		if n != 2 {
			t.Errorf("ZRANK failed %s %d", err, n)
		}

	}()
	zrevrank := p.ZRevRank("foo", "foo")
	defer func() {
		n, err := zrevrank.Reply()
		if err != nil {
			t.Errorf("ZREVRANK failed %s", err)
		}
		if n != 0 {
			t.Errorf("ZREVRANK failed %s %d", err, n)
		}

	}()
	zscore := p.ZScore("foo", "foo")
	defer func() {
		score, err := zscore.Reply()
		if err != nil {
			t.Errorf("ZSCORE failed %s", err)
		}
		if score != 4.0 {
			t.Errorf("ZSCORE failed %s %f", err, score)
		}

	}()
	zrangebyscore := p.ZRangeByScore("foo", red.MinScore(), red.MaxScore(), 0, 3)
	defer func() {
		entries, err := zrangebyscore.Reply()
		if err != nil {
			t.Errorf("ZRANGEBYSCORE failed %s", err)
		}
		if !reflect.DeepEqual(entries, []string{
			"bar",
			"baz",
			"foo",
		}) {
			t.Errorf("ZRANGEBYSCORE invalid entries %v", entries)
		}
	}()
	zrevrangebyscore := p.ZRevRangeByScore("foo", red.MaxScore(), red.MinScore(), 0, 3)
	defer func() {
		entries, err := zrevrangebyscore.Reply()
		if err != nil {
			t.Errorf("ZREVRANGEBYSCORE failed %s", err)
		}
		if !reflect.DeepEqual(entries, []string{
			"foo",
			"baz",
			"bar",
		}) {
			t.Errorf("ZREVRANGEBYSCORE invalid entries %v", entries)
		}
	}()
	zrangebyscorew := p.ZRangeByScoreWithScores("foo", red.MinScore(), red.MaxScore(), 0, 3)
	defer func() {
		entries, err := zrangebyscorew.Reply()
		if err != nil {
			t.Errorf("ZRANGEBYSCORE failed %s", err)
		}
		if !reflect.DeepEqual(entries, []red.ZEntry{
			{"bar", 2.0},
			{"baz", 4.0},
			{"foo", 4.0},
		}) {
			t.Errorf("ZRANGEBYSCORE invalid entries %v", entries)
		}
	}()
	zrevrangebyscorew := p.ZRevRangeByScoreWithScores("foo", red.MaxScore(), red.MinScore(), 0, 2)
	defer func() {
		entries, err := zrevrangebyscorew.Reply()
		if err != nil {
			t.Errorf("ZREVRANGEBYSCORE failed %s", err)
		}
		if !reflect.DeepEqual(entries, []red.ZEntry{
			{"foo", 4.0},
			{"baz", 4.0},
			// {"bar", 2.0},
		}) {
			t.Errorf("ZREVRANGEBYSCORE invalid entries %v", entries)
		}
	}()

	zrangebylex := p.ZRangeByLex("foo", red.MinLex(), red.MaxLex(), 0, 3)
	defer func() {
		entries, err := zrangebylex.Reply()
		if err != nil {
			t.Errorf("ZRANGEBYLEX failed %s", err)
		}
		if !reflect.DeepEqual(entries, []string{
			"bar",
			"baz",
			"foo",
		}) {
			t.Errorf("ZRANGEBYLEX invalid entries %v", entries)
		}
	}()
	zrevrangebylex := p.ZRevRangeByLex("foo", red.MaxLex(), red.MinLex(), 0, 3)
	defer func() {
		entries, err := zrevrangebylex.Reply()
		if err != nil {
			t.Errorf("ZREVRANGEBYLEX failed %s", err)
		}
		if !reflect.DeepEqual(entries, []string{
			"foo",
			"baz",
			"bar",
		}) {
			t.Errorf("ZREVRANGEBYLEX invalid entries %v", entries)
		}
	}()
	defer conn.DoBatch(p)

}
