package red_test

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alxarch/red"
	"github.com/alxarch/red/resp"
)

func Test_Pool(t *testing.T) {
	pool, err := red.ParseURL("redis://:6379/1?wait-timeout=1s&max-connections=2&max-idle-time=100s")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if pool.MaxConnections != 2 {
		t.Errorf("Invalid max connections: %d", pool.MaxConnections)
	}
	var total int64
	defer pool.Close()
	defer pool.DoCommand(nil, "FLUSHDB")
	c, err := pool.Client()
	if err != nil {
		t.Errorf("Unexpected client error: %s", err)
		return
	}
	if err := c.Close(); err != nil {
		t.Errorf("Unexpected close error: %s", err)
		return
	}

	now := time.Now()
	key := fmt.Sprintf("fastredis:%d", now.UnixNano())
	wg := new(sync.WaitGroup)
	for i := 0; i < 10; i++ {
		field := fmt.Sprintf("foo-%d", i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p, err := pool.Client()
			if err != nil {
				t.Errorf("Unexpected managed error: %s", err)
				return
			}
			var n resp.Integer
			{
				hset := p.HSet(key, field, "baz")
				hset.Tee(&n)
				p.Sync()
				n, err := hset.Reply()
				if err != nil {
					t.Errorf("WTF %s %d", err, n)
				}
			}
			atomic.AddInt64(&total, int64(n))
			if err := p.Close(); err != nil {
				t.Errorf("Unexpected close error: %s", err)
			}
		}(i)
	}
	wg.Wait()
	if total != 10 {
		t.Errorf("Unexpected HSET result: %d", total)
	}
	var values map[string]string
	if err := pool.DoCommand(&values, "HGETALL", red.Key(key)); err != nil {
		t.Errorf("Failed to exec pipeline: %s", err)
	}
	if !reflect.DeepEqual(values, map[string]string{
		"foo-0": "baz",
		"foo-1": "baz",
		"foo-2": "baz",
		"foo-3": "baz",
		"foo-4": "baz",
		"foo-5": "baz",
		"foo-6": "baz",
		"foo-7": "baz",
		"foo-8": "baz",
		"foo-9": "baz",
	}) {
		t.Errorf("Failed to read reply: %v", values)
	}
	stats := pool.Stats()

	if stats.Hits == 0 {
		t.Errorf("Invalid stats %#v", stats)
	}
	if stats.Dials > 2 {
		t.Errorf("Invalid stats %#v", stats)
	}
	if stats.Idle != 2 {
		t.Errorf("Invalid stats %#v", stats)
	}
}
