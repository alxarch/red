package red_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/alxarch/red"
	"github.com/alxarch/red/resp"
)

func dialer() func() (*red.Client, error) {
	var pool *red.Pool
	now := time.Now().UnixNano()
	url := fmt.Sprintf("redis://:6379/1?wait-timeout=1s&max-connections=2&max-idle-time=1s&key-prefix=%d-", now)
	pool, err := red.ParseURL(url)
	if err != nil {
		return func() (*red.Client, error) {
			return nil, err
		}
	}
	return pool.Client
}

func TestClient_Multi(t *testing.T) {
	dial := dialer()
	p, err := dial()
	if err != nil {
		t.Fatalf("Dial failed %s", err)
	}
	defer p.Sync()
	defer p.FlushDB(false)

	// Initalize the key 'foo' and set WATCH on the first connection

	// Increment the key
	// p.CommandRESP("HINCRBY", resp.Key("foo"), resp.String("bar"), resp.Int(1))
	p.HIncrBy("foo", "bar", 1)

	// Watch for changes on `foo`
	p.Watch("foo")

	if err := p.Sync(); err != nil {
		t.Errorf("WATCH failed %s", err)
	}

	// Do a read command outside MULTI (using Batch.Tee())
	var bar int64
	p.HGet("foo", "bar").Tee(&bar)
	// p.CommandRESP("HGET", resp.Key("foo"), resp.String("bar")).Tee(&bar)

	if err := p.Sync(); err != nil {
		t.Errorf("HGET failed %s", err)
	}

	if bar != 1 {
		t.Errorf("Invalid bar %d", bar)
	}

	{
		var n int64
		p, _ := dial()
		func() {
			defer p.Sync()
			p.Multi()
			p.HIncrBy("foo", "bar", 2)
			p.HIncrBy("foo", "bar", 2)
			p.HIncrBy("foo", "bar", 2).Tee(&n)
			p.Exec()
		}()
		if n != 7 {
			t.Errorf("TASK did not do all %d", n)
		}
	}

	// Do a MULTI/EXEC with an HSET on the modified key

	var n int64
	p.Multi()
	hset := p.HSet("foo", "bar", "43")
	hset.Tee(&n)
	multi := p.Exec()

	// There should be no error on the task
	if err := p.Sync(); err != nil {
		t.Errorf("Multi failed %s", err)
	}

	// But HSET should fail because it was inside MULTI
	if _, err := multi.Reply(); err != resp.ErrNull {
		t.Errorf("MULTI err %s", err)
	}
	if n, err := hset.Reply(); err == nil {
		t.Errorf("HSET err %s %d", err, n)
	}
	if n != 0 {
		t.Errorf("HSET err %s", err)
	}

	p.Sync()

	// Check the values
	hget := p.HGet("foo", "bar")
	if err := p.Sync(); err != nil {
		t.Errorf("Multi failed %s", err)
	}
	result, err := hget.Reply()
	if err != nil {
		t.Errorf("HGET failed %s", err)
	}
	if result != "7" {
		t.Errorf("HGET value invalid %q", result)
	}
	p.Sync()

}

// // // func BenchmarkPipeline(b *testing.B) {
// // // 	b.ReportAllocs()
// // // 	p := AcquirePipeline(nil)
// // // 	defer ReleasePipeline(p)
// // // 	for i := 0; i < b.N; i++ {
// // // 		p.Reset()
// // // 		p.WritePipeline(commands.HIncrBy{Key: "foo", Field: "bar", Incr: 1})
// // // 	}
// // // }

// func TestConn_Batch(t *testing.T) {
// 	conn, err := redis.DialURL("redis:///1?read-timeout=1s&write-timeout=1s&write-buffer-size=4096&read-buffer-size=1024")
// 	if err != nil {
// 		t.Fatalf(`Dial nil failed: %s`, err)
// 	}
// 	defer conn.Close()
// 	conn.RunTaskFunc(func(p *redis.Batch) error {
// 		var (
// 			set   resp.SimpleString
// 			keys  []string
// 			flush resp.SimpleString
// 		)
// 		sel := p.Select(10)
// 		p.Set("foo", "bar", 0).Tee(&set)
// 		p.Keys("*").Tee(&keys)
// 		p.FlushDB(false).Tee(&flush)
// 		if err := p.Sync(); err != nil {
// 			t.Errorf("Failed to execute pipeline: %s", err)
// 		}

// 		if _, err := sel.Reply(); err != nil {
// 			t.Errorf("Reply SELECT failed %s", err)
// 		}
// 		if set != redis.StatusOK {
// 			t.Errorf("Reply SET failed %s", set)
// 		}
// 		if !reflect.DeepEqual(keys, []string{"foo"}) {
// 			t.Errorf("Invalid KEYS reply: %v", keys)
// 		}
// 		if flush != redis.StatusOK {
// 			t.Errorf("Reply FLUSHDB failed %s", flush)
// 		}
// 		return nil
// 	})

// }
