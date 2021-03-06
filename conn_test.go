package red_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/alxarch/red"
	"github.com/alxarch/red/resp"
)

func TestConn(t *testing.T) {
	conn, err := red.Dial(":6379", &red.ConnOptions{
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})

	if err != nil {
		t.Fatalf(`Dial nil failed: %s`, err)
	}
	defer conn.Close()
	defer conn.DoCommand(nil, "FLUSHDB")
	conn.WriteCommand("SELECT", red.Int(10))
	conn.WriteQuick("SET", "foo", "bar")
	conn.WriteCommand("KEYS", red.String("*"))
	conn.WriteCommand("FLUSHDB")
	if err := conn.DoCommand(nil, "PING"); err == nil {
		t.Errorf("Do should fail")
	}

	var ok red.AssertOK
	if err := conn.Scan(&ok); err != nil {
		t.Errorf("Reply SELECT failed %s", err)
	}
	if err := conn.Scan(&ok); err != nil {
		t.Errorf("Reply SET failed %s", err)
	}
	var keys []string
	if err := conn.Scan(&keys); err != nil {
		t.Errorf("Reply KEYS failed %s", err)
	}
	if !reflect.DeepEqual(keys, []string{"foo"}) {
		t.Errorf("Invalid KEYS reply: %v", keys)
	}
	if err := conn.Scan(&ok); err != nil {
		t.Errorf("Reply FLUSHDB failed %s", err)
	}
	if err := conn.Scan(&ok); err != red.ErrNoReplies {
		t.Errorf("Scan after end %s", err)
	}
}

func TestConn_LoadScript(t *testing.T) {
	conn, err := red.Dial(":6379", nil)
	if err != nil {
		t.Fatal(err)
	}
	src := "return {KEYS[1],ARGV[1],KEYS[2],ARGV[2]}"
	sha1, err := conn.LoadScript(src)
	if err != nil {
		t.Fatal(err)
	}
	if sha1 != `da95252e2c27e41cd53b9114f28b4ba84e7d64d4` {
		t.Errorf("Invalid SHA1: %s", sha1)
	}
	{
		// Check EVALSHA
		var result map[string]string
		_ = conn.WriteEval(sha1, 2, "foo", "bar", "bar", "baz")
		if err := conn.Scan(&result); err != nil {
			t.Errorf("EVAL failed %s", err)
		}
		expect := map[string]string{
			"foo": "bar",
			"bar": "baz",
		}
		if !reflect.DeepEqual(result, expect) {
			t.Errorf("Invalid value: %v %v", result, expect)
		}
	}

}

func TestConn_ScriptInject(t *testing.T) {
	conn, err := red.Dial(":6379", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	src := "return {KEYS[1],ARGV[1],KEYS[2],ARGV[2]}"
	var result map[string]string
	if err := conn.Eval(&result, src, 2, "foo", "bar", "bar", "baz"); err != nil {
		t.Errorf("EVAL failed %s", err)
	}
	expect := map[string]string{
		"foo": "bar",
		"bar": "baz",
	}
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("Invalid value: %v %v", result, expect)
	}

}

func TestConn_Multi(t *testing.T) {
	conn, err := red.Dial(":6379", &red.ConnOptions{
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	defer conn.DoCommand(nil, "FLUSHDB")
	conn.DoCommand(nil, "SET", red.QuickArgs("foo", "bar")...)
	conn.WriteQuick("WATCH", "foo")
	conn.WriteQuick("GET", "foo")
	var ok resp.SimpleString
	conn.Scan(&ok)
	if ok != red.StatusOK {
		t.Errorf("WATCH not OK %q", ok)
	}
	var bar string
	if err := conn.Scan(&bar); err != nil {
		t.Errorf("GET failed %s", err)
	}

	if bar != "bar" {
		t.Errorf("Invalid foo %q", bar)
	}
	{
		conn, err := red.Dial(":6379", nil)
		if err != nil {
			t.Fatal(err)
		}
		p := red.AcquireBatch()
		defer red.ReleaseBatch(p)
		var bar string
		p.GetSet("foo", "baz").Bind(&bar)
		if err := conn.DoBatch(p); err != nil {
			t.Errorf("GETSET failed %s", err)
		}
		if bar != "bar" {
			t.Errorf("Invalid foo %q", bar)
		}
	}

	{
		conn.WriteCommand("MULTI")
		conn.WriteCommand("SET", red.QuickArgs("foo", "foo")...)
		conn.WriteCommand("EXEC")
		var ok resp.SimpleString
		if err := conn.ScanMulti(&ok); err == nil {
			t.Errorf("Multi didn't fail")
		}
		if err := conn.Scan(nil); err != red.ErrNoReplies {
			t.Errorf("Multi didn't consume all replies %s", err)

		}
		// if ok != red.StatusOK {
		// 	t.Errorf("SET not OK %q", ok)
		// }
		// var queued resp.SimpleString
		// conn.Scan(&queued)
		// if queued != red.StatusQueued {
		// 	t.Errorf("Set not Queued %q", queued)
		// }
		// var foo string
		// if err := conn.Scan([]interface{}{&foo}); err == nil {
		// 	t.Errorf("MULTI didn't fail %q", foo)
		// }

	}

}

// func Benchmark_Writer(b *testing.B) {
// 	buf := bytes.Buffer{}
// 	w := red.Writer{}
// 	w.Reset(bufio.NewWriter(&buf))

// 	b.ReportAllocs()
// 	for i := 0; i <= b.N; i++ {
// 		w.WriteCommand("SELECT", red.Int(1))
// 	}

// }

// // func AppendArray(buf []byte, size int64) []byte {
// // 	buf = append(buf, byte(resp.TypeArray))
// // 	buf = strconv.AppendInt(buf, size, 10)
// // 	return append(buf, resp.CRLF...)
// // }
