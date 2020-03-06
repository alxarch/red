package red_test

import (
	"reflect"
	"testing"

	"github.com/alxarch/red/resp"
)

func TestAPI_Hashes(t *testing.T) {
	dial := dialer()
	p, err := dial()
	if err != nil {
		t.Fatalf("Dial failed %s", err)
	}
	defer p.Sync()
	defer p.FlushDB(false)

	p.HSet("foo", "bar", "baz")
	hexists := p.HExists("foo", "bar")
	defer func() {
		ok, err := hexists.Reply()
		if err != nil {
			t.Errorf("HEXISTS err %s", err)
		}
		if !ok {
			t.Errorf("HEXISTS failed %t", ok)
		}
	}()
	p.HSet("foo", "foo", "bar")
	hdel := p.HDel("foo", "bar", "baz")
	defer func() {
		n, err := hdel.Reply()
		if err != nil {
			t.Errorf("HDEL failed %s", err)
		}
		if n != 1 {
			t.Errorf("HDEL count %d", n)
		}
	}()
	hgetnull := p.HGet("foo", "baz")
	defer func() {
		s, err := hgetnull.Reply()
		if err != resp.ErrNull {
			t.Errorf("HGET not null %s %s", s, err)
		}
	}()
	p.HSet("foo", "foo", "bar", "bar", "baz", "baz", "foo")
	var fields map[string]string
	p.HGetAll("foo").Bind(&fields)
	defer func() {
		if !reflect.DeepEqual(fields, map[string]string{
			"foo": "bar",
			"bar": "baz",
			"baz": "foo",
		}) {
			t.Errorf("Invalid HGETALL %v", fields)
		}

	}()
	hlen := p.HLen("foo")
	defer func() {
		n, err := hlen.Reply()
		if err != nil {
			t.Errorf("HLEN failed %s", err)
		}
		if n != 3 {
			t.Errorf("Invalid HLEN %d", n)
		}
	}()

	hkeys := p.HKeys("foo")
	defer func() {
		keys, err := hkeys.Reply()
		if err != nil {
			t.Errorf("HKEYS failed %s", err)
		}
		if !reflect.DeepEqual(keys, []string{
			"foo",
			"bar",
			"baz",
		}) {
			t.Errorf("Invalid HKEYS %v", keys)
		}

	}()
	hvals := p.HVals("foo")
	defer func() {
		vals, err := hvals.Reply()
		if err != nil {
			t.Errorf("HVALS failed %s", err)
		}
		if !reflect.DeepEqual(vals, []string{
			"bar",
			"baz",
			"foo",
		}) {
			t.Errorf("Invalid HVALS %v", vals)
		}
	}()
	hmget := p.HMGet("foo", "bar", "baz")
	defer func() {
		vals, err := hmget.Reply()
		if err != nil {
			t.Errorf("HMGET failed %s", err)
		}
		if !reflect.DeepEqual(vals, []string{
			"baz",
			"foo",
		}) {
			t.Errorf("Invalid HMGET %v", vals)
		}
	}()

	hstrlen := p.HStrLen("foo", "bar")
	defer func() {
		n, err := hstrlen.Reply()
		if err != nil {
			t.Errorf("HSTRLEN failed %s", err)
		}
		if n != 3 {
			t.Errorf("Invalid HSTRLEN %d", n)
		}
	}()

	hsetnx := p.HSetNX("foo", "goo", "baz")
	defer func() {
		ok, err := hsetnx.Reply()
		if err != nil {
			t.Errorf("HSETNX failed %s", err)
		}
		if !ok {
			t.Errorf("Invalid HSETNX %t", ok)
		}
	}()

	p.Sync()
}
