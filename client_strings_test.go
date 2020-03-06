package red_test

import (
	"reflect"
	"testing"
)

func TestAPI_Strings(t *testing.T) {
	dial := dialer()
	p, err := dial()
	if err != nil {
		t.Fatalf("Dial failed %s", err)
	}
	defer p.Sync()
	defer p.FlushDB(false)

	p.Set("foo", "bar", 0)
	p.Append("foo", "baz")
	var foo string
	p.Get("foo").Bind(&foo)
	var baz string
	p.GetRange("foo", 3, 8).Bind(&baz)
	var barfoo string
	p.SetRange("foo", 3, "foo").Bind(&barfoo)
	p.GetSet("foo", "foobar").Bind(&barfoo)
	var n6 int64
	p.StrLen("foo").Bind(&n6)
	p.Set("bar", "42", 0)
	var n43 int64
	p.Incr("bar").Bind(&n43)
	var n42 int64
	p.Decr("bar").Bind(&n42)
	var n52 int64
	p.IncrBy("bar", 10).Bind(&n52)
	var n32 int64
	p.DecrBy("bar", 20).Bind(&n32)
	var f42 float64
	p.IncrByFloat("bar", 10.0).Bind(&f42)
	var values []string
	p.MSet(
		"foo", "bar",
		"bar", "baz",
		"baz", "foo",
	)
	p.MGet("foo", "bar", "baz").Bind(&values)
	if err := p.Sync(); err != nil {
		t.Errorf("Exec failed %s", err)
	}
	if foo != "barbaz" {
		t.Errorf("Invalid reply %s", foo)
	}
	if baz != "baz" {
		t.Errorf("Invalid reply %s", baz)
	}
	if barfoo != "barfoo" {
		t.Errorf("Invalid reply %s", barfoo)
	}
	if n6 != 6 {
		t.Errorf("Invalid reply %d", n6)
	}
	if n43 != 43 {
		t.Errorf("Invalid reply %d", n43)
	}
	if n42 != 42 {
		t.Errorf("Invalid reply %d", n42)
	}
	if n52 != 52 {
		t.Errorf("Invalid reply %d", n52)
	}
	if n32 != 32 {
		t.Errorf("Invalid reply %d", n32)
	}
	if f42 != 42 {
		t.Errorf("Invalid reply %f", f42)
	}
	if !reflect.DeepEqual(values, []string{"bar", "baz", "foo"}) {
		t.Errorf("Invalid MGET %v", values)
	}

	p.Sync()
}
