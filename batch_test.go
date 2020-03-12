package red_test

import (
	"testing"

	"github.com/alxarch/red"
)

func Test_Batch(t *testing.T) {
	conn, err := red.Dial(":6379", nil)
	if err != nil {
		t.Fatalf("Dial failed %s", err)
	}
	defer conn.Close()
	defer conn.DoCommand(nil, "FLUSDB")
	p, err := conn.Pipeline()
	if err != nil {
		t.Fatalf("Batch error %s", err)
	}
	p.Do("SET", red.String("foo"), red.String("bar"))
	var bar string
	p.Do("GETSET", red.String("foo"), red.String("baz")).Bind(&bar)
	if err := p.Sync(); err != nil {
		t.Errorf("Sync failed %s", err)
	}
	if bar != "bar" {
		t.Errorf("Invalid bind %q", bar)
	}
	var invalidDest chan struct{}
	get := p.Get("foo")
	get.Bind(&invalidDest)
	if err := p.Sync(); err != nil {
		t.Errorf("Batch sync error %s", err)
	}
	if err := get.Err(); err == nil {
		t.Errorf("Batch get error %s", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("Batch close error %s", err)
	}
	var baz string
	if err := conn.DoCommand(&baz, "GET", red.String("foo")); err != nil {
		t.Errorf("GET failed %s", err)
	}
	if baz != "baz" {
		t.Errorf("Invalid GET %q", baz)
	}

}
