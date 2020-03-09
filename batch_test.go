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
	b := new(red.Batch)
	defer conn.DoBatch(b)
	defer b.FlushDB(false)
	b.Do("SET", red.String("foo"), red.String("bar"))
	var bar string
	b.Do("GETSET", red.String("foo"), red.String("baz")).Bind(&bar)
	if err := conn.DoBatch(b); err != nil {
		t.Errorf("Sync failed %s", err)
	}
	if bar != "bar" {
		t.Errorf("Invalid bind %q", bar)
	}
	var baz string
	if err := conn.DoCommand(&baz, "GET", red.String("foo")); err != nil {
		t.Errorf("GET failed %s", err)
	}
	if baz != "baz" {
		t.Errorf("Invalid GET %q", baz)
	}

}
