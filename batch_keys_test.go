package red_test

import (
	"testing"

	"github.com/alxarch/red"
	"github.com/alxarch/red/resp"
)

func TestAPI_Keys(t *testing.T) {
	dial := dialer()
	conn, err := dial()
	if err != nil {
		t.Fatalf("Dial failed %s", err)
	}
	p := new(red.Batch)
	defer conn.DoBatch(p)
	defer p.FlushDB(false)

	set := p.Set("foo", "bar", 0)
	del := p.Del("foo")
	if err := conn.DoBatch(p); err != nil {
		t.Errorf("Failed %s", err)
	}
	ok, err := set.Reply()
	if err != nil || !ok {
		t.Errorf("SET Failed %s", err)
	}
	n, err := del.Reply()
	if err != nil || n != 1 {
		t.Errorf("DEL Failed %s %d", err, n)
	}
	foo := p.Get("foo")
	conn.DoBatch(p)
	if foo, err := foo.Reply(); err != resp.ErrNull {
		t.Errorf("Did not delete %q: %s", foo, err)
	}
}
