package red_test

import (
	"testing"

	"github.com/alxarch/red/resp"
)

func TestAPI_Keys(t *testing.T) {
	dial := dialer()
	p, err := dial()
	if err != nil {
		t.Fatalf("Dial failed %s", err)
	}
	defer p.Sync()
	defer p.FlushDB(false)

	set := p.Set("foo", "bar", 0)
	del := p.Del("foo")
	if err := p.Sync(); err != nil {
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
	p.Sync()
	if foo, err := foo.Reply(); err != resp.ErrNull {
		t.Errorf("Did not delete %q: %s", foo, err)
	}
}
