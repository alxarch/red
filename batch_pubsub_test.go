package red_test

import "testing"

func TestSubscriber(t *testing.T) {
	dial := dialer()
	conn, err := dial()
	if err != nil {
		t.Fatalf("Dial failed %s", err)
	}
	sub, err := conn.Subscriber(0)
	if err != nil {
		t.Errorf("Subscriber error %s", err)
	}
	if err := conn.DoCommand(nil, "PING"); err == nil {
		t.Errorf("PING error %s", err)
	}
	sub.Subscribe("foo", "bar")
	sub.Unsubscribe("bar")
	if err := sub.Close(); err != nil {
		t.Errorf("Close error %s", err)
	}
	var pong string
	if err := conn.DoCommand(&pong, "PING"); err != nil {
		t.Errorf("PING error %s", err)
	}

}
