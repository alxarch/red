package red

import (
	"github.com/alxarch/red/resp"
)

type Sub struct {
	conn     *Conn
	args     ArgBuilder
	channels []string
	patterns []string
	messages <-chan string
}

type Subscription struct {
	Channel string
	Pattern bool
}
type PubSubMessage struct {
}
type Subscriber struct {
	subscribe   <-chan Subscription
	unsubscribe <-chan Subscription
	messages    chan PubSubMessage
}

func (conn *Conn) PSubscribe(pattern string, patterns ...string) (*Sub, error) {
	sub := Sub{
		conn: conn,
	}
	if err := sub.PSubscribe(pattern, patterns...); err != nil {
		return nil, err
	}
	return &sub, nil
}
func (conn *Conn) Subscribe(channel string, channels ...string) (*Sub, error) {
	sub := Sub{
		conn: conn,
	}
	if err := sub.Subscribe(channel, channels...); err != nil {
		return nil, err
	}
	return &sub, nil
}

func (sub *Sub) Subscribe(channel string, channels ...string) error {
	sub.args.Unique(channel, channels...)
	var n resp.Integer
	if err := sub.conn.DoCommand(&n, "SUBSCRIBE", sub.args.Args()...); err != nil {
		return err
	}
	ch := make(chan string, n)
	go func() {
		defer close(ch)
		for {
			var msg resp.BulkString
			v, err := sub.conn.r.Next()
			if err != nil {
				return
			}
			if err := msg.UnmarshalRESP(v); err == nil {
				ch <- msg.String
			}
		}
	}()
	return nil
}

func (sub *Sub) Unsubscribe(channel string, channels ...string) error {
	sub.args.Unique(channel, channels...)
	var n resp.Integer
	return sub.conn.DoCommand(&n, "UNSUBSCRIBE", sub.args.Args()...)
}

func (sub *Sub) PSubscribe(pattern string, patterns ...string) error {
	sub.args.Unique(pattern, patterns...)
	var n resp.Integer
	return sub.conn.DoCommand(&n, "PSUBSCRIBE", sub.args.Args()...)
}

// Command implements Commander interface
func (b *batchAPI) Unsubscribe(channels ...string) *ReplyInteger {
	b.args.Strings(channels...)
	return b.doInteger("UNSUBSCRIBE")
}

// Command implements Commander interface
func (b *batchAPI) PUnsubscribe(patterns ...string) *ReplyInteger {
	b.args.Strings(patterns...)
	return b.doInteger("PUNSUBSCRIBE")
}

// Command implements Commander interface
func (b *batchAPI) PSubscribe(patterns ...string) *ReplyInteger {
	b.args.Strings(patterns...)
	return b.doInteger("PSUBSCRIBE")
}

// Command implements Commander interface
func (b *batchAPI) Publish(channel, msg string) *ReplyInteger {
	b.args.String(channel)
	b.args.String(msg)
	return b.doInteger("PUBLISH")
}

// Command implements Commander interface
func (b batchAPI) PubSubChannels(match string) *ReplyBulkStringArray {
	b.args.String("CHANNELS")
	if match != "" {
		b.args.String(match)
	}
	return b.doBulkStringArray("PUBSUB")
}

// Command implements Commander interface
func (b *batchAPI) PubSubNumSub(channels ...string) *ReplyInteger {
	b.args.String("NUMSUB")
	b.args.Strings(channels...)
	return b.doInteger("PUBSUB")
}

// Command implements Commander interface
func (b *batchAPI) PubSubNumPat() *ReplyInteger {
	b.args.String("NUMPAT")
	return b.doInteger("PUBSUB")
}
