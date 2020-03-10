package pubsub

import (
	"context"
	"fmt"
	"net"

	"github.com/alxarch/red"
	"github.com/alxarch/red/resp"
)

type MessageKind string

const (
	KindSubscribe   MessageKind = "subscribe"
	KindUnsubscribe MessageKind = "unsubscribe"
	KindMessage     MessageKind = "message"
)

type Message struct {
	kind        MessageKind
	payload     string
	numChannels int64
	channel     string
}

func (m *Message) Kind() MessageKind {
	return m.kind
}

func (m *Message) Channel() string {
	return m.channel
}
func (m *Message) Payload() (string, bool) {
	return m.payload, m.kind == KindMessage
}
func (m *Message) NumChannels() (int64, bool) {
	return m.numChannels, m.kind == KindSubscribe || m.kind == KindUnsubscribe
}

func (m *Message) UnmarshalRESP(value resp.Value) error {
	var kind resp.BulkString
	var channel resp.BulkString
	var payload resp.Any
	if err := value.Decode([]interface{}{&kind, &channel, &payload}); err != nil {
		return err
	}
	switch m.kind, m.channel = MessageKind(kind.String), channel.String; m.kind {
	case KindMessage:
		str, _ := payload.(*resp.BulkString)
		m.payload = str.String
		return nil
	case KindUnsubscribe, KindSubscribe:
		n, _ := payload.(resp.Integer)
		m.numChannels = int64(n)
		return nil
	default:
		return fmt.Errorf("[PUBSUB] Invalid message kind %q", kind.String)
	}
}

type Subscription struct {
	Channels []string
	Pattern bool
}
type unSubscription Subscription

func (s *unSubscription) BuildCommand(args red.ArgBuilder) string {
	args.Strings(s.Channels...)
	if s.Pattern {
		return "PUNSUBSCRIBE"
	}
	return "UNSUBSCRIBE"
}
func (s *Subscription) BuildCommand(args red.ArgBuilder) string {
	args.Strings(s.Channels...)
	if s.Pattern {
		return "PSUBSCRIBE"
	}
	return "SUBSCRIBE"
}
type Subscriber struct {
	messages      <-chan *Message
	subscribe     chan<- Subscription
	unsubscribe   chan<- Subscription
	subscriptions map[string]bool
	cancel        context.CancelFunc
}

func SubscribeContext(ctx context.Context, conn net.Conn) *Subscriber {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	r := resp.NewStream(conn)
	w := resp.NewWriter(conn)
	messages := make(chan *Message)
	subscribe := make(chan Subscription)
	unsubscribe := make(chan Subscription)
	go func() {
		args := red.ArgBuilder{}
		defer conn.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case sub := <-subscribe:
				cmd := sub.BuildCommand(&args)
				args.Reset()
			case usub :=<-unsubscribe:
				cmd := (*unSubscription)(&usub).BuildCommand(args)
				args.Reset()

			}
		}

	}()
	go func() {
		for {
			defer close(messages)
			v, err := r.Next()
			if err != nil {
				return
			}
			msg := Message{}
			if err := msg.UnmarshalRESP(v); err != nil {
				return
			}
			select {
			case messages <- &msg:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		defer conn.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case sub := <- subscribe:
				w.WriteArray(int64(1+len(sub.Channels)))
				if sub.Pattern {
					w.WriteBulkString("PSUBSCRIBE")
				} else {
					w.WriteBulkString("PUNSUBSCRIBE")
				}
				sub.
			case msg := <-messages:
				switch msg.Kind() {
				case KindMessage:
					select {
					case out <- msg:
					case <-ctx.Done():
						return
					}
				case KindSubscribe:
					subscriptions[msg.Channel()] = true
				case KindUnsubscribe:
					if n, ok := msg.NumChannels(); ok && n <= 0 {
						return
					}
					subscriptions[msg.Channel()] = false
				}

			}
		}

	}()
	return nil
}
