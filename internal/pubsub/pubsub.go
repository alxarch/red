package pubsub

import (
	"fmt"
	"sync"

	"github.com/alxarch/red/resp"
)

type MessageKind string

const (
	KindSubscribe    MessageKind = "subscribe"
	KindSubscribeP   MessageKind = "psubscribe"
	KindUnsubscribe  MessageKind = "unsubscribe"
	KindUnsubscribeP MessageKind = "punsubscribe"
	KindMessage      MessageKind = "message"
	KindPong         MessageKind = "pong"
)

type IncomingMessage struct {
	kind        MessageKind
	payload     string
	numChannels int64
	channel     string
}

func (m *IncomingMessage) Kind() MessageKind {
	return m.kind
}

func (m *IncomingMessage) Channel() (string, bool) {
	switch m.kind {
	case KindMessage, KindSubscribe, KindUnsubscribe:
		return m.channel, true
	}
	return "", false
}

func (m *IncomingMessage) Pattern() (string, bool) {
	switch m.kind {
	case KindSubscribeP, KindUnsubscribeP:
		return m.channel, true
	}
	return "", false
}

func (m *IncomingMessage) Payload() (string, bool) {
	return m.payload, m.kind == KindMessage
}
func (m *IncomingMessage) NumChannels() (int64, bool) {
	return m.numChannels, m.kind == KindSubscribe || m.kind == KindUnsubscribe
}

func (m *IncomingMessage) UnmarshalRESP(value resp.Value) error {
	var kind resp.BulkString
	iter := value.Iter()
	if err := kind.UnmarshalRESP(iter.Value()); err != nil {
		return fmt.Errorf("Invalid incoming message %v", value.Any())
	}
	switch m.kind = MessageKind(kind.String); m.kind {
	case KindMessage:
		var str resp.BulkString
		if !iter.More() {
			return fmt.Errorf("Invalid incoming message %v", value.Any())
		}
		iter.Next()
		if err := str.UnmarshalRESP(iter.Value()); err != nil {
			return fmt.Errorf("Invalid incoming message %v", value.Any())
		}
		if !str.Valid {
			return fmt.Errorf("Invalid incoming message %v", value.Any())
		}
		m.channel = str.String
		if !iter.More() {
			return fmt.Errorf("Invalid incoming message %v", value.Any())
		}
		iter.Next()
		if err := str.UnmarshalRESP(iter.Value()); err != nil {
			return fmt.Errorf("Invalid incoming message %v", value.Any())
		}
		if !str.Valid {
			return fmt.Errorf("Invalid incoming message %v", value.Any())
		}
		m.payload = str.String
		return nil
	case KindPong:
		if !iter.More() {
			return fmt.Errorf("Invalid incoming message %v", value.Any())
		}
		iter.Next()
		var payload resp.BulkString
		if err := payload.UnmarshalRESP(iter.Value()); err != nil {
			return fmt.Errorf("Invalid incoming message %v", value.Any())
		}
		if !payload.Valid {
			return fmt.Errorf("Invalid incoming message %v", value.Any())
		}
		m.payload = payload.String
		return nil
	case KindSubscribe, KindUnsubscribe, KindSubscribeP, KindUnsubscribeP:
		var str resp.BulkString
		if !iter.More() {
			return fmt.Errorf("Invalid incoming message %v", value.Any())
		}
		iter.Next()
		if err := str.UnmarshalRESP(iter.Value()); err != nil {
			return fmt.Errorf("Invalid incoming message %v", value.Any())
		}
		if !str.Valid {
			return fmt.Errorf("Invalid incoming message %v", value.Any())
		}
		m.channel = str.String
		if !iter.More() {
			return fmt.Errorf("Invalid incoming message %v", value.Any())
		}
		iter.Next()
		var numChannels resp.Integer
		if err := numChannels.UnmarshalRESP(iter.Value()); err != nil {
			return fmt.Errorf("Invalid incoming message %v", value.Any())
		}
		m.numChannels = int64(numChannels)
		return nil
	default:
		return fmt.Errorf("Invalid incoming message %v", value.Any())
	}
}

// type Subscriber struct {
// 	messages      <-chan *Message
// 	subscribe     chan<- Subscription
// 	unsubscribe   chan<- Subscription
// 	subscriptions map[string]bool
// 	cancel        context.CancelFunc
// }

// func SubscribeContext(ctx context.Context, conn net.Conn) *Subscriber {
// 	if ctx == nil {
// 		ctx = context.Background()
// 	}
// 	ctx, cancel := context.WithCancel(ctx)
// 	r := resp.NewStream(conn)
// 	w := resp.NewWriter(conn)
// 	messages := make(chan *Message)
// 	subscribe := make(chan Subscription)
// 	unsubscribe := make(chan Subscription)
// 	go func() {
// 		args := red.ArgBuilder{}
// 		defer conn.Close()
// 		for {
// 			select {
// 			case <-ctx.Done():
// 				return
// 			case sub := <-subscribe:
// 				cmd := sub.BuildCommand(&args)
// 				args.Reset()
// 			case usub :=<-unsubscribe:
// 				cmd := (*unSubscription)(&usub).BuildCommand(args)
// 				args.Reset()

// 			}
// 		}

// 	}()
// 	go func() {
// 		for {
// 			defer close(messages)
// 			v, err := r.Next()
// 			if err != nil {
// 				return
// 			}
// 			msg := Message{}
// 			if err := msg.UnmarshalRESP(v); err != nil {
// 				return
// 			}
// 			select {
// 			case messages <- &msg:
// 			case <-ctx.Done():
// 				return
// 			}
// 		}
// 	}()
// 	go func() {
// 		defer conn.Close()
// 		for {
// 			select {
// 			case <-ctx.Done():
// 				return
// 			case sub := <- subscribe:
// 				w.WriteArray(int64(1+len(sub.Channels)))
// 				if sub.Pattern {
// 					w.WriteBulkString("PSUBSCRIBE")
// 				} else {
// 					w.WriteBulkString("PUNSUBSCRIBE")
// 				}
// 				sub.
// 			case msg := <-messages:
// 				switch msg.Kind() {
// 				case KindMessage:
// 					select {
// 					case out <- msg:
// 					case <-ctx.Done():
// 						return
// 					}
// 				case KindSubscribe:
// 					subscriptions[msg.Channel()] = true
// 				case KindUnsubscribe:
// 					if n, ok := msg.NumChannels(); ok && n <= 0 {
// 						return
// 					}
// 					subscriptions[msg.Channel()] = false
// 				}

// 			}
// 		}

// 	}()
// 	return nil
// }

type Subscription struct {
	Channel string
	Pattern bool
}

type Subscriptions struct {
	mu      sync.RWMutex
	entries map[Subscription]struct{}
}

func (s *Subscriptions) Subscribe(ch string, pattern bool) {
	entry := Subscription{
		Channel: ch,
		Pattern: pattern,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries == nil {
		s.entries = make(map[Subscription]struct{})
	}
	s.entries[entry] = struct{}{}
}

func (s *Subscriptions) Unsubscribe(ch string, pattern bool) {
	entry := Subscription{
		Channel: ch,
		Pattern: pattern,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries == nil {
		return
	}
	delete(s.entries, entry)
}

func (s *Subscriptions) Active() (channels, patterns []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for sub := range s.entries {
		if sub.Pattern {
			patterns = append(patterns, sub.Channel)
		} else {
			channels = append(channels, sub.Channel)
		}
	}
	return
}
