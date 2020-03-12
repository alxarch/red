package red

import (
	"errors"
	"sync"
	"time"

	"github.com/alxarch/red/internal/pubsub"
)

// Publish publishes a message on a channel
func (b *batchAPI) Publish(channel, msg string) *ReplyInteger {
	b.args.String(channel)
	b.args.String(msg)
	return b.doInteger("PUBLISH")
}

// PubSubChannels lists all active channels.
// PUBSUB CHANNELS [pattern]
// If no pattern is specified all channels are listed,
// otherwise if pattern is specified only channels matching the specified glob-style pattern are listed.
func (b batchAPI) PubSubChannels(pattern string) *ReplyBulkStringArray {
	b.args.String("CHANNELS")
	if pattern != "" {
		b.args.String(pattern)
	}
	return b.doBulkStringArray("PUBSUB")
}

// PubSubNumSub returns the number of subscribers (not counting clients subscribed to patterns) for the specified channels.
// PUBSUB NUMSUB [channel-1 ... channel-N]
func (b *batchAPI) PubSubNumSub(channels ...string) *ReplyInteger {
	b.args.String("NUMSUB")
	b.args.Strings(channels...)
	return b.doInteger("PUBSUB")
}

// PubSubNumPat returns the number of subscriptions to patterns (that are performed using the PSUBSCRIBE command).
// Note that this is not just the count of clients subscribed to patterns but the total number of patterns all the clients are subscribed to.
// PUBSUB NUMPAT
func (b *batchAPI) PubSubNumPat() *ReplyInteger {
	b.args.String("NUMPAT")
	return b.doInteger("PUBSUB")
}

// Subscriber subscribes to redis PUB/SUB channels
type Subscriber struct {
	messages <-chan *PubSubMessage

	once    sync.Once
	closeCh chan struct{} // signals closing
	doneCh  chan struct{} // signals no more channels

	writeLock sync.Mutex
	args      ArgBuilder
	conn      managedConn
	pending   int

	subscriptions pubsub.Subscriptions
}

// PubSubMessage is a message from a PUB/SUB channel
type PubSubMessage struct {
	Channel string
	Payload string
}

func (sub *Subscriber) isClosed() bool {
	select {
	case <-sub.closeCh:
		return true
	case <-sub.doneCh:
		return true
	default:
		return false
	}
}

// Close closes the subscriber
func (sub *Subscriber) Close() (err error) {
	sub.closeOnce()
	channels, patterns := sub.subscriptions.Active()
	_ = sub.unsubscribe(channels...)
	_ = sub.punsubscribe(patterns...)
	<-sub.doneCh
	return nil
}
func (sub *Subscriber) closeConn() {
	sub.writeLock.Lock()
	defer sub.writeLock.Unlock()
	sub.conn.Close()
}

var errSubscriberClosed = errors.New("Subscriber closed")

// Subscribe subscribes to channels
func (sub *Subscriber) Subscribe(channels ...string) error {
	if sub.isClosed() {
		return errSubscriberClosed
	}
	if len(channels) == 0 {
		return nil
	}
	return sub.do("SUBSCRIBE", channels...)
}

// PSubscribe subscribes to channels matching patterns
func (sub *Subscriber) PSubscribe(patterns ...string) error {
	if sub.isClosed() {
		return errSubscriberClosed
	}
	if len(patterns) == 0 {
		return nil
	}
	return sub.do("PSUBSCRIBE", patterns...)
}

func (sub *Subscriber) do(cmd string, args ...string) error {
	sub.writeLock.Lock()
	defer sub.writeLock.Unlock()
	sub.args.Reset()
	sub.args.Strings(args...)
	sub.pending += len(args)
	_ = sub.conn.w.WriteCommand(cmd, sub.args.Args()...)
	err := sub.conn.w.Flush()
	if err != nil {
		_ = sub.conn.Close()
		return err
	}

	return err
}
func (sub *Subscriber) done() (n int) {
	sub.writeLock.Lock()
	sub.pending--
	n = sub.pending
	sub.writeLock.Unlock()
	return
}

func (sub *Subscriber) unsubscribe(channels ...string) error {
	if len(channels) == 0 {
		return nil
	}
	return sub.do("UNSUBSCRIBE", channels...)
}
func (sub *Subscriber) punsubscribe(patterns ...string) error {
	if len(patterns) == 0 {
		return nil
	}
	return sub.do("PUNSUBSCRIBE", patterns...)
}

// Unsubscribe unsubscribes from channels
func (sub *Subscriber) Unsubscribe(channels ...string) error {
	if sub.isClosed() {
		return errSubscriberClosed
	}
	return sub.unsubscribe(channels...)
}

// PUnsubscribe unsubscribes from channels matching pattenrs
func (sub *Subscriber) PUnsubscribe(patterns ...string) error {
	if sub.isClosed() {
		return errSubscriberClosed
	}
	return sub.punsubscribe(patterns...)
}

// Messages returns a channel of incoming PUB/SUB messages
func (sub *Subscriber) Messages() <-chan *PubSubMessage {
	return sub.messages
}

// Get waits timeout for a message
func (sub *Subscriber) Get() (*PubSubMessage, error) {
	select {
	case msg, ok := <-sub.Messages():
		if ok {
			return msg, nil
		}
		return nil, errSubscriberClosed
	default:
		return nil, nil
	}
}

// Block waits forever for a message
func (sub *Subscriber) Block() (*PubSubMessage, error) {
	if msg, ok := <-sub.Messages(); ok {
		return msg, nil
	}
	return nil, errSubscriberClosed
}

type timeoutError struct{}

func (timeoutError) Error() string {
	return "Timeout expired"
}
func (timeoutError) Timeout() bool {
	return true
}

// Wait waits `timeout` for a message
func (sub *Subscriber) Wait(timeout time.Duration) (*PubSubMessage, error) {
	t := time.NewTimer(timeout)
	select {
	case <-t.C:
		return nil, timeoutError{}
	case msg, ok := <-sub.Messages():
		if ok {
			return msg, nil
		}
		return nil, errSubscriberClosed
	}
}

func (sub *Subscriber) closeOnce() {
	sub.once.Do(func() {
		close(sub.closeCh)
	})
}

// func isTimeoutError(err error) bool {
// 	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
// 		return true
// 	}
// 	return false
// }

func (sub *Subscriber) listenPubSub(messages chan<- *PubSubMessage) {
	defer func() {
		sub.closeConn()
		close(sub.doneCh)
	}()
	defer sub.closeOnce()
	defer close(messages)
	if timeout := sub.conn.options.ReadTimeout; timeout > 0 {
		go func() {
			tick := time.NewTicker(timeout)
			defer tick.Stop()
			for {
				select {
				case <-sub.closeCh:
					return
				case <-tick.C:
					if err := sub.do("PING", "PONG"); err != nil {
						sub.closeOnce()
						return
					}
				}
			}
		}()
	}

	netConn := sub.conn.conn
	if err := netConn.SetReadDeadline(time.Time{}); err != nil {
		return
	}
	var numChannels int64
	for {
		msg := new(pubsub.IncomingMessage)
		if err := sub.conn.r.Decode(msg); err != nil {
			return
		}

		switch msg.Kind() {
		case pubsub.KindMessage:
			payload, _ := msg.Payload()
			channel, _ := msg.Channel()
			select {
			case messages <- (&PubSubMessage{
				Channel: channel,
				Payload: payload,
			}):
			case <-sub.closeCh:
			}
		case pubsub.KindUnsubscribe:
			channel, _ := msg.Channel()
			sub.subscriptions.Unsubscribe(channel, false)
			numChannels, _ = msg.NumChannels()
			p := sub.done()
			if numChannels == 0 && p <= 0 {
				return
			}
		case pubsub.KindUnsubscribeP:
			pattern, _ := msg.Pattern()
			numChannels, _ = msg.NumChannels()
			sub.subscriptions.Unsubscribe(pattern, true)
			p := sub.done()
			if numChannels == 0 && p <= 0 {
				return
			}
		case pubsub.KindSubscribe:
			ch, _ := msg.Channel()
			numChannels, _ = msg.NumChannels()
			_ = sub.done()
			if sub.isClosed() {
				sub.unsubscribe(ch)
			} else {
				sub.subscriptions.Subscribe(ch, false)
			}
		case pubsub.KindSubscribeP:
			pat, _ := msg.Pattern()
			numChannels, _ = msg.NumChannels()
			_ = sub.done()
			if sub.isClosed() {
				sub.punsubscribe(pat)
			} else {
				sub.subscriptions.Subscribe(pat, true)
			}
		case pubsub.KindPong:
			p := sub.done()
			if p == 0 && numChannels == 0 {
				return
			}
			// if err := resetTimeout(); err != nil {
			// 	return
			// }
		}
	}
}

// Subscriber enables pub/sub subscriber mode for a connection
func (conn *Conn) Subscriber(queueSize int) (*Subscriber, error) {
	if err := conn.Err(); err != nil {
		return nil, err
	}
	if conn.state.CountReplies() > 0 {
		return nil, ErrReplyPending
	}
	conn.managed = true
	if queueSize < 0 {
		queueSize = 0
	}
	messages := make(chan *PubSubMessage, queueSize)
	sub := Subscriber{
		conn: managedConn{
			Conn: conn,
		},
		closeCh:  make(chan struct{}),
		doneCh:   make(chan struct{}),
		messages: messages,
	}
	go sub.listenPubSub(messages)
	return &sub, nil
}
