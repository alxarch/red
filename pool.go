package red

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Pool is a pool of redis connections
type Pool struct {
	noCopy noCopy //nolint:unused,structcheck

	Dial           func() (*Conn, error) // Dialer for redis connections (required)
	MaxConnections int                   // Maximum number of connection to open on demand (defaults to 1)
	MinConnections int                   // Minimum number of connections to keep open once dialed (defaults to 1)
	MaxIdleTime    time.Duration         // Max time a connection will be left idling (0 => no limit)
	ClockInterval  time.Duration         // Minimum unit of time for timeouts and intervals (defaults to 50ms)

	once      sync.Once
	closeChan chan struct{}
	cond      sync.Cond

	mu     sync.Mutex
	open   int
	closed bool
	wall   time.Time
	idle   []*Conn
	queue  []*Conn

	// queueLock sync.Mutex

	// activeLock  sync.RWMutex
	connections map[*Conn]struct{}

	stats struct {
		dials, hits, misses, timeouts int64
	}
	clients sync.Pool // local pool of clients
}

// PoolStats counts pool statistics
type PoolStats struct {
	Hits, Misses, Timeouts, Dials int64
	Idle, Active                  int
}

// Stats returns current pool statistics
func (p *Pool) Stats() PoolStats {
	stats := PoolStats{
		Hits:     atomic.LoadInt64(&p.stats.hits),
		Misses:   atomic.LoadInt64(&p.stats.misses),
		Timeouts: atomic.LoadInt64(&p.stats.timeouts),
		Dials:    atomic.LoadInt64(&p.stats.dials),
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	stats.Idle = len(p.idle) + len(p.queue)
	stats.Active = len(p.connections)
	return stats
}

// Client waits indefinetely for a client to become available
func (p *Pool) Client() (*Client, error) {
	return p.ClientTimeout(0)
}

// ClientTimeout waits `timeout` for a client to become available
func (p *Pool) ClientTimeout(timeout time.Duration) (*Client, error) {
	conn, err := p.GetTimeout(timeout)
	if err != nil {
		return nil, err
	}
	return conn.Client()
}

// GetTimeout waits `timeout` to get a connection from the pool
//
// To release the connection back to the pool use `Pool.Put(*Conn)`
func (p *Pool) GetTimeout(timeout time.Duration) (*Conn, error) {
	if timeout > 0 {
		deadline := time.Now().Add(timeout)
		return p.GetDeadline(deadline)
	}
	return p.GetDeadline(time.Time{})
}

// Get waits indefinitely to get a connection from the pool
//
// To release the connection back to the pool use `Pool.Put(*Conn)`
func (p *Pool) Get() (*Conn, error) {
	return p.GetDeadline(time.Time{})
}

// Close closes a pool and all it's connections
// TODO: [pool] Implement grace period for when closing a pool
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errPoolClosed
	}
	p.closed = true
	var idle []*Conn
	idle, p.idle = p.idle, nil
	for _, conn := range idle {
		conn.closeWithError(errPoolClosed)
	}
	idle, p.queue = p.queue, nil
	for _, conn := range idle {
		conn.closeWithError(errPoolClosed)
	}
	if ch := p.closeChan; ch != nil {
		p.closeChan = nil
		close(ch)
	}
	// Notify all goroutines waiting on pool.get()
	p.cond.Broadcast()
	return nil
}

// DoCommand executes cmd on a new connection
func (p *Pool) DoCommand(dest interface{}, cmd string, args ...Arg) error {
	conn, err := p.Get()
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.DoCommand(dest, cmd, args...)
}

var (
	errPoolClosed       = errors.New("Pool closed")
	errDeadlineExceeded = errors.New("Deadline exceeded")
)

func (p *Pool) discard(c *Conn) {
	c.pool = nil
	defer c.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.connections, c)
	p.open--
}

func (p *Pool) put(c *Conn) error {
	if p == nil {
		return nil
	}
	if c == nil {
		return nil
	}
	if err := c.Reset(nil); err != nil {
		p.discard(c)
		return err
	}
	if c.err != nil {
		p.discard(c)
		return c.err
	}
	p.once.Do(p.init)
	max := p.minConnections()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		c.closeWithError(errPoolClosed)
		p.discard(c)
		return errPoolClosed
	}
	if len(p.idle) < max {
		p.queue = append(p.queue, c)
		p.mu.Unlock()
		p.cond.Signal()
		return nil
	}
	p.mu.Unlock()
	c.err = errors.New("Dropping connection")
	p.discard(c)
	return errConnClosed
}

func (p *Pool) maxConnections() int {
	if p.MaxConnections > 0 {
		return p.MaxConnections
	}
	return 1
}
func (p *Pool) minConnections() int {
	if p.MinConnections > 0 {
		return p.MinConnections
	}
	return p.maxConnections()
}

func (p *Pool) getClient() *Client {
	if client, ok := p.clients.Get().(*Client); ok {
		return client
	}
	return new(Client)
}

func (p *Pool) putClient(client *Client) {
	if client == nil {
		return
	}
	client.clear()
	p.clients.Put(client)
}

func (p *Pool) init() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closeChan != nil {
		panic("Pool already opened")
	}
	p.closeChan = make(chan struct{})
	p.cond.L = &p.mu
	p.connections = make(map[*Conn]struct{})
	go p.run()
}

const defaultClockInterval = 100 * time.Millisecond

func (p *Pool) run() {
	clockInterval := defaultClockInterval
	if p.ClockInterval > 0 {
		clockInterval = p.ClockInterval
	}
	clock := time.NewTicker(clockInterval)
	defer clock.Stop()
	var cleanInterval <-chan time.Time
	interval := p.MaxIdleTime
	if interval > 0 {
		tick := time.NewTicker(interval)
		defer tick.Stop()
		cleanInterval = tick.C
	}
	for {
		select {
		case t := <-clock.C:
			p.mu.Lock()
			p.wall = t
			p.mu.Unlock()
			// pool.cond.Broadcast()
		case <-p.closeChan:
			return
		case t := <-cleanInterval:
			p.cleanup(t)
		}
	}
}

func (p *Pool) cleanup(now time.Time) (int, error) {
	maxAge := p.MaxIdleTime
	if maxAge <= 0 {
		return 0, nil
	}
	minT := now.Add(-maxAge)
	size := p.MinConnections
	if size <= 0 {
		size = 1
	}
	del := make([]*Conn, 0, size)
	defer func() {
		for _, c := range del {
			p.discard(c)
		}
	}()
	idle := make([]*Conn, 0, size)
	defer p.cond.Signal()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, errPoolClosed
	}
	idle, p.idle = p.idle, idle
	for _, c := range idle {
		if c.lastUsedAt.Before(minT) {
			del = append(del, c)
		} else {
			p.idle = append(p.idle, c)
		}
	}
	return len(del), nil
}

// TODO: Add option to limit concurrent dials
func (p *Pool) dial() (*Conn, error) {
	atomic.AddInt64(&p.stats.dials, 1)
	conn, err := p.Dial()
	if err != nil {
		defer p.cond.Signal()
		p.mu.Lock()
		// Unreserve dial slot
		p.open--
		p.mu.Unlock()
		return nil, err
	}
	// Link connection to pool
	conn.pool = p

	// Register connection
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connections[conn] = struct{}{}
	return conn, nil
}

func (p *Pool) popLocked() (conn *Conn) {
	if i := len(p.idle) - 1; 0 <= i && i < len(p.idle) {
		// Elide bounds check by keeping everything in one statement
		conn, p.idle, p.idle[i] = p.idle[i], p.idle[:i], nil
		return
	}
	// Flush queue in reverse so that pool.idle acts as FIFO
	if q := p.queue; len(q) > 0 {
		conn, q[0], q = q[0], nil, q[1:]
		for i := len(q) - 1; 0 <= i && i < len(q); i-- {
			p.idle, q[i] = append(p.idle, q[i]), nil
		}
		p.queue = p.queue[:0]
	}
	return
}

// GetDeadline waits until deadline for a connection
func (p *Pool) GetDeadline(deadline time.Time) (c *Conn, err error) {
	max := p.maxConnections()
	isTimeout := !deadline.IsZero()
	p.once.Do(p.init)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errPoolClosed
	}
	if c = p.popLocked(); c != nil {
		p.mu.Unlock()
		atomic.AddInt64(&p.stats.hits, 1)
		return
	}
	if p.open < max {
		p.open++
		p.mu.Unlock()
		p.cond.Signal()
		return p.dial()
	}

	for c == nil {
		// Block waiting for broadcast
		p.cond.Wait()
		// This happens after cond locks again
		if p.closed {
			p.mu.Unlock()
			return nil, errPoolClosed
		}
		if isTimeout && p.wall.After(deadline) {
			p.mu.Unlock()
			// There might be available connections for others
			p.cond.Signal()
			// Update stats
			atomic.AddInt64(&p.stats.timeouts, 1)
			return nil, errDeadlineExceeded
		}
		if p.open < max {
			p.open++
			p.mu.Unlock()
			p.cond.Signal()
			return p.dial()
		}
		c = p.popLocked()
	}
	p.mu.Unlock()

	// Update stats
	atomic.AddInt64(&p.stats.misses, 1)
	if c == nil {
		panic("Nil connection")
	}
	return
}
