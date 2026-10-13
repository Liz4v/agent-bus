// Package broker implements the agent-bus broker: storage, sweeper and HTTP API.
package broker

import "sync"

// Notify is a per-session broadcast hub. Waiters subscribe to a session and are
// woken by every Broadcast that names it. Sessions without subscribers hold no
// state.
type Notify struct {
	mu   sync.Mutex
	subs map[int64]map[*subscriber]struct{}
}

type subscriber struct {
	ch chan struct{}
}

// NewNotify returns an empty hub.
func NewNotify() *Notify {
	return &Notify{subs: make(map[int64]map[*subscriber]struct{})}
}

// Subscribe registers a waiter on a session. The returned channel receives a
// (coalesced) signal for every broadcast after this call returns. cancel
// removes the subscription; it is safe to call more than once.
func (n *Notify) Subscribe(sessionID int64) (<-chan struct{}, func()) {
	s := &subscriber{ch: make(chan struct{}, 1)}
	n.mu.Lock()
	m := n.subs[sessionID]
	if m == nil {
		m = make(map[*subscriber]struct{})
		n.subs[sessionID] = m
	}
	m[s] = struct{}{}
	n.mu.Unlock()
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			if m := n.subs[sessionID]; m != nil {
				delete(m, s)
				if len(m) == 0 {
					delete(n.subs, sessionID)
				}
			}
		})
	}
	return s.ch, cancel
}

// Broadcast wakes every subscriber of each named session.
func (n *Notify) Broadcast(ids ...int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, id := range ids {
		for s := range n.subs[id] {
			select {
			case s.ch <- struct{}{}:
			default: // a signal is already pending
			}
		}
	}
}

// subscriberCount is used by tests.
func (n *Notify) subscriberCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	c := 0
	for _, m := range n.subs {
		c += len(m)
	}
	return c
}
