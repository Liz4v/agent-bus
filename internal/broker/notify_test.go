package broker

import (
	"sync"
	"testing"
	"time"
)

func recv(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("subscriber not woken")
	}
}

func TestNotifyWakesAllSubscribers(t *testing.T) {
	n := NewNotify()
	a, ca := n.Subscribe(1)
	b, cb := n.Subscribe(1)
	other, co := n.Subscribe(2)
	defer ca()
	defer cb()
	defer co()
	n.Broadcast(1)
	recv(t, a)
	recv(t, b)
	select {
	case <-other:
		t.Fatal("subscriber of another session woken")
	default:
	}
}

func TestNotifySubscribeBeforeBroadcastNotMissed(t *testing.T) {
	n := NewNotify()
	ch, cancel := n.Subscribe(7)
	defer cancel()
	n.Broadcast(7) // before anyone reads the channel
	n.Broadcast(7)
	recv(t, ch)
}

func TestNotifyBroadcastMultipleSessions(t *testing.T) {
	n := NewNotify()
	a, ca := n.Subscribe(1)
	b, cb := n.Subscribe(2)
	defer ca()
	defer cb()
	n.Broadcast(1, 2)
	recv(t, a)
	recv(t, b)
}

func TestNotifyCancelRemoves(t *testing.T) {
	n := NewNotify()
	ch, cancel := n.Subscribe(1)
	cancel()
	cancel() // idempotent
	n.Broadcast(1)
	select {
	case <-ch:
		t.Fatal("cancelled subscriber woken")
	default:
	}
	if n.subscriberCount() != 0 {
		t.Fatal("subscriber leaked")
	}
}

func TestNotifyNoLeak(t *testing.T) {
	n := NewNotify()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				_, cancel := n.Subscribe(int64(i % 5))
				n.Broadcast(int64(i % 5))
				cancel()
			}
		}(g)
	}
	wg.Wait()
	if n.subscriberCount() != 0 {
		t.Fatalf("leaked %d subscribers", n.subscriberCount())
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.subs) != 0 {
		t.Fatalf("leaked %d session maps", len(n.subs))
	}
}
