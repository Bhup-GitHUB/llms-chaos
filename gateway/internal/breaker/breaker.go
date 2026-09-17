package breaker

import (
	"sync"
	"time"
)

type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

type Breaker struct {
	mu        sync.Mutex
	state     State
	failures  int
	threshold int
	cooldown  time.Duration
	openedAt  time.Time
}

type Registry struct {
	mu        sync.Mutex
	breakers  map[string]*Breaker
	threshold int
	cooldown  time.Duration
}

func NewRegistry(threshold int, cooldown time.Duration) *Registry {
	return &Registry{breakers: map[string]*Breaker{}, threshold: threshold, cooldown: cooldown}
}

func (r *Registry) For(id string) *Breaker {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.breakers[id]; ok {
		return b
	}
	b := &Breaker{threshold: r.threshold, cooldown: r.cooldown}
	r.breakers[id] = b
	return b
}

func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == Open && time.Since(b.openedAt) >= b.cooldown {
		b.state = HalfOpen
	}
	return b.state != Open
}

func (b *Breaker) Record(ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ok {
		b.failures = 0
		b.state = Closed
		return
	}
	b.failures++
	if b.state == HalfOpen || b.failures >= b.threshold {
		b.state = Open
		b.openedAt = time.Now()
	}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
