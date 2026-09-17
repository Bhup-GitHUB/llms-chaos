package limiter

import "sync"

type Limiter struct {
	mu       sync.Mutex
	caps     map[string]int
	inflight map[string]int
	initial  int
	min      int
	max      int
}

func New(initial, min, max int) *Limiter {
	if min < 1 {
		min = 1
	}
	if max < min {
		max = min
	}
	if initial < min {
		initial = min
	}
	if initial > max {
		initial = max
	}
	return &Limiter{
		caps:     map[string]int{},
		inflight: map[string]int{},
		initial:  initial,
		min:      min,
		max:      max,
	}
}

func (l *Limiter) TryAcquire(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.caps[id]
	if !ok {
		c = l.initial
		l.caps[id] = c
	}
	n := l.inflight[id]
	if n >= c {
		return false
	}
	l.inflight[id] = n + 1
	return true
}

func (l *Limiter) Release(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n, ok := l.inflight[id]; ok && n > 0 {
		l.inflight[id] = n - 1
	}
}

func (l *Limiter) OnSuccess(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.caps[id]
	if !ok {
		c = l.initial
	}
	if c < l.max {
		c++
	}
	l.caps[id] = c
}

func (l *Limiter) OnTimeout(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.caps[id]
	if !ok {
		c = l.initial
	}
	c = c / 2
	if c < l.min {
		c = l.min
	}
	l.caps[id] = c
}

func (l *Limiter) Cap(id string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if c, ok := l.caps[id]; ok {
		return c
	}
	return l.initial
}

func (l *Limiter) Inflight(id string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inflight[id]
}
