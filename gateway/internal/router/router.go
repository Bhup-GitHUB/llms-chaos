package router

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

type Worker struct {
	ID       string
	Host     string
	Port     int
	State    string
	Model    string
	inflight atomic.Int64
	ewmaMs   atomic.Uint64
}

func (w *Worker) score() float64 {
	return float64(w.inflight.Load())*1000 + float64(w.ewmaMs.Load())
}

func (w *Worker) Observe(rttMs uint64) {
	for {
		old := w.ewmaMs.Load()
		next := uint64(0.8*float64(old) + 0.2*float64(rttMs))
		if w.ewmaMs.CompareAndSwap(old, next) {
			return
		}
	}
}

type Snapshot struct {
	ID    string
	Host  string
	Port  int
	State string
	Model string
}

type Pool struct {
	mu      sync.RWMutex
	workers map[string]*Worker
}

func New() *Pool {
	return &Pool{workers: map[string]*Worker{}}
}

func (p *Pool) Update(snapshots []Snapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := map[string]bool{}
	for _, s := range snapshots {
		seen[s.ID] = true
		if w, ok := p.workers[s.ID]; ok {
			w.Host = s.Host
			w.Port = s.Port
			w.State = s.State
			w.Model = s.Model
			continue
		}
		p.workers[s.ID] = &Worker{ID: s.ID, Host: s.Host, Port: s.Port, State: s.State, Model: s.Model}
	}
	for id := range p.workers {
		if !seen[id] {
			delete(p.workers, id)
		}
	}
}

func (p *Pool) ready() []*Worker {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := []*Worker{}
	for _, w := range p.workers {
		if w.State == "ready" {
			out = append(out, w)
		}
	}
	return out
}

func (p *Pool) Pick() *Worker {
	candidates := p.ready()
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return candidates[0]
	}
	a := candidates[rand.Intn(len(candidates))]
	b := candidates[rand.Intn(len(candidates))]
	if a.score() <= b.score() {
		return a
	}
	return b
}

func (p *Pool) Models() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	seen := map[string]bool{}
	out := []string{}
	for _, w := range p.workers {
		if w.Model != "" && !seen[w.Model] {
			seen[w.Model] = true
			out = append(out, w.Model)
		}
	}
	return out
}
