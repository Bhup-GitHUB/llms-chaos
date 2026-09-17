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
	mu       sync.RWMutex
	workers  map[string]*Worker
	affinity map[string]string
}

func New() *Pool {
	return &Pool{workers: map[string]*Worker{}, affinity: map[string]string{}}
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
	if p.affinity == nil {
		p.affinity = map[string]string{}
	}
	for k, v := range p.affinity {
		w, ok := p.workers[v]
		if !ok || w.State != "ready" {
			delete(p.affinity, k)
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

func (p *Pool) PickForKey(key string) *Worker {
	if key == "" {
		return p.Pick()
	}
	p.mu.RLock()
	id, ok := p.affinity[key]
	w, wok := p.workers[id]
	p.mu.RUnlock()
	if ok && wok && w.State == "ready" {
		return w
	}
	chosen := p.Pick()
	if chosen == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.affinity == nil {
		p.affinity = map[string]string{}
	}
	if id2, ok2 := p.affinity[key]; ok2 {
		if w2, wok2 := p.workers[id2]; wok2 && w2.State == "ready" {
			return w2
		}
	}
	p.affinity[key] = chosen.ID
	return chosen
}

func (p *Pool) PickExcept(excludeID string) *Worker {
	candidates := p.ready()
	filtered := make([]*Worker, 0, len(candidates))
	for _, w := range candidates {
		if w.ID != excludeID {
			filtered = append(filtered, w)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	if len(filtered) == 1 {
		return filtered[0]
	}
	a := filtered[rand.Intn(len(filtered))]
	b := filtered[rand.Intn(len(filtered))]
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
