package hedge

import (
	"context"
	"sort"
	"sync"
	"time"
)

type Budget struct {
	mu      sync.Mutex
	samples []float64
	def     time.Duration
}

func NewBudget(def time.Duration) *Budget {
	if def <= 0 {
		def = 800 * time.Millisecond
	}
	return &Budget{def: def}
}

func (b *Budget) Observe(d time.Duration) {
	if d <= 0 {
		return
	}
	s := d.Seconds()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.samples = append(b.samples, s)
	if len(b.samples) > 4096 {
		trimmed := make([]float64, 2048)
		copy(trimmed, b.samples[len(b.samples)-2048:])
		b.samples = trimmed
	}
}

func (b *Budget) Value() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.samples) == 0 {
		return b.def
	}
	cp := make([]float64, len(b.samples))
	copy(cp, b.samples)
	sort.Float64s(cp)
	idx := int(float64(len(cp)) * 0.95)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	v := cp[idx]
	if v <= 0 {
		return b.def
	}
	return time.Duration(v * float64(time.Second))
}

type result[T any] struct {
	value T
	err   error
}

func Race[T any](ctx context.Context, budget time.Duration, primary func(context.Context) (T, error), secondary func(context.Context) (T, error)) (T, error, bool) {
	return RaceWithCleanup(ctx, budget, primary, secondary, nil)
}

func RaceWithCleanup[T any](ctx context.Context, budget time.Duration, primary func(context.Context) (T, error), secondary func(context.Context) (T, error), cleanup func(T)) (T, error, bool) {
	var zero T
	if primary == nil {
		if secondary == nil {
			return zero, context.Canceled, false
		}
		v, err := secondary(ctx)
		return v, err, false
	}
	if secondary == nil {
		v, err := primary(ctx)
		return v, err, false
	}
	if budget <= 0 {
		pctx, pcancel := context.WithCancel(ctx)
		sctx, scancel := context.WithCancel(ctx)
		pch := make(chan result[T], 1)
		sch := make(chan result[T], 1)
		go func() {
			v, err := primary(pctx)
			pch <- result[T]{value: v, err: err}
		}()
		go func() {
			v, err := secondary(sctx)
			sch <- result[T]{value: v, err: err}
		}()
		select {
		case r := <-pch:
			scancel()
			go func() {
				s := <-sch
				if s.err == nil && cleanup != nil {
					cleanup(s.value)
				}
			}()
			return r.value, r.err, true
		case r := <-sch:
			pcancel()
			go func() {
				p := <-pch
				if p.err == nil && cleanup != nil {
					cleanup(p.value)
				}
			}()
			return r.value, r.err, true
		case <-ctx.Done():
			pcancel()
			scancel()
			return zero, ctx.Err(), true
		}
	}
	pctx, pcancel := context.WithCancel(ctx)
	pch := make(chan result[T], 1)
	go func() {
		v, err := primary(pctx)
		pch <- result[T]{value: v, err: err}
	}()
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case r := <-pch:
		return r.value, r.err, false
	case <-timer.C:
	case <-ctx.Done():
		pcancel()
		return zero, ctx.Err(), false
	}
	sctx, scancel := context.WithCancel(ctx)
	sch := make(chan result[T], 1)
	go func() {
		v, err := secondary(sctx)
		sch <- result[T]{value: v, err: err}
	}()
	select {
	case r := <-pch:
		scancel()
		go func() {
			s := <-sch
			if s.err == nil && cleanup != nil {
				cleanup(s.value)
			}
		}()
		return r.value, r.err, true
	case r := <-sch:
		pcancel()
		go func() {
			p := <-pch
			if p.err == nil && cleanup != nil {
				cleanup(p.value)
			}
		}()
		return r.value, r.err, true
	case <-ctx.Done():
		pcancel()
		scancel()
		return zero, ctx.Err(), true
	}
}
