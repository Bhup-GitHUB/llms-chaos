package middleware

import (
	"net/http"
	"sync"
	"sync/atomic"
)

type Metrics struct {
	requests      atomic.Uint64
	failures      atomic.Uint64
	retries       atomic.Uint64
	rejected      atomic.Uint64
	inflight      atomic.Int64
	ttftMu        sync.Mutex
	ttftSamples   []float64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) Begin() bool {
	return true
}

func (m *Metrics) End(ok bool) {
	m.requests.Add(1)
	if !ok {
		m.failures.Add(1)
	}
}

func (m *Metrics) Retry() {
	m.retries.Add(1)
}

func (m *Metrics) Reject() {
	m.rejected.Add(1)
}

func (m *Metrics) ObserveTTFT(s float64) {
	m.ttftMu.Lock()
	defer m.ttftMu.Unlock()
	m.ttftSamples = append(m.ttftSamples, s)
	if len(m.ttftSamples) > 4096 {
		m.ttftSamples = m.ttftSamples[2048:]
	}
}

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = newID()
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r)
	})
}

var idCounter atomic.Uint64
