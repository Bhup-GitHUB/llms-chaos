package middleware

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
)

var globalID atomic.Uint64

func newID() string {
	return fmt.Sprintf("req-%d", globalID.Add(1))
}

func (m *Metrics) Render(poolReady int, version int64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "gateway_requests_total %d\n", m.requests.Load())
	fmt.Fprintf(&b, "gateway_failures_total %d\n", m.failures.Load())
	fmt.Fprintf(&b, "gateway_retries_total %d\n", m.retries.Load())
	fmt.Fprintf(&b, "gateway_rejected_total %d\n", m.rejected.Load())
	fmt.Fprintf(&b, "gateway_inflight %d\n", m.inflight.Load())
	fmt.Fprintf(&b, "gateway_workers_ready %d\n", poolReady)
	fmt.Fprintf(&b, "cluster_state_version %d\n", version)
	m.ttftMu.Lock()
	samples := append([]float64{}, m.ttftSamples...)
	m.ttftMu.Unlock()
	if len(samples) > 0 {
		sort.Float64s(samples)
		fmt.Fprintf(&b, "gateway_ttft_seconds_p50 %f\n", samples[len(samples)/2])
		fmt.Fprintf(&b, "gateway_ttft_seconds_p99 %f\n", samples[int(float64(len(samples))*0.99)])
	}
	return b.String()
}

func (m *Metrics) InflightDelta(d int64) {
	m.inflight.Add(d)
}
