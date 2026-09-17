package reporter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type Result struct {
	Start     time.Time
	End       time.Time
	SuspectAt time.Time
	DeadAt    time.Time
	ReadyAt   time.Time
	MTTD      time.Duration
	MTTR      time.Duration
	P99TTFT   float64
	Path      string
}

type sample struct {
	T time.Time
	V float64
}

type rangeResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Values [][]any           `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	case int:
		return float64(x), true
	default:
		return 0, false
	}
}

func fetchMatrix(baseURL string, query string, start time.Time, end time.Time, step time.Duration) ([]sample, error) {
	u, err := url.Parse(baseURL + "/api/v1/query_range")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("query", query)
	q.Set("start", strconv.FormatInt(start.Unix(), 10))
	q.Set("end", strconv.FormatInt(end.Unix(), 10))
	q.Set("step", fmt.Sprintf("%ds", int(step.Seconds())))
	u.RawQuery = q.Encode()
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("prometheus status %d", resp.StatusCode)
	}
	var rr rangeResponse
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return nil, err
	}
	out := []sample{}
	for _, series := range rr.Data.Result {
		for _, pair := range series.Values {
			if len(pair) != 2 {
				continue
			}
			ts, ok := toFloat(pair[0])
			if !ok {
				continue
			}
			val, ok := toFloat(pair[1])
			if !ok {
				continue
			}
			out = append(out, sample{T: time.Unix(int64(ts), 0).UTC(), V: val})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].T.Before(out[j].T) })
	return out, nil
}

func versionEvents(points []sample) (time.Time, time.Time, time.Time) {
	var suspect, dead, ready time.Time
	if len(points) == 0 {
		return suspect, dead, ready
	}
	current := points[0].V
	found := []time.Time{}
	for _, p := range points[1:] {
		if p.V > current {
			current = p.V
			found = append(found, p.T)
		}
	}
	if len(found) > 0 {
		suspect = found[0]
	}
	if len(found) > 1 {
		dead = found[1]
	}
	if len(found) > 2 {
		ready = found[2]
	}
	return suspect, dead, ready
}

func maxValue(points []sample) float64 {
	max := 0.0
	for _, p := range points {
		if p.V > max {
			max = p.V
		}
	}
	return max
}

func formatTS(t time.Time) string {
	if t.IsZero() {
		return "unknown"
	}
	return t.UTC().Format(time.RFC3339)
}

func Generate(promURL string, start time.Time, end time.Time, outDir string) (Result, error) {
	if promURL == "" {
		promURL = "http://localhost:9090"
	}
	if outDir == "" {
		outDir = "reports"
	}
	step := 15 * time.Second
	statePoints, err := fetchMatrix(promURL, "cluster_state_version", start, end, step)
	if err != nil {
		return Result{}, err
	}
	ttftPoints, err := fetchMatrix(promURL, "histogram_quantile(0.99, sum(rate(ttft_seconds_bucket[5m])) by (le))", start, end, step)
	if err != nil {
		return Result{}, err
	}
	suspect, dead, ready := versionEvents(statePoints)
	var mttd, mttr time.Duration
	if !suspect.IsZero() && !dead.IsZero() {
		mttd = dead.Sub(suspect)
	}
	if !dead.IsZero() && !ready.IsZero() {
		mttr = ready.Sub(dead)
	}
	p99 := maxValue(ttftPoints)
	res := Result{
		Start:     start.UTC(),
		End:       end.UTC(),
		SuspectAt: suspect,
		DeadAt:    dead,
		ReadyAt:   ready,
		MTTD:      mttd,
		MTTR:      mttr,
		P99TTFT:   p99,
	}
	md := fmt.Sprintf("# Chaos Report\n\nWindow: %s to %s\n\n| Metric | Value |\n|---|---|\n| Suspect at | %s |\n| Dead at | %s |\n| Ready at | %s |\n| MTTD (suspect-to-dead) | %s |\n| MTTR (dead-to-ready) | %s |\n| TTFT p99 during window (s) | %.4f |\n\nSource queries: `cluster_state_version` for state transitions, `histogram_quantile(0.99, sum(rate(ttft_seconds_bucket[5m])) by (le))` for TTFT p99.\n",
		res.Start.Format(time.RFC3339),
		res.End.Format(time.RFC3339),
		formatTS(res.SuspectAt),
		formatTS(res.DeadAt),
		formatTS(res.ReadyAt),
		res.MTTD.String(),
		res.MTTR.String(),
		res.P99TTFT,
	)
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return Result{}, err
	}
	path := filepath.Join(outDir, fmt.Sprintf("report-%s.md", start.UTC().Format("20060102-150405")))
	if err := os.WriteFile(path, []byte(md), 0644); err != nil {
		return Result{}, err
	}
	res.Path = path
	return res, nil
}
