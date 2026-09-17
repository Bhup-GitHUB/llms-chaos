package experiment

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/bhupesh/llms-chaos/chaosd/internal/faults"
)

type Step struct {
	Fault     string `json:"fault"`
	Target    string `json:"target"`
	LatencyMs int    `json:"latency_ms"`
	JitterMs  int    `json:"jitter_ms"`
	WaitS     int    `json:"wait_s"`
}

type Definition struct {
	Name         string  `json:"name"`
	Hypothesis   string  `json:"hypothesis"`
	GatewayURL   string  `json:"gateway_url"`
	MaxP99TTFT   float64 `json:"max_p99_ttft"`
	MaxErrorRate float64 `json:"max_error_rate"`
	Steps        []Step  `json:"steps"`
}

func Load(path string) (Definition, error) {
	var d Definition
	raw, err := os.ReadFile(path)
	if err != nil {
		return d, err
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return d, err
	}
	if d.GatewayURL == "" {
		d.GatewayURL = "http://localhost:8000"
	}
	return d, nil
}

func Run(store *faults.Store, d Definition) error {
	client := &http.Client{Timeout: 5 * time.Second}
	for i, step := range d.Steps {
		switch step.Fault {
		case "kill":
			if _, err := store.Kill(step.Target); err != nil {
				return err
			}
		case "latency":
			if _, err := store.Latency(step.Target, step.LatencyMs, step.JitterMs); err != nil {
				return err
			}
		case "partition":
			if _, err := store.Partition(step.Target); err != nil {
				return err
			}
		default:
			return fmt.Errorf("step %d: unknown fault %s", i, step.Fault)
		}
		wait := step.WaitS
		if wait <= 0 {
			wait = 10
		}
		time.Sleep(time.Duration(wait) * time.Second)
		resp, err := client.Get(d.GatewayURL + "/readyz")
		if err != nil {
			_ = store.Reset()
			return fmt.Errorf("step %d: gateway unreachable, rolled back: %w", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			_ = store.Reset()
			return fmt.Errorf("step %d: gateway not ready, rolled back", i)
		}
	}
	return store.Reset()
}
