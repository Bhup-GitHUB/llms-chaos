package main

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/bhupesh/llms-chaos/gateway/internal/breaker"
	"github.com/bhupesh/llms-chaos/gateway/internal/config"
	"github.com/bhupesh/llms-chaos/gateway/internal/hedge"
	"github.com/bhupesh/llms-chaos/gateway/internal/limiter"
	"github.com/bhupesh/llms-chaos/gateway/internal/middleware"
	"github.com/bhupesh/llms-chaos/gateway/internal/proxy"
	"github.com/bhupesh/llms-chaos/gateway/internal/router"
)

type registryResponse struct {
	Version int64 `json:"version"`
	Workers []struct {
		ID    string `json:"id"`
		Host  string `json:"host"`
		Port  int    `json:"port"`
		State string `json:"state"`
		Model string `json:"model"`
	} `json:"workers"`
}

func main() {
	cfg := config.Load()
	pool := router.New()
	breakers := breaker.NewRegistry(3, 5*time.Second)
	metrics := middleware.NewMetrics()
	hedgeBudget := hedge.NewBudget(time.Duration(cfg.HeaderTimeout) * time.Second)
	maxInflight := cfg.MaxInflight
	if maxInflight <= 0 {
		maxInflight = 256
	}
	adaptive := limiter.New(32, 1, maxInflight)
	fwd := &proxy.Forwarder{
		Pool:          pool,
		Breakers:      breakers,
		Metrics:       metrics,
		DialTimeout:   time.Duration(cfg.DialTimeout) * time.Second,
		HeaderTimeout: time.Duration(cfg.HeaderTimeout) * time.Second,
		Hedge:         hedgeBudget,
		Limiter:       adaptive,
	}

	var version atomic.Int64
	go func() {
		client := &http.Client{Timeout: 3 * time.Second}
		for {
			func() {
				resp, err := client.Get(cfg.RegistryURL)
				if err != nil {
					return
				}
				defer resp.Body.Close()
				var rr registryResponse
				if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
					return
				}
				snaps := make([]router.Snapshot, 0, len(rr.Workers))
				for _, w := range rr.Workers {
					snaps = append(snaps, router.Snapshot{ID: w.ID, Host: w.Host, Port: w.Port, State: w.State, Model: w.Model})
				}
				pool.Update(snaps)
				version.Store(rr.Version)
			}()
			time.Sleep(time.Second)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if pool.Pick() == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"ready":false}`))
			return
		}
		w.Write([]byte(`{"ready":true}`))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		ready := 0
		if pool.Pick() != nil {
			ready = 1
		}
		w.Write([]byte(metrics.Render(ready, version.Load())))
	})
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		models := pool.Models()
		items := []any{}
		for _, m := range models {
			items = append(items, map[string]any{"id": m, "object": "model"})
		}
		raw, _ := json.Marshal(map[string]any{"object": "list", "data": items})
		w.Header().Set("Content-Type", "application/json")
		w.Write(raw)
	})
	mux.HandleFunc("/v1/chat/completions", fwd.Serve)

	handler := middleware.RequestID(mux)
	srv := &http.Server{Addr: cfg.ListenAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	_ = srv.ListenAndServe()
}
