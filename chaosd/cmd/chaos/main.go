package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bhupesh/llms-chaos/chaosd/internal/experiment"
	"github.com/bhupesh/llms-chaos/chaosd/internal/faults"
	"github.com/bhupesh/llms-chaos/chaosd/internal/toxiproxy"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	store := faults.NewStore(toxiproxy.New(os.Getenv("TOXIPROXY_URL")))
	switch os.Args[1] {
	case "init":
		fatal(store.EnsureProxies())
		fmt.Println("proxies ready")
	case "kill":
		requireArg(3)
		f, err := store.Kill(os.Args[2])
		fatal(err)
		fmt.Println(f.ID, f.Detail)
	case "latency":
		requireArg(4)
		ms := atoi(os.Args[3])
		jitter := 0
		if len(os.Args) > 4 {
			jitter = atoi(os.Args[4])
		}
		f, err := store.Latency(os.Args[2], ms, jitter)
		fatal(err)
		fmt.Println(f.ID, f.Detail)
	case "partition":
		requireArg(3)
		f, err := store.Partition(os.Args[2])
		fatal(err)
		fmt.Println(f.ID, f.Detail)
	case "heal":
		requireArg(3)
		fatal(store.Heal(os.Args[2]))
		fmt.Println("healed", os.Args[2])
	case "memory":
		requireArg(4)
		mb := atoi(os.Args[3])
		fatal(postWorker(os.Args[2], "/debug/stress", map[string]any{"mb": mb}))
		fmt.Println("stress", os.Args[2], mb)
	case "oom":
		requireArg(3)
		_ = postWorker(os.Args[2], "/debug/stress", map[string]any{"mb": 8192})
		f, err := store.Kill(os.Args[2])
		fatal(err)
		fmt.Println(f.ID, "oom simulated")
	case "slow-model":
		requireArg(4)
		fmt.Println("slow-model", os.Args[2], os.Args[3], "set SIM_TOKEN_MS on next restart")
	case "reset":
		fatal(store.Reset())
		fmt.Println("reset")
	case "schedule":
		requireArg(4)
		if os.Args[2] != "run" {
			usage()
			os.Exit(1)
		}
		d, err := experiment.Load(os.Args[3])
		fatal(err)
		fmt.Println("running", d.Name, "-", d.Hypothesis)
		fatal(experiment.Run(store, d))
		fmt.Println("experiment complete, faults rolled back")
	case "serve":
		serve(store)
	default:
		usage()
		os.Exit(1)
	}
}

func serve(store *faults.Store) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/faults", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			raw, _ := json.Marshal(store.List())
			w.Header().Set("Content-Type", "application/json")
			w.Write(raw)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
	mux.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		_ = store.Reset()
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "chaos_fault_active %d\n", store.Active())
	})
	addr := os.Getenv("CHAOS_ADDR")
	if addr == "" {
		addr = ":8020"
	}
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	_ = srv.ListenAndServe()
}

var workerPorts = map[string]int{"worker-1": 9001, "worker-2": 9002, "worker-3": 9003}

func postWorker(target, path string, payload map[string]any) error {
	port, ok := workerPorts[target]
	if !ok {
		return fmt.Errorf("unknown worker %s", target)
	}
	raw, _ := json.Marshal(payload)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(fmt.Sprintf("http://127.0.0.1:%d%s", port, path), "application/json", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func requireArg(n int) {
	if len(os.Args) < n {
		usage()
		os.Exit(1)
	}
}

func atoi(s string) int {
	v, err := strconv.Atoi(s)
	fatal(err)
	return v
}

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Println("usage: chaos <init|kill|latency|partition|heal|memory|oom|slow-model|reset|schedule|serve> ...")
}
