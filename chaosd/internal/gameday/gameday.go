package gameday

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/bhupesh/llms-chaos/chaosd/internal/faults"
)

type Config struct {
	Workers   []string
	Kinds     []string
	Seed      int64
	Rounds    int
	Cooldown  time.Duration
	LatencyMs int
	JitterMs  int
}

type Event struct {
	Round  int
	Worker string
	Kind   string
	At     time.Time
}

func withDefaults(cfg Config) Config {
	if len(cfg.Workers) == 0 {
		cfg.Workers = []string{"worker-1", "worker-2", "worker-3"}
	}
	if len(cfg.Kinds) == 0 {
		cfg.Kinds = []string{"kill", "latency", "partition"}
	}
	if cfg.Rounds <= 0 {
		cfg.Rounds = 6
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 10 * time.Second
	}
	if cfg.LatencyMs <= 0 {
		cfg.LatencyMs = 500
	}
	if cfg.JitterMs < 0 {
		cfg.JitterMs = 0
	}
	return cfg
}

func Plan(cfg Config) []Event {
	cfg = withDefaults(cfg)
	rng := rand.New(rand.NewSource(cfg.Seed))
	events := make([]Event, 0, cfg.Rounds)
	for i := 0; i < cfg.Rounds; i++ {
		w := cfg.Workers[rng.Intn(len(cfg.Workers))]
		k := cfg.Kinds[rng.Intn(len(cfg.Kinds))]
		events = append(events, Event{Round: i + 1, Worker: w, Kind: k})
	}
	return events
}

func Run(store *faults.Store, cfg Config) ([]Event, error) {
	cfg = withDefaults(cfg)
	plan := Plan(cfg)
	done := make([]Event, 0, len(plan))
	for _, e := range plan {
		e.At = time.Now()
		var err error
		switch e.Kind {
		case "kill":
			_, err = store.Kill(e.Worker)
		case "latency":
			_, err = store.Latency(e.Worker, cfg.LatencyMs, cfg.JitterMs)
		case "partition":
			_, err = store.Partition(e.Worker)
		default:
			return done, fmt.Errorf("unknown fault kind %s", e.Kind)
		}
		if err != nil {
			_ = store.Reset()
			return done, err
		}
		done = append(done, e)
		time.Sleep(cfg.Cooldown)
		_ = store.Heal(e.Worker)
		_ = store.Reset()
	}
	return done, nil
}
