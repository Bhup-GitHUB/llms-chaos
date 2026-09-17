package faults

import (
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/bhupesh/llms-chaos/chaosd/internal/toxiproxy"
)

var ports = map[string]int{"worker-1": 9001, "worker-2": 9002, "worker-3": 9003}

type Fault struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	Target    string    `json:"target"`
	Detail    string    `json:"detail"`
	CreatedAt time.Time `json:"created_at"`
}

type Store struct {
	mu     sync.Mutex
	faults map[string]Fault
	seq    int
	toxi   *toxiproxy.Client
}

func NewStore(toxi *toxiproxy.Client) *Store {
	return &Store{faults: map[string]Fault{}, toxi: toxi}
}

func (s *Store) add(kind, target, detail string) Fault {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	f := Fault{ID: fmt.Sprintf("flt-%d", s.seq), Kind: kind, Target: target, Detail: detail, CreatedAt: time.Now()}
	s.faults[f.ID] = f
	return f
}

func (s *Store) Remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.faults, id)
}

func (s *Store) List() []Fault {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Fault{}
	for _, f := range s.faults {
		out = append(out, f)
	}
	return out
}

func (s *Store) Active() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.faults)
}

func (s *Store) EnsureProxies() error {
	proxies := []map[string]any{}
	for id, port := range ports {
		num := id[len(id)-1:]
		proxies = append(proxies, map[string]any{
			"name": id, "listen": fmt.Sprintf("0.0.0.0:91%02s", num),
			"upstream": fmt.Sprintf("127.0.0.1:%d", port), "enabled": true,
		})
	}
	return s.toxi.Populate(proxies)
}

func (s *Store) Kill(target string) (Fault, error) {
	port, ok := ports[target]
	if !ok {
		return Fault{}, fmt.Errorf("unknown worker %s", target)
	}
	cmd := exec.Command("pkill", "-9", "-f", fmt.Sprintf("uvicorn.*%d", port))
	_ = cmd.Run()
	return s.add("kill", target, fmt.Sprintf("sigkill port %d", port)), nil
}

func (s *Store) Latency(target string, ms, jitter int) (Fault, error) {
	err := s.toxi.AddToxic(target, "latency_downstream", "latency", "downstream", 1.0,
		map[string]any{"latency": ms, "jitter": jitter})
	if err != nil {
		return Fault{}, err
	}
	return s.add("latency", target, fmt.Sprintf("%dms jitter %dms", ms, jitter)), nil
}

func (s *Store) Partition(target string) (Fault, error) {
	if err := s.toxi.Toggle(target, false); err != nil {
		return Fault{}, err
	}
	return s.add("partition", target, "proxy disabled"), nil
}

func (s *Store) Heal(target string) error {
	_ = s.toxi.Toggle(target, true)
	return nil
}

func (s *Store) Reset() error {
	return s.toxi.Reset()
}
