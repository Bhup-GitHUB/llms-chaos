# llms-chaos

Chaos-resilient LLM inference cluster on Apple Silicon. Go gateway, Ray control plane, MLX workers, Toxiproxy fault injection, Prometheus SLOs.

```
k6 --> Go gateway :8000 --> Toxiproxy :9101-9103 --> workers :9001-9003 (MLX/sim)
              |                       |
        registry :8010           chaos CLI :8020
              |                       |
     Ray supervisor            Prometheus :9090 --> Grafana :3000
```

## Layout

- `gateway/` Go ingress, P2C routing, circuit breakers, retry, SSE proxy
- `worker/` FastAPI inference workers, sim and MLX backends
- `controlplane/` Ray supervisor, heartbeats, versioned registry
- `chaosd/` chaos CLI and experiment runner
- `deployments/` Prometheus, Grafana, Loki
- `experiments/` declarative fault scenarios
- `loadtest/` k6 profiles
- `k8s/` cloud migration manifests

## Quickstart

```
make up
chaos init
WORKER_ID=worker-1 WORKER_PORT=9001 uvicorn app.main:app --port 9001
go run ./gateway/cmd/gateway
curl localhost:8000/v1/chat/completions -d '{"model":"sim","messages":[{"role":"user","content":"hi"}],"max_tokens":32}'
chaos kill worker-2
chaos schedule run experiments/worker-crash.json
```

## Failure numbers

Target MTTD 3s, recovery 8-15s. Failover keeps p99 TTFT under 2s with one worker dead.
