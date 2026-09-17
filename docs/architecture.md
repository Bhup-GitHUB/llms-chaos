# Architecture

## HLD

Clients speak OpenAI-compatible HTTP to the Go gateway. The gateway polls a versioned registry owned by the Ray supervisor and routes to workers through Toxiproxy proxies. Each worker serves SSE tokens from MLX or a deterministic sim engine. The chaos controller injects process, network, and memory faults. Prometheus scrapes gateway, workers, chaos, and Toxiproxy. Grafana renders SLOs with fault windows annotated from chaos metrics.

Requests flow through admission control, P2C selection, circuit breaker, single retry, then SSE proxy. Streaming responses translate worker token frames into chat.completion.chunk events. Non-streaming responses accumulate tokens into one chat.completion object.

## LLD

Gateway pool holds worker snapshots with atomic inflight counters and EWMA latency. Pick samples two ready workers and routes to the lower score. Breakers open after 3 consecutive failures and half-open after 5s. Retry happens at most once and only before response headers are sent. Registry polling runs every 1s and stores a monotonic version exposed as cluster_state_version.

Workers expose POST /generate as SSE, GET /healthz with model_loaded, GET /metrics in Prometheus text format, and POST /debug/stress for memory faults. SimEngine derives its token stream from SHA256 of the prompt so tests are deterministic. MLXEngine lazy-loads mlx-lm on first request.

Supervisor spawns three Ray actors, each owning one worker process. Heartbeats carry seq, load, and memory. Missing beats mark a worker suspect after 2s and dead after 3s. Dead actors are killed and respawned, and the registry version increments on every membership change.

Chaos faults map to mechanisms. Kill sends SIGKILL to the worker process. Latency adds a Toxiproxy toxic. Partition disables the proxy. Memory posts to /debug/stress. OOM combines stress with SIGKILL. Experiments are JSON definitions with steps, gateway health checks, and automatic rollback on abort.
