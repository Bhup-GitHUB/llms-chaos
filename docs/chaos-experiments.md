# Chaos experiments

| Experiment | Fault | Hypothesis |
|---|---|---|
| worker-crash | kill worker-2 | gateway stays ready, p99 TTFT under 2s |
| worker-latency | 500ms + 100ms jitter on worker-1 | traffic shifts, p99 TTFT under 2.5s |
| worker-partition | disable worker-3 proxy | worker leaves routing, no gateway outage |

Run with `chaos schedule run experiments/<name>.json`. Each run checks /readyz between steps and calls /reset on abort. Fault windows appear in Grafana from chaos_fault_active.
