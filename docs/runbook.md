# Runbook

## Worker down

1. Check `cluster_state_version` and `gateway_workers_ready` in Grafana.
2. Confirm supervisor marked the worker dead and respawned it.
3. Run `chaos reset` if a stale toxic is suspected.
4. Restart the worker process with the same WORKER_ID and port.

## Degraded latency

1. Identify the slow worker from p50/p99 TTFT panels.
2. Add a latency toxic to reproduce, then remove it.
3. Verify P2C routing drains the degraded worker via inflight gauges.
4. Check breaker states through gateway metrics.

## Gateway saturated

1. Check `gateway_inflight` against the 256 admission cap.
2. Scale workers or reduce k6 VUs.
3. Confirm 503s carry Retry-After and error budget burn stops.
