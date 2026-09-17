#!/usr/bin/env bash
set -euo pipefail
export SUSPECT_THRESHOLD_S="${SUSPECT_THRESHOLD_S:-2.0}"
export DEAD_THRESHOLD_S="${DEAD_THRESHOLD_S:-3.0}"
export HEARTBEAT_RPC_TIMEOUT_S="${HEARTBEAT_RPC_TIMEOUT_S:-0.4}"
export RAY_HEARTBEAT_TIMEOUT_MS="${RAY_HEARTBEAT_TIMEOUT_MS:-10000}"
export RAY_HEARTBEAT_CHECK_INTERVAL_MS="${RAY_HEARTBEAT_CHECK_INTERVAL_MS:-1000}"
export RAY_NAMESPACE="${RAY_NAMESPACE:-llms-chaos}"
export SUPERVISOR_PORT="${SUPERVISOR_PORT:-8000}"
ray stop --force || true
RAY_heartbeat_timeout_ms="$RAY_HEARTBEAT_TIMEOUT_MS" RAY_heartbeat_check_interval_ms="$RAY_HEARTBEAT_CHECK_INTERVAL_MS" ray start --head --port=6379 --dashboard-host=0.0.0.0 --dashboard-port=8265 --num-cpus=4
trap 'ray stop --force || true' EXIT INT TERM
exec uvicorn supervisor:app --host 0.0.0.0 --port "$SUPERVISOR_PORT"
