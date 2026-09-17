from __future__ import annotations

import os


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(float(raw))
    except ValueError:
        return default


def _get_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw


def _get_port_list(name: str, default: tuple[int, ...]) -> tuple[int, ...]:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    parts = [p.strip() for p in raw.split(",")]
    ports: list[int] = []
    for p in parts:
        if p == "":
            continue
        try:
            ports.append(int(float(p)))
        except ValueError:
            continue
    if not ports:
        return default
    return tuple(ports)


HOST = _get_str("LLMS_CHAOS_HOST", "127.0.0.1")
MODEL = _get_str("LLMS_CHAOS_MODEL", "sim")
RAY_NAMESPACE = _get_str("RAY_NAMESPACE", "llms-chaos")
RAY_ADDRESS = _get_str("RAY_ADDRESS", "auto")
RAY_DASHBOARD_PORT = _get_int("RAY_DASHBOARD_PORT", 8265)
SUPERVISOR_PORT = _get_int("SUPERVISOR_PORT", 8000)

SUSPECT_THRESHOLD_S = _get_float("SUSPECT_THRESHOLD_S", 2.0)
DEAD_THRESHOLD_S = _get_float("DEAD_THRESHOLD_S", 3.0)
HEARTBEAT_RPC_TIMEOUT_S = _get_float("HEARTBEAT_RPC_TIMEOUT_S", 0.4)

RAY_HEARTBEAT_TIMEOUT_MS = _get_int("RAY_HEARTBEAT_TIMEOUT_MS", 10000)
RAY_HEARTBEAT_CHECK_INTERVAL_MS = _get_int("RAY_HEARTBEAT_CHECK_INTERVAL_MS", 1000)

WORKER_IDS = ("worker-1", "worker-2", "worker-3")
BACKEND_PORTS = _get_port_list("WORKER_BACKEND_PORTS", (9001, 9002, 9003))
ADVERTISE_PORTS = _get_port_list("WORKER_ADVERTISE_PORTS", (9101, 9102, 9103))

SPECS: tuple[tuple[str, int], ...] = tuple(zip(WORKER_IDS, BACKEND_PORTS))

ADVERTISE_BY_ID: dict[str, int] = {
    wid: ADVERTISE_PORTS[i % len(ADVERTISE_PORTS)] for i, wid in enumerate(WORKER_IDS)
}

BACKEND_BY_ID: dict[str, int] = {
    wid: BACKEND_PORTS[i % len(BACKEND_PORTS)] for i, wid in enumerate(WORKER_IDS)
}


def advertise_port_for(worker_id: str) -> int:
    if worker_id in ADVERTISE_BY_ID:
        return ADVERTISE_BY_ID[worker_id]
    try:
        suffix = int(worker_id.rsplit("-", 1)[1])
        return 9100 + suffix
    except (ValueError, IndexError):
        return ADVERTISE_PORTS[0]


def backend_port_for(worker_id: str) -> int:
    if worker_id in BACKEND_BY_ID:
        return BACKEND_BY_ID[worker_id]
    try:
        suffix = int(worker_id.rsplit("-", 1)[1])
        return 9000 + suffix
    except (ValueError, IndexError):
        return BACKEND_PORTS[0]
