from __future__ import annotations

import os

import ray
from ray.util.placement_group import placement_group


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(float(raw))
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


NUM_WORKERS = _get_int("PLACEMENT_NUM_WORKERS", 3)
CPUS_PER_WORKER = _get_int("PLACEMENT_CPUS_PER_WORKER", 1)
MEMORY_MB_PER_WORKER = _get_int("PLACEMENT_MEMORY_MB_PER_WORKER", 512)
PLACEMENT_TIMEOUT_S = _get_float("PLACEMENT_READY_TIMEOUT_S", 30.0)


def ensure_spread_placement(
    num_workers: int = NUM_WORKERS,
    cpus_per_worker: int = CPUS_PER_WORKER,
    memory_mb_per_worker: int = MEMORY_MB_PER_WORKER,
    timeout_s: float = PLACEMENT_TIMEOUT_S,
):
    bundles = [
        {"CPU": cpus_per_worker, "memory": memory_mb_per_worker * 1024 * 1024}
        for _ in range(num_workers)
    ]
    pg = placement_group(bundles, strategy="SPREAD")
    ray.get(pg.ready(), timeout=timeout_s)
    return pg


def strategy_for_index(pg, index: int):
    from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

    return PlacementGroupSchedulingStrategy(
        placement_group=pg,
        placement_group_bundle_index=index,
        placement_group_capture_child_tasks=True,
    )


def worker_bundle_index(worker_id: str, total: int = NUM_WORKERS) -> int:
    try:
        suffix = int(worker_id.rsplit("-", 1)[1])
        return (suffix - 1) % total
    except (ValueError, IndexError):
        return 0
