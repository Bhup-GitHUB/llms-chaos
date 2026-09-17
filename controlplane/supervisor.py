from __future__ import annotations

import time

import ray
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from registry import Registry

import actors
import placement
import settings

app = FastAPI(title="llms-chaos-supervisor")


def _init_ray() -> None:
    try:
        ray.init(
            address=settings.RAY_ADDRESS,
            namespace=settings.RAY_NAMESPACE,
            ignore_reinit_error=True,
        )
        if not ray.is_initialized():
            raise RuntimeError("ray init failed")
    except Exception:
        ray.init(namespace=settings.RAY_NAMESPACE, ignore_reinit_error=True)


_init_ray()


def _get_registry():
    try:
        return ray.get_actor("llms-chaos-registry")
    except ValueError:
        return Registry.options(
            name="llms-chaos-registry",
            lifetime="detached",
            max_restarts=-1,
        ).remote()


registry = _get_registry()


def _get_placement_group():
    try:
        return placement.ensure_spread_placement(
            num_workers=len(settings.WORKER_IDS),
            cpus_per_worker=placement.CPUS_PER_WORKER,
            memory_mb_per_worker=placement.MEMORY_MB_PER_WORKER,
            timeout_s=placement.PLACEMENT_TIMEOUT_S,
        )
    except Exception:
        return None


_pg = _get_placement_group()

handles: dict[str, ray.actor.ActorHandle] = {}
last_beat: dict[str, float] = {}
states: dict[str, str] = {}


def _spawn(worker_id: str, port: int) -> None:
    handle = None
    if _pg is not None:
        try:
            index = placement.worker_bundle_index(worker_id, len(settings.WORKER_IDS))
            strategy = placement.strategy_for_index(_pg, index)
            handle = actors.InferenceWorker.options(
                scheduling_strategy=strategy
            ).remote(worker_id, port)
        except Exception:
            handle = None
    if handle is None:
        handle = actors.InferenceWorker.remote(worker_id, port)
    ray.get(handle.start.remote())
    handles[worker_id] = handle
    states[worker_id] = "starting"
    last_beat[worker_id] = time.time()
    ray.get(
        registry.update.remote(
            worker_id,
            settings.HOST,
            settings.advertise_port_for(worker_id),
            "starting",
            settings.MODEL,
        )
    )


for wid, port in settings.SPECS:
    _spawn(wid, port)


@app.get("/v1/healthz")
def healthz() -> JSONResponse:
    return JSONResponse({"status": "ok", "workers": len(handles)})


@app.get("/v1/cluster/state")
def state() -> JSONResponse:
    now = time.time()
    for wid, handle in list(handles.items()):
        try:
            beat = ray.get(
                handle.heartbeat.remote(),
                timeout=settings.HEARTBEAT_RPC_TIMEOUT_S,
            )
        except Exception:
            beat = {"alive": False, "seq": -1}
        if beat.get("alive"):
            last_beat[wid] = now
            if states.get(wid) != "ready":
                states[wid] = "ready"
                ray.get(
                    registry.update.remote(
                        wid,
                        settings.HOST,
                        settings.advertise_port_for(wid),
                        "ready",
                        settings.MODEL,
                    )
                )
            continue
        gap = now - last_beat.get(wid, now)
        if gap > settings.SUSPECT_THRESHOLD_S and states.get(wid) != "suspect":
            states[wid] = "suspect"
            ray.get(
                registry.update.remote(
                    wid,
                    settings.HOST,
                    settings.advertise_port_for(wid),
                    "suspect",
                    settings.MODEL,
                )
            )
        if gap > settings.DEAD_THRESHOLD_S:
            states[wid] = "dead"
            ray.get(registry.remove.remote(wid))
            try:
                ray.kill(handles.pop(wid))
            except Exception:
                handles.pop(wid, None)
            _spawn(wid, settings.backend_port_for(wid))
    return JSONResponse(ray.get(registry.state.remote()))
