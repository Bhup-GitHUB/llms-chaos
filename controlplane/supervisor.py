from __future__ import annotations

import time

import ray
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from registry import Registry

import actors

app = FastAPI(title="llms-chaos-supervisor")

ray.init(ignore_reinit_error=True, namespace="llms-chaos")

registry = Registry.remote()

SPECS = [
    ("worker-1", 9001),
    ("worker-2", 9002),
    ("worker-3", 9003),
]

handles: dict[str, ray.actor.ActorHandle] = {}
last_beat: dict[str, float] = {}
states: dict[str, str] = {}


def _spawn(worker_id: str, port: int) -> None:
    handle = actors.InferenceWorker.remote(worker_id, port)
    ray.get(handle.start.remote())
    handles[worker_id] = handle
    states[worker_id] = "starting"
    last_beat[worker_id] = time.time()
    ray.get(
        registry.update.remote(worker_id, "127.0.0.1", 9100 + int(worker_id.split("-")[1]), "starting", "sim")
    )


for wid, port in SPECS:
    _spawn(wid, port)


@app.get("/v1/cluster/state")
def state() -> JSONResponse:
    now = time.time()
    for wid, handle in list(handles.items()):
        try:
            beat = ray.get(handle.heartbeat.remote(), timeout=0.4)
        except Exception:
            beat = {"alive": False, "seq": -1}
        if beat.get("alive"):
            last_beat[wid] = now
            if states.get(wid) != "ready":
                states[wid] = "ready"
                ray.get(
                    registry.update.remote(
                        wid, "127.0.0.1", 9100 + int(wid.split("-")[1]), "ready", "sim"
                    )
                )
            continue
        gap = now - last_beat.get(wid, now)
        if gap > 2.0 and states.get(wid) != "suspect":
            states[wid] = "suspect"
            ray.get(
                registry.update.remote(
                    wid, "127.0.0.1", 9100 + int(wid.split("-")[1]), "suspect", "sim"
                )
            )
        if gap > 3.0:
            states[wid] = "dead"
            ray.get(registry.remove.remote(wid))
            try:
                ray.kill(handles.pop(wid))
            except Exception:
                handles.pop(wid, None)
            _spawn(wid, 9000 + int(wid.split("-")[1]))
    return JSONResponse(ray.get(registry.state.remote()))
