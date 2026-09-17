from __future__ import annotations

import time

import ray


@ray.remote
class Registry:
    def __init__(self) -> None:
        self.version = 0
        self.workers: dict[str, dict] = {}

    def update(self, worker_id: str, host: str, port: int, state: str, model: str) -> int:
        self.workers[worker_id] = {
            "id": worker_id,
            "host": host,
            "port": port,
            "state": state,
            "model": model,
            "updated_at": time.time(),
        }
        self.version += 1
        return self.version

    def remove(self, worker_id: str) -> int:
        self.workers.pop(worker_id, None)
        self.version += 1
        return self.version

    def state(self) -> dict:
        return {"version": self.version, "workers": list(self.workers.values())}
