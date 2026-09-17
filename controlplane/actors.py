from __future__ import annotations

import subprocess
import time

import ray


@ray.remote(max_restarts=5, max_task_retries=-1)
class InferenceWorker:
    def __init__(self, worker_id: str, port: int) -> None:
        self.worker_id = worker_id
        self.port = port
        self.seq = 0
        self.proc: subprocess.Popen | None = None

    def start(self) -> dict:
        import os

        env = dict(os.environ)
        env["WORKER_ID"] = self.worker_id
        env["WORKER_PORT"] = str(self.port)
        self.proc = subprocess.Popen(
            ["uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(self.port)],
            cwd=os.path.join(os.path.dirname(__file__), "..", "..", "worker"),
            env=env,
        )
        return {"worker_id": self.worker_id, "port": self.port, "pid": self.proc.pid}

    def heartbeat(self, load: float = 0.0, mem_used: int = 0) -> dict:
        self.seq += 1
        alive = self.proc is not None and self.proc.poll() is None
        return {
            "worker_id": self.worker_id,
            "seq": self.seq,
            "ts": time.time(),
            "load": load,
            "mem_used": mem_used,
            "alive": alive,
        }

    def stop(self) -> dict:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
        return {"worker_id": self.worker_id, "stopped": True}
