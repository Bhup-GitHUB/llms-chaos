from __future__ import annotations

import json
import os
import signal
import time
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

from app.cache import PrefixCache
from app.drain import clear_drain, is_draining, set_drain
from app.engine import InferenceEngine, SimEngine
from app.metrics import registry
from app.scheduler import BoundedScheduler, QueueFullError
from app.schemas import GenerateRequest

try:
    from app.mlx_engine import MLXEngine
except Exception:
    MLXEngine = None

WORKER_ID = os.getenv("WORKER_ID", "worker-1")


def _handle_sigterm(signum, frame) -> None:
    set_drain()


try:
    signal.signal(signal.SIGTERM, _handle_sigterm)
except ValueError:
    pass


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    previous = signal.getsignal(signal.SIGTERM)

    def _graceful_sigterm(signum, frame) -> None:
        set_drain()
        if callable(previous):
            previous(signum, frame)

    try:
        signal.signal(signal.SIGTERM, _graceful_sigterm)
    except ValueError:
        pass
    yield


app = FastAPI(title="llms-chaos-worker", lifespan=lifespan)

_holder: list[bytes] = []

scheduler = BoundedScheduler(
    capacity=int(os.getenv("SCHEDULER_CAPACITY", "64")),
    max_batch=int(os.getenv("SCHEDULER_MAX_BATCH", "4")),
    batch_wait_ms=float(os.getenv("SCHEDULER_BATCH_WAIT_MS", "5")),
)

cache = PrefixCache(
    max_entries=int(os.getenv("CACHE_MAX_ENTRIES", "512")),
    ttl_s=float(os.getenv("CACHE_TTL_S", "600")),
)

QUEUE_TIMEOUT_S = float(os.getenv("SCHEDULER_QUEUE_TIMEOUT_S", "30"))


def _engine() -> InferenceEngine:
    backend = os.getenv("ENGINE", "sim")
    if backend == "mlx" and MLXEngine is not None:
        return MLXEngine(
            model_id=os.getenv("MODEL_ID", "mlx-community/Qwen2.5-3B-Instruct-4bit"),
        )
    return SimEngine(
        ttft_ms=float(os.getenv("SIM_TTFT_MS", "40")),
        token_ms=float(os.getenv("SIM_TOKEN_MS", "12")),
    )


engine = _engine()


def _prompt(req: GenerateRequest) -> str:
    return "\n".join(f"{m.role}: {m.content}" for m in req.messages)


@app.get("/healthz")
def healthz() -> dict:
    loaded = getattr(engine, "loaded", True)
    return {"status": "ok", "worker_id": WORKER_ID, "model_loaded": bool(loaded)}


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    return PlainTextResponse(registry.render())


@app.get("/drain")
def get_drain() -> dict:
    return {"draining": is_draining(), "worker_id": WORKER_ID}


@app.post("/drain")
def post_drain(payload: dict | None = None) -> dict:
    body = payload or {}
    if body.get("draining") is False or body.get("enabled") is False:
        clear_drain()
    else:
        set_drain()
    return {"draining": is_draining(), "worker_id": WORKER_ID}


@app.post("/debug/stress")
def stress(payload: dict) -> dict:
    mb = int(payload.get("mb", 512))
    _holder.append(bytearray(mb * 1024 * 1024))
    return {"status": "ok", "held_mb": sum(len(b) for b in _holder) // (1024 * 1024)}


@app.post("/debug/release")
def release() -> dict:
    _holder.clear()
    return {"status": "ok"}


async def _stream(req: GenerateRequest, lease) -> AsyncIterator[str]:
    request_id = req.request_id or f"req-{uuid.uuid4().hex[:12]}"
    registry.inc("worker_requests_total")
    registry.inc("inflight")
    start = time.perf_counter()
    first = True
    index = 0
    count = 0
    try:
        prompt = _prompt(req)
        limit = max(1, req.max_tokens)
        cached = cache.get(prompt, limit)
        if cached is not None:
            registry.inc("worker_cache_hits")
            registry.observe_ttft(time.perf_counter() - start)
            first = False
            for token in cached:
                payload = {
                    "token": token,
                    "index": index,
                    "finish_reason": "",
                    "request_id": request_id,
                }
                index += 1
                count += 1
                yield f"data: {json.dumps(payload)}\n\n"
        else:
            registry.inc("worker_cache_misses")
            collected: list[str] = []
            for token in engine.complete(prompt, limit):
                now = time.perf_counter()
                if first:
                    registry.observe_ttft(now - start)
                    first = False
                payload = {
                    "token": token,
                    "index": index,
                    "finish_reason": "",
                    "request_id": request_id,
                }
                index += 1
                count += 1
                collected.append(token)
                yield f"data: {json.dumps(payload)}\n\n"
            elapsed = time.perf_counter() - start
            if elapsed > 0 and count > 0:
                registry.observe_tps(count / elapsed)
            cache.put(prompt, collected)
        done = {"token": "", "index": index, "finish_reason": "stop", "request_id": request_id}
        yield f"data: {json.dumps(done)}\n\n"
        yield "data: [DONE]\n\n"
    finally:
        lease.release()
        registry.set("worker_queue_depth", scheduler.depth())
        registry.inc("inflight", "_neg")
        registry.inc("worker_responses_total")


@app.post("/generate")
def generate(req: GenerateRequest) -> StreamingResponse | JSONResponse:
    if is_draining():
        return JSONResponse(
            status_code=503,
            content={"error": "draining", "worker_id": WORKER_ID},
            headers={"Retry-After": "5"},
        )
    try:
        lease = scheduler.acquire(timeout=QUEUE_TIMEOUT_S)
    except QueueFullError:
        return JSONResponse(
            status_code=503,
            content={"error": "queue_full", "worker_id": WORKER_ID},
            headers={"Retry-After": "1"},
        )
    registry.set("worker_queue_depth", scheduler.depth())
    return StreamingResponse(_stream(req, lease), media_type="text/event-stream")
