from __future__ import annotations

import json
import os
import time
import uuid
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, StreamingResponse

from app.engine import InferenceEngine, SimEngine
from app.metrics import registry
from app.schemas import GenerateRequest

try:
    from app.mlx_engine import MLXEngine
except Exception:
    MLXEngine = None

WORKER_ID = os.getenv("WORKER_ID", "worker-1")

app = FastAPI(title="llms-chaos-worker")

_holder: list[bytes] = []


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


@app.post("/debug/stress")
def stress(payload: dict) -> dict:
    mb = int(payload.get("mb", 512))
    _holder.append(bytearray(mb * 1024 * 1024))
    return {"status": "ok", "held_mb": sum(len(b) for b in _holder) // (1024 * 1024)}


@app.post("/debug/release")
def release() -> dict:
    _holder.clear()
    return {"status": "ok"}


async def _stream(req: GenerateRequest) -> AsyncIterator[str]:
    request_id = req.request_id or f"req-{uuid.uuid4().hex[:12]}"
    registry.inc("worker_requests_total")
    registry.inc("inflight")
    start = time.perf_counter()
    first = True
    index = 0
    count = 0
    try:
        for token in engine.complete(_prompt(req), max(1, req.max_tokens)):
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
            yield f"data: {json.dumps(payload)}\n\n"
        elapsed = time.perf_counter() - start
        if elapsed > 0 and count > 0:
            registry.observe_tps(count / elapsed)
        done = {"token": "", "index": index, "finish_reason": "stop", "request_id": request_id}
        yield f"data: {json.dumps(done)}\n\n"
        yield "data: [DONE]\n\n"
    finally:
        registry.inc("inflight", "_neg")
        registry.inc("worker_responses_total")


@app.post("/generate")
def generate(req: GenerateRequest) -> StreamingResponse:
    return StreamingResponse(_stream(req), media_type="text/event-stream")
