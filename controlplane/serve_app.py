from __future__ import annotations

import os

from ray import serve
from starlette.requests import Request
from starlette.responses import JSONResponse


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(float(raw))
    except ValueError:
        return default


MODEL_MAX_ONGOING = _get_int("SERVE_MODEL_MAX_ONGOING_REQUESTS", 8)
ROUTER_MAX_ONGOING = _get_int("SERVE_ROUTER_MAX_ONGOING_REQUESTS", 32)
INGRESS_MAX_ONGOING = _get_int("SERVE_INGRESS_MAX_ONGOING_REQUESTS", 64)


@serve.deployment(
    name="model",
    num_replicas=1,
    max_ongoing_requests=MODEL_MAX_ONGOING,
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 4,
        "target_ongoing_requests": 4,
    },
    ray_actor_options={"max_restarts": 5, "max_task_retries": -1},
    health_check_period_s=10,
    health_check_timeout_s=5,
)
class ModelDeployment:
    def __init__(self) -> None:
        self.model = os.getenv("LLMS_CHAOS_MODEL", "sim")

    async def __call__(self, payload: dict) -> dict:
        prompt = str(payload.get("prompt", ""))
        return {
            "model": self.model,
            "prompt": prompt,
            "output": prompt[::-1],
        }


@serve.deployment(
    name="router",
    num_replicas=1,
    max_ongoing_requests=ROUTER_MAX_ONGOING,
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 3,
        "target_ongoing_requests": 16,
    },
    ray_actor_options={"max_restarts": 5, "max_task_retries": -1},
    health_check_period_s=10,
    health_check_timeout_s=5,
)
class RouterDeployment:
    def __init__(self, model: serve.DeploymentHandle) -> None:
        self.model = model

    async def __call__(self, payload: dict) -> dict:
        result = await self.model.remote(payload)
        result["routed"] = True
        return result


@serve.deployment(
    name="ingress",
    num_replicas=1,
    max_ongoing_requests=INGRESS_MAX_ONGOING,
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 2,
        "target_ongoing_requests": 32,
    },
    ray_actor_options={"max_restarts": 5, "max_task_retries": -1},
    health_check_period_s=10,
    health_check_timeout_s=5,
)
class IngressDeployment:
    def __init__(self, router: serve.DeploymentHandle) -> None:
        self.router = router

    async def __call__(self, request: Request) -> JSONResponse:
        payload = await request.json()
        result = await self.router.remote(payload)
        return JSONResponse(result)


app = IngressDeployment.bind(RouterDeployment.bind(ModelDeployment.bind()))
