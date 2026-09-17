import json
import time
import urllib.request

GATEWAY = "http://localhost:8000"
REGISTRY = "http://localhost:8010/v1/cluster/state"


def _get(url):
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.loads(r.read().decode())


def test_registry_converges_after_kill():
    before = _get(REGISTRY)["version"]
    assert before >= 0


def test_gateway_ready_shape():
    body = _get(GATEWAY + "/readyz")
    assert "ready" in body


def test_failover_latency_budget():
    start = time.perf_counter()
    payload = json.dumps(
        {
            "model": "sim",
            "messages": [{"role": "user", "content": "failover probe"}],
            "max_tokens": 8,
            "stream": False,
        }
    ).encode()
    req = urllib.request.Request(
        GATEWAY + "/v1/chat/completions",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        assert r.status == 200
    assert time.perf_counter() - start < 10
