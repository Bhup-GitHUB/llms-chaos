from __future__ import annotations

import hashlib
import time
from typing import Iterator, Protocol


class InferenceEngine(Protocol):
    model_name: str

    def complete(self, prompt: str, max_tokens: int) -> Iterator[str]:
        ...


class SimEngine:
    model_name = "sim"

    def __init__(self, ttft_ms: float = 40.0, token_ms: float = 12.0) -> None:
        self.ttft_ms = ttft_ms
        self.token_ms = token_ms
        self._vocab = [
            "the", "cluster", "routes", "around", "failure", "with", "bounded",
            "latency", "and", "backpressure", "while", "heartbeats", "track",
            "membership", "across", "workers", "under", "chaos",
        ]

    def complete(self, prompt: str, max_tokens: int) -> Iterator[str]:
        seed = int.from_bytes(hashlib.sha256(prompt.encode()).digest()[:8], "big")
        time.sleep(self.ttft_ms / 1000.0)
        for i in range(max_tokens):
            time.sleep(self.token_ms / 1000.0)
            yield self._vocab[(seed + i) % len(self._vocab)] + (" " if i < max_tokens - 1 else "")
