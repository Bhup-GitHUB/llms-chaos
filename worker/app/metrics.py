from __future__ import annotations

import threading


class Registry:
    def __init__(self) -> None:
        self._mu = threading.Lock()
        self._counters: dict[str, float] = {}
        self._ttft: list[float] = []
        self._tps: list[float] = []

    def inc(self, name: str, labels: str = "") -> None:
        with self._mu:
            self._counters[name + labels] = self._counters.get(name + labels, 0) + 1

    def observe_ttft(self, seconds: float) -> None:
        with self._mu:
            self._ttft.append(seconds)
            if len(self._ttft) > 4096:
                del self._ttft[:2048]

    def observe_tps(self, value: float) -> None:
        with self._mu:
            self._tps.append(value)
            if len(self._tps) > 4096:
                del self._tps[:2048]

    def render(self) -> str:
        with self._mu:
            lines = []
            for key, val in sorted(self._counters.items()):
                lines.append(f"{key} {val}")
            if self._ttft:
                ordered = sorted(self._ttft)
                lines.append(f"worker_ttft_seconds_p50 {ordered[len(ordered)//2]}")
                lines.append(f"worker_ttft_seconds_p99 {ordered[int(len(ordered)*0.99)]}")
            lines.append(f"worker_requests_inflight {self._counters.get('inflight', 0)}")
            return "\n".join(lines) + "\n"


registry = Registry()
