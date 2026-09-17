from __future__ import annotations

import threading
import time


class PrefixCache:
    def __init__(self, max_entries: int = 512, ttl_s: float = 600.0) -> None:
        self.max_entries = max(1, int(max_entries))
        self.ttl_s = max(0.0, float(ttl_s))
        self._mu = threading.Lock()
        self._store: dict[str, tuple[list[str], float]] = {}
        self._hits = 0
        self._misses = 0

    def _purge_locked(self) -> None:
        if self.ttl_s <= 0:
            return
        now = time.monotonic()
        expired = [key for key, (_, stamp) in self._store.items() if now - stamp >= self.ttl_s]
        for key in expired:
            del self._store[key]

    def get(self, prompt: str, max_tokens: int = 0) -> list[str] | None:
        need = max(0, int(max_tokens))
        with self._mu:
            self._purge_locked()
            entry = self._store.get(prompt)
            if entry is not None and len(entry[0]) >= need:
                self._hits += 1
                return list(entry[0][:need] if need else entry[0])
            best_key: str | None = None
            best_tokens: list[str] | None = None
            for key, (tokens, _) in self._store.items():
                if key != prompt and prompt.startswith(key) and len(tokens) >= need:
                    if best_key is None or len(key) > len(best_key):
                        best_key = key
                        best_tokens = tokens
            if best_tokens is not None:
                self._hits += 1
                return list(best_tokens[:need] if need else best_tokens)
            self._misses += 1
            return None

    def put(self, prompt: str, tokens: list[str]) -> None:
        with self._mu:
            self._purge_locked()
            self._store[prompt] = (list(tokens), time.monotonic())
            while len(self._store) > self.max_entries:
                oldest = min(self._store.items(), key=lambda kv: kv[1][1])[0]
                del self._store[oldest]

    @property
    def hits(self) -> int:
        with self._mu:
            return self._hits

    @property
    def misses(self) -> int:
        with self._mu:
            return self._misses

    def hit_ratio(self) -> float:
        with self._mu:
            total = self._hits + self._misses
            return (self._hits / total) if total else 0.0

    def __len__(self) -> int:
        with self._mu:
            return len(self._store)

    def stats(self) -> dict:
        with self._mu:
            total = self._hits + self._misses
            return {
                "entries": len(self._store),
                "hits": self._hits,
                "misses": self._misses,
                "hit_ratio": (self._hits / total) if total else 0.0,
            }
