from __future__ import annotations

import os
from typing import Iterator


class MLXEngine:
    model_name = "mlx"

    def __init__(self, model_id: str = "") -> None:
        self.model_id = model_id or os.getenv("MODEL_ID", "mlx-community/Qwen2.5-3B-Instruct-4bit")
        self.loaded = False
        self._model = None
        self._tokenizer = None

    def _ensure(self) -> None:
        if self.loaded:
            return
        from mlx_lm import load

        self._model, self._tokenizer = load(self.model_id)
        self.loaded = True

    def complete(self, prompt: str, max_tokens: int) -> Iterator[str]:
        self._ensure()
        from mlx_lm import stream_generate

        for chunk in stream_generate(self._model, self._tokenizer, prompt, max_tokens=max_tokens):
            text = getattr(chunk, "text", str(chunk))
            if text:
                yield text
