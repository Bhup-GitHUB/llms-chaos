from __future__ import annotations

import os

_draining = False


def drain_path() -> str:
    return os.getenv("DRAIN_FILE", "/tmp/llms-chaos-worker.drain")


def is_draining() -> bool:
    if _draining:
        return True
    if os.getenv("DRAIN", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    try:
        return os.path.exists(drain_path())
    except OSError:
        return False


def set_drain() -> bool:
    global _draining
    _draining = True
    try:
        with open(drain_path(), "w") as handle:
            handle.write("draining")
    except OSError:
        pass
    return True


def clear_drain() -> bool:
    global _draining
    _draining = False
    try:
        os.remove(drain_path())
    except OSError:
        pass
    return False
