from __future__ import annotations

import collections
import threading
import time


class QueueFullError(Exception):
    pass


class _Lease:
    def __init__(self, scheduler: BoundedScheduler) -> None:
        self._scheduler = scheduler
        self._released = False

    def release(self) -> None:
        scheduler = self._scheduler
        if scheduler is None or self._released:
            return
        self._released = True
        self._scheduler = None
        scheduler._release()

    def __enter__(self) -> _Lease:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.release()
        return False


class BoundedScheduler:
    def __init__(self, capacity: int = 64, max_batch: int = 4, batch_wait_ms: float = 5.0) -> None:
        self.capacity = max(1, int(capacity))
        self.max_batch = max(1, int(max_batch))
        self.batch_wait_ms = max(0.0, float(batch_wait_ms))
        self._mu = threading.Lock()
        self._queue: collections.deque = collections.deque()
        self._arrived: dict = {}
        self._active = 0

    def depth(self) -> int:
        with self._mu:
            return len(self._queue) + self._active

    def queued(self) -> int:
        with self._mu:
            return len(self._queue)

    def active(self) -> int:
        with self._mu:
            return self._active

    def acquire(self, timeout: float | None = None) -> _Lease:
        ticket = threading.Event()
        now = time.monotonic()
        with self._mu:
            if len(self._queue) + self._active >= self.capacity:
                raise QueueFullError("scheduler queue full")
            self._queue.append(ticket)
            self._arrived[ticket] = now
        deadline = None if timeout is None else now + max(0.0, float(timeout))
        try:
            while True:
                wait_s = self.batch_wait_ms / 1000.0
                with self._mu:
                    if ticket not in self._queue:
                        raise QueueFullError("scheduler ticket lost")
                    position = self._queue.index(ticket)
                    if position < self.max_batch and self._active < self.max_batch:
                        head_arrived = self._arrived.get(self._queue[0], now)
                        linger = self.batch_wait_ms / 1000.0 - (time.monotonic() - head_arrived)
                        if position == 0 and self._active == 0 and linger > 0 and len(self._queue) > 1:
                            wait_s = linger
                        else:
                            self._queue.remove(ticket)
                            self._arrived.pop(ticket, None)
                            self._active += 1
                            return _Lease(self)
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            self._queue.remove(ticket)
                            self._arrived.pop(ticket, None)
                            for other in self._queue:
                                other.set()
                            raise QueueFullError("scheduler queue wait timed out")
                        wait_s = min(wait_s, remaining)
                ticket.wait(wait_s if wait_s > 0 else 0.001)
                ticket.clear()
        except BaseException:
            with self._mu:
                if ticket in self._queue:
                    self._queue.remove(ticket)
                    self._arrived.pop(ticket, None)
                    for other in self._queue:
                        other.set()
            raise

    def _release(self) -> None:
        with self._mu:
            if self._active > 0:
                self._active -= 1
            for ticket in self._queue:
                ticket.set()
