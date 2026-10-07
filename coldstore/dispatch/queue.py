"""Priority queue with a sliding window rate limiter."""

from __future__ import annotations

import heapq
import threading

from .report import LoadRequest


class RequestQueue:
    """Admits load requests highest priority first, FIFO inside a priority."""

    def __init__(self) -> None:
        self._heap: list[tuple[int, int, LoadRequest]] = []
        self._seq = 0
        self._lock = threading.RLock()

    def push(self, room_id: str, priority: int) -> LoadRequest:
        with self._lock:
            self._seq += 1
            request = LoadRequest(room_id=room_id, priority=priority, seq=self._seq)
            heapq.heappush(self._heap, (-priority, self._seq, request))
            return request

    def pop(self) -> LoadRequest | None:
        with self._lock:
            if not self._heap:
                return None
            return heapq.heappop(self._heap)[2]

    def peek(self, limit: int) -> list[LoadRequest]:
        """The next requests in pop order, without removing them."""

        with self._lock:
            ordered = sorted(self._heap)
            return [item[2] for item in ordered[: max(0, limit)]]

    def pending(self) -> int:
        with self._lock:
            return len(self._heap)

    def clear(self) -> None:
        with self._lock:
            self._heap.clear()


class RateLimiter:
    """Sliding window allowance over admission timestamps."""

    def __init__(self, limit: int, window: float) -> None:
        self._limit = limit
        self._window = window
        self._stamps: list[float] = []
        self._lock = threading.RLock()

    def configure(self, limit: int, window: float) -> None:
        with self._lock:
            self._limit = limit
            self._window = window

    def available(self, now: float) -> int:
        with self._lock:
            self._prune_locked(now)
            return max(0, self._limit - len(self._stamps))

    def record(self, now: float, count: int) -> None:
        with self._lock:
            self._prune_locked(now)
            self._stamps.extend([now] * max(0, count))

    def _prune_locked(self, now: float) -> None:
        horizon = now - self._window
        self._stamps = [stamp for stamp in self._stamps if stamp > horizon]
