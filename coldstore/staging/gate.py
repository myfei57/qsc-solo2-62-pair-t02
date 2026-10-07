"""Priority queue and admission rate limit for concurrent loading requests.

Many rooms can ask for capacity at once. Requests enter a priority queue
(lower priority value first, arrival order breaking ties) and leave it through
a token bucket: one token is spent per admitted request and tokens refill at a
fixed rate, bounding how often the bank can be asked to stage up.
"""

from __future__ import annotations

import itertools
import threading
import time
from dataclasses import dataclass

from .report import GateState, TicketView


@dataclass(frozen=True)
class _Ticket:
    id: int
    room_id: str
    priority: int
    seq: int

    def key(self) -> tuple[int, int]:
        return (self.priority, self.seq)


class PriorityGate:
    """Thread safe priority queue with token bucket admission control."""

    def __init__(
        self,
        rate: float = 1.0,
        burst: int = 2,
        max_depth: int = 0,
        clock: "callable | None" = None,
    ) -> None:
        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._rate = rate
        self._burst = burst
        self._max_depth = max_depth
        self._clock = clock or time.monotonic
        self._tokens = float(burst)
        self._last = self._clock()
        self._pending: list[_Ticket] = []
        self._ids = itertools.count(1)
        self._seqs = itertools.count(1)
        self._admitted = 0
        self._rejected = 0

    def configure(self, rate: float, burst: int) -> None:
        with self._cond:
            self._rate = rate
            self._burst = burst
            self._tokens = min(self._tokens, float(burst)) if self._tokens > burst else self._tokens
            self._cond.notify_all()

    def refill(self) -> float:
        """Add tokens from elapsed time, capped at the burst size."""

        with self._lock:
            return self._refill_locked()

    def _refill_locked(self) -> float:
        now = self._clock()
        elapsed = now - self._last
        if elapsed > 0:
            before = self._tokens
            self._tokens = min(float(self._burst), self._tokens + elapsed * self._rate)
            self._last = now
            if before < 1.0 <= self._tokens and self._pending:
                self._cond.notify_all()
        return self._tokens

    def top_up(self, tokens: float) -> float:
        """Manually add token budget (used by tests and operator overrides)."""

        with self._lock:
            self._tokens = min(float(self._burst), self._tokens + tokens)
            return self._tokens

    def submit(self, room_id: str, priority: int) -> _Ticket:
        with self._cond:
            if self._max_depth and len(self._pending) >= self._max_depth:
                self._rejected += 1
                raise ValueError("loading queue is full")
            ticket = _Ticket(
                id=next(self._ids),
                room_id=room_id,
                priority=priority,
                seq=next(self._seqs),
            )
            self._pending.append(ticket)
            self._pending.sort(key=_Ticket.key)
            self._cond.notify_all()
            return ticket

    def admit(self, block: bool = False, timeout: float | None = None) -> _Ticket | None:
        """Take the highest priority request when a token is available."""

        with self._cond:
            deadline = None if timeout is None else self._clock() + timeout
            self.refill()
            while not self._pending or self._tokens < 1.0:
                if not block:
                    return None
                remaining = None if deadline is None else deadline - self._clock()
                if remaining is not None and remaining <= 0:
                    return None
                self._cond.wait(remaining)
                self.refill()
            ticket = self._pending.pop(0)
            self._tokens -= 1.0
            self._admitted += 1
            self._cond.notify_all()
            return ticket

    def drop(self, ticket_id: int) -> bool:
        with self._cond:
            for index, ticket in enumerate(self._pending):
                if ticket.id == ticket_id:
                    del self._pending[index]
                    self._rejected += 1
                    self._cond.notify_all()
                    return True
            return False

    def clear(self) -> int:
        with self._cond:
            dropped = len(self._pending)
            self._pending.clear()
            self._rejected += dropped
            self._cond.notify_all()
            return dropped

    def _views(self) -> list[TicketView]:
        return [
            TicketView(
                id=ticket.id,
                room_id=ticket.room_id,
                priority=ticket.priority,
                seq=ticket.seq,
                status="pending",
                position=position + 1,
            )
            for position, ticket in enumerate(self._pending)
        ]

    def state(self) -> GateState:
        with self._lock:
            self.refill()
            return GateState(
                depth=len(self._pending),
                pending=self._views(),
                rate=self._rate,
                burst=self._burst,
                tokens=self._tokens,
                admitted=self._admitted,
                rejected=self._rejected,
            )
