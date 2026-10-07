"""Staged loading dispatcher.

Rooms ask for cooling through a priority queue; a sliding window limiter
caps how many requests one cycle admits. Each admitted room becomes a
demand with a target stage derived from its cooling load, and demands are
ordered either by room priority or by unit efficiency. A per cycle stage
budget caps a single run; when the budget cannot cover every demand the
configured policy chooses between satisfying the largest load first or
sharing the budget at the current average. Every run is recorded as a
versioned batch so the loading plan, the unit order and the reason for
each switch can be replayed later.
"""

from __future__ import annotations

import json
import math
import threading
import time
from dataclasses import dataclass

from coldstore.comp import Bank
from coldstore.load import LoadCalculator
from coldstore.room import RoomRegistry
from coldstore.store.commands import append_command, load_commands
from coldstore.store.epoch import load_float, save_float
from coldstore.store.store import Store

from .queue import RateLimiter, RequestQueue
from .report import (
    ORDER_EFFICIENCY,
    ORDER_ROOM_PRIORITY,
    POLICY_MAX_FIRST,
    BatchRecord,
    DeferredRequest,
    DispatchPlan,
    DispatchState,
    LoadRequest,
    PlanEntry,
    encode_stage,
    is_legacy,
    validate_efficiency,
    validate_max_step,
    validate_order_mode,
    validate_policy,
    validate_rate_limit,
    validate_rate_window,
    validate_stage_capacity,
)

CAPACITY_KEY = "dispatch:stage-capacity"
MAX_STEP_KEY = "dispatch:max-step"
RATE_LIMIT_KEY = "dispatch:rate-limit"
RATE_WINDOW_KEY = "dispatch:rate-window"
POLICY_KEY = "dispatch:budget-policy"
ORDER_KEY = "dispatch:order-mode"
EFFICIENCY_KEY = "dispatch:efficiency:"
SEQ_KEY = "dispatch:seq"
LATEST_KEY = "dispatch:latest"
BATCHES_KEY = "dispatch:batches"

DEFAULT_STAGE_CAPACITY = 3.0
DEFAULT_MAX_STEP = 2
DEFAULT_RATE_LIMIT = 4
DEFAULT_RATE_WINDOW = 1.0
DEFAULT_POLICY = POLICY_MAX_FIRST
DEFAULT_ORDER = ORDER_ROOM_PRIORITY
DEFAULT_EFFICIENCY = 1.0


def target_for(load: float, capacity: float, stages: int) -> int:
    """Stages needed to cover a load, clamped to what the unit can encode."""

    if load <= 0 or capacity <= 0:
        return 0
    return min(int(math.ceil(load / capacity)), stages)


@dataclass
class _Row:
    """Mutable per unit allocation row, frozen into a PlanEntry afterwards."""

    compressor: str
    rooms: list[str]
    load: float
    target: int
    from_stage: int
    stages: int
    priority: int
    efficiency: float
    granted: int = 0

    @property
    def need(self) -> int:
        return max(0, self.target - self.from_stage)


def _order_key(row: _Row, mode: str) -> tuple[object, ...]:
    if mode == ORDER_EFFICIENCY:
        return (-row.efficiency, -row.priority, -row.load, row.compressor)
    return (-row.priority, -row.load, -row.efficiency, row.compressor)


def _allocate(rows: list[_Row], budget: int, policy: str) -> None:
    if budget <= 0:
        return
    if policy == POLICY_MAX_FIRST:
        remaining = budget
        for row in rows:
            grant = min(row.need, remaining)
            row.granted += grant
            remaining -= grant
        return
    active = [row for row in rows if row.need > 0]
    if not active:
        return
    share = max(1, budget // len(active))
    remaining = budget
    for row in active:
        grant = min(row.need, share, remaining)
        row.granted += grant
        remaining -= grant
    for row in active:
        if remaining <= 0:
            break
        extra = min(row.need - row.granted, remaining)
        row.granted += extra
        remaining -= extra


def _reason(row: _Row) -> str:
    parts = []
    if len(row.rooms) > 1:
        parts.append("shared by " + ",".join(row.rooms))
    if row.granted >= row.need:
        parts.append(f"target {row.target} within budget")
    elif row.granted > 0:
        parts.append(f"budget cap: {row.granted} of {row.need} stages granted")
    else:
        parts.append("budget exhausted")
    if is_legacy(row.stages):
        parts.append("legacy 4-stage encoding")
    return "; ".join(parts)


class Dispatcher:
    """Turns queued room requests into versioned compressor loading runs."""

    def __init__(self, store: Store) -> None:
        self._store = store
        self._queue = RequestQueue()
        self._limiter = RateLimiter(DEFAULT_RATE_LIMIT, DEFAULT_RATE_WINDOW)
        self._run_lock = threading.Lock()

    def stage_capacity(self) -> float:
        value, present = load_float(self._store, CAPACITY_KEY)
        return value if present else DEFAULT_STAGE_CAPACITY

    def set_stage_capacity(self, value: float) -> float:
        validate_stage_capacity(value)
        save_float(self._store, CAPACITY_KEY, value)
        return value

    def max_step(self) -> int:
        value, present = load_float(self._store, MAX_STEP_KEY)
        return int(value) if present else DEFAULT_MAX_STEP

    def set_max_step(self, value: int) -> int:
        validate_max_step(value)
        save_float(self._store, MAX_STEP_KEY, float(value))
        return value

    def rate_limit(self) -> int:
        value, present = load_float(self._store, RATE_LIMIT_KEY)
        return int(value) if present else DEFAULT_RATE_LIMIT

    def rate_window(self) -> float:
        value, present = load_float(self._store, RATE_WINDOW_KEY)
        return value if present else DEFAULT_RATE_WINDOW

    def set_rate_limit(self, value: int) -> int:
        validate_rate_limit(value)
        save_float(self._store, RATE_LIMIT_KEY, float(value))
        self._limiter.configure(value, self.rate_window())
        return value

    def set_rate_window(self, value: float) -> float:
        validate_rate_window(value)
        save_float(self._store, RATE_WINDOW_KEY, value)
        self._limiter.configure(self.rate_limit(), value)
        return value

    def policy(self) -> str:
        raw, present = self._store.get(POLICY_KEY)
        return raw if present else DEFAULT_POLICY

    def set_policy(self, policy: str) -> str:
        validate_policy(policy)
        self._store.put(POLICY_KEY, policy)
        return policy

    def order_mode(self) -> str:
        raw, present = self._store.get(ORDER_KEY)
        return raw if present else DEFAULT_ORDER

    def set_order_mode(self, mode: str) -> str:
        validate_order_mode(mode)
        self._store.put(ORDER_KEY, mode)
        return mode

    def efficiency(self, comp_id: str) -> float:
        value, present = load_float(self._store, EFFICIENCY_KEY + comp_id)
        return value if present else DEFAULT_EFFICIENCY

    def set_efficiency(self, comp_id: str, value: float) -> float:
        validate_efficiency(value)
        save_float(self._store, EFFICIENCY_KEY + comp_id, value)
        return value

    def config(self) -> dict[str, object]:
        return {
            "stage_capacity": self.stage_capacity(),
            "max_step": self.max_step(),
            "rate_limit": self.rate_limit(),
            "rate_window": self.rate_window(),
            "policy": self.policy(),
            "order_mode": self.order_mode(),
        }

    def submit(self, room_id: str, priority: int = 0) -> LoadRequest:
        return self._queue.push(room_id, priority)

    def pending(self) -> int:
        return self._queue.pending()

    def clear(self) -> None:
        self._queue.clear()

    def plan(
        self,
        rooms: RoomRegistry,
        bank: Bank,
        load: LoadCalculator,
        now: float | None = None,
    ) -> DispatchPlan:
        """Preview the run the current backlog would produce, mutating nothing."""

        moment = time.monotonic() if now is None else now
        requests = self._dedupe(self._queue.peek(self._limiter.available(moment)))
        return self._build_plan(requests, rooms, bank, load)

    def run(
        self,
        rooms: RoomRegistry,
        bank: Bank,
        load: LoadCalculator,
        now: float | None = None,
    ) -> BatchRecord:
        """Admit requests, apply the plan and record it as the next version."""

        with self._run_lock:
            moment = time.monotonic() if now is None else now
            admitted = self._drain(moment)
            plan = self._build_plan(admitted, rooms, bank, load)
            for entry in plan.entries:
                if entry.granted > 0:
                    bank.set_stage(entry.compressor, entry.to_stage)
            record = BatchRecord(
                version=self._next_version(),
                at=int(time.time()),
                policy=plan.policy,
                order_mode=plan.order_mode,
                budget=plan.budget,
                granted=plan.granted,
                admitted=[request.room_id for request in admitted],
                order=plan.order,
                entries=plan.entries,
                deferred=plan.deferred,
            )
            append_command(
                self._store, BATCHES_KEY, json.dumps(record.as_dict(), ensure_ascii=False)
            )
            self._store.put(LATEST_KEY, json.dumps(record.as_dict(), ensure_ascii=False))
            return record

    def batches(self) -> list[dict[str, object]]:
        """Every recorded batch, oldest first."""

        records = []
        for item in load_commands(self._store, BATCHES_KEY):
            try:
                payload = json.loads(item)
            except ValueError:
                continue
            if isinstance(payload, dict):
                records.append(payload)
        return records

    def batch_count(self) -> int:
        return len(load_commands(self._store, BATCHES_KEY))

    def versions(self) -> list[dict[str, object]]:
        """One summary row per recorded batch version."""

        return [
            {
                "version": int(record.get("version", 0)),
                "at": int(record.get("at", 0)),
                "granted": int(record.get("granted", 0)),
                "entries": len(record.get("entries", [])),
            }
            for record in self.batches()
        ]

    def latest(self) -> dict[str, object] | None:
        raw, present = self._store.get(LATEST_KEY)
        if not present:
            return None
        try:
            payload = json.loads(raw)
        except ValueError:
            return None
        return payload if isinstance(payload, dict) else None

    def state(self) -> DispatchState:
        return DispatchState(
            stage_capacity=self.stage_capacity(),
            max_step=self.max_step(),
            rate_limit=self.rate_limit(),
            rate_window=self.rate_window(),
            policy=self.policy(),
            order_mode=self.order_mode(),
            pending=self._queue.pending(),
            batches=self.batch_count(),
            last_version=self._version(),
        )

    def describe(self) -> str:
        state = self.state()
        return (
            f"dispatch pending={state.pending} batches={state.batches} "
            f"policy={state.policy} order={state.order_mode}"
        )

    def _drain(self, now: float) -> list[LoadRequest]:
        allowance = self._limiter.available(now)
        popped: list[LoadRequest] = []
        for _ in range(allowance):
            request = self._queue.pop()
            if request is None:
                break
            popped.append(request)
        self._limiter.record(now, len(popped))
        return self._dedupe(popped)

    @staticmethod
    def _dedupe(requests: list[LoadRequest]) -> list[LoadRequest]:
        """Keep the first occurrence of each room; the queue pops priority first."""

        seen: set[str] = set()
        unique: list[LoadRequest] = []
        for request in requests:
            if request.room_id in seen:
                continue
            seen.add(request.room_id)
            unique.append(request)
        return unique

    def _build_plan(
        self,
        requests: list[LoadRequest],
        rooms: RoomRegistry,
        bank: Bank,
        load: LoadCalculator,
    ) -> DispatchPlan:
        rows: list[_Row] = []
        deferred: list[DeferredRequest] = []
        for request in requests:
            room = rooms.room(request.room_id)
            if room is None:
                deferred.append(DeferredRequest(request.room_id, "unknown room"))
                continue
            if not room.compressor:
                deferred.append(DeferredRequest(request.room_id, "no compressor assigned"))
                continue
            compressor = bank.compressor(room.compressor)
            if compressor is None:
                deferred.append(DeferredRequest(request.room_id, "compressor not registered"))
                continue
            demand = load.load_for(room.temp)
            target = target_for(demand, self.stage_capacity(), compressor.stages)
            if target <= 0:
                deferred.append(DeferredRequest(request.room_id, "no load"))
                continue
            if target <= compressor.stage:
                deferred.append(DeferredRequest(request.room_id, "already at target"))
                continue
            rows.append(
                _Row(
                    compressor=compressor.id,
                    rooms=[request.room_id],
                    load=demand,
                    target=target,
                    from_stage=compressor.stage,
                    stages=compressor.stages,
                    priority=request.priority,
                    efficiency=self.efficiency(compressor.id),
                )
            )
        merged = self._merge(rows)
        mode = self.order_mode()
        merged.sort(key=lambda row: _order_key(row, mode))
        policy = self.policy()
        budget = self.max_step()
        _allocate(merged, budget, policy)
        entries = [
            PlanEntry(
                compressor=row.compressor,
                rooms=row.rooms,
                load=row.load,
                target=row.target,
                from_stage=row.from_stage,
                granted=row.granted,
                to_stage=row.from_stage + row.granted,
                encoded=encode_stage(row.stages, row.from_stage + row.granted),
                legacy=is_legacy(row.stages),
                reason=_reason(row),
            )
            for row in merged
        ]
        return DispatchPlan(
            policy=policy,
            order_mode=mode,
            budget=budget,
            granted=sum(entry.granted for entry in entries),
            order=[entry.compressor for entry in entries],
            entries=entries,
            deferred=deferred,
        )

    @staticmethod
    def _merge(rows: list[_Row]) -> list[_Row]:
        """Fold rooms sharing a compressor into one row; the worst room wins."""

        by_compressor: dict[str, _Row] = {}
        for row in rows:
            existing = by_compressor.get(row.compressor)
            if existing is None:
                by_compressor[row.compressor] = row
                continue
            existing.rooms.extend(row.rooms)
            existing.load = max(existing.load, row.load)
            existing.target = max(existing.target, row.target)
            existing.priority = max(existing.priority, row.priority)
        merged = list(by_compressor.values())
        for row in merged:
            row.rooms.sort()
        return merged

    def _version(self) -> int:
        raw, present = self._store.get(SEQ_KEY)
        if not present:
            return 0
        try:
            return int(raw)
        except ValueError:
            return 0

    def _next_version(self) -> int:
        version = self._version() + 1
        self._store.put(SEQ_KEY, str(version))
        return version
