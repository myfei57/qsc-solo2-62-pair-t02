"""Hierarchical staged loading scheduler.

The scheduler answers the whole operator question in one place:

1. each room's cooling demand becomes a target number of compressor stages;
2. rooms are queued by priority and matched to machines, whose bring-up order
   follows room priority or compressor efficiency;
3. a stage budget is shared out under an explicit policy when capacity cannot
   satisfy everyone at once;
4. every granted switch is rendered as both a native stage and a four gear
   legacy command and published into an immutable batch and plan version.
"""

from __future__ import annotations

import math
import threading

from coldstore.comp import Bank
from coldstore.load import LoadCalculator
from coldstore.room import RoomRegistry
from coldstore.store import Store, append_event, load_float, save_float

from .budget import allocate
from .encoding import encode_stage
from .gate import PriorityGate
from .history import BatchArchive, BatchRecord
from .report import (
    AllocationView,
    LoadingPlan,
    MachineStep,
    ORDER_COMP_EFFICIENCY,
    ORDER_ROOM_PRIORITY,
    POLICY_MAX_COOLING,
    RoomDemand,
    StageCode,
    validate_cap,
    validate_efficiency,
    validate_order_policy,
    validate_per_stage,
    validate_policy,
    validate_rate,
)

POLICY_KEY = "staging:budget-policy"
ORDER_KEY = "staging:order-policy"
PER_STAGE_KEY = "staging:cooling-per-stage"
CAP_TOTAL_KEY = "staging:cap-total"
CAP_COMP_KEY = "staging:cap-comp"
RATE_KEY = "staging:throttle-rate"
BURST_KEY = "staging:throttle-burst"
EFFICIENCY_KEY = "staging:efficiency:{}"
PRIORITY_KEY = "staging:priority:{}"
EVENT_KEY = "staging:events"

DEFAULT_PER_STAGE = 4.0
DEFAULT_PRIORITY = 1000
DEFAULT_EFFICIENCY = 1.0
DEFAULT_RATE = 1.0
DEFAULT_BURST = 2


class LoadingScheduler:
    """Computes loading plans, applies stage changes and records batches."""

    def __init__(
        self,
        store: Store,
        rooms: RoomRegistry,
        bank: Bank,
        load: LoadCalculator,
        gate: PriorityGate | None = None,
    ) -> None:
        self._store = store
        self._rooms = rooms
        self._bank = bank
        self._load = load
        self._archive = BatchArchive(store)
        self._gate = gate or PriorityGate(rate=self.rate(), burst=self.burst())
        self._apply_lock = threading.RLock()

    # ---- configuration -------------------------------------------------

    def policy(self) -> str:
        raw, present = self._store.get(POLICY_KEY)
        return raw if present else POLICY_MAX_COOLING

    def set_policy(self, value: str) -> str:
        validate_policy(value)
        self._store.put(POLICY_KEY, value)
        append_event(self._store, EVENT_KEY, "policy", value)
        return value

    def order_policy(self) -> str:
        raw, present = self._store.get(ORDER_KEY)
        return raw if present else ORDER_ROOM_PRIORITY

    def set_order_policy(self, value: str) -> str:
        validate_order_policy(value)
        self._store.put(ORDER_KEY, value)
        append_event(self._store, EVENT_KEY, "order-policy", value)
        return value

    def per_stage(self) -> float:
        value, present = load_float(self._store, PER_STAGE_KEY)
        return value if present else DEFAULT_PER_STAGE

    def set_per_stage(self, value: float) -> float:
        validate_per_stage(value)
        save_float(self._store, PER_STAGE_KEY, value)
        return value

    def cap_total(self) -> int:
        raw, present = self._store.get(CAP_TOTAL_KEY)
        try:
            return int(raw) if present else 0
        except ValueError:
            return 0

    def set_cap_total(self, value: int) -> int:
        validate_cap(value)
        self._store.put(CAP_TOTAL_KEY, str(value))
        return value

    def cap_comp(self) -> int:
        raw, present = self._store.get(CAP_COMP_KEY)
        try:
            return int(raw) if present else 0
        except ValueError:
            return 0

    def set_cap_comp(self, value: int) -> int:
        validate_cap(value)
        self._store.put(CAP_COMP_KEY, str(value))
        return value

    def rate(self) -> float:
        value, present = load_float(self._store, RATE_KEY)
        return value if present else DEFAULT_RATE

    def burst(self) -> int:
        raw, present = self._store.get(BURST_KEY)
        try:
            return int(raw) if present else DEFAULT_BURST
        except ValueError:
            return DEFAULT_BURST

    def set_throttle(self, rate: float, burst: int) -> tuple[float, int]:
        validate_rate(rate)
        validate_cap(burst)
        if burst <= 0:
            raise ValueError("burst must be positive")
        save_float(self._store, RATE_KEY, rate)
        self._store.put(BURST_KEY, str(burst))
        self._gate.configure(rate, burst)
        return rate, burst

    def efficiency(self, comp_id: str) -> float:
        value, present = load_float(self._store, EFFICIENCY_KEY.format(comp_id))
        return value if present else DEFAULT_EFFICIENCY

    def set_efficiency(self, comp_id: str, value: float) -> float:
        validate_efficiency(value)
        save_float(self._store, EFFICIENCY_KEY.format(comp_id), value)
        return value

    def priority(self, room_id: str) -> int:
        raw, present = self._store.get(PRIORITY_KEY.format(room_id))
        try:
            return int(raw) if present else DEFAULT_PRIORITY
        except ValueError:
            return DEFAULT_PRIORITY

    def set_priority(self, room_id: str, value: int) -> int:
        if value < 0:
            raise ValueError("priority must be non-negative")
        self._store.put(PRIORITY_KEY.format(room_id), str(value))
        return value

    # ---- request gating ------------------------------------------------

    @property
    def gate(self) -> PriorityGate:
        return self._gate

    @property
    def rooms(self) -> RoomRegistry:
        return self._rooms

    def submit_request(self, room_id: str, priority: int | None = None):
        room_priority = self.priority(room_id) if priority is None else priority
        return self._gate.submit(room_id, room_priority)

    # ---- target stages and machine order -------------------------------

    def target_for(self, temp: float) -> int:
        load = self._load.load_for(temp)
        if load <= 0:
            return 0
        return max(1, int(math.ceil(load / self.per_stage())))

    def _demands(self, room_filter: set[str] | None) -> list[RoomDemand]:
        demands: list[RoomDemand] = []
        for room in self._rooms.rooms():
            if room_filter is not None and room.id not in room_filter:
                continue
            target = self.target_for(room.temp)
            if target <= 0:
                continue
            demands.append(
                RoomDemand(
                    room_id=room.id,
                    temp=room.temp,
                    load=self._load.load_for(room.temp),
                    target_stages=target,
                    priority=self.priority(room.id),
                    compressor=room.compressor,
                )
            )
        demands.sort(key=lambda demand: (demand.priority, -demand.load, demand.room_id))
        return demands

    def ranked_machines(self) -> list[str]:
        """Compressor bring-up order under the configured order policy."""

        order_policy = self.order_policy()
        if order_policy == ORDER_COMP_EFFICIENCY:
            return [
                comp.id
                for comp in sorted(
                    self._bank.compressors(),
                    key=lambda comp: (-self.efficiency(comp.id), comp.id),
                )
            ]
        # Room priority: a machine inherits the priority of the highest
        # priority room assigned to it; idle machines follow by identifier.
        def rank(comp_id: str) -> tuple[int, str]:
            served = self._rooms.rooms_for_compressor(comp_id)
            if served:
                return min(self.priority(room_id) for room_id in served), comp_id
            return DEFAULT_PRIORITY + 1, comp_id

        return sorted((compressor.id for compressor in self._bank.compressors()), key=rank)

    def _match(self, demands: list[RoomDemand]) -> tuple[dict[str, list[RoomDemand]], list[RoomDemand]]:
        """Assign queued rooms to machines with remaining headroom."""

        ranking = self.ranked_machines()
        claimed: dict[str, int] = {comp_id: self._bank.stage(comp_id)[0] for comp_id in ranking}
        served: dict[str, list[RoomDemand]] = {comp_id: [] for comp_id in ranking}
        deferred: list[RoomDemand] = []
        for demand in demands:
            candidates = ranking
            preferred = demand.compressor
            if preferred in claimed:
                candidates = [preferred] + [comp_id for comp_id in ranking if comp_id != preferred]
            for comp_id in candidates:
                compressor = self._bank.compressor(comp_id)
                if compressor is None:
                    continue
                if claimed[comp_id] >= compressor.stages:
                    continue
                claimed[comp_id] = min(compressor.stages, claimed[comp_id] + demand.target_stages)
                served[comp_id].append(demand)
                break
            else:
                deferred.append(demand)
        return served, deferred

    # ---- planning ------------------------------------------------------

    def plan(self, room_ids: list[str] | None = None, budget: int = 0) -> LoadingPlan:
        """Compute the loading plan without touching the compressor bank."""

        room_filter = set(room_ids) if room_ids else None
        demands = self._demands(room_filter)
        policy = self.policy()
        order_policy = self.order_policy()
        per_stage = self.per_stage()
        cap_total = self.cap_total()
        cap_comp = self.cap_comp()

        served, deferred_rooms = self._match(demands)

        wanted: dict[str, int] = {}
        room_of: dict[str, str] = {}
        target_of: dict[str, int] = {}
        for comp_id, room_demands in served.items():
            if not room_demands:
                continue
            compressor = self._bank.compressor(comp_id)
            if compressor is None:
                continue
            headroom = compressor.stages - compressor.stage
            total = sum(demand.target_stages for demand in room_demands)
            limited = min(total, headroom)
            if cap_comp:
                limited = min(limited, cap_comp)
            wanted[comp_id] = limited
            room_of[comp_id] = room_demands[0].room_id
            target_of[comp_id] = total

        total_headroom = self._bank.max_stage() - self._bank.total_stage()
        available = total_headroom
        if cap_total:
            available = min(available, cap_total)
        if budget > 0:
            available = min(available, budget)
        available = max(0, available)

        room_order = [comp_id for comp_id, _room in sorted(room_of.items(), key=lambda item: self._room_rank(item[1], item[0]))]
        grants = allocate(wanted, available, policy, room_order)

        allocations: list[AllocationView] = []
        machine_steps: list[MachineStep] = []
        used = 0
        order = 0
        for comp_id in room_order:
            compressor = self._bank.compressor(comp_id)
            if compressor is None:
                continue
            granted = grants.get(comp_id, 0)
            from_stage = compressor.stage
            to_stage = from_stage + granted
            code = encode_stage(to_stage, compressor.stages)
            used += granted
            order += 1
            basis = [
                f"policy:{policy}",
                f"order:{order_policy}",
                f"room:{room_of[comp_id]}",
                f"load-target:{target_of[comp_id]}",
                f"legacy:{code.code}/{code.label}",
            ]
            if cap_comp and wanted[comp_id] >= cap_comp:
                basis.append(f"cap-comp:{cap_comp}")
            status = "ready" if granted > 0 else "waiting-budget"
            if granted < wanted[comp_id]:
                status = "partial" if granted > 0 else "deferred-budget"
                basis.append(f"deferred:{wanted[comp_id] - granted}")
            allocations.append(
                AllocationView(
                    comp_id=comp_id,
                    room_id=room_of[comp_id],
                    from_stage=from_stage,
                    to_stage=to_stage,
                    granted=granted,
                    target_stages=target_of[comp_id],
                    code=code,
                    status=status,
                    basis=basis,
                )
            )
            machine_steps.append(
                MachineStep(
                    order=order,
                    comp_id=comp_id,
                    room_id=room_of[comp_id],
                    stages=compressor.stages,
                    stage=to_stage,
                    headroom=compressor.stages - to_stage,
                    efficiency=self.efficiency(comp_id),
                    basis=f"{order_policy}:{'='.join(d.room_id for d in served[comp_id])}",
                )
            )

        deferred_views = [
            AllocationView(
                comp_id="",
                room_id=demand.room_id,
                from_stage=0,
                to_stage=0,
                granted=0,
                target_stages=demand.target_stages,
                code=StageCode(0, 0, 0, "00", "off"),
                status="deferred-no-capacity",
                basis=[
                    f"policy:{policy}",
                    f"priority:{demand.priority}",
                    "deferred:no-machine-headroom",
                ],
            )
            for demand in deferred_rooms
        ]

        return LoadingPlan(
            policy=policy,
            order_policy=order_policy,
            per_stage=per_stage,
            total_load=sum(demand.load for demand in demands),
            target_stages=sum(demand.target_stages for demand in demands),
            headroom=total_headroom,
            budget=available,
            used=used,
            cap_total=cap_total,
            cap_comp=cap_comp,
            demands=demands,
            machine_order=machine_steps,
            allocations=allocations,
            deferred=deferred_views,
        )

    def _room_rank(self, room_id: str, comp_id: str) -> tuple[int, float, str]:
        room = self._rooms.room(room_id)
        load = self._load.load_for(room.temp) if room else 0.0
        return self.priority(room_id), -load, comp_id

    # ---- application and publishing ------------------------------------

    def apply(
        self,
        *,
        budget: int = 0,
        reason: str = "planned",
        room_ids: list[str] | None = None,
        requests: list[dict[str, object]] | None = None,
    ) -> BatchRecord:
        """Plan, stage the bank one granted step at a time and publish."""

        with self._apply_lock:
            plan = self.plan(room_ids=room_ids, budget=budget)
            for allocation in plan.allocations:
                if allocation.granted <= 0:
                    continue
                for _ in range(allocation.granted):
                    self._bank.stage_up(allocation.comp_id)
            machine_order = [step.comp_id for step in plan.machine_order if step.stage > 0]
            record = self._archive.publish(
                plan.as_dict(),
                machine_order,
                applied=True,
                reason=reason,
                requests=requests,
            )
            append_event(
                self._store,
                EVENT_KEY,
                "batch",
                f"#{record.seq} used={record.used}/{record.budget} policy={record.policy}",
            )
            return record

    def publish_plan(self, *, budget: int = 0, reason: str = "monitor", room_ids: list[str] | None = None) -> BatchRecord:
        """Publish a plan version without moving the bank (dry run)."""

        plan = self.plan(room_ids=room_ids, budget=budget)
        machine_order = [step.comp_id for step in plan.machine_order]
        return self._archive.publish(
            plan.as_dict(),
            machine_order,
            applied=False,
            reason=reason,
        )

    def archive(self) -> BatchArchive:
        return self._archive

    def describe(self) -> str:
        plan = self.plan()
        return (
            f"staging policy={plan.policy} order={plan.order_policy} "
            f"target={plan.target_stages} budget={plan.budget} used={plan.used}"
        )
