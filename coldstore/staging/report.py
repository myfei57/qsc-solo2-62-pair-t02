"""Staged loading projections and validation rules.

A loading plan turns each room's cooling demand into a target number of
compressor stages, orders the machines that should pick up the work and
records the basis for every stage change. The projections are frozen
snapshots so concurrent console requests cannot mutate a published plan.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Budget policies when the available stage budget cannot satisfy every room.
POLICY_MAX_COOLING = "max_cooling"
POLICY_MEAN_QUEUE = "mean_queue"
BUDGET_POLICIES = (POLICY_MAX_COOLING, POLICY_MEAN_QUEUE)

# Machine bring-up ordering policies.
ORDER_ROOM_PRIORITY = "room_priority"
ORDER_COMP_EFFICIENCY = "comp_efficiency"
ORDER_POLICIES = (ORDER_ROOM_PRIORITY, ORDER_COMP_EFFICIENCY)

# Legacy machine command vocabulary: four gears encoded on two bits.
LEGACY_GEARS = (
    (0, "00", "off"),
    (1, "01", "low"),
    (2, "10", "medium"),
    (3, "11", "high"),
)


@dataclass(frozen=True)
class StageCode:
    """A target stage rendered both natively and as a legacy four gear code."""

    native_stage: int
    stages: int
    gear: int
    code: str
    label: str

    def as_dict(self) -> dict[str, object]:
        return {
            "native_stage": self.native_stage,
            "stages": self.stages,
            "gear": self.gear,
            "code": self.code,
            "label": self.label,
            "legacy": self.gear,
        }


@dataclass(frozen=True)
class RoomDemand:
    """One room's demand and the number of stages it asks for."""

    room_id: str
    temp: float
    load: float
    target_stages: int
    priority: int
    compressor: str

    def as_dict(self) -> dict[str, object]:
        return {
            "room_id": self.room_id,
            "temp": self.temp,
            "load": self.load,
            "target_stages": self.target_stages,
            "priority": self.priority,
            "compressor": self.compressor,
        }


@dataclass(frozen=True)
class MachineStep:
    """One machine's position in the bring-up order."""

    order: int
    comp_id: str
    room_id: str
    stages: int
    stage: int
    headroom: int
    efficiency: float
    basis: str

    def as_dict(self) -> dict[str, object]:
        return {
            "order": self.order,
            "comp_id": self.comp_id,
            "room_id": self.room_id,
            "stages": self.stages,
            "stage": self.stage,
            "headroom": self.headroom,
            "efficiency": self.efficiency,
            "basis": self.basis,
        }


@dataclass(frozen=True)
class AllocationView:
    """The stages granted to one machine and the evidence behind the switch."""

    comp_id: str
    room_id: str
    from_stage: int
    to_stage: int
    granted: int
    target_stages: int
    code: StageCode
    status: str
    basis: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "comp_id": self.comp_id,
            "room_id": self.room_id,
            "from_stage": self.from_stage,
            "to_stage": self.to_stage,
            "granted": self.granted,
            "target_stages": self.target_stages,
            "code": self.code.as_dict(),
            "status": self.status,
            "basis": list(self.basis),
        }


@dataclass(frozen=True)
class LoadingPlan:
    """The full staged loading plan for one scheduling moment."""

    policy: str
    order_policy: str
    per_stage: float
    total_load: float
    target_stages: int
    headroom: int
    budget: int
    used: int
    cap_total: int
    cap_comp: int
    demands: list[RoomDemand]
    machine_order: list[MachineStep]
    allocations: list[AllocationView]
    deferred: list[AllocationView]

    def as_dict(self) -> dict[str, object]:
        return {
            "policy": self.policy,
            "order_policy": self.order_policy,
            "per_stage": self.per_stage,
            "total_load": self.total_load,
            "target_stages": self.target_stages,
            "headroom": self.headroom,
            "budget": self.budget,
            "used": self.used,
            "cap_total": self.cap_total,
            "cap_comp": self.cap_comp,
            "demands": [demand.as_dict() for demand in self.demands],
            "machine_order": [step.as_dict() for step in self.machine_order],
            "allocations": [allocation.as_dict() for allocation in self.allocations],
            "deferred": [allocation.as_dict() for allocation in self.deferred],
        }


@dataclass(frozen=True)
class TicketView:
    """One queued or admitted loading request."""

    id: int
    room_id: str
    priority: int
    seq: int
    status: str
    position: int

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "room_id": self.room_id,
            "priority": self.priority,
            "seq": self.seq,
            "status": self.status,
            "position": self.position,
        }


@dataclass(frozen=True)
class GateState:
    """Priority queue depth and the token bucket that limits admissions."""

    depth: int
    pending: list[TicketView]
    rate: float
    burst: int
    tokens: float
    admitted: int
    rejected: int

    def as_dict(self) -> dict[str, object]:
        return {
            "depth": self.depth,
            "pending": [ticket.as_dict() for ticket in self.pending],
            "rate": self.rate,
            "burst": self.burst,
            "tokens": self.tokens,
            "admitted": self.admitted,
            "rejected": self.rejected,
        }


def validate_policy(value: str) -> None:
    """Reject an unknown short budget policy."""

    if value not in BUDGET_POLICIES:
        raise ValueError(f"policy must be one of {', '.join(BUDGET_POLICIES)}")


def validate_order_policy(value: str) -> None:
    """Reject an unknown machine ordering policy."""

    if value not in ORDER_POLICIES:
        raise ValueError(f"order policy must be one of {', '.join(ORDER_POLICIES)}")


def validate_per_stage(value: float) -> None:
    """Cooling delivered by one stage must be positive."""

    if value <= 0:
        raise ValueError("cooling per stage must be positive")


def validate_cap(value: int) -> None:
    """A stage cap is zero (unlimited) or positive."""

    if value < 0:
        raise ValueError("stage cap must be zero (unlimited) or positive")


def validate_rate(value: float) -> None:
    """Token refill rates must be positive."""

    if value <= 0:
        raise ValueError("rate must be positive")


def validate_efficiency(value: float) -> None:
    """Reject implausible compressor efficiency coefficients."""

    if value <= 0 or value > 10:
        raise ValueError("efficiency must be between 0 and 10")
