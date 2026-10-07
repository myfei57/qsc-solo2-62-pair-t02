"""Dispatch projections and validation rules."""

from __future__ import annotations

from dataclasses import dataclass

POLICY_MAX_FIRST = "max-first"
POLICY_AVERAGE_SHARE = "average-share"
POLICIES = (POLICY_MAX_FIRST, POLICY_AVERAGE_SHARE)

ORDER_ROOM_PRIORITY = "room-priority"
ORDER_EFFICIENCY = "efficiency"
ORDER_MODES = (ORDER_ROOM_PRIORITY, ORDER_EFFICIENCY)

LEGACY_STAGES = 4


def is_legacy(stages: int) -> bool:
    """A unit with at most four stage positions speaks the legacy encoding."""

    return stages <= LEGACY_STAGES


def encode_stage(stages: int, stage: int) -> int:
    """Clamp a stage command to the range the unit can encode."""

    if stage < 0:
        return 0
    return min(stage, stages)


@dataclass(frozen=True)
class LoadRequest:
    """One queued request for compressor loading."""

    room_id: str
    priority: int
    seq: int

    def as_dict(self) -> dict[str, object]:
        return {"room_id": self.room_id, "priority": self.priority, "seq": self.seq}


@dataclass(frozen=True)
class DeferredRequest:
    """A request that could not become a demand, with the reason."""

    room_id: str
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {"room_id": self.room_id, "reason": self.reason}


@dataclass(frozen=True)
class PlanEntry:
    """One compressor's planned move inside a dispatch run."""

    compressor: str
    rooms: list[str]
    load: float
    target: int
    from_stage: int
    granted: int
    to_stage: int
    encoded: int
    legacy: bool
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "compressor": self.compressor,
            "rooms": self.rooms,
            "load": self.load,
            "target": self.target,
            "from_stage": self.from_stage,
            "granted": self.granted,
            "to_stage": self.to_stage,
            "encoded": self.encoded,
            "legacy": self.legacy,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class DispatchPlan:
    """The loading plan a run would apply: entries in unit start order."""

    policy: str
    order_mode: str
    budget: int
    granted: int
    order: list[str]
    entries: list[PlanEntry]
    deferred: list[DeferredRequest]

    def as_dict(self) -> dict[str, object]:
        return {
            "policy": self.policy,
            "order_mode": self.order_mode,
            "budget": self.budget,
            "granted": self.granted,
            "order": self.order,
            "entries": [entry.as_dict() for entry in self.entries],
            "deferred": [deferred.as_dict() for deferred in self.deferred],
        }


@dataclass(frozen=True)
class BatchRecord:
    """One executed dispatch run, kept as a versioned history record."""

    version: int
    at: int
    policy: str
    order_mode: str
    budget: int
    granted: int
    admitted: list[str]
    order: list[str]
    entries: list[PlanEntry]
    deferred: list[DeferredRequest]

    def as_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "at": self.at,
            "policy": self.policy,
            "order_mode": self.order_mode,
            "budget": self.budget,
            "granted": self.granted,
            "admitted": self.admitted,
            "order": self.order,
            "entries": [entry.as_dict() for entry in self.entries],
            "deferred": [deferred.as_dict() for deferred in self.deferred],
        }


@dataclass(frozen=True)
class DispatchState:
    """Dispatcher configuration and backlog at a glance."""

    stage_capacity: float
    max_step: int
    rate_limit: int
    rate_window: float
    policy: str
    order_mode: str
    pending: int
    batches: int
    last_version: int

    def as_dict(self) -> dict[str, object]:
        return {
            "stage_capacity": self.stage_capacity,
            "max_step": self.max_step,
            "rate_limit": self.rate_limit,
            "rate_window": self.rate_window,
            "policy": self.policy,
            "order_mode": self.order_mode,
            "pending": self.pending,
            "batches": self.batches,
            "last_version": self.last_version,
        }


def validate_policy(policy: str) -> None:
    """Reject an unknown budget policy."""

    if policy not in POLICIES:
        raise ValueError("policy must be max-first or average-share")


def validate_order_mode(mode: str) -> None:
    """Reject an unknown unit ordering mode."""

    if mode not in ORDER_MODES:
        raise ValueError("order mode must be room-priority or efficiency")


def validate_max_step(value: int) -> None:
    """Reject a per cycle stage budget outside the supported range."""

    if value < 1:
        raise ValueError("max step must be positive")
    if value > 64:
        raise ValueError("max step exceeds the supported range")


def validate_rate_limit(value: int) -> None:
    """Reject an admission limit outside the supported range."""

    if value < 1:
        raise ValueError("rate limit must be positive")
    if value > 1000:
        raise ValueError("rate limit exceeds the supported range")


def validate_rate_window(value: float) -> None:
    """Reject a limiter window outside the supported range."""

    if value <= 0:
        raise ValueError("rate window must be positive")
    if value > 3600:
        raise ValueError("rate window exceeds the supported range")


def validate_stage_capacity(value: float) -> None:
    """Reject a stage capacity outside the supported range."""

    if value <= 0:
        raise ValueError("stage capacity must be positive")
    if value > 1000:
        raise ValueError("stage capacity exceeds the supported range")


def validate_efficiency(value: float) -> None:
    """Reject a unit efficiency rating outside the supported range."""

    if value <= 0:
        raise ValueError("efficiency must be positive")
    if value > 100:
        raise ValueError("efficiency exceeds the supported range")
