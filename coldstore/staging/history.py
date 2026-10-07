"""Append only scheduling batches and versioned loading plans.

Every staged loading batch is persisted once under ``staging:batch:<n>`` and
its machine order and allocation under a versioned ``staging:version:<n>``.
The documents are immutable, so the exact plan that moved a machine can be
reconstructed after the fact. An index key tracks every published batch.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass

from coldstore.store.commands import append_command, load_commands
from coldstore.store.store import Store

BATCH_INDEX_KEY = "staging:batches"
VERSION_INDEX_KEY = "staging:versions"
BATCH_KEY = "staging:batch:{:06d}"
VERSION_KEY = "staging:version:{:06d}"
COUNTER_KEY = "staging:batch-seq"


@dataclass(frozen=True)
class BatchRecord:
    """One staged loading batch with the evidence behind every switch."""

    seq: int
    at: int
    policy: str
    order_policy: str
    budget: int
    used: int
    target_stages: int
    total_load: float
    per_stage: float
    machine_order: list[str]
    applied: bool
    reason: str
    plan: dict[str, object]
    requests: list[dict[str, object]]

    def as_dict(self) -> dict[str, object]:
        return {
            "seq": self.seq,
            "at": self.at,
            "policy": self.policy,
            "order_policy": self.order_policy,
            "budget": self.budget,
            "used": self.used,
            "target_stages": self.target_stages,
            "total_load": self.total_load,
            "per_stage": self.per_stage,
            "machine_order": list(self.machine_order),
            "applied": self.applied,
            "reason": self.reason,
            "plan": self.plan,
            "requests": list(self.requests),
        }


class BatchArchive:
    """Persists batches and the versioned plan behind each one."""

    def __init__(self, store: Store) -> None:
        self._store = store

    def _next_seq(self) -> int:
        raw, present = self._store.get(COUNTER_KEY)
        try:
            current = int(raw) if present else 0
        except ValueError:
            current = 0
        current += 1
        self._store.put(COUNTER_KEY, str(current))
        return current

    def publish(
        self,
        plan: dict[str, object],
        machine_order: list[str],
        *,
        applied: bool,
        reason: str,
        requests: list[dict[str, object]] | None = None,
        seq: int | None = None,
    ) -> BatchRecord:
        """Persist a batch and its versioned plan and return the record."""

        number = seq or self._next_seq()
        record = BatchRecord(
            seq=number,
            at=int(time.time()),
            policy=str(plan.get("policy", "")),
            order_policy=str(plan.get("order_policy", "")),
            budget=int(plan.get("budget", 0)),
            used=int(plan.get("used", 0)),
            target_stages=int(plan.get("target_stages", 0)),
            total_load=float(plan.get("total_load", 0.0)),
            per_stage=float(plan.get("per_stage", 0.0)),
            machine_order=list(machine_order),
            applied=applied,
            reason=reason,
            plan=plan,
            requests=list(requests or []),
        )
        payload = json.dumps(record.as_dict(), ensure_ascii=False, sort_keys=True)
        self._store.put(BATCH_KEY.format(number), payload)
        append_command(self._store, BATCH_INDEX_KEY, BATCH_KEY.format(number))
        self._store.put(
            VERSION_KEY.format(number),
            json.dumps(
                {
                    "version": number,
                    "at": record.at,
                    "machine_order": machine_order,
                    "allocations": plan.get("allocations", []),
                    "deferred": plan.get("deferred", []),
                    "basis": "versioned loading plan",
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
        )
        append_command(self._store, VERSION_INDEX_KEY, VERSION_KEY.format(number))
        return record

    def batch(self, number: int) -> BatchRecord | None:
        raw, present = self._store.get(BATCH_KEY.format(number))
        if not present:
            return None
        try:
            payload = json.loads(raw)
        except ValueError:
            return None
        if not isinstance(payload, dict):
            return None
        return _record_from(payload)

    def version(self, number: int) -> dict[str, object] | None:
        raw, present = self._store.get(VERSION_KEY.format(number))
        if not present:
            return None
        try:
            payload = json.loads(raw)
        except ValueError:
            return None
        return payload if isinstance(payload, dict) else None

    def batches(self) -> list[BatchRecord]:
        records: list[BatchRecord] = []
        for key in load_commands(self._store, BATCH_INDEX_KEY):
            raw, present = self._store.get(key)
            if not present:
                continue
            try:
                payload = json.loads(raw)
            except ValueError:
                continue
            if isinstance(payload, dict):
                record = _record_from(payload)
                if record is not None:
                    records.append(record)
        records.sort(key=lambda record: record.seq)
        return records

    def latest(self) -> BatchRecord | None:
        records = self.batches()
        return records[-1] if records else None

    def versions(self) -> list[dict[str, object]]:
        documents: list[dict[str, object]] = []
        for key in load_commands(self._store, VERSION_INDEX_KEY):
            document = self.version_key(key)
            if document is not None:
                documents.append(document)
        return documents

    def version_key(self, key: str) -> dict[str, object] | None:
        raw, present = self._store.get(key)
        if not present:
            return None
        try:
            payload = json.loads(raw)
        except ValueError:
            return None
        return payload if isinstance(payload, dict) else None

    def count(self) -> int:
        return len(load_commands(self._store, BATCH_INDEX_KEY))


def _record_from(payload: dict[str, object]) -> BatchRecord | None:
    plan = payload.get("plan", {})
    if not isinstance(plan, dict):
        plan = {}
    requests = payload.get("requests", [])
    if not isinstance(requests, list):
        requests = []
    order = payload.get("machine_order", [])
    if not isinstance(order, list):
        order = []
    try:
        return BatchRecord(
            seq=int(payload.get("seq", 0)),
            at=int(payload.get("at", 0)),
            policy=str(payload.get("policy", "")),
            order_policy=str(payload.get("order_policy", "")),
            budget=int(payload.get("budget", 0)),
            used=int(payload.get("used", 0)),
            target_stages=int(payload.get("target_stages", 0)),
            total_load=float(payload.get("total_load", 0.0)),
            per_stage=float(payload.get("per_stage", 0.0)),
            machine_order=[str(item) for item in order],
            applied=bool(payload.get("applied", False)),
            reason=str(payload.get("reason", "")),
            plan=plan,
            requests=[item for item in requests if isinstance(item, dict)],
        )
    except (TypeError, ValueError):
        return None
