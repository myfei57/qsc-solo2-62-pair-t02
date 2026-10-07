"""Console endpoints for hierarchical staged loading."""

from __future__ import annotations

from coldstore.store import append_event

from .http import Request, RequestError, Response, json_response
from .runtime import Runtime

EVENT_KEY = "staging:events"


def _scheduler(rt: Runtime):
    return rt.staging


def staging_state(rt: Runtime, request: Request) -> Response:
    scheduler = _scheduler(rt)
    budget = request.int_field("budget", 0)
    room_ids = _room_ids(request)
    plan = scheduler.plan(room_ids=room_ids, budget=budget)
    latest = scheduler.archive().latest()
    return json_response(
        {
            "plan": plan.as_dict(),
            "gate": scheduler.gate.state().as_dict(),
            "policy": scheduler.policy(),
            "order_policy": scheduler.order_policy(),
            "per_stage": scheduler.per_stage(),
            "cap_total": scheduler.cap_total(),
            "cap_comp": scheduler.cap_comp(),
            "throttle": {"rate": scheduler.rate(), "burst": scheduler.burst()},
            "last_batch": None if latest is None else latest.as_dict()["seq"],
            "batch_count": scheduler.archive().count(),
        }
    )


def staging_plan(rt: Runtime, request: Request) -> Response:
    scheduler = _scheduler(rt)
    plan = scheduler.plan(room_ids=_room_ids(request), budget=request.int_field("budget", 0))
    return json_response(plan.as_dict())


def staging_publish(rt: Runtime, request: Request) -> Response:
    scheduler = _scheduler(rt)
    record = scheduler.publish_plan(
        budget=request.int_field("budget", 0),
        reason=request.str_field("reason", "monitor"),
        room_ids=_room_ids(request),
    )
    return json_response({"batch": record.as_dict()})


def staging_apply(rt: Runtime, request: Request) -> Response:
    scheduler = _scheduler(rt)
    room_ids = _room_ids(request)
    record = scheduler.apply(
        budget=request.int_field("budget", 0),
        reason=request.str_field("reason", "manual"),
        room_ids=room_ids,
    )
    return json_response({"batch": record.as_dict()})


def staging_request(rt: Runtime, request: Request) -> Response:
    scheduler = _scheduler(rt)
    room_id = request.str_field("room")
    if not room_id:
        raise RequestError(400, "room is required")
    if scheduler.rooms.room(room_id) is None:
        raise RequestError(400, f"cold room {room_id} not found")
    priority = request.int_field("priority", -1)
    try:
        ticket = scheduler.submit_request(room_id, None if priority < 0 else priority)
    except ValueError as exc:
        raise RequestError(429, str(exc)) from exc
    append_event(rt.store, EVENT_KEY, "request", f"{room_id} priority={ticket.priority}")
    return json_response(
        {"ticket": ticket.id, "room": ticket.room_id, "priority": ticket.priority, "queued": True}
    )


def staging_queue(rt: Runtime, request: Request) -> Response:
    return json_response(_scheduler(rt).gate.state().as_dict())


def staging_admit(rt: Runtime, request: Request) -> Response:
    """Admit queued requests by priority under the token limit, then load."""

    scheduler = _scheduler(rt)
    limit = request.int_field("limit", 1)
    if limit <= 0:
        limit = 1
    rooms: list[str] = []
    tickets: list[dict[str, object]] = []
    for _ in range(limit):
        ticket = scheduler.gate.admit(block=False)
        if ticket is None:
            break
        rooms.append(ticket.room_id)
        tickets.append(
            {"id": ticket.id, "room": ticket.room_id, "priority": ticket.priority}
        )
    if not rooms:
        return json_response(
            {"applied": False, "queued": True, "gate": scheduler.gate.state().as_dict()},
            status=202,
        )
    record = scheduler.apply(
        budget=request.int_field("budget", 0),
        reason=request.str_field("reason", "priority-queue"),
        room_ids=rooms,
        requests=tickets,
    )
    return json_response({"applied": True, "admitted": tickets, "batch": record.as_dict()})


def staging_queue_clear(rt: Runtime, request: Request) -> Response:
    return json_response({"dropped": _scheduler(rt).gate.clear()})


def staging_tokens(rt: Runtime, request: Request) -> Response:
    tokens = request.float_field("tokens")
    return json_response({"tokens": _scheduler(rt).gate.top_up(tokens)})


def staging_machines(rt: Runtime, request: Request) -> Response:
    scheduler = _scheduler(rt)
    bank = rt.compressors
    return json_response(
        {
            "order_policy": scheduler.order_policy(),
            "order": scheduler.ranked_machines(),
            "machines": [
                {
                    "id": comp_id,
                    "stage": bank.stage(comp_id)[0],
                    "stages": bank.compressor(comp_id).stages if bank.compressor(comp_id) else 0,
                    "efficiency": scheduler.efficiency(comp_id),
                    "reserved_by": bank.reserved_by(comp_id),
                }
                for comp_id in scheduler.ranked_machines()
            ],
        }
    )


def staging_set_policy(rt: Runtime, request: Request) -> Response:
    value = request.str_field("policy")
    try:
        chosen = _scheduler(rt).set_policy(value)
    except ValueError as exc:
        raise RequestError(400, str(exc)) from exc
    return json_response({"policy": chosen})


def staging_set_order(rt: Runtime, request: Request) -> Response:
    value = request.str_field("policy")
    try:
        chosen = _scheduler(rt).set_order_policy(value)
    except ValueError as exc:
        raise RequestError(400, str(exc)) from exc
    return json_response({"order_policy": chosen})


def staging_per_stage(rt: Runtime, request: Request) -> Response:
    scheduler = _scheduler(rt)
    try:
        value = scheduler.set_per_stage(request.float_field("value"))
    except ValueError as exc:
        raise RequestError(400, str(exc)) from exc
    return json_response({"per_stage": value})


def staging_cap(rt: Runtime, request: Request) -> Response:
    scheduler = _scheduler(rt)
    total = request.int_field("total", scheduler.cap_total())
    comp = request.int_field("comp", scheduler.cap_comp())
    try:
        scheduler.set_cap_total(total)
        scheduler.set_cap_comp(comp)
    except ValueError as exc:
        raise RequestError(400, str(exc)) from exc
    return json_response({"cap_total": total, "cap_comp": comp})


def staging_throttle(rt: Runtime, request: Request) -> Response:
    scheduler = _scheduler(rt)
    try:
        rate, burst = scheduler.set_throttle(
            request.float_field("rate", scheduler.rate()),
            request.int_field("burst", scheduler.burst()),
        )
    except ValueError as exc:
        raise RequestError(400, str(exc)) from exc
    return json_response({"rate": rate, "burst": burst})


def staging_efficiency(rt: Runtime, request: Request) -> Response:
    scheduler = _scheduler(rt)
    comp_id = request.str_field("id")
    try:
        value = scheduler.set_efficiency(comp_id, request.float_field("value"))
    except ValueError as exc:
        raise RequestError(400, str(exc)) from exc
    return json_response({"id": comp_id, "efficiency": value})


def staging_priority(rt: Runtime, request: Request) -> Response:
    scheduler = _scheduler(rt)
    room_id = request.str_field("room")
    try:
        value = scheduler.set_priority(room_id, request.int_field("value"))
    except ValueError as exc:
        raise RequestError(400, str(exc)) from exc
    return json_response({"room": room_id, "priority": value})


def staging_batches(rt: Runtime, request: Request) -> Response:
    archive = _scheduler(rt).archive()
    records = archive.batches()
    return json_response(
        {"count": len(records), "batches": [record.as_dict() for record in records]}
    )


def staging_batch(rt: Runtime, request: Request) -> Response:
    seq = request.int_field("seq", 0)
    record = _scheduler(rt).archive().batch(seq)
    if record is None:
        raise RequestError(404, f"batch {seq} not found")
    return json_response({"batch": record.as_dict()})


def staging_versions(rt: Runtime, request: Request) -> Response:
    archive = _scheduler(rt).archive()
    return json_response({"count": archive.count(), "versions": archive.versions()})


def staging_version(rt: Runtime, request: Request) -> Response:
    seq = request.int_field("version", request.int_field("seq", 0))
    document = _scheduler(rt).archive().version(seq)
    if document is None:
        raise RequestError(404, f"version {seq} not found")
    return json_response(document)


def _room_ids(request: Request) -> list[str] | None:
    value = request.payload.get("rooms")
    if value is None:
        single = request.str_field("room")
        return [single] if single else None
    if not isinstance(value, list):
        raise RequestError(400, "rooms must be a list")
    rooms = [str(item) for item in value]
    return rooms or None
