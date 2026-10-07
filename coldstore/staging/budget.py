"""Budget allocation when there are not enough stage steps for every room.

The two policies are deliberately explicit so an operator can pick one and
have the choice reproduced in the batch record:

* ``max_cooling`` satisfies the biggest load in full first, descending, so a
  single hot room reaches its target before smaller demands are touched.
* ``mean_queue`` aims for the current mean load: every room climbs towards a
  fair share, queue order first, rather than one room taking the whole budget.
"""

from __future__ import annotations

from .report import POLICY_MAX_COOLING, POLICY_MEAN_QUEUE


def fair_targets(wanted: dict[str, int]) -> dict[str, int]:
    """Stages each room needs to reach the current mean demand.

    The mean is taken over stage demand (zero demand rooms included). A room
    asking for less than the mean only needs its own demand; a room asking for
    more needs the mean and must queue again later for the remainder.
    """

    if not wanted:
        return {}
    mean = sum(wanted.values()) / len(wanted)
    return {room_id: min(stages, max(1, int(mean + 0.9999))) for room_id, stages in wanted.items()}


def allocate(
    wanted: dict[str, int],
    budget: int,
    policy: str,
    room_order: list[str],
) -> dict[str, int]:
    """Grant stage steps out of ``budget`` under the chosen policy.

    ``room_order`` is the priority queue used by mean queue and is also used to
    break ties for max cooling so both policies are deterministic.
    """

    grants = {room_id: 0 for room_id in wanted}
    remaining = max(0, budget)

    if policy == POLICY_MEAN_QUEUE:
        caps = fair_targets(wanted)
        for room_id in room_order:
            if remaining <= 0:
                break
            step = min(caps.get(room_id, 0), wanted.get(room_id, 0), remaining)
            grants[room_id] = step
            remaining -= step
        # Any budget left after everyone reaches the mean funds another fair
        # round, again in queue order.
        while remaining > 0:
            progressed = False
            for room_id in room_order:
                if remaining <= 0:
                    break
                if grants[room_id] < wanted.get(room_id, 0):
                    grants[room_id] += 1
                    remaining -= 1
                    progressed = True
            if not progressed:
                break
        return grants

    if policy == POLICY_MAX_COOLING:
        rank = {room_id: index for index, room_id in enumerate(room_order)}
        ordered = sorted(wanted, key=lambda room_id: (-wanted[room_id], rank.get(room_id, 0)))
        for room_id in ordered:
            if remaining <= 0:
                break
            step = min(wanted[room_id], remaining)
            grants[room_id] = step
            remaining -= step
        return grants

    raise ValueError(f"unknown budget policy {policy!r}")
