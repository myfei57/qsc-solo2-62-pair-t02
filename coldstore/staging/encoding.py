"""Stage command encoding for modern and four gear legacy machines.

Modern compressors may expose up to eight stages. Legacy machines only
understand four gears (off/low/medium/high) carried as two bit codes, so a
native target stage is projected onto the closest gear below it.
"""

from __future__ import annotations

from .report import LEGACY_GEARS, StageCode

LEGACY_STAGES = 4


def gear_for_stage(native_stage: int, stages: int) -> int:
    """Map a native stage onto the closest legacy gear at or below it.

    A machine that natively has at most four gears uses its stage directly;
    a finer grained machine is quantized so every switch remains expressible
    as one of the four legacy commands.
    """

    if stages <= 0:
        return 0
    bounded = max(0, min(native_stage, stages))
    if stages <= LEGACY_STAGES:
        return min(bounded, LEGACY_STAGES - 1)
    if bounded <= 0:
        return 0
    if bounded >= stages:
        return LEGACY_STAGES - 1
    # Spread stages 1..stages-1 across gears 1..3, rounding down so a
    # command never asks a legacy machine for more capacity than planned.
    return min(LEGACY_STAGES - 1, (bounded * LEGACY_STAGES) // stages)


def encode_stage(native_stage: int, stages: int) -> StageCode:
    """Render a target stage both natively and as a four gear legacy code."""

    gear = gear_for_stage(native_stage, stages)
    _, code, label = LEGACY_GEARS[gear]
    return StageCode(
        native_stage=native_stage,
        stages=stages,
        gear=gear,
        code=code,
        label=label,
    )


def decode_code(code: str) -> int:
    """Recover the gear number from a two bit legacy command."""

    for gear, bits, _label in LEGACY_GEARS:
        if bits == code:
            return gear
    raise ValueError(f"unknown legacy stage code {code!r}")
