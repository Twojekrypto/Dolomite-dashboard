from dataclasses import dataclass
from typing import Mapping, Sequence


WEEK_SECONDS = 7 * 24 * 60 * 60


@dataclass(frozen=True)
class GlobalPoint:
    bias: int
    slope: int
    timestamp: int
    block: int


def decode_signed_word(word: str) -> int:
    raw = int(word.removeprefix("0x"), 16)
    return raw - (1 << 256) if raw >= (1 << 255) else raw


def decode_global_point(result: str) -> GlobalPoint:
    payload = result.removeprefix("0x")
    if len(payload) != 4 * 64:
        raise ValueError("global point response must contain exactly four 32-byte words")
    try:
        words = [payload[offset : offset + 64] for offset in range(0, len(payload), 64)]
        [int(word, 16) for word in words]
    except ValueError as exc:
        raise ValueError("global point response must be hexadecimal") from exc

    return GlobalPoint(
        bias=decode_signed_word(words[0]),
        slope=decode_signed_word(words[1]),
        timestamp=int(words[2], 16),
        block=int(words[3], 16),
    )


def evaluate_vote_power_at(
    observation_ts: int,
    points: Sequence[GlobalPoint],
    slope_changes: Mapping[int, int],
    week_seconds: int = WEEK_SECONDS,
) -> int:
    anchor = max(
        (point for point in points if point.timestamp <= observation_ts),
        key=lambda point: (point.timestamp, point.block),
    )
    bias, slope, last_ts = anchor.bias, anchor.slope, anchor.timestamp
    boundary = (last_ts // week_seconds) * week_seconds
    while last_ts < observation_ts:
        boundary = min(boundary + week_seconds, observation_ts)
        bias -= slope * (boundary - last_ts)
        if boundary == observation_ts:
            break
        slope += slope_changes.get(boundary, 0)
        last_ts = boundary
    return max(0, bias)
