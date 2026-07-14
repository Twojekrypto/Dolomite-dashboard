from dataclasses import dataclass
from typing import Mapping, Sequence


WEEK_SECONDS = 7 * 24 * 60 * 60
DAY_SECONDS = 24 * 60 * 60
WEI_DECIMALS = 18
VEDOLO_CONTRACT = "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4"


@dataclass(frozen=True)
class GlobalPoint:
    bias: int
    slope: int
    timestamp: int
    block: int


@dataclass(frozen=True)
class CanonicalSnapshot:
    block_number: int
    timestamp: int
    total_supply_wei: int
    locked_supply_wei: int
    epoch: int


def decode_signed_word(word: str) -> int:
    if (
        not isinstance(word, str)
        or not word.startswith("0x")
        or len(word) != 66
        or any(character not in "0123456789abcdefABCDEF" for character in word[2:])
    ):
        raise ValueError(
            "response must be exactly one ABI word: a string starting with 0x "
            "followed by 64 hexadecimal characters"
        )
    raw = int(word[2:], 16)
    return raw - (1 << 256) if raw >= (1 << 255) else raw


def decode_global_point(result: str) -> GlobalPoint:
    if (
        not isinstance(result, str)
        or not result.startswith("0x")
        or len(result) != 2 + 4 * 64
        or any(character not in "0123456789abcdefABCDEF" for character in result[2:])
    ):
        raise ValueError(
            "global point response must be 0x followed by exactly 256 hexadecimal characters"
        )
    payload = result[2:]
    try:
        words = [payload[offset : offset + 64] for offset in range(0, len(payload), 64)]
        [int(word, 16) for word in words]
    except ValueError as exc:
        raise ValueError("global point response must be hexadecimal") from exc

    return GlobalPoint(
        bias=decode_signed_word("0x" + words[0]),
        slope=decode_signed_word("0x" + words[1]),
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


def wei_to_decimal(value_wei: int) -> str:
    if value_wei < 0:
        raise ValueError("wei value cannot be negative")
    whole, fraction = divmod(value_wei, 10 ** WEI_DECIMALS)
    if fraction == 0:
        return str(whole)
    return f"{whole}.{fraction:018d}".rstrip("0")


def build_vote_power_payload(
    snapshot: CanonicalSnapshot,
    points: Sequence[GlobalPoint],
    slope_changes: Mapping[int, int],
    day_seconds: int = DAY_SECONDS,
) -> dict:
    if not points:
        raise ValueError("global point history cannot be empty")
    if day_seconds <= 0:
        raise ValueError("day_seconds must be positive")

    week_seconds = WEEK_SECONDS if day_seconds == DAY_SECONDS else day_seconds

    first_observation = min(point.timestamp for point in points)
    if first_observation > snapshot.timestamp:
        raise ValueError("global point history starts after the target timestamp")

    observation_timestamps = [first_observation]
    next_midnight = ((first_observation // day_seconds) + 1) * day_seconds
    while next_midnight < snapshot.timestamp:
        observation_timestamps.append(next_midnight)
        next_midnight += day_seconds
    if observation_timestamps[-1] != snapshot.timestamp:
        observation_timestamps.append(snapshot.timestamp)

    observations = [
        [
            timestamp,
            wei_to_decimal(
                evaluate_vote_power_at(
                    timestamp, points, slope_changes, week_seconds=week_seconds
                )
            ),
        ]
        for timestamp in observation_timestamps
    ]
    last_value_wei = evaluate_vote_power_at(
        snapshot.timestamp, points, slope_changes, week_seconds=week_seconds
    )
    if last_value_wei != snapshot.total_supply_wei:
        raise ValueError(
            f"vote power {last_value_wei} does not match totalSupply "
            f"{snapshot.total_supply_wei}"
        )

    return {
        "schemaVersion": 1,
        "metric": "votePower",
        "chain": "berachain",
        "contract": VEDOLO_CONTRACT,
        "source": "global-point-history",
        "targetBlock": snapshot.block_number,
        "targetTimestamp": snapshot.timestamp,
        "totalSupplyWei": str(snapshot.total_supply_wei),
        "lockedSupplyWei": str(snapshot.locked_supply_wei),
        "lastPointWei": str(last_value_wei),
        "coverage": {
            "from": first_observation,
            "through": snapshot.timestamp,
        },
        "points": observations,
    }
