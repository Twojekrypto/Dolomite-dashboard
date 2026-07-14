#!/usr/bin/env python3
"""Generate the canonical veDOLO global vote-power history."""

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from rpc_client import get_endpoints, rpc_batch_requests, rpc_single_request
from vedolo_vote_power import (
    WEEK_SECONDS,
    CanonicalSnapshot,
    GlobalPoint,
    VEDOLO_CONTRACT,
    build_vote_power_payload,
    decode_global_point,
    decode_signed_word,
)


EPOCH_SELECTOR = "0x900cf0cf"
SUPPLY_SELECTOR = "0x047fc9aa"
TOTAL_SUPPLY_SELECTOR = "0x18160ddd"
POINT_HISTORY_SELECTOR = "0xd1febfb9"
SLOPE_CHANGES_SELECTOR = "0x71197484"

PUBLIC_OUTPUT_PATH = Path("data/vedolo-vote-power-history.json")
STATE_PATH = Path("vedolo_vote_power_history_state.json")
STATS_PATH = Path("vedolo_stats.json")
HOLDERS_PATH = Path("vedolo_holders.json")
CACHE_SCHEMA_VERSION = 1
ABI_WORD_ERROR = (
    "response must be exactly one ABI word: a string starting with 0x followed by "
    "64 hexadecimal characters"
)


def _word(value: int) -> str:
    if value < 0:
        raise ValueError("contract argument cannot be negative")
    return f"{value:064x}"


def _decode_uint(result: str, description: str) -> int:
    payload = str(result or "").removeprefix("0x")
    if not payload:
        raise RuntimeError(f"Pinned veDOLO {description} response was incomplete")
    try:
        return int(payload, 16)
    except ValueError as exc:
        raise RuntimeError(f"Pinned veDOLO {description} response was not hexadecimal") from exc


def _require_abi_word(result: object) -> str:
    if (
        not isinstance(result, str)
        or not result.startswith("0x")
        or len(result) != 66
        or any(character not in "0123456789abcdefABCDEF" for character in result[2:])
    ):
        raise RuntimeError(ABI_WORD_ERROR)
    return result


def call_at_block(data: str, block_tag: str) -> str:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{"to": VEDOLO_CONTRACT, "data": data}, block_tag],
    }
    response = rpc_single_request(
        get_endpoints("berachain"),
        payload,
        describe="veDOLO vote power",
    )
    if response.get("error") or not response.get("result"):
        raise RuntimeError("Pinned veDOLO eth_call failed")
    return _require_abi_word(response["result"])


def fetch_canonical_snapshot() -> CanonicalSnapshot:
    endpoints = get_endpoints("berachain")
    latest = rpc_single_request(
        endpoints,
        {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
        describe="veDOLO target block",
    )
    block_number = _decode_uint(latest.get("result"), "target block")
    block_tag = hex(block_number)
    block = rpc_single_request(
        endpoints,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": [block_tag, False],
        },
        describe="veDOLO target block timestamp",
    )
    if block.get("error") or not isinstance(block.get("result"), dict):
        raise RuntimeError("Pinned veDOLO block read failed")

    timestamp = _decode_uint(block["result"].get("timestamp"), "target timestamp")
    return CanonicalSnapshot(
        block_number=block_number,
        timestamp=timestamp,
        epoch=_decode_uint(call_at_block(EPOCH_SELECTOR, block_tag), "epoch"),
        locked_supply_wei=_decode_uint(call_at_block(SUPPLY_SELECTOR, block_tag), "supply"),
        total_supply_wei=_decode_uint(
            call_at_block(TOTAL_SUPPLY_SELECTOR, block_tag), "totalSupply"
        ),
    )


def _read_cached_points(state_path: Path) -> Optional[Tuple[int, List[GlobalPoint]]]:
    try:
        with state_path.open() as file:
            state = json.load(file)
        epoch = state["epoch"]
        encoded_points = state["points"]
        if (
            state.get("schemaVersion") != CACHE_SCHEMA_VERSION
            or not isinstance(epoch, int)
            or epoch < 0
            or not isinstance(encoded_points, list)
            or len(encoded_points) != epoch + 1
            or not all(isinstance(point, str) for point in encoded_points)
        ):
            return None
        points = [decode_global_point(point) for point in encoded_points]
        _validate_global_points(points, epoch)
        return epoch, points
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _validate_global_points(points: List[GlobalPoint], epoch: int) -> None:
    if len(points) != epoch + 1:
        raise ValueError("global point indexes must be contiguous through the epoch")

    previous = None
    for point in points:
        if point.bias < 0 or point.slope < 0:
            raise ValueError("global point bias and slope must be nonnegative")
        if previous and point.timestamp < previous.timestamp:
            raise ValueError("global point timestamps must be nondecreasing")
        previous = point


def _atomic_write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as temp_file:
            temp_name = temp_file.name
            json.dump(value, temp_file, indent=2, sort_keys=True)
            temp_file.write("\n")
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_name, path)
    finally:
        if temp_name and os.path.exists(temp_name):
            os.unlink(temp_name)


def apply_canonical_vote_weight(stats: dict, snapshot: CanonicalSnapshot) -> dict:
    holder_sum = stats.get("total_vote_weight_holder_sum", stats.get("total_vote_weight", 0))
    stats["total_vote_weight_holder_sum"] = round(float(holder_sum), 4)
    stats["total_vote_weight"] = round(snapshot.total_supply_wei / 10**18, 4)
    stats["total_vote_weight_source"] = "contract_totalSupply"
    stats["total_vote_weight_block"] = snapshot.block_number
    stats["total_vote_weight_timestamp"] = snapshot.timestamp
    return stats


def sync_vote_weight_stats(
    snapshot: CanonicalSnapshot,
    stats_path: Path = STATS_PATH,
    holders_path: Path = HOLDERS_PATH,
) -> None:
    """Apply one canonical snapshot to both published veDOLO stats payloads."""
    for path in (Path(stats_path), Path(holders_path)):
        try:
            with path.open(encoding="utf-8") as file:
                payload = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Could not load veDOLO stats for canonical sync: {path}") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("stats"), dict):
            raise RuntimeError(f"veDOLO stats payload is missing stats object: {path}")

        apply_canonical_vote_weight(payload["stats"], snapshot)
        _atomic_write_json(path, payload)


def _fetch_global_point_results(indices: Iterable[int], block_tag: str) -> list[str]:
    indices = list(indices)
    if not indices:
        return []
    payloads = [
        {
            "jsonrpc": "2.0",
            "id": f"point:{index}",
            "method": "eth_call",
            "params": [
                {"to": VEDOLO_CONTRACT, "data": POINT_HISTORY_SELECTOR + _word(index)},
                block_tag,
            ],
        }
        for index in indices
    ]
    responses, missing_ids = rpc_batch_requests(
        get_endpoints("berachain"),
        payloads,
        describe="veDOLO global point history",
    )
    if missing_ids:
        raise RuntimeError("Pinned veDOLO global point batch was incomplete")

    results = []
    for index in indices:
        response = responses.get(f"point:{index}")
        if not isinstance(response, dict) or response.get("error") or not response.get("result"):
            raise RuntimeError("Pinned veDOLO global point read failed")
        results.append(response["result"])
    return results


def fetch_global_points(
    snapshot: CanonicalSnapshot, state_path: Path = STATE_PATH
) -> list[GlobalPoint]:
    state_path = Path(state_path)
    cached = _read_cached_points(state_path)
    if cached and cached[0] <= snapshot.epoch:
        cached_epoch, points = cached
        start_index = cached_epoch + 1
    else:
        points = []
        start_index = 0

    block_tag = hex(snapshot.block_number)
    encoded_points = _fetch_global_point_results(
        range(start_index, snapshot.epoch + 1), block_tag
    )
    points.extend(decode_global_point(result) for result in encoded_points)
    try:
        _validate_global_points(points, snapshot.epoch)
    except ValueError as exc:
        raise RuntimeError(f"Pinned veDOLO point history was not canonical: {exc}") from exc

    _atomic_write_json(
        state_path,
        {
            "schemaVersion": CACHE_SCHEMA_VERSION,
            "epoch": snapshot.epoch,
            "points": [
                "0x"
                + "".join(
                    f"{value & ((1 << 256) - 1):064x}"
                    for value in (point.bias, point.slope, point.timestamp, point.block)
                )
                for point in points
            ],
        },
    )
    return points


def fetch_slope_changes(
    points: list[GlobalPoint], snapshot: CanonicalSnapshot
) -> dict[int, int]:
    if not points:
        raise ValueError("global point history cannot be empty")
    first_timestamp = min(point.timestamp for point in points)
    first_week = ((first_timestamp // WEEK_SECONDS) + 1) * WEEK_SECONDS
    timestamps = list(range(first_week, snapshot.timestamp + 1, WEEK_SECONDS))
    if not timestamps:
        return {}

    block_tag = hex(snapshot.block_number)
    payloads = [
        {
            "jsonrpc": "2.0",
            "id": f"slope:{timestamp}",
            "method": "eth_call",
            "params": [
                {
                    "to": VEDOLO_CONTRACT,
                    "data": SLOPE_CHANGES_SELECTOR + _word(timestamp),
                },
                block_tag,
            ],
        }
        for timestamp in timestamps
    ]
    responses, missing_ids = rpc_batch_requests(
        get_endpoints("berachain"),
        payloads,
        describe="veDOLO slope changes",
    )
    if missing_ids:
        raise RuntimeError("Pinned veDOLO slope-change batch was incomplete")

    slope_changes = {}
    for timestamp in timestamps:
        response = responses.get(f"slope:{timestamp}")
        result = response.get("result") if isinstance(response, dict) else None
        if not result or response.get("error"):
            raise RuntimeError("Pinned veDOLO slope-change read failed")
        slope_changes[timestamp] = decode_signed_word(result)
    return slope_changes


def write_vote_power_history(
    output_path: Path = PUBLIC_OUTPUT_PATH,
    state_path: Path = STATE_PATH,
    *,
    sync_stats: bool = False,
    stats_path: Path = STATS_PATH,
    holders_path: Path = HOLDERS_PATH,
) -> dict:
    snapshot = fetch_canonical_snapshot()
    points = fetch_global_points(snapshot, state_path)
    slope_changes = fetch_slope_changes(points, snapshot)
    payload = build_vote_power_payload(snapshot, points, slope_changes)
    _atomic_write_json(Path(output_path), payload)
    if sync_stats:
        sync_vote_weight_stats(snapshot, stats_path, holders_path)
    return payload


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sync-stats",
        action="store_true",
        help="synchronize veDOLO stats files with the history snapshot",
    )
    args = parser.parse_args()
    write_vote_power_history(sync_stats=args.sync_stats)
