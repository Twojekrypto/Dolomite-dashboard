#!/usr/bin/env python3
"""Build the veDOLO early-unlock simulation from one pinned Berachain block."""

import json
import os
from datetime import datetime, timezone
from decimal import Decimal

from eth_abi import decode
from web3 import Web3

from rpc_client import RpcClient


VEDOLO_CONTRACT = "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4"
MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"
WAD = 10**18
WEEK = 7 * 24 * 60 * 60
DEFAULT_BATCH_SIZE = 200
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(DATA_DIR, "vedolo_early_unlock.json")

VEDOLO_ABI = [
    {
        "inputs": [],
        "name": "feeCalculator",
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "tokenId",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "tokenId", "type": "uint256"}],
        "name": "ownerOf",
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "tokenId", "type": "uint256"}],
        "name": "locked",
        "outputs": [
            {"name": "amount", "type": "int128"},
            {"name": "end", "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "tokenId", "type": "uint256"}],
        "name": "balanceOfNFT",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

FEE_CALCULATOR_ABI = [
    {
        "inputs": [
            {"name": "amount", "type": "uint256"},
            {"name": "lockEnd", "type": "uint256"},
        ],
        "name": "getEarlyWithdrawalFees",
        "outputs": [
            {"name": "burnFee", "type": "uint256"},
            {"name": "recoupFee", "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]

MULTICALL3_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"name": "target", "type": "address"},
                    {"name": "allowFailure", "type": "bool"},
                    {"name": "callData", "type": "bytes"},
                ],
                "name": "calls",
                "type": "tuple[]",
            }
        ],
        "name": "aggregate3",
        "outputs": [
            {
                "components": [
                    {"name": "success", "type": "bool"},
                    {"name": "returnData", "type": "bytes"},
                ],
                "name": "returnData",
                "type": "tuple[]",
            }
        ],
        "stateMutability": "payable",
        "type": "function",
    }
]


class SimulationDataError(RuntimeError):
    """Raised when a complete, trustworthy simulation cannot be produced."""


def build_current_holders(token_ids, owners):
    """Group token IDs by their owner at the pinned source block."""
    zero_address = "0x" + "0" * 40
    by_owner = {}
    for token_id in token_ids:
        owner = str(owners.get(int(token_id)) or "")
        if not owner or owner.lower() == zero_address:
            continue
        by_owner.setdefault(owner.lower(), {"address": owner, "token_ids": []})["token_ids"].append(int(token_id))
    holders = sorted(by_owner.values(), key=lambda holder: holder["address"].lower())
    for holder in holders:
        holder["token_ids"].sort()
    return holders


def _token_decimal(value_wei):
    value = Decimal(value_wei) / Decimal(WAD)
    return format(value.quantize(Decimal("0.000000000000000001")), "f").rstrip("0").rstrip(".") or "0"


def _position_payload(token_id, position, quote, snapshot_timestamp):
    amount_wei = int(position["amount_wei"])
    vote_weight_wei = int(position["vote_weight_wei"])
    end = int(position["end"])
    burn_fee_wei = int(quote["burn_fee_wei"])
    recoup_fee_wei = int(quote["recoup_fee_wei"])
    penalty_wei = burn_fee_wei + recoup_fee_wei
    if min(amount_wei, vote_weight_wei, burn_fee_wei, recoup_fee_wei) < 0:
        raise SimulationDataError(f"Negative onchain value for veDOLO position {token_id}")
    if penalty_wei > amount_wei:
        raise SimulationDataError(f"Fees exceed locked DOLO for veDOLO position {token_id}")

    remaining_seconds = max(end - snapshot_timestamp, 0)
    available_wei = amount_wei - penalty_wei
    penalty_pct = float(Decimal(penalty_wei) * 100 / Decimal(amount_wei)) if amount_wei else 0.0
    return {
        "id": int(token_id),
        "status": "early_exit" if remaining_seconds else "expired",
        "end": end,
        "remainingSeconds": remaining_seconds,
        "lockedDoloWei": str(amount_wei),
        "lockedDolo": _token_decimal(amount_wei),
        "veDoloWei": str(vote_weight_wei),
        "veDolo": _token_decimal(vote_weight_wei),
        "availableAfterExitWei": str(available_wei),
        "availableAfterExit": _token_decimal(available_wei),
        "burnFeeWei": str(burn_fee_wei),
        "recoupFeeWei": str(recoup_fee_wei),
        "penaltyWei": str(penalty_wei),
        "penalty": _token_decimal(penalty_wei),
        "penaltyPct": penalty_pct,
    }


def build_simulation_payload(
    holders,
    position_reads,
    fee_quotes,
    *,
    snapshot_timestamp,
    source_block,
    fee_calculator,
):
    """Aggregate exact position quotes into wallet-level simulation rows."""
    snapshot_timestamp = int(snapshot_timestamp)
    rows = []
    position_count = 0

    for holder in holders:
        positions = []
        for token_id_value in holder.get("token_ids", []):
            token_id = int(token_id_value)
            if token_id not in position_reads:
                raise SimulationDataError(f"Missing onchain position data for veDOLO position {token_id}")
            if token_id not in fee_quotes:
                raise SimulationDataError(f"Missing fee quote for veDOLO position {token_id}")
            position = _position_payload(
                token_id,
                position_reads[token_id],
                fee_quotes[token_id],
                snapshot_timestamp,
            )
            if int(position["lockedDoloWei"]) > 0:
                positions.append(position)

        if not any(position["status"] == "early_exit" for position in positions):
            continue

        positions.sort(key=lambda item: (item["status"] == "expired", item["end"], item["id"]))
        locked_wei = sum(int(position["lockedDoloWei"]) for position in positions)
        vote_weight_wei = sum(int(position["veDoloWei"]) for position in positions)
        available_wei = sum(int(position["availableAfterExitWei"]) for position in positions)
        penalty_wei = sum(int(position["penaltyWei"]) for position in positions)
        weighted_remaining = sum(
            int(position["lockedDoloWei"]) * int(position["remainingSeconds"])
            for position in positions
        )
        avg_weeks = float(Decimal(weighted_remaining) / Decimal(locked_wei * WEEK)) if locked_wei else 0.0
        penalty_pct = float(Decimal(penalty_wei) * 100 / Decimal(locked_wei)) if locked_wei else 0.0
        rows.append({
            "address": holder["address"],
            "rank": 0,
            "lockedDoloWei": str(locked_wei),
            "lockedDolo": _token_decimal(locked_wei),
            "veDoloWei": str(vote_weight_wei),
            "veDolo": _token_decimal(vote_weight_wei),
            "availableAfterExitWei": str(available_wei),
            "availableAfterExit": _token_decimal(available_wei),
            "penaltyWei": str(penalty_wei),
            "penalty": _token_decimal(penalty_wei),
            "penaltyPct": penalty_pct,
            "avgWeeksUntilUnlock": avg_weeks,
            "positions": positions,
        })
        position_count += len(positions)

    rows.sort(key=lambda item: (-int(item["availableAfterExitWei"]), item["address"].lower()))
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank

    source_dt = datetime.fromtimestamp(snapshot_timestamp, tz=timezone.utc)
    return {
        "schemaVersion": 1,
        "sourceBlock": int(source_block),
        "sourceTimestamp": source_dt.isoformat().replace("+00:00", "Z"),
        "generatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "feeCalculator": fee_calculator,
        "walletCount": len(rows),
        "positionCount": position_count,
        "wallets": rows,
    }


def _multicall(client, web3, calls, block_number, batch_size):
    multicall = web3.eth.contract(
        address=Web3.to_checksum_address(MULTICALL3_ADDRESS),
        abi=MULTICALL3_ABI,
    )
    output = []
    for start in range(0, len(calls), batch_size):
        chunk = calls[start:start + batch_size]
        call_data = multicall.functions.aggregate3(chunk)._encode_transaction_data()
        result_hex = client.call(
            "eth_call",
            [{"to": multicall.address, "data": call_data}, hex(block_number)],
        )
        if not isinstance(result_hex, str) or not result_hex.startswith("0x"):
            raise SimulationDataError("Multicall returned malformed data")
        (results,) = decode(["(bool,bytes)[]"], bytes.fromhex(result_hex[2:]))
        if len(results) != len(chunk):
            raise SimulationDataError("Multicall returned an incomplete result set")
        for offset, (success, return_data) in enumerate(results):
            if not success:
                raise SimulationDataError(f"Onchain read failed at call index {start + offset}")
            output.append(bytes(return_data))
    return output


def fetch_onchain_inputs():
    client = RpcClient(chain="berachain", timeout=30, retries_per_endpoint=2)
    web3 = Web3()
    block_number = int(client.call("eth_blockNumber", []), 16)
    block = client.call("eth_getBlockByNumber", [hex(block_number), False])
    if not isinstance(block, dict) or not block.get("timestamp"):
        raise SimulationDataError("Pinned Berachain block metadata is unavailable")
    snapshot_timestamp = int(block["timestamp"], 16)
    batch_size = max(1, int(os.environ.get("VEDOLO_EARLY_UNLOCK_BATCH_SIZE", DEFAULT_BATCH_SIZE)))
    vedolo = web3.eth.contract(address=Web3.to_checksum_address(VEDOLO_CONTRACT), abi=VEDOLO_ABI)
    fee_call_data = vedolo.functions.feeCalculator()._encode_transaction_data()
    fee_result = client.call(
        "eth_call",
        [{"to": vedolo.address, "data": fee_call_data}, hex(block_number)],
    )
    if not isinstance(fee_result, str) or not fee_result.startswith("0x"):
        raise SimulationDataError("veDOLO fee calculator address is unavailable")
    (fee_calculator,) = decode(["address"], bytes.fromhex(fee_result[2:]))
    calculator = web3.eth.contract(
        address=Web3.to_checksum_address(fee_calculator),
        abi=FEE_CALCULATOR_ABI,
    )

    token_id_call_data = vedolo.functions.tokenId()._encode_transaction_data()
    token_id_result = client.call(
        "eth_call",
        [{"to": vedolo.address, "data": token_id_call_data}, hex(block_number)],
    )
    if not isinstance(token_id_result, str) or not token_id_result.startswith("0x"):
        raise SimulationDataError("veDOLO token ID counter is unavailable")
    (maximum_token_id,) = decode(["uint256"], bytes.fromhex(token_id_result[2:]))
    token_ids = list(range(1, int(maximum_token_id) + 1))
    position_calls = []
    for token_id in token_ids:
        position_calls.extend([
            (vedolo.address, False, bytes.fromhex(vedolo.functions.ownerOf(token_id)._encode_transaction_data()[2:])),
            (vedolo.address, False, bytes.fromhex(vedolo.functions.locked(token_id)._encode_transaction_data()[2:])),
            (vedolo.address, False, bytes.fromhex(vedolo.functions.balanceOfNFT(token_id)._encode_transaction_data()[2:])),
        ])
    position_results = _multicall(client, web3, position_calls, block_number, batch_size)

    position_reads = {}
    owners = {}
    for index, token_id in enumerate(token_ids):
        (owner,) = decode(["address"], position_results[index * 3])
        amount_wei, end = decode(["int128", "uint256"], position_results[index * 3 + 1])
        (vote_weight_wei,) = decode(["uint256"], position_results[index * 3 + 2])
        owners[token_id] = owner
        position_reads[token_id] = {
            "amount_wei": int(amount_wei),
            "end": int(end),
            "vote_weight_wei": int(vote_weight_wei),
        }

    fee_quotes = {}
    active_ids = []
    fee_calls = []
    for token_id in token_ids:
        position = position_reads[token_id]
        if position["amount_wei"] <= 0 or position["end"] <= snapshot_timestamp:
            fee_quotes[token_id] = {"burn_fee_wei": 0, "recoup_fee_wei": 0}
            continue
        active_ids.append(token_id)
        encoded = calculator.functions.getEarlyWithdrawalFees(
            position["amount_wei"], position["end"]
        )._encode_transaction_data()
        fee_calls.append((calculator.address, False, bytes.fromhex(encoded[2:])))

    for token_id, result in zip(active_ids, _multicall(client, web3, fee_calls, block_number, batch_size)):
        burn_fee_wei, recoup_fee_wei = decode(["uint256", "uint256"], result)
        fee_quotes[token_id] = {
            "burn_fee_wei": int(burn_fee_wei),
            "recoup_fee_wei": int(recoup_fee_wei),
        }

    current_holders = build_current_holders(token_ids, owners)
    return current_holders, position_reads, fee_quotes, snapshot_timestamp, block_number, fee_calculator


def main():
    inputs = fetch_onchain_inputs()
    payload = build_simulation_payload(
        inputs[0],
        inputs[1],
        inputs[2],
        snapshot_timestamp=inputs[3],
        source_block=inputs[4],
        fee_calculator=inputs[5],
    )
    temp_path = OUTPUT_PATH + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"))
        handle.write("\n")
    os.replace(temp_path, OUTPUT_PATH)
    print(
        f"Wrote {os.path.basename(OUTPUT_PATH)}: "
        f"{payload['walletCount']:,} wallets, {payload['positionCount']:,} positions, "
        f"block {payload['sourceBlock']:,}"
    )


if __name__ == "__main__":
    main()
