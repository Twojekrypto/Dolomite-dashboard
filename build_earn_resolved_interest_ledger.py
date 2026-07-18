#!/usr/bin/env python3
"""Build strict static EARN interest ledgers from pinned snapshots and canonical history.

The producer is intentionally conservative. It publishes pure supply markets
whose capital-flow cycles can be resolved without inferring an event interest
index. Borrow routes, unknown accounts, partial reductions, stale histories and
snapshot mismatches are skipped instead of being promoted to verified data.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from scan_earn_netflow import CHAINS as EARN_CHAIN_CONFIG


ROOT = Path(__file__).resolve().parent
SNAPSHOT_DIR = ROOT / "data" / "earn-snapshots"
HISTORY_DIR = ROOT / "data" / "earn-subaccount-history"
OUTPUT_DIR = ROOT / "data" / "earn-resolved-interest-ledger"
INDEX_SCALE = 10**18


def _read_json(path: Path, default=None):
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return default


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"), ensure_ascii=True)
    temporary.replace(path)


def _integer(value, default=0):
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def _par_to_wei_round_half_up(par: int, index: int) -> int:
    quotient, remainder = divmod(int(par) * int(index), INDEX_SCALE)
    return quotient + (1 if remainder * 2 >= INDEX_SCALE else 0)


def _ordered_events(market_payload: dict, comparison_block: int):
    events = []
    for event in (market_payload or {}).get("events") or []:
        if not isinstance(event, dict):
            return None
        block_number = _integer(event.get("blockNumber"), -1)
        if block_number < 0:
            return None
        if block_number > comparison_block:
            continue
        if event.get("accountKnown") is not True:
            return None
        events.append(event)
    return sorted(events, key=lambda event: (
        _integer(event.get("blockNumber"), 0),
        _integer(event.get("transactionIndex"), 0),
        _integer(event.get("logIndex"), 0),
    ))


def _resolve_supply_account(account: str, market_id: str, events: list, supply_index: int):
    par = 0
    cycle_flow = 0
    settled_yield = 0
    had_supply = False

    for event in events:
        new_par = _integer(event.get("newPar"), 0)
        delta_wei = _integer(event.get("deltaWei"), 0)
        if new_par < 0:
            return None
        if par > 0 and 0 < new_par < par:
            return None
        if par == 0 and new_par == 0:
            if delta_wei != 0:
                return None
            continue

        if par == 0 and new_par > 0:
            cycle_flow = delta_wei
            had_supply = True
        elif par > 0 and new_par > 0:
            if new_par < par:
                return None
            cycle_flow += delta_wei
        elif par > 0 and new_par == 0:
            cycle_flow += delta_wei
            settled_yield += -cycle_flow
            cycle_flow = 0
        par = new_par

    current_wei = _par_to_wei_round_half_up(par, supply_index) if par > 0 else 0
    open_yield = current_wei - cycle_flow if par > 0 else 0
    state = {
        "account": str(account),
        "marketId": str(market_id),
        "par": str(par),
        "lastIndex": str(supply_index) if par > 0 else None,
        "settledYield": str(settled_yield),
        "settledSupplyYield": str(settled_yield),
        "settledBorrowYield": "0",
        "liveYield": str(open_yield),
        "hadSupply": had_supply,
        "hadBorrow": False,
    }
    return {
        "par": par,
        "wei": current_wei,
        "settledYield": settled_yield,
        "openYield": open_yield,
        "state": state,
    }


def _verification_entry(market: dict, expected_par: int, expected_wei: int) -> dict:
    actual_par = _integer(market.get("par"), 0)
    actual_wei = _integer(market.get("wei"), 0)
    return {
        "status": "verified",
        "strictStatus": "verified",
        "counted": True,
        "canVerify": True,
        "rawVerified": True,
        "strictVerified": True,
        "snapshotIncomplete": False,
        "subgraphReplayTruncated": False,
        "replayStateAdjusted": False,
        "decimals": _integer(market.get("decimals"), 18),
        "expectedSupplyPar": str(expected_par),
        "expectedSupplyWei": str(expected_wei),
        "expectedCollateralPar": "0",
        "expectedCollateralWei": "0",
        "expectedBorrowPar": "0",
        "expectedBorrowWei": "0",
        "actualSupplyPar": str(actual_par),
        "actualSupplyWei": str(actual_wei),
        "actualCollateralPar": "0",
        "actualCollateralWei": "0",
        "actualBorrowPar": "0",
        "actualBorrowWei": "0",
        "supplyParDiff": str(actual_par - expected_par),
        "supplyWeiDiff": str(actual_wei - expected_wei),
        "collateralParDiff": "0",
        "collateralWeiDiff": "0",
        "borrowParDiff": "0",
        "borrowWeiDiff": "0",
        "parTolerance": "0",
        "supplyWeiTolerance": "0",
        "collateralWeiTolerance": "0",
        "borrowWeiTolerance": "0",
    }


def build_resolved_ledger(
    chain: str,
    address: str,
    snapshot_date: str,
    snapshot_payload: dict,
    history_payload: dict,
    *,
    generated_at: Optional[str] = None,
):
    chain = str(chain).lower()
    address = str(address).lower()
    if not isinstance(snapshot_payload, dict) or not isinstance(history_payload, dict):
        return None
    if str(history_payload.get("chain") or "").lower() != chain:
        return None
    if str(history_payload.get("address") or "").lower() != address:
        return None
    source_date = str(((history_payload.get("sourceMetadata") or {}).get("latestSnapshotDate") or ""))
    if source_date != str(snapshot_date):
        return None
    canonical_start_block = _integer((EARN_CHAIN_CONFIG.get(chain) or {}).get("start_block"), 0)
    scan_from_block = _integer(((history_payload.get("scanRange") or {}).get("fromBlock")), 0)
    if canonical_start_block > 0 and (scan_from_block <= 0 or scan_from_block > canonical_start_block):
        return None

    chain_metadata = ((snapshot_payload.get("chainMetadata") or {}).get(chain) or {})
    comparison_block = _integer(chain_metadata.get("blockNumber"), 0)
    if comparison_block <= 0 or _integer(history_payload.get("lastScannedBlock"), 0) < comparison_block:
        return None
    wallet = ((((snapshot_payload.get("snapshots") or {}).get(chain) or {}).get(address)) or {})
    snapshot_markets = wallet.get("markets") or {}
    if not isinstance(snapshot_markets, dict) or not snapshot_markets:
        return None
    index_map = chain_metadata.get("interestIndexes") or {}
    accounts = history_payload.get("accounts") or {}
    if not isinstance(accounts, dict) or not accounts:
        return None
    if any(not isinstance(account_data, dict) or account_data.get("accountKnown") is not True or account_data.get("hasBorrow") is True
           for account_data in accounts.values()):
        return None

    markets_out = {}
    verification_out = {}
    replay_state_out = {}
    account_state_out = {}
    current_index_map = {}

    for market_id, snapshot_market in snapshot_markets.items():
        market_id = str(market_id)
        index_meta = index_map.get(market_id) or {}
        supply_index = _integer(index_meta.get("supplyIndex"), 0)
        borrow_index = _integer(index_meta.get("borrowIndex"), 0)
        if supply_index <= 0 or borrow_index <= 0:
            continue

        resolved_accounts = []
        market_failed = False
        for account_key, account_data in accounts.items():
            market_history = ((account_data.get("markets") or {}).get(market_id) or {})
            events = _ordered_events(market_history, comparison_block)
            if events is None:
                market_failed = True
                break
            if not events:
                continue
            resolved = _resolve_supply_account(str(account_key), market_id, events, supply_index)
            if resolved is None:
                market_failed = True
                break
            resolved_accounts.append(resolved)
        if market_failed or not resolved_accounts:
            continue

        expected_par = sum(row["par"] for row in resolved_accounts)
        expected_wei = sum(row["wei"] for row in resolved_accounts)
        if expected_par != _integer(snapshot_market.get("par"), 0):
            continue
        if expected_wei != _integer(snapshot_market.get("wei"), 0):
            continue

        settled_yield = sum(row["settledYield"] for row in resolved_accounts)
        open_yield = sum(row["openYield"] for row in resolved_accounts)
        earn_yield = settled_yield + open_yield
        markets_out[market_id] = {
            "earnYield": str(earn_yield),
            "settledYield": str(settled_yield),
            "settledSupplyYield": str(settled_yield),
            "settledBorrowYield": "0",
            "openBorrowYield": "0",
            "openSupplyYield": str(open_yield),
            "openCollateralYield": "0",
            "currentBorrowPar": "0",
            "currentSupplyPar": str(expected_par),
            "currentCollateralSupplyPar": "0",
            "hadSupply": True,
            "hadBorrow": False,
            "strictStatus": "verified",
            "strictMethod": "interest-ledger",
        }
        verification_out[market_id] = _verification_entry(snapshot_market, expected_par, expected_wei)
        replay_state_out[market_id] = {
            "expectedSupplyPar": str(expected_par),
            "expectedSupplyWei": str(expected_wei),
            "expectedCollateralSupplyPar": "0",
            "expectedCollateralSupplyWei": "0",
            "expectedBorrowPar": "0",
            "expectedBorrowWei": "0",
            "hadSupply": True,
            "hadBorrow": False,
            "canVerify": True,
        }
        current_index_map[market_id] = {
            "supplyIndex": str(supply_index),
            "borrowIndex": str(borrow_index),
        }
        for row in resolved_accounts:
            state = row["state"]
            account_state_out[f"{state['account']}|{market_id}"] = state

    if not markets_out:
        return None
    return {
        "version": 1,
        "chain": chain,
        "address": address,
        "snapshotDate": str(snapshot_date),
        "comparisonBlock": comparison_block,
        "generatedAt": generated_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "strictStatus": "verified",
        "strictMethod": "interest-ledger",
        "canonicalHistoryCoverageStatus": "fresh",
        "markets": markets_out,
        "replayVerificationData": verification_out,
        "replayStateData": replay_state_out,
        "accountStateData": account_state_out,
        "currentIndexMap": current_index_map,
        "openBorrowAccounts": [],
        "borrowAccountsOverride": [],
    }


def _read_addresses(paths: Iterable[Path]) -> set[str]:
    addresses = set()
    for path in paths:
        if not path.is_file():
            continue
        for raw in path.read_text(encoding="utf-8").splitlines():
            address = raw.split("#", 1)[0].strip().lower()
            if address:
                if not address.startswith("0x") or len(address) != 42:
                    raise SystemExit(f"Invalid address in {path}: {raw}")
                addresses.add(address)
    return addresses


def _latest_snapshot(chain: str):
    manifest = _read_json(SNAPSHOT_DIR / "manifest.json", {}) or {}
    dates = sorted(
        str(date) for date in (manifest.get("dates") or [])
        if chain in ((manifest.get("chains") or {}).get(date) or [])
    )
    if not dates:
        return None, None
    latest_date = dates[-1]
    return latest_date, _read_json(SNAPSHOT_DIR / f"{latest_date}.json", None)


def _existing_addresses(output_dir: Path, chain: str) -> set[str]:
    chain_dir = output_dir / chain
    if not chain_dir.is_dir():
        return set()
    return {path.stem.lower() for path in chain_dir.glob("0x*.json") if len(path.stem) == 42}


def _update_manifest(output_dir: Path, chain: str, snapshot_date: str) -> None:
    manifest_path = output_dir / "manifest.json"
    manifest = _read_json(manifest_path, {}) or {}
    chains = manifest.get("chains") or {}
    chain_dir = output_dir / chain
    address_count = len(list(chain_dir.glob("0x*.json"))) if chain_dir.is_dir() else 0
    chains[chain] = {"snapshotDate": snapshot_date, "addressCount": address_count}
    manifest.update({
        "version": 1,
        "generatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "chains": chains,
    })
    _write_json(manifest_path, manifest)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build strict resolved EARN interest ledgers")
    parser.add_argument("--chain", action="append", required=True)
    parser.add_argument("--address", action="append", default=[])
    parser.add_argument("--address-file", action="append", default=[])
    parser.add_argument("--all-active", action="store_true")
    parser.add_argument("--existing-addresses", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args()

    requested = {str(address).lower() for address in args.address}
    requested.update(_read_addresses(Path(path) for path in args.address_file))
    if not requested and not args.all_active and not args.existing_addresses:
        raise SystemExit("Provide --address, --address-file, --all-active, or --existing-addresses")

    for chain in sorted({str(value).lower() for value in args.chain}):
        snapshot_date, snapshot_payload = _latest_snapshot(chain)
        if not snapshot_date or not isinstance(snapshot_payload, dict):
            print(f"[{chain}] no snapshot available")
            continue
        addresses = set(requested)
        if args.all_active:
            addresses.update((((snapshot_payload.get("snapshots") or {}).get(chain)) or {}).keys())
        if args.existing_addresses:
            addresses.update(_existing_addresses(args.output_dir, chain))

        wrote = 0
        removed = 0
        for address in sorted(addresses):
            history = _read_json(HISTORY_DIR / chain / f"{address}.json", None)
            ledger = build_resolved_ledger(chain, address, snapshot_date, snapshot_payload, history)
            output_path = args.output_dir / chain / f"{address}.json"
            if ledger:
                _write_json(output_path, ledger)
                wrote += 1
            elif output_path.is_file():
                output_path.unlink()
                removed += 1
        _update_manifest(args.output_dir, chain, snapshot_date)
        print(f"[{chain}] resolved ledgers: wrote={wrote} removed={removed} selected={len(addresses)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
