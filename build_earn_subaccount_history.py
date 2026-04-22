#!/usr/bin/env python3
"""
Build canonical per-address EARN subaccount history from onchain margin events.

Outputs:
  - data/earn-subaccount-history/{chain}/{address}.json
  - data/earn-subaccount-history/manifest.json

The output is intentionally strict:
  - current-layout events keep exact owner/account/market deltas and newPar values
  - legacy events that do not expose account numbers are preserved under the
    synthetic account id "legacy-unknown" instead of being heuristically assigned
    to a real subaccount
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence

from scan_earn_netflow import (
    ALL_EVENTS,
    BLOCK_CHUNK,
    CHAINS,
    CURRENT_LOG_BUY,
    CURRENT_LOG_LIQUIDATE,
    CURRENT_LOG_SELL,
    CURRENT_LOG_TRADE,
    CURRENT_LOG_TRANSFER,
    CURRENT_LOG_VAPORIZE,
    LEGACY_LOG_LIQUIDATE,
    LEGACY_LOG_TRADE,
    LEGACY_LOG_TRANSFER,
    LOG_DEPOSIT,
    LOG_WITHDRAW,
    SECOND_OWNER_EVENTS,
    _addr_topic,
    _dedupe_logs,
    decode_log_entries,
    get_block_number,
    get_logs,
)


ROOT = Path(__file__).resolve().parent
SNAPSHOT_DIR = ROOT / "data" / "earn-snapshots"
NETFLOW_DIR = ROOT / "data" / "earn-netflow"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "earn-subaccount-history"
LEGACY_UNKNOWN_ACCOUNT = "legacy-unknown"
ADDRESS_TOPIC_CHUNK = 100
SUBACCOUNT_HISTORY_VERSION = 1

EVENT_META = {
    LOG_DEPOSIT: ("deposit", "d"),
    LOG_WITHDRAW: ("withdraw", "w"),
    LEGACY_LOG_TRADE: ("trade", "s"),
    LEGACY_LOG_TRANSFER: ("transfer", "x"),
    LEGACY_LOG_LIQUIDATE: ("liquidate", "l"),
    CURRENT_LOG_TRANSFER: ("transfer", "x"),
    CURRENT_LOG_BUY: ("buy", "s"),
    CURRENT_LOG_SELL: ("sell", "s"),
    CURRENT_LOG_TRADE: ("trade", "s"),
    CURRENT_LOG_LIQUIDATE: ("liquidate", "l"),
    CURRENT_LOG_VAPORIZE: ("vaporize", "v"),
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path, default):
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)
        handle.write("\n")
    tmp.replace(path)


def _chunked(items: Sequence[str], size: int) -> Iterator[List[str]]:
    for idx in range(0, len(items), size):
        yield list(items[idx: idx + size])


def _select_addresses(
    chain: str,
    *,
    explicit_addresses: Sequence[str],
    address_files: Sequence[str],
    use_all_known_addresses: bool,
    limit: Optional[int],
) -> List[str]:
    file_addresses = set()
    for file_path in address_files:
        path = Path(str(file_path))
        if not path.exists():
            raise FileNotFoundError(f"Address file not found: {path}")
        for line in path.read_text(encoding="utf-8").splitlines():
            address = str(line).strip().lower()
            if address:
                file_addresses.add(address)

    explicit = {
        str(address).strip().lower()
        for address in explicit_addresses
        if str(address).strip()
    }
    if explicit or file_addresses:
        addresses = sorted(explicit | file_addresses)
    else:
        addresses = _load_known_addresses(chain) if use_all_known_addresses else []

    if limit is not None:
        addresses = addresses[: max(0, int(limit))]
    return addresses


def _load_known_addresses(chain: str) -> List[str]:
    addresses = set()

    manifest = _read_json(SNAPSHOT_DIR / "manifest.json", {})
    chain_dates = [
        str(date)
        for date in (manifest.get("dates") or [])
        if chain in ((manifest.get("chains") or {}).get(date) or [])
    ]
    if chain_dates:
        latest_date = sorted(chain_dates)[-1]
        payload = _read_json(SNAPSHOT_DIR / f"{latest_date}.json", {})
        chain_snapshots = ((payload.get("snapshots") or {}).get(chain) or {})
        addresses.update(str(address).lower() for address in chain_snapshots.keys())

    netflow_payload = _read_json(NETFLOW_DIR / f"{chain}.json", {})
    netflows = netflow_payload.get("netflows") or {}
    addresses.update(str(address).lower() for address in netflows.keys())

    return sorted(address for address in addresses if address.startswith("0x") and len(address) == 42)


def _get_latest_snapshot_date(chain: str) -> Optional[str]:
    manifest = _read_json(SNAPSHOT_DIR / "manifest.json", {})
    chain_dates = [
        str(date)
        for date in (manifest.get("dates") or [])
        if chain in ((manifest.get("chains") or {}).get(date) or [])
    ]
    return sorted(chain_dates)[-1] if chain_dates else None


def _get_netflow_last_block(chain: str) -> int:
    payload = _read_json(NETFLOW_DIR / f"{chain}.json", {})
    try:
        return int(payload.get("lastBlock") or 0)
    except Exception:
        return 0


def _sort_logs(logs: Iterable[dict]) -> List[dict]:
    return sorted(
        logs,
        key=lambda log: (
            int(log.get("blockNumber", "0x0"), 16),
            int(log.get("transactionIndex", "0x0"), 16),
            int(log.get("logIndex", "0x0"), 16),
        ),
    )


def _empty_history(
    chain: str,
    contract: str,
    address: str,
    latest_block: int,
    *,
    start_block: int,
    source_snapshot_date: Optional[str],
    source_netflow_last_block: int,
) -> dict:
    return {
        "version": SUBACCOUNT_HISTORY_VERSION,
        "chain": chain,
        "marginContract": contract.lower(),
        "address": address.lower(),
        "lastScannedBlock": latest_block,
        "generatedAt": _utc_now_iso(),
        "scanRange": {
            "fromBlock": start_block,
            "toBlock": latest_block,
        },
        "sourceMetadata": {
            "latestSnapshotDate": source_snapshot_date,
            "lastNetflowBlock": source_netflow_last_block,
        },
        "accounts": {},
        "summary": {
            "accountCount": 0,
            "marketCount": 0,
            "eventCount": 0,
            "unknownAccountEventCount": 0,
            "borrowAccountCount": 0,
        },
    }


def _normalize_account(entry: dict) -> str:
    if entry.get("account") is None:
        return LEGACY_UNKNOWN_ACCOUNT
    return str(entry.get("account"))


def _ensure_market_state(history: dict, account: str, market: str) -> dict:
    accounts = history.setdefault("accounts", {})
    account_state = accounts.setdefault(
        account,
        {
            "account": account,
            "accountKnown": account != LEGACY_UNKNOWN_ACCOUNT,
            "markets": {},
        },
    )
    markets = account_state["markets"]
    if market not in markets:
        markets[market] = {
            "marketId": market,
            "eventCount": 0,
            "firstBlock": None,
            "lastBlock": None,
            "totalDeltaWei": "0",
            "latestPar": None,
            "peakPar": None,
            "lowestPar": None,
            "events": [],
        }
    return markets[market]


def _normalized_event_from_decoded(decoded: dict, log: dict) -> Optional[dict]:
    address = str(decoded.get("owner") or "").lower()
    if not address:
        return None
    topic0 = str((log.get("topics") or [""])[0]).lower()
    event_name, flow_type = EVENT_META.get(topic0, ("unknown", "u"))
    delta = int(decoded["delta"])
    new_par_raw = decoded.get("new_par")
    new_par = None if new_par_raw is None else int(new_par_raw)
    block_number = int(log.get("blockNumber", "0x0"), 16)
    tx_index = int(log.get("transactionIndex", "0x0"), 16)
    log_index = int(log.get("logIndex", "0x0"), 16)
    tx_hash = str(log.get("transactionHash") or "").lower()

    return {
        "owner": address,
        "account": _normalize_account(decoded),
        "market": str(decoded["market"]),
        "topic0": topic0,
        "event": event_name,
        "flowType": flow_type,
        "deltaWei": str(delta),
        "newPar": None if new_par is None else str(new_par),
        "blockNumber": block_number,
        "transactionIndex": tx_index,
        "logIndex": log_index,
        "transactionHash": tx_hash,
        "accountKnown": _normalize_account(decoded) != LEGACY_UNKNOWN_ACCOUNT,
    }


def _append_normalized_event(history: dict, event: dict) -> None:
    address = str(event.get("owner") or "").lower()
    if address != history["address"]:
        return

    account = str(event["account"])
    market = str(event["market"])
    market_state = _ensure_market_state(history, account, market)

    delta = int(event["deltaWei"])
    new_par_value = event.get("newPar")
    new_par = None if new_par_value is None else int(new_par_value)
    block_number = int(event["blockNumber"])
    tx_index = int(event["transactionIndex"])
    log_index = int(event["logIndex"])
    tx_hash = str(event.get("transactionHash") or "").lower()

    market_state["eventCount"] += 1
    market_state["totalDeltaWei"] = str(int(market_state["totalDeltaWei"]) + delta)
    market_state["firstBlock"] = block_number if market_state["firstBlock"] is None else min(market_state["firstBlock"], block_number)
    market_state["lastBlock"] = block_number if market_state["lastBlock"] is None else max(market_state["lastBlock"], block_number)

    if new_par is not None:
        market_state["latestPar"] = str(new_par)
        market_state["peakPar"] = str(new_par if market_state["peakPar"] is None else max(int(market_state["peakPar"]), new_par))
        market_state["lowestPar"] = str(new_par if market_state["lowestPar"] is None else min(int(market_state["lowestPar"]), new_par))

    market_state["events"].append({
        "blockNumber": block_number,
        "transactionIndex": tx_index,
        "logIndex": log_index,
        "transactionHash": tx_hash,
        "event": str(event.get("event") or "unknown"),
        "flowType": str(event.get("flowType") or "u"),
        "deltaWei": str(delta),
        "newPar": None if new_par is None else str(new_par),
        "accountKnown": account != LEGACY_UNKNOWN_ACCOUNT,
    })


def _append_event(history: dict, decoded: dict, log: dict) -> None:
    normalized = _normalized_event_from_decoded(decoded, log)
    if normalized is None:
        return
    _append_normalized_event(history, normalized)


def _finalize_history(history: dict) -> dict:
    accounts = history.get("accounts") or {}
    account_count = len(accounts)
    market_count = 0
    event_count = 0
    unknown_account_event_count = 0
    borrow_account_count = 0

    for account, account_state in accounts.items():
        markets = account_state.get("markets") or {}
        market_count += len(markets)
        has_borrow = False
        for market_state in markets.values():
            event_count += int(market_state.get("eventCount") or 0)
            if account == LEGACY_UNKNOWN_ACCOUNT:
                unknown_account_event_count += int(market_state.get("eventCount") or 0)
            latest_par = market_state.get("latestPar")
            if latest_par is not None and int(latest_par) < 0:
                has_borrow = True
        account_state["hasBorrow"] = has_borrow
        if has_borrow:
            borrow_account_count += 1

    history["summary"] = {
        "accountCount": account_count,
        "marketCount": market_count,
        "eventCount": event_count,
        "unknownAccountEventCount": unknown_account_event_count,
        "borrowAccountCount": borrow_account_count,
    }
    return history


def _fetch_logs_for_addresses(
    rpcs: Sequence[str],
    rpc_idx: List[int],
    contract: str,
    addresses: Sequence[str],
    from_block: int,
    to_block: int,
) -> List[dict]:
    all_logs = []
    for address_chunk in _chunked(list(addresses), ADDRESS_TOPIC_CHUNK):
        topics_chunk = [_addr_topic(address) for address in address_chunk]
        topic_one_logs = get_logs(rpcs, rpc_idx, contract, [ALL_EVENTS, topics_chunk], from_block, to_block) or []
        topic_two_logs = get_logs(rpcs, rpc_idx, contract, [SECOND_OWNER_EVENTS, None, topics_chunk], from_block, to_block) or []
        all_logs.extend(_dedupe_logs([*topic_one_logs, *topic_two_logs]))
    return _sort_logs(_dedupe_logs(all_logs))


def build_history_for_addresses(chain: str, addresses: Sequence[str]) -> Dict[str, dict]:
    return build_history_for_addresses_in_block_range(chain, addresses)


def build_history_for_addresses_in_block_range(
    chain: str,
    addresses: Sequence[str],
    from_block: Optional[int] = None,
    to_block: Optional[int] = None,
) -> Dict[str, dict]:
    config = CHAINS[chain]
    contract = config["margin"]
    rpcs = config["rpcs"]
    rpc_idx = [0]
    latest_chain_block = get_block_number(rpcs, rpc_idx)
    start_block = config["start_block"] if from_block is None else int(from_block)
    end_block = latest_chain_block if to_block is None else min(int(to_block), latest_chain_block)
    if end_block < start_block:
        raise ValueError(f"Invalid block range: from={start_block} to={end_block}")
    source_snapshot_date = _get_latest_snapshot_date(chain)
    source_netflow_last_block = _get_netflow_last_block(chain)

    histories = {
        address.lower(): _empty_history(
            chain,
            contract,
            address.lower(),
            end_block,
            start_block=start_block,
            source_snapshot_date=source_snapshot_date,
            source_netflow_last_block=source_netflow_last_block,
        )
        for address in addresses
    }

    current = start_block
    total_logs = 0

    while current <= end_block:
        chunk_end = min(current + BLOCK_CHUNK - 1, end_block)
        logs = _fetch_logs_for_addresses(rpcs, rpc_idx, contract, addresses, current, chunk_end)
        total_logs += len(logs)

        for log in logs:
            entries = decode_log_entries(log) or []
            for entry in entries:
                owner = str(entry.get("owner") or "").lower()
                history = histories.get(owner)
                if history is None:
                    continue
                _append_event(history, entry, log)

        processed = chunk_end - start_block
        total_range = max(1, end_block - start_block)
        pct = (processed / total_range) * 100
        if logs or processed % 500_000 < BLOCK_CHUNK:
            print(f"[{chain}] [{pct:5.1f}%] block {chunk_end:,} logs={len(logs)} total_logs={total_logs}")
        current = chunk_end + 1

    return {address: _finalize_history(history) for address, history in histories.items()}


def _count_history_files(chain_dir: Path) -> int:
    if not chain_dir.exists():
        return 0
    return sum(1 for path in chain_dir.glob("*.json") if path.is_file())


def _write_histories(
    output_dir: Path,
    chain: str,
    histories: Dict[str, dict],
    latest_block: int,
    *,
    start_block: Optional[int] = None,
    selection_address_count: Optional[int] = None,
) -> None:
    chain_dir = output_dir / chain
    chain_dir.mkdir(parents=True, exist_ok=True)
    wrote = 0
    for address, payload in histories.items():
        _write_json(chain_dir / f"{address}.json", payload)
        wrote += 1

    manifest_path = output_dir / "manifest.json"
    manifest = _read_json(manifest_path, {"chains": {}})
    chains = manifest.setdefault("chains", {})
    previous_chain_payload = chains.get(chain) or {}
    previous_selection_count = None
    try:
        previous_selection_count = int(previous_chain_payload.get("selectionAddressCount"))
    except Exception:
        previous_selection_count = None
    latest_snapshot_date = None
    last_netflow_block = 0
    if histories:
        sample_payload = next(iter(histories.values()))
        source_meta = sample_payload.get("sourceMetadata") or {}
        latest_snapshot_date = source_meta.get("latestSnapshotDate")
        try:
            last_netflow_block = int(source_meta.get("lastNetflowBlock") or 0)
        except Exception:
            last_netflow_block = 0
        if start_block is None:
            scan_range = sample_payload.get("scanRange") or {}
            try:
                start_block = int(scan_range.get("fromBlock"))
            except Exception:
                start_block = None
    address_count = _count_history_files(chain_dir)
    selection_count_candidates = [address_count]
    if previous_selection_count is not None:
        selection_count_candidates.append(previous_selection_count)
    if selection_address_count is not None:
        try:
            selection_count_candidates.append(int(selection_address_count))
        except Exception:
            pass
    chains[chain] = {
        "version": SUBACCOUNT_HISTORY_VERSION,
        "updatedAt": _utc_now_iso(),
        "lastBlock": latest_block,
        "fromBlock": start_block,
        "addressCount": address_count,
        "lastBatchAddressCount": wrote,
        "selectionAddressCount": max(selection_count_candidates) if selection_count_candidates else None,
        "latestSnapshotDate": latest_snapshot_date,
        "lastNetflowBlock": last_netflow_block,
    }
    _write_json(manifest_path, manifest)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build canonical EARN subaccount history from onchain events")
    parser.add_argument("--chain", action="append", choices=sorted(CHAINS.keys()), help="Specific chain to scan (repeatable)")
    parser.add_argument("--address", action="append", default=[], help="Owner address to build history for (repeatable)")
    parser.add_argument("--address-file", action="append", default=[], help="Path to newline-delimited address file (repeatable)")
    parser.add_argument("--all-known-addresses", action="store_true", help="Use addresses seen in latest snapshot or netflow data")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit when using --all-known-addresses")
    parser.add_argument("--from-block", type=int, default=None, help="Optional starting block for targeted backfill/testing")
    parser.add_argument("--to-block", type=int, default=None, help="Optional ending block for targeted backfill/testing")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for per-address subaccount history files",
    )
    args = parser.parse_args()

    selected_chains = args.chain or ["arbitrum"]
    output_dir = Path(args.output_dir)

    if not args.address and not args.address_file and not args.all_known_addresses:
        raise SystemExit("Pass --address ... or --all-known-addresses")

    for chain in selected_chains:
        addresses = _select_addresses(
            chain,
            explicit_addresses=args.address or [],
            address_files=args.address_file or [],
            use_all_known_addresses=bool(args.all_known_addresses),
            limit=args.limit,
        )
        if not addresses:
            print(f"[{chain}] no addresses selected, skipping")
            continue

        print(f"[{chain}] building subaccount history for {len(addresses)} address(es)")
        histories = build_history_for_addresses_in_block_range(
            chain,
            addresses,
            from_block=args.from_block,
            to_block=args.to_block,
        )
        latest_block = next(iter(histories.values()))["lastScannedBlock"] if histories else 0
        _write_histories(
            output_dir,
            chain,
            histories,
            latest_block,
            start_block=args.from_block,
            selection_address_count=len(addresses),
        )
        print(f"[{chain}] wrote {len(histories)} history file(s) to {output_dir / chain}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
