#!/usr/bin/env python3
"""
Build per-address verified EARN ledger snapshots from existing local data.

Input data:
  - data/earn-snapshots/{date}.json + manifest.json
  - data/earn-netflow/{chain}.json

Output data:
  - data/earn-verified-ledger/{chain}/{address}.json
  - data/earn-verified-ledger/manifest.json

The generated ledger stores per-market cumulative yield plus verification
metadata comparing snapshot-derived yield vs netflow-derived yield.
Generated files are intended for local/private use and should not be committed.
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SNAPSHOT_DIR = ROOT / "data" / "earn-snapshots"
NETFLOW_DIR = ROOT / "data" / "earn-netflow"
OUTPUT_DIR = ROOT / "data" / "earn-verified-ledger"


def _read_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), ensure_ascii=True)
    tmp.replace(path)


def _parse_int(value, default=0):
    try:
        return int(str(value))
    except Exception:
        return default


def _to_date(date_str):
    return datetime.fromisoformat(date_str + "T00:00:00+00:00")


def _market_sort_key(mid):
    try:
        return (0, int(mid))
    except Exception:
        return (1, str(mid))


def _get_tolerance(decimals):
    # Align with frontend tolerance logic: 10^(decimals-12), min 1.
    d = max(0, _parse_int(decimals, 18))
    return 10 ** max(0, d - 12)


def _get_pre_snapshot_residual_tolerance(decimals):
    # Allow a few base units of residual drift when snapshot math spans a tiny par change.
    return max(1, _get_tolerance(decimals) * 4)


def _collect_chain_snapshot_dates(manifest, chain):
    dates = sorted(manifest.get("dates", []))
    out = []
    for date_str in dates:
        chains = manifest.get("chains", {}).get(date_str, [])
        if chain in chains:
            out.append(date_str)
    return out


def _load_chain_snapshots(chain, chain_dates):
    snapshots = []
    for date_str in chain_dates:
        path = SNAPSHOT_DIR / f"{date_str}.json"
        if not path.exists():
            continue
        payload = _read_json(path)
        chain_data = (payload.get("snapshots", {}) or {}).get(chain, {}) or {}
        snapshots.append((date_str, chain_data))
    return snapshots


def _get_pre_snapshot_carry_meta(decimals, snapshot_yield, diff, first_par, first_wei, last_par, last_wei, netflow_t, has_static_par_window):
    if snapshot_yield is None or diff is None:
        return None
    if first_par <= 0 or first_wei < netflow_t:
        return None

    carry = first_wei - netflow_t
    if carry < 0:
        return None

    residual = diff - carry
    post_snapshot_delta = last_wei - first_wei
    exact_post_snapshot_window = snapshot_yield == post_snapshot_delta
    if has_static_par_window and exact_post_snapshot_window:
        return {
            "carry": carry,
            "residual": residual,
            "tinyParDriftWindow": False,
        }

    par_drift = abs(last_par - first_par)
    if par_drift > _get_tolerance(decimals):
        return None

    if post_snapshot_delta < 0 or snapshot_yield < 0:
        return None

    if abs(residual) > _get_pre_snapshot_residual_tolerance(decimals):
        return None

    return {
        "carry": carry,
        "residual": residual,
        "tinyParDriftWindow": True,
    }


def _build_address_ledger(address, chain, latest_date, snapshots, netflow_by_addr):
    address = address.lower()
    history_by_market = {}
    meta_by_market = {}

    for date_str, chain_data in snapshots:
        addr_data = chain_data.get(address)
        if not addr_data:
            continue
        markets = (addr_data.get("markets") or {})
        for mid_raw, m in markets.items():
            mid = str(mid_raw)
            history_by_market.setdefault(mid, []).append({
                "date": date_str,
                "par": _parse_int(m.get("par", "0")),
                "wei": _parse_int(m.get("wei", "0")),
            })
            # Keep latest-known token metadata for market.
            meta_by_market[mid] = {
                "token": str(m.get("token", "")).lower(),
                "symbol": str(m.get("symbol", "UNK")),
                "decimals": _parse_int(m.get("decimals", 18)),
            }

    addr_flows = netflow_by_addr.get(address, {}) or {}
    market_ids = set(history_by_market.keys()) | {str(k) for k in addr_flows.keys()}
    if not market_ids:
        return None

    markets_out = {}
    summary = {
        "verified": 0,
        "pre_snapshot_carry": 0,
        "mismatch": 0,
        "no_netflow": 0,
        "no_snapshot": 0,
        "unavailable": 0,
    }

    for mid in sorted(market_ids, key=_market_sort_key):
        hist = history_by_market.get(mid, [])
        meta = meta_by_market.get(mid, {})
        decimals = _parse_int(meta.get("decimals", 18))

        snapshot_yield = None
        first_par = 0
        first_wei = 0
        last_par = 0
        last_wei = 0
        first_date = ""
        last_date = ""
        days = 0
        is_latest_snapshot = False
        has_static_par_window = False

        if hist:
            cumulative = 0
            for i in range(1, len(hist)):
                prev = hist[i - 1]
                curr = hist[i]
                if prev["par"] == 0 or curr["par"] == 0:
                    continue
                daily = (prev["par"] * curr["wei"] - prev["wei"] * curr["par"]) // curr["par"]
                cumulative += daily
            snapshot_yield = cumulative
            first_date = hist[0]["date"]
            first_par = hist[0]["par"]
            first_wei = hist[0]["wei"]
            last_date = hist[-1]["date"]
            last_par = hist[-1]["par"]
            last_wei = hist[-1]["wei"]
            has_static_par_window = all(point["par"] == first_par for point in hist)
            is_latest_snapshot = (last_date == latest_date)
            try:
                days = max(1, (_to_date(last_date) - _to_date(first_date)).days + 1)
            except Exception:
                days = 0

        flow_entry = addr_flows.get(mid)
        netflow_t = None
        if isinstance(flow_entry, dict):
            netflow_t = _parse_int(flow_entry.get("t", "0"))
        elif flow_entry is not None:
            netflow_t = _parse_int(flow_entry)

        netflow_yield = None
        if snapshot_yield is not None and netflow_t is not None:
            netflow_yield = last_wei - netflow_t

        diff = None
        if snapshot_yield is not None and netflow_yield is not None:
            diff = netflow_yield - snapshot_yield

        status = "unavailable"
        method = "unavailable"
        canonical_yield = 0
        pre_snapshot_carry = None
        pre_snapshot_residual = None
        tiny_par_drift_window = False

        if snapshot_yield is not None and netflow_yield is not None:
            pre_snapshot_meta = _get_pre_snapshot_carry_meta(
                decimals,
                snapshot_yield,
                diff,
                first_par,
                first_wei,
                last_par,
                last_wei,
                netflow_t,
                has_static_par_window,
            )
            if abs(diff) <= _get_tolerance(decimals):
                status = "verified"
                method = "netflow+snapshot"
                canonical_yield = netflow_yield
            elif pre_snapshot_meta is not None:
                status = "pre_snapshot_carry"
                method = "netflow+pre-snapshot-carry"
                canonical_yield = netflow_yield
                pre_snapshot_carry = pre_snapshot_meta["carry"]
                pre_snapshot_residual = pre_snapshot_meta["residual"]
                tiny_par_drift_window = pre_snapshot_meta["tinyParDriftWindow"]
            else:
                status = "mismatch"
                method = "snapshot-fallback"
                canonical_yield = snapshot_yield
        elif snapshot_yield is not None:
            status = "no_netflow"
            method = "snapshot-only"
            canonical_yield = snapshot_yield
        elif netflow_t is not None:
            status = "no_snapshot"
            method = "insufficient-history"
            canonical_yield = 0

        summary[status] = summary.get(status, 0) + 1

        payload = {
            "token": str(meta.get("token", "")).lower(),
            "symbol": str(meta.get("symbol", "UNK")),
            "decimals": decimals,
            "cumulativeYield": str(canonical_yield),
            "status": status,
            "method": method,
            "firstDate": first_date,
            "firstPar": str(first_par),
            "firstWei": str(first_wei),
            "lastDate": last_date,
            "lastPar": str(last_par),
            "lastWei": str(last_wei),
            "days": days,
            "isLatestSnapshot": bool(is_latest_snapshot),
            "hasStaticParWindow": bool(has_static_par_window),
        }
        if snapshot_yield is not None:
            payload["snapshotYield"] = str(snapshot_yield)
        if netflow_yield is not None:
            payload["netflowYield"] = str(netflow_yield)
        if diff is not None:
            payload["diff"] = str(diff)
        if pre_snapshot_carry is not None:
            payload["preSnapshotCarryYield"] = str(pre_snapshot_carry)
        if pre_snapshot_residual is not None:
            payload["preSnapshotCarryResidual"] = str(pre_snapshot_residual)
        if tiny_par_drift_window:
            payload["hasTinyParDriftWindow"] = True

        markets_out[mid] = payload

    return {
        "version": 1,
        "chain": chain,
        "address": address,
        "snapshotDate": latest_date,
        "generatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "summary": summary,
        "markets": markets_out,
    }


def _load_netflow_for_chain(chain):
    path = NETFLOW_DIR / f"{chain}.json"
    if not path.exists():
        return {"lastBlock": 0, "netflows": {}}
    payload = _read_json(path)
    return {
        "lastBlock": _parse_int(payload.get("lastBlock", 0)),
        "netflows": payload.get("netflows", {}) or {},
    }


def _discover_existing_addresses(output_dir, chain):
    chain_dir = output_dir / chain
    if not chain_dir.exists():
        return set()
    out = set()
    for path in chain_dir.glob("*.json"):
        address = path.stem.lower()
        if address.startswith("0x") and len(address) == 42:
            out.add(address)
    return out


def _update_manifest(output_dir, chain_meta):
    manifest_path = output_dir / "manifest.json"
    manifest = {"version": 1, "generatedAt": "", "chains": {}}
    if manifest_path.exists():
        try:
            manifest = _read_json(manifest_path)
        except Exception:
            manifest = {"version": 1, "generatedAt": "", "chains": {}}
    manifest["version"] = 1
    manifest["generatedAt"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    chains = manifest.get("chains", {})
    for chain, meta in chain_meta.items():
        chains[chain] = meta
    manifest["chains"] = chains
    _write_json(manifest_path, manifest)


def main():
    parser = argparse.ArgumentParser(description="Build local/private verified EARN ledger files")
    parser.add_argument("--chain", action="append", required=True,
                        help="Chain id (repeatable), e.g. --chain arbitrum --chain ethereum")
    parser.add_argument("--address", action="append", default=[],
                        help="Address to generate (repeatable). If omitted, use --all-addresses.")
    parser.add_argument("--all-addresses", action="store_true",
                        help="Generate files for all addresses seen in latest snapshot or netflow")
    parser.add_argument("--existing-addresses", action="store_true",
                        help="Refresh only addresses that already exist in the output directory")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR),
                        help="Output directory (default: data/earn-verified-ledger, kept local/private)")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    manifest = _read_json(SNAPSHOT_DIR / "manifest.json")

    requested_addresses = {a.lower() for a in args.address if a}
    if not requested_addresses and not args.all_addresses and not args.existing_addresses:
        raise SystemExit("Provide --address ..., --existing-addresses, or use --all-addresses")

    chain_meta = {}
    for chain in sorted({c.lower() for c in args.chain}):
        chain_dates = _collect_chain_snapshot_dates(manifest, chain)
        if not chain_dates:
            print(f"[{chain}] no snapshot dates found, skipping")
            continue

        snapshots = _load_chain_snapshots(chain, chain_dates)
        if not snapshots:
            print(f"[{chain}] no snapshot payloads loaded, skipping")
            continue

        latest_date, latest_chain_data = snapshots[-1]
        netflow_payload = _load_netflow_for_chain(chain)
        netflow_by_addr = netflow_payload["netflows"]
        existing_addresses = _discover_existing_addresses(output_dir, chain) if args.existing_addresses else set()

        if args.all_addresses:
            addresses = set(latest_chain_data.keys()) | set(netflow_by_addr.keys()) | requested_addresses | existing_addresses
        else:
            addresses = set(requested_addresses) | existing_addresses

        print(f"[{chain}] snapshot={latest_date} addresses={len(addresses)}")
        wrote = 0
        for address in sorted(addresses):
            if not (address.startswith("0x") and len(address) == 42):
                continue
            ledger = _build_address_ledger(address, chain, latest_date, snapshots, netflow_by_addr)
            if not ledger or not ledger.get("markets"):
                continue
            out_path = output_dir / chain / f"{address}.json"
            _write_json(out_path, ledger)
            wrote += 1

        chain_meta[chain] = {
            "snapshotDate": latest_date,
            "lastNetflowBlock": netflow_payload["lastBlock"],
            "addressCount": wrote,
        }
        print(f"[{chain}] wrote {wrote} address ledger files")

    if chain_meta:
        _update_manifest(output_dir, chain_meta)
        print(f"Updated manifest: {output_dir / 'manifest.json'}")


if __name__ == "__main__":
    main()
