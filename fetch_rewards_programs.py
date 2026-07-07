#!/usr/bin/env python3
"""Fetch Dolomite reward-program data for the Rewards dashboard tab.

Sources:
- Merkl v4 opportunities API (`mainProtocolId=dolomite`) — APR, TVL, daily
  rewards, campaign windows, and reward tokens per program.
- Dolomite oDOLO liquidity-mining metadata — weekly oDOLO allocation per
  supply market; APR is estimated as weeklyAlloc × DOLO price × 52 / supplyUsd
  using `dolo_price.json` and `data/supply-health/latest.json`.

Each run appends a daily {apr, tvl, dailyRewards} snapshot per program to
`data/rewards-programs-history.json` so 7d/30d trends accumulate over time.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import request
from urllib.error import HTTPError, URLError


MERKL_OPPORTUNITIES_URL = "https://api.merkl.xyz/v4/opportunities?mainProtocolId=dolomite&items=100"
ODOLO_METADATA_URL = "https://api.dolomite.io/liquidity-mining/odolo/metadata"

CHAIN_ID_NAMES = {
    1: "ethereum",
    42161: "arbitrum",
    80094: "berachain",
    5000: "mantle",
    196: "xlayer",
    3637: "botanix",
    1101: "polygon_zkevm",
    8453: "base",
    56: "bnb",
}

HISTORY_MAX_SNAPSHOTS = 400

# Botanix is being sunset (July 2026) — skip its oDOLO allocations.
EXCLUDED_CHAINS = {"botanix"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def float_or_zero(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if parsed == parsed and parsed not in (float("inf"), float("-inf")) else 0.0


def round_or_none(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def get_json(url: str, retries: int = 3) -> Any:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        req = request.Request(url, headers={"User-Agent": "DolomiteDashboardRewards/1.0"})
        try:
            with request.urlopen(req, timeout=60) as res:
                return json.loads(res.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.5 * attempt)
    raise RuntimeError(f"Request failed after {retries} attempts: {url}: {last_error}") from last_error


def load_json(path: Path) -> Optional[Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def build_symbol_map(supply_history_manifest: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """chain -> tokenId -> symbol, from the supply-history manifest."""
    result: Dict[str, Dict[str, str]] = {}
    for chain_entry in (supply_history_manifest or {}).get("chains") or []:
        chain = str(chain_entry.get("chain") or "")
        if not chain:
            continue
        tokens = {}
        for token in chain_entry.get("tokens") or []:
            token_id = str(token.get("tokenId") or "").lower()
            symbol = str(token.get("symbol") or "")
            if token_id and symbol:
                tokens[token_id] = symbol
        result[chain] = tokens
    return result


def build_supply_usd_map(supply_health: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """"chain:tokenId" -> {supplyUsd, wallets, symbol} from supply-health latest."""
    result: Dict[str, Dict[str, Any]] = {}
    for market in (supply_health or {}).get("markets") or []:
        key = f"{market.get('chain')}:{str(market.get('tokenId') or '').lower()}"
        result[key] = {
            "supplyUsd": float_or_zero(market.get("supplyUsd")),
            "wallets": market.get("wallets"),
            "symbol": market.get("symbol"),
        }
    return result


def int_or_none(value: Any) -> Optional[int]:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def normalize_merkl_program(opportunity: Dict[str, Any]) -> Dict[str, Any]:
    chain_id = int(opportunity.get("chainId") or 0)
    reward_tokens = []
    for breakdown in (opportunity.get("rewardsRecord") or {}).get("breakdowns") or []:
        token = breakdown.get("token") or {}
        symbol = token.get("symbol")
        if symbol and symbol not in reward_tokens:
            reward_tokens.append(symbol)
    market_tokens = []
    for token in opportunity.get("tokens") or []:
        symbol = token.get("symbol")
        if symbol and symbol not in market_tokens:
            market_tokens.append(symbol)
    return {
        "key": f"merkl:{opportunity.get('id')}",
        "provider": "Merkl",
        "programId": str(opportunity.get("id") or ""),
        "identifier": opportunity.get("identifier"),
        "name": opportunity.get("name"),
        "description": opportunity.get("description"),
        "status": opportunity.get("status"),
        "action": opportunity.get("action"),
        "chainId": chain_id,
        "chain": CHAIN_ID_NAMES.get(chain_id, str(chain_id)),
        "apr": round_or_none(float_or_zero(opportunity.get("apr")), 4),
        "totalApr": round_or_none(float_or_zero(opportunity.get("totalApr")), 4),
        "nativeApr": round_or_none(float_or_zero(opportunity.get("nativeApr")), 4),
        "aprSource": "merkl",
        "tvlUsd": round_or_none(float_or_zero(opportunity.get("tvl")), 2),
        "dailyRewardsUsd": round_or_none(float_or_zero(opportunity.get("dailyRewards")), 2),
        "rewardTokens": reward_tokens,
        "marketTokens": market_tokens,
        "liveCampaigns": int(opportunity.get("liveCampaigns") or 0),
        "campaignStart": int_or_none(opportunity.get("earliestCampaignStart")),
        "campaignEnd": int_or_none(opportunity.get("latestCampaignEnd")),
        "depositUrl": opportunity.get("depositUrl"),
        "explorerAddress": opportunity.get("explorerAddress"),
    }


def build_odolo_programs(
    metadata: Dict[str, Any],
    symbol_map: Dict[str, Dict[str, str]],
    supply_map: Dict[str, Dict[str, Any]],
    dolo_price_usd: float,
) -> List[Dict[str, Any]]:
    programs: List[Dict[str, Any]] = []
    epoch_rewards = float_or_zero(metadata.get("currentEpochRewards"))
    epoch_index = metadata.get("currentEpochIndex")
    epoch_start = int_or_none(metadata.get("currentEpochStartTimestamp"))
    for chain_id_text, weights in (metadata.get("allChainWeights") or {}).items():
        chain_id = int(chain_id_text) if str(chain_id_text).isdigit() else 0
        chain = CHAIN_ID_NAMES.get(chain_id, str(chain_id))
        if chain in EXCLUDED_CHAINS or not isinstance(weights, dict):
            continue
        for token_id, weekly_alloc_text in weights.items():
            weekly_alloc = float_or_zero(weekly_alloc_text)
            if weekly_alloc <= 0:
                continue
            token_id = str(token_id).lower()
            market = supply_map.get(f"{chain}:{token_id}") or {}
            symbol = market.get("symbol") or symbol_map.get(chain, {}).get(token_id) or f"{token_id[:6]}…{token_id[-4:]}"
            supply_usd = float_or_zero(market.get("supplyUsd"))
            weekly_usd = weekly_alloc * dolo_price_usd if dolo_price_usd > 0 else None
            apr = None
            if weekly_usd is not None and supply_usd > 0:
                apr = weekly_usd * 52.0 / supply_usd * 100.0
            programs.append({
                "key": f"odolo:{chain}:{token_id}",
                "provider": "Dolomite",
                "programId": f"odolo-{chain}-{token_id}",
                "name": f"Supply {symbol} · oDOLO",
                "description": f"Weekly oDOLO emissions for supplying {symbol} on Dolomite ({chain}).",
                "status": "LIVE",
                "action": "LEND",
                "chainId": chain_id,
                "chain": chain,
                "apr": round_or_none(apr, 4),
                "aprSource": "estimated",
                "tvlUsd": round_or_none(supply_usd, 2) if supply_usd > 0 else None,
                "dailyRewardsUsd": round_or_none(weekly_usd / 7.0, 2) if weekly_usd is not None else None,
                "rewardTokens": ["oDOLO"],
                "marketTokens": [symbol],
                "weeklyRewardTokens": round(weekly_alloc, 2),
                "eligibleWallets": market.get("wallets"),
                "epochIndex": epoch_index,
                "epochStart": epoch_start,
                "epochRewardsTotal": round(epoch_rewards, 2) if epoch_rewards > 0 else None,
                "tokenId": token_id,
            })
    programs.sort(key=lambda program: -(program.get("tvlUsd") or 0))
    return programs


def update_history(history: Dict[str, Any], programs: List[Dict[str, Any]], now_ts: int) -> Dict[str, Any]:
    snapshots = history.get("programs") if isinstance(history.get("programs"), dict) else {}
    day_ts = (now_ts // 86400) * 86400
    for program in programs:
        key = program["key"]
        row = [
            day_ts,
            program.get("apr"),
            program.get("tvlUsd"),
            program.get("dailyRewardsUsd"),
        ]
        rows = [item for item in (snapshots.get(key) or []) if isinstance(item, list) and len(item) >= 4]
        if rows and int(rows[-1][0]) == day_ts:
            rows[-1] = row
        else:
            rows.append(row)
        snapshots[key] = rows[-HISTORY_MAX_SNAPSHOTS:]
    return {"schemaVersion": 1, "programs": snapshots}


def trend_from_history(snapshots: Dict[str, Any], key: str, field_index: int, now_ts: int, days: int) -> Optional[float]:
    rows = snapshots.get(key) or []
    target = ((now_ts // 86400) * 86400) - days * 86400
    tolerance = max(2, int(days * 0.2)) * 86400
    best: Optional[List[Any]] = None
    best_distance: Optional[int] = None
    for row in rows:
        distance = abs(int(row[0]) - target)
        if distance <= tolerance and (best_distance is None or distance < best_distance):
            best = row
            best_distance = distance
    if not best:
        return None
    past = best[field_index]
    if past is None:
        return None
    current = rows[-1][field_index] if rows else None
    if current is None:
        return None
    return float(current) - float(past)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Dolomite rewards program data")
    parser.add_argument("--out", default="rewards_programs.json")
    parser.add_argument("--history-out", default="data/rewards-programs-history.json")
    parser.add_argument("--supply-health", default="data/supply-health/latest.json")
    parser.add_argument("--supply-history-manifest", default="data/supply-history/manifest.json")
    parser.add_argument("--dolo-price", default="dolo_price.json")
    args = parser.parse_args()

    generated_at = utc_now()
    now_ts = int(generated_at.timestamp())
    errors: List[str] = []

    merkl_programs: List[Dict[str, Any]] = []
    try:
        opportunities = get_json(MERKL_OPPORTUNITIES_URL)
        if not isinstance(opportunities, list):
            raise RuntimeError("Merkl opportunities response is not a list")
        merkl_programs = [normalize_merkl_program(item) for item in opportunities]
        print(f"Merkl: {len(merkl_programs)} programs", flush=True)
    except RuntimeError as exc:
        errors.append(f"merkl: {exc}"[:500])
        print(f"Merkl fetch failed: {exc}", file=sys.stderr, flush=True)

    symbol_map = build_symbol_map(load_json(Path(args.supply_history_manifest)))
    supply_map = build_supply_usd_map(load_json(Path(args.supply_health)))
    dolo_price = float_or_zero((load_json(Path(args.dolo_price)) or {}).get("price"))

    odolo_programs: List[Dict[str, Any]] = []
    try:
        metadata = (get_json(ODOLO_METADATA_URL) or {}).get("metadata") or {}
        odolo_programs = build_odolo_programs(metadata, symbol_map, supply_map, dolo_price)
        print(f"oDOLO: {len(odolo_programs)} live market allocations", flush=True)
    except RuntimeError as exc:
        errors.append(f"odolo: {exc}"[:500])
        print(f"oDOLO fetch failed: {exc}", file=sys.stderr, flush=True)

    programs = merkl_programs + odolo_programs
    if not programs:
        print("No programs fetched — refusing to overwrite existing data", file=sys.stderr)
        return 1

    history_path = Path(args.history_out)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history = update_history(load_json(history_path) or {}, programs, now_ts)
    snapshots = history["programs"]
    for program in programs:
        program["aprTrend7d"] = round_or_none(trend_from_history(snapshots, program["key"], 1, now_ts, 7), 4)
        program["aprTrend30d"] = round_or_none(trend_from_history(snapshots, program["key"], 1, now_ts, 30), 4)
        program["tvlTrend7dUsd"] = round_or_none(trend_from_history(snapshots, program["key"], 2, now_ts, 7), 2)

    payload = {
        "schemaVersion": 1,
        "generatedAt": generated_at.isoformat().replace("+00:00", "Z"),
        "doloPriceUsd": dolo_price if dolo_price > 0 else None,
        "errors": errors,
        "programs": programs,
    }
    Path(args.out).write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")
    history["updatedAt"] = generated_at.isoformat().replace("+00:00", "Z")
    history_path.write_text(json.dumps(history, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(programs)} programs to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
