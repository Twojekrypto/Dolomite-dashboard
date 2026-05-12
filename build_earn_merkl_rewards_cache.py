#!/usr/bin/env python3
"""Build a static MERKL rewards fallback for Earn wallets.

The dashboard still tries MERKL live first. This cache is a browser-safe fallback
for known Earn addresses when api.merkl.xyz is blocked by the client.
"""

from __future__ import annotations

import argparse
import json
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


ROOT = Path(__file__).resolve().parent
LEDGER_DIR = ROOT / "data" / "earn-verified-ledger"
OUT_DIR = ROOT / "data" / "earn-merkl-rewards"

CHAIN_IDS = {
    "ethereum": 1,
}

ELIGIBLE_MARKETS = {
    "ethereum": {
        "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d": "USD1",
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
    },
}

CAMPAIGN_TOKEN_MAP = {
    "ethereum": {
        "576387d3d84237f5": "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d",
        "2ed15ca6f6a47991": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    },
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def decimal_number(raw: Optional[Any], decimals: int) -> float:
    try:
        return int(str(raw or "0")) / (10 ** int(decimals or 18))
    except (TypeError, ValueError, OverflowError):
        return 0.0


def normalize_campaign_id(value: Any) -> str:
    return str(value or "").strip().lower().removeprefix("0x")


def normalize_evm_address(value: Any) -> str:
    match = re.search(r"0x[a-fA-F0-9]{40}", str(value or ""))
    return match.group(0).lower() if match else ""


def resolve_reason_token(chain: str, reason_key: str, reward: dict[str, Any]) -> str:
    if reason_key.startswith("Dolomite_"):
        return normalize_evm_address(reason_key.replace("Dolomite_", "", 1))
    if not reason_key.startswith("MultiLogPerAdditionalParam_"):
        return ""
    direct = normalize_evm_address(reward.get("mainParameter"))
    if direct:
        return direct
    campaign_id = normalize_campaign_id(reward.get("mainParameter"))
    return CAMPAIGN_TOKEN_MAP.get(chain, {}).get(campaign_id, "")


def reward_bucket(decimals: int, token: Any) -> dict[str, Any]:
    return {
        "accumulated": 0.0,
        "unclaimed": 0.0,
        "decimals": decimals,
        "token": token,
        "perToken": {},
        "unclaimedPerToken": {},
        "perAccountToken": {},
        "unclaimedPerAccountToken": {},
        "assignedPerToken": {},
        "assignedUnclaimedPerToken": {},
    }


def parse_merkl_rewards(chain: str, payload: dict[str, Any]) -> dict[str, Any]:
    numeric_id = str(CHAIN_IDS[chain])
    chain_data = payload.get(numeric_id) or {}
    rewards: dict[str, Any] = {}
    unresolved_symbols: set[str] = set()

    for reasons in (chain_data.get("campaignData") or {}).values():
        if not isinstance(reasons, dict):
            continue
        for reason_key, reward in reasons.items():
            if not isinstance(reward, dict):
                continue
            token_addr = resolve_reason_token(chain, str(reason_key), reward)
            symbol = str(reward.get("symbol") or "").strip()
            decimals = int(reward.get("decimals") or 18)
            if not symbol:
                continue
            if not token_addr:
                unresolved_symbols.add(symbol)
                continue
            bucket = rewards.setdefault(symbol, reward_bucket(decimals, reward.get("token")))
            amount = decimal_number(reward.get("accumulated"), decimals)
            unclaimed = decimal_number(reward.get("unclaimed"), decimals)
            bucket["accumulated"] += amount
            bucket["unclaimed"] += unclaimed
            bucket["perToken"][token_addr] = bucket["perToken"].get(token_addr, 0.0) + amount
            bucket["unclaimedPerToken"][token_addr] = bucket["unclaimedPerToken"].get(token_addr, 0.0) + unclaimed

            account_match = re.match(r"^MultiLogPerAdditionalParam_accountNumber_([^_]+)_", str(reason_key))
            if account_match:
                account_number = account_match.group(1)
                account_bucket = bucket["perAccountToken"].setdefault(account_number, {})
                account_bucket[token_addr] = account_bucket.get(token_addr, 0.0) + amount
                account_unclaimed_bucket = bucket["unclaimedPerAccountToken"].setdefault(account_number, {})
                account_unclaimed_bucket[token_addr] = account_unclaimed_bucket.get(token_addr, 0.0) + unclaimed
                bucket["assignedPerToken"][token_addr] = bucket["assignedPerToken"].get(token_addr, 0.0) + amount
                bucket["assignedUnclaimedPerToken"][token_addr] = bucket["assignedUnclaimedPerToken"].get(token_addr, 0.0) + unclaimed

    for token_data in (chain_data.get("tokenData") or {}).values():
        if not isinstance(token_data, dict):
            continue
        symbol = str(token_data.get("symbol") or "").strip()
        if not symbol or symbol not in unresolved_symbols:
            continue
        decimals = int(token_data.get("decimals") or 18)
        bucket = rewards.setdefault(symbol, reward_bucket(decimals, token_data.get("token")))
        bucket["accumulated"] = max(bucket.get("accumulated") or 0.0, decimal_number(token_data.get("accumulated"), decimals))
        bucket["unclaimed"] = max(bucket.get("unclaimed") or 0.0, decimal_number(token_data.get("unclaimed"), decimals))

    return {
        symbol: data
        for symbol, data in rewards.items()
        if (data.get("accumulated") or 0) > 0 or (data.get("unclaimed") or 0) > 0
    }


def ledger_addresses(chain: str) -> list[str]:
    eligible_tokens = set(ELIGIBLE_MARKETS.get(chain, {}))
    chain_dir = LEDGER_DIR / chain
    addresses: list[str] = []
    for path in sorted(chain_dir.glob("0x*.json")):
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            print(f"warning: skipping unreadable ledger {path}: {exc}")
            continue
        markets = payload.get("markets") or {}
        market_values = markets.values() if isinstance(markets, dict) else markets
        has_eligible_market = any(
            isinstance(market, dict) and str(market.get("token") or "").lower() in eligible_tokens
            for market in market_values
        )
        if has_eligible_market:
            addresses.append(str(payload.get("address") or path.stem).lower())
    return addresses


def fetch_merkl(chain: str, address: str, timeout: int, retries: int) -> dict[str, Any]:
    numeric_id = CHAIN_IDS[chain]
    url = f"https://api.merkl.xyz/v3/rewards?chainIds={numeric_id}&user={address}"
    request = urllib.request.Request(
        url,
        headers={
            "accept": "application/json",
            "user-agent": "DolomiteDashboardMerklCache/1.0",
        },
    )
    last_error: Optional[Exception] = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in {429, 500, 502, 503, 504} or attempt >= retries:
                raise
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt >= retries:
                raise
        time.sleep(min(2.0 * attempt, 8.0))
    raise RuntimeError(f"MERKL request failed after {retries} attempts: {last_error}")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


def build_cache(chain: str, limit: Optional[int], only_address: Optional[str], sleep_seconds: float, timeout: int, retries: int) -> dict[str, Any]:
    generated_at = utc_now()
    addresses = [only_address.lower()] if only_address else ledger_addresses(chain)
    if limit is not None:
        addresses = addresses[:limit]

    out_chain_dir = OUT_DIR / chain
    out_chain_dir.mkdir(parents=True, exist_ok=True)

    failures: list[dict[str, str]] = []
    reward_addresses: set[str] = set()
    successful_empty: set[str] = set()

    for index, address in enumerate(addresses, start=1):
        try:
            payload = fetch_merkl(chain, address, timeout, retries)
            rewards = parse_merkl_rewards(chain, payload)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
            failures.append({"address": address, "error": str(exc)})
            print(f"[{index}/{len(addresses)}] {address}: failed ({exc})")
        else:
            if rewards:
                reward_addresses.add(address)
                write_json(
                    out_chain_dir / f"{address}.json",
                    {
                        "version": 1,
                        "chain": chain,
                        "address": address,
                        "generatedAt": generated_at,
                        "source": "merkl-v3-rewards",
                        "rewards": rewards,
                    },
                )
                print(f"[{index}/{len(addresses)}] {address}: {', '.join(rewards)}")
            else:
                successful_empty.add(address)
                stale_path = out_chain_dir / f"{address}.json"
                if stale_path.exists():
                    stale_path.unlink()
                print(f"[{index}/{len(addresses)}] {address}: no rewards")
        if sleep_seconds > 0 and index < len(addresses):
            time.sleep(sleep_seconds)

    if only_address is None:
        current_scope = set(addresses)
        for stale_path in out_chain_dir.glob("0x*.json"):
            if stale_path.stem in current_scope and stale_path.stem not in reward_addresses and stale_path.stem in successful_empty:
                stale_path.unlink(missing_ok=True)

    manifest = {
        "version": 1,
        "generatedAt": generated_at,
        "chains": {
            chain: {
                "eligibleAddressCount": len(addresses),
                "rewardAddressCount": len(reward_addresses),
                "failureCount": len(failures),
                "markets": ELIGIBLE_MARKETS.get(chain, {}),
            }
        },
        "failures": {chain: failures[:25]},
    }
    write_json(OUT_DIR / "manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Build static MERKL rewards cache for Earn")
    parser.add_argument("--chain", choices=sorted(CHAIN_IDS), default="ethereum")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--address")
    parser.add_argument("--sleep", type=float, default=0.25)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--retries", type=int, default=3)
    args = parser.parse_args()

    manifest = build_cache(args.chain, args.limit, args.address, args.sleep, args.timeout, args.retries)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0 if manifest["chains"][args.chain]["failureCount"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
