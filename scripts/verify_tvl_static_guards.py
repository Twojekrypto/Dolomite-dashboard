#!/usr/bin/env python3
"""Pre-deploy TVL guards for assets that must not disappear from Pages."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WLFI_MIN_USD = 10_000_000


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"TVL static guard failed: {message}")


def numeric(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def main() -> int:
    route = (ROOT / "tvl" / "index.html").read_text(encoding="utf-8")
    preview = (ROOT / "tvl-preview.html").read_text(encoding="utf-8")
    tvl_data = json.loads((ROOT / "dolomite_tvl.json").read_text(encoding="utf-8"))

    latest = (tvl_data.get("tokensInUsd") or [{}])[-1].get("tokens", {})
    chain_tokens = tvl_data.get("chainTokensInUsd", {})
    wlfi_global = numeric(latest.get("WLFI"))
    wlfi_ethereum = numeric(chain_tokens.get("Ethereum", {}).get("WLFI"))

    require(wlfi_global >= WLFI_MIN_USD, f"global WLFI too small or missing (${wlfi_global:,.0f})")
    require(wlfi_ethereum >= WLFI_MIN_USD, f"Ethereum WLFI too small or missing (${wlfi_ethereum:,.0f})")

    require("stale-token-fix-20260528" in route, "TVL route is missing current preview cache-bust")
    require("Supply Over Time" in preview, "TVL chart label regressed")
    require("TVL Over Time" not in preview, "old TVL chart label returned")
    require("WLFI.2c1e57c98e3cad7282c40941b36ee606.svg" in preview, "WLFI icon mapping missing")
    require(
        "const tokens = officialTokens[chain] || llamaTokens[chain];" in preview,
        "token composition no longer prefers official Dolomite tokens",
    )
    require(
        "staleKeys.has(chainKey(chain)) && llamaTokens[chain]" not in preview,
        "stale-chain fallback can hide official token composition",
    )

    print(f"TVL static guards passed. WLFI=${wlfi_global:,.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
