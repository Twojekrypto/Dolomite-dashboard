#!/usr/bin/env python3
"""Audit DOLO holder/flow addresses against Etherscan nametag metadata.

The nametag endpoint is a paid Etherscan endpoint. This script is intentionally
safe when a free key is used: it still builds a ranked candidate report, records
the API access error, and never promotes heuristic addresses to confirmed CEX.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parent
LABELS_JS = ROOT / "dolo-address-labels.js"
HOLDERS_JSON = ROOT / "dolo_holders.json"
FLOWS_JSON = ROOT / "dolo_flows.json"
DEFAULT_OUTPUT = ROOT / "dolo_cex_label_audit.json"
ETHERSCAN_V2 = "https://api.etherscan.io/v2/api"

CEX_KEYWORDS = (
    "binance",
    "coinbase",
    "gate.io",
    "gateio",
    "bybit",
    "mexc",
    "kraken",
    "kucoin",
    "bitget",
    "bitpanda",
    "bitmart",
    "bingx",
    "okx",
    "okex",
    "crypto.com",
    "bitfinex",
    "huobi",
    "htx",
    "upbit",
    "bithumb",
    "phemex",
    "coinstore",
    "cobo",
    "falconx",
    "ceffu",
    "blofin",
    "revolut",
    "lbank",
    "bitvavo",
    "ascendex",
    "bitmex",
    "deribit",
    "exchange",
)

SKIP_LABEL_TYPES = {"protocol", "lp", "contract", "dead", "investor", "bot", "liquidator"}


def is_address(value: str) -> bool:
    return bool(re.fullmatch(r"0x[a-fA-F0-9]{40}", value or ""))


def load_labels() -> dict[str, dict[str, Any]]:
    text = LABELS_JS.read_text(encoding="utf-8")
    labels: dict[str, dict[str, Any]] = {}
    for match in re.finditer(r'"(0x[a-fA-F0-9]{40})"\s*:\s*\{([^}]+)\}', text):
        address = match.group(1).lower()
        body = match.group(2)
        label_match = re.search(r'label\s*:\s*"([^"]+)"', body)
        type_match = re.search(r'type\s*:\s*"([^"]+)"', body)
        source_match = re.search(r'source\s*:\s*"([^"]+)"', body)
        confidence_match = re.search(r'confidence\s*:\s*"([^"]+)"', body)
        label_type = type_match.group(1) if type_match else ""
        labels[address] = {
            "label": label_match.group(1) if label_match else "",
            "type": label_type,
            "source": source_match.group(1) if source_match else "",
            "confidence": confidence_match.group(1) if confidence_match else ("potential" if label_type == "watch" else "confirmed"),
        }
    return labels


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def add_candidate(candidates: dict[str, dict[str, Any]], address: str, reason: str, **fields: Any) -> None:
    if not is_address(address):
        return
    key = address.lower()
    row = candidates.setdefault(
        key,
        {
            "address": key,
            "reasons": [],
            "maxAbsFlow": 0.0,
            "currentBalance": 0.0,
            "txCount": 0,
            "chains": set(),
            "periods": set(),
        },
    )
    if reason not in row["reasons"]:
        row["reasons"].append(reason)
    if "balance" in fields:
        row["currentBalance"] = max(row["currentBalance"], float(fields["balance"] or 0))
    if "net_flow" in fields:
        row["maxAbsFlow"] = max(row["maxAbsFlow"], abs(float(fields["net_flow"] or 0)))
    if "tx_count" in fields:
        row["txCount"] += int(fields["tx_count"] or 0)
    if "chain" in fields and fields["chain"]:
        row["chains"].add(str(fields["chain"]))
    if "period" in fields and fields["period"]:
        row["periods"].add(str(fields["period"]))


def collect_candidates(
    labels: dict[str, dict[str, Any]],
    holder_min: float,
    flow_min: float,
    max_candidates: int,
    include_known_cex: bool,
) -> list[dict[str, Any]]:
    holders = load_json(HOLDERS_JSON).get("holders", [])
    flows = load_json(FLOWS_JSON)
    candidates: dict[str, dict[str, Any]] = {}

    for holder in holders:
        address = str(holder.get("address", "")).lower()
        balance = float(holder.get("balance") or 0)
        info = labels.get(address, {})
        label_type = info.get("type", "")
        if label_type in SKIP_LABEL_TYPES:
            continue
        if label_type == "cex" and not include_known_cex:
            continue
        if balance >= holder_min or label_type == "watch":
            add_candidate(
                candidates,
                address,
                "top-holder" if balance >= holder_min else "watch-label",
                balance=balance,
                chain=",".join(holder.get("chains") or []),
            )

    for period, chains in flows.get("periods", {}).items():
        if not isinstance(chains, dict):
            continue
        for chain, payload in chains.items():
            if not isinstance(payload, dict):
                continue
            for side in ("accumulators", "sellers"):
                for row in payload.get(side) or []:
                    address = str(row.get("address", "")).lower()
                    info = labels.get(address, {})
                    label_type = info.get("type", "")
                    if label_type in SKIP_LABEL_TYPES:
                        continue
                    if label_type == "cex" and not include_known_cex:
                        continue
                    net_flow = float(row.get("net_flow") or 0)
                    if abs(net_flow) >= flow_min or label_type == "watch":
                        add_candidate(
                            candidates,
                            address,
                            f"{period}-{chain}-{side}",
                            net_flow=net_flow,
                            tx_count=row.get("tx_count"),
                            balance=row.get("balance"),
                            chain=chain,
                            period=period,
                        )

    rows = []
    for row in candidates.values():
        label_info = labels.get(row["address"], {})
        # Rank by economic relevance. Transaction count is only a small tie-breaker:
        # high-frequency bots can have huge counts but are not automatically CEXs.
        score = row["currentBalance"] + row["maxAbsFlow"] * 2 + min(row["txCount"], 1000) * 100
        rows.append(
            {
                "address": row["address"],
                "label": label_info.get("label", ""),
                "labelType": label_info.get("type", ""),
                "labelSource": label_info.get("source", ""),
                "currentBalance": round(row["currentBalance"], 6),
                "maxAbsFlow": round(row["maxAbsFlow"], 6),
                "txCount": row["txCount"],
                "chains": sorted(row["chains"]),
                "periods": sorted(row["periods"], key=lambda p: ["1d", "7d", "30d", "90d", "180d", "all"].index(p) if p in ["1d", "7d", "30d", "90d", "180d", "all"] else 99),
                "reasons": row["reasons"],
                "score": round(score, 6),
            }
        )

    rows.sort(key=lambda item: item["score"], reverse=True)
    return rows[:max_candidates]


def is_cex_metadata(meta: dict[str, Any]) -> bool:
    haystack_parts = [
        str(meta.get("nametag") or ""),
        str(meta.get("url") or ""),
        str(meta.get("shortdescription") or ""),
        " ".join(str(x) for x in meta.get("labels") or []),
        " ".join(str(x) for x in meta.get("labels_slug") or []),
    ]
    haystack = " ".join(haystack_parts).lower()
    return any(keyword in haystack for keyword in CEX_KEYWORDS)


def clean_suggestion_label(meta: dict[str, Any]) -> str:
    nametag = str(meta.get("nametag") or "").strip()
    return nametag[:80] if nametag else ""


def fetch_nametag(address: str, api_key: str, session: requests.Session) -> tuple[dict[str, Any] | None, str | None]:
    params = {
        "chainid": "1",
        "module": "nametag",
        "action": "getaddresstag",
        "address": address,
        "apikey": api_key,
    }
    try:
        response = session.get(ETHERSCAN_V2, params=params, timeout=30)
        payload = response.json()
    except Exception as exc:
        return None, f"request_failed: {exc}"

    if payload.get("status") != "1":
        message = str(payload.get("message") or payload.get("result") or "unknown_error")
        return None, message

    result = payload.get("result")
    if not isinstance(result, list) or not result:
        return None, None
    first = result[0]
    if not isinstance(first, dict):
        return None, "unexpected_result_shape"
    return first, None


def run_api_audit(candidates: list[dict[str, Any]], api_key: str, delay: float) -> dict[str, Any]:
    session = requests.Session()
    confirmed: list[dict[str, Any]] = []
    non_cex_tagged: list[dict[str, Any]] = []
    no_tag: list[str] = []
    errors: dict[str, str] = {}

    for idx, candidate in enumerate(candidates, start=1):
        address = candidate["address"]
        meta, error = fetch_nametag(address, api_key, session)
        if error:
            errors[address] = error
            # Stop early on plan/permission failures; repeating 100 times adds no signal.
            if "pro" in error.lower() or "invalid" in error.lower() or "notok" in error.lower():
                break
        elif not meta:
            no_tag.append(address)
        elif is_cex_metadata(meta):
            confirmed.append(
                {
                    **candidate,
                    "suggestedLabel": clean_suggestion_label(meta),
                    "etherscan": {
                        "nametag": meta.get("nametag"),
                        "url": meta.get("url"),
                        "labels": meta.get("labels"),
                        "labels_slug": meta.get("labels_slug"),
                        "lastupdatedtimestamp": meta.get("lastupdatedtimestamp"),
                    },
                }
            )
        else:
            non_cex_tagged.append(
                {
                    **candidate,
                    "etherscan": {
                        "nametag": meta.get("nametag"),
                        "url": meta.get("url"),
                        "labels": meta.get("labels"),
                        "labels_slug": meta.get("labels_slug"),
                    },
                }
            )

        if idx < len(candidates):
            time.sleep(delay)

    return {
        "confirmedCexSuggestions": confirmed,
        "nonCexTagged": non_cex_tagged,
        "noPublicTag": no_tag,
        "errors": errors,
        "queriedCount": len(confirmed) + len(non_cex_tagged) + len(no_tag) + len(errors),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit DOLO CEX labels with Etherscan nametag metadata.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Path to write JSON report.")
    parser.add_argument("--holder-min", type=float, default=float(os.environ.get("DOLO_CEX_AUDIT_HOLDER_MIN", "100000")))
    parser.add_argument("--flow-min", type=float, default=float(os.environ.get("DOLO_CEX_AUDIT_FLOW_MIN", "100000")))
    parser.add_argument("--max-candidates", type=int, default=int(os.environ.get("DOLO_CEX_AUDIT_MAX_CANDIDATES", "120")))
    parser.add_argument("--delay", type=float, default=float(os.environ.get("DOLO_CEX_AUDIT_DELAY", "0.55")))
    parser.add_argument("--include-known-cex", action="store_true")
    parser.add_argument("--no-api", action="store_true", help="Build only the local candidate report.")
    args = parser.parse_args()

    labels = load_labels()
    candidates = collect_candidates(
        labels,
        holder_min=args.holder_min,
        flow_min=args.flow_min,
        max_candidates=args.max_candidates,
        include_known_cex=args.include_known_cex,
    )

    existing_cex = sorted(
        [
            {"address": address, "label": info.get("label", ""), "source": info.get("source", "")}
            for address, info in labels.items()
            if info.get("type") == "cex"
        ],
        key=lambda row: (row["label"], row["address"]),
    )
    watch_labels = sorted(
        [
            {"address": address, "label": info.get("label", ""), "source": info.get("source", "")}
            for address, info in labels.items()
            if info.get("type") == "watch"
        ],
        key=lambda row: (row["label"], row["address"]),
    )

    api_key = os.environ.get("ETHERSCAN_API_KEY", "").strip()
    api_report: dict[str, Any] = {
        "confirmedCexSuggestions": [],
        "nonCexTagged": [],
        "noPublicTag": [],
        "errors": {},
        "queriedCount": 0,
    }
    api_status = "skipped"
    if args.no_api:
        api_status = "disabled_by_flag"
    elif not api_key:
        api_status = "missing_ETHERSCAN_API_KEY"
    else:
        api_status = "attempted"
        api_report = run_api_audit(candidates, api_key, delay=args.delay)
        if api_report["errors"] and api_report["queriedCount"] <= 2:
            api_status = "blocked_or_unavailable"
        elif api_report["queriedCount"]:
            api_status = "completed"

    report = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "sourceFiles": {
            "labels": str(LABELS_JS.name),
            "holders": str(HOLDERS_JSON.name),
            "flows": str(FLOWS_JSON.name),
        },
        "filters": {
            "holderMin": args.holder_min,
            "flowMin": args.flow_min,
            "maxCandidates": args.max_candidates,
            "includeKnownCex": args.include_known_cex,
        },
        "api": {
            "provider": "Etherscan V2 nametag",
            "status": api_status,
            "note": "Nametag metadata is an Etherscan Pro Plus endpoint; free keys may return an access error.",
            **api_report,
        },
        "existingCexLabels": existing_cex,
        "watchLabels": watch_labels,
        "rankedCandidates": candidates,
    }

    output = Path(args.output)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"DOLO CEX label audit written to {output}")
    print(f"  existing CEX labels: {len(existing_cex)}")
    print(f"  watch labels: {len(watch_labels)}")
    print(f"  ranked candidates: {len(candidates)}")
    print(f"  api status: {api_status}")
    print(f"  confirmed CEX suggestions: {len(api_report['confirmedCexSuggestions'])}")
    if api_report["errors"]:
        first_addr, first_error = next(iter(api_report["errors"].items()))
        print(f"  first API error: {first_addr} -> {first_error}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
