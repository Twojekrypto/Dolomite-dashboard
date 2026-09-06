#!/usr/bin/env python3
"""Audit DOLO holder/flow addresses against Etherscan and DeBank labels.

The nametag endpoint is a paid Etherscan endpoint. This script is intentionally
safe when a free key is used: it still builds a ranked candidate report, records
the API access error, and never promotes heuristic addresses to confirmed CEX.
"""
from __future__ import annotations

import argparse
import html
import json
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parent
LABELS_JS = ROOT / "dolo-address-labels.js"
HOLDERS_JSON = ROOT / "dolo_holders.json"
FLOWS_JSON = ROOT / "dolo_flows.json"
DEFAULT_OUTPUT = ROOT / "dolo_cex_label_audit.json"
DEFAULT_ROTATION_STATE = ROOT / "dolo_cex_label_audit_state.json"
ETHERSCAN_V2 = "https://api.etherscan.io/v2/api"
ETHERSCAN_ADDRESS_PAGE = "https://etherscan.io/address/{address}"
PUBLIC_EXPLORER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
DEBANK_PROFILE_PAGE = "https://debank.com/profile/{address}"
DEBANK_RENDER_TIMEOUT = int(os.environ.get("DOLO_CEX_DEBANK_RENDER_TIMEOUT", "25"))
DEBANK_VIRTUAL_TIME_BUDGET_MS = int(
    os.environ.get("DOLO_CEX_DEBANK_VIRTUAL_TIME_BUDGET_MS", "7000")
)

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
ROTATION_STATE_SCHEMA_VERSION = 1


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


class _DeBankCexTagParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.nametag = ""

    def handle_starttag(self, _tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.nametag:
            return
        values = {str(key).lower(): (value or "") for key, value in attrs}
        classes = set(values.get("class", "").split())
        if {"db-user-tag", "is-cex"}.issubset(classes):
            self.nametag = html.unescape(values.get("title", "")).strip()


def extract_debank_cex_metadata(page_html: str) -> dict[str, Any]:
    """Extract only DeBank's explicit CEX entity badge.

    Text such as "Funded By Coinbase" is deliberately ignored because it
    describes transaction provenance, not ownership of the inspected wallet.
    """
    parser = _DeBankCexTagParser()
    try:
        parser.feed(page_html or "")
    except (TypeError, ValueError):
        return {}
    nametag = parser.nametag
    if not nametag:
        return {}
    metadata = {
        "nametag": nametag,
        "url": "",
        "shortdescription": "",
        "labels": ["cex"],
        "labels_slug": ["cex"],
    }
    # `is-cex` is DeBank's explicit entity classification. Do not limit it to
    # our current keyword vocabulary or newly listed exchanges would be lost.
    return metadata


def find_chrome_binary() -> str:
    configured = os.environ.get("DOLO_CEX_DEBANK_CHROME", "").strip()
    candidates = [
        configured,
        shutil.which("google-chrome"),
        shutil.which("google-chrome-stable"),
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ]
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate
    return ""


def fetch_debank_cex_metadata(
    address: str,
    chrome_binary: str,
) -> tuple[dict[str, Any] | None, str | None]:
    if not chrome_binary:
        return None, "chrome_unavailable"
    try:
        proc = subprocess.run(
            [
                chrome_binary,
                "--headless=new",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                f"--virtual-time-budget={DEBANK_VIRTUAL_TIME_BUDGET_MS}",
                "--dump-dom",
                DEBANK_PROFILE_PAGE.format(address=address),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=DEBANK_RENDER_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return None, "debank_timeout"
    except OSError as exc:
        return None, f"debank_browser_error: {exc}"
    if proc.returncode != 0:
        return None, f"debank_chrome_exit_{proc.returncode}"
    metadata = extract_debank_cex_metadata(proc.stdout)
    return (metadata or None), None


def run_debank_page_audit(
    candidates: list[dict[str, Any]],
    delay: float,
    chrome_binary: str,
) -> dict[str, Any]:
    """Audit explicit DeBank CEX badges; suggestions remain review-only."""
    confirmed: list[dict[str, Any]] = []
    no_tag: list[str] = []
    errors: dict[str, str] = {}
    for idx, candidate in enumerate(candidates, start=1):
        address = candidate["address"]
        metadata, error = fetch_debank_cex_metadata(address, chrome_binary)
        if error:
            errors[address] = error
        elif metadata:
            confirmed.append(
                {
                    **candidate,
                    "suggestedLabel": clean_suggestion_label(metadata),
                    "source": "debank-public-label",
                    "debank": {"nametag": metadata.get("nametag")},
                }
            )
        else:
            no_tag.append(address)
        if idx < len(candidates):
            time.sleep(delay)
    return {
        "confirmedCexSuggestions": confirmed,
        "nonCexTagged": [],
        "noPublicTag": no_tag,
        "errors": errors,
        "queriedCount": len(confirmed) + len(no_tag) + len(errors),
    }


def empty_debank_rotation_state() -> dict[str, Any]:
    return {
        "schemaVersion": ROTATION_STATE_SCHEMA_VERSION,
        "debank": {"addresses": {}},
    }


def normalize_debank_rotation_state(value: Any) -> dict[str, Any]:
    """Keep the persistent audit queue small and tolerant of old state files."""
    state = empty_debank_rotation_state()
    if not isinstance(value, dict):
        return state
    debank = value.get("debank")
    addresses = debank.get("addresses") if isinstance(debank, dict) else None
    if not isinstance(addresses, dict):
        return state
    state["debank"]["addresses"] = {
        str(address).lower(): dict(metadata)
        for address, metadata in addresses.items()
        if is_address(str(address)) and isinstance(metadata, dict)
    }
    return state


def load_debank_rotation_state(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as handle:
            return normalize_debank_rotation_state(json.load(handle))
    except FileNotFoundError:
        return empty_debank_rotation_state()
    except (OSError, json.JSONDecodeError) as exc:
        print(f"  ⚠️ Could not load DeBank audit rotation state: {exc}")
        return empty_debank_rotation_state()


def save_debank_rotation_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(normalize_debank_rotation_state(state), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def select_debank_rotation_candidates(
    candidates: list[dict[str, Any]],
    state: dict[str, Any],
    confirmed_addresses: set[str],
    limit: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Choose fresh candidates first, then the least-recently checked ones.

    A failed DeBank render is still recorded as an attempt, so one transient
    browser failure cannot starve the rest of the candidate queue. It never
    changes a wallet's classification.
    """
    normalized_state = normalize_debank_rotation_state(state)
    checked = normalized_state["debank"]["addresses"]
    known = {str(address).lower() for address in confirmed_addresses if is_address(str(address))}
    eligible = [
        row for row in candidates
        if is_address(str(row.get("address") or ""))
        and str(row["address"]).lower() not in known
    ]
    fresh = [row for row in eligible if str(row["address"]).lower() not in checked]
    rechecks = [row for row in eligible if str(row["address"]).lower() in checked]
    rechecks.sort(
        key=lambda row: (
            str(checked[str(row["address"]).lower()].get("lastAttemptAt") or ""),
            str(row["address"]).lower(),
        )
    )
    selected = (fresh + rechecks)[:max(0, int(limit))]
    fresh_selected = sum(1 for row in selected if str(row["address"]).lower() not in checked)
    return selected, {
        "eligible": len(eligible),
        "selected": len(selected),
        "newCandidates": fresh_selected,
        "rechecks": len(selected) - fresh_selected,
        "excludedConfirmed": max(0, len(candidates) - len(eligible)),
    }


def record_debank_rotation_results(
    state: dict[str, Any],
    report: dict[str, Any],
    attempted_at: str,
) -> dict[str, Any]:
    """Persist only audit outcomes; labels remain review-only suggestions."""
    next_state = normalize_debank_rotation_state(state)
    addresses = next_state["debank"]["addresses"]
    outcomes = [
        (row.get("address"), "confirmed_cex", "")
        for row in report.get("confirmedCexSuggestions", [])
        if isinstance(row, dict)
    ]
    outcomes.extend((address, "no_cex_badge", "") for address in report.get("noPublicTag", []))
    outcomes.extend((address, "error", error) for address, error in (report.get("errors") or {}).items())
    for raw_address, outcome, error in outcomes:
        address = str(raw_address or "").lower()
        if not is_address(address):
            continue
        entry = addresses.setdefault(address, {})
        entry["lastAttemptAt"] = attempted_at
        entry["outcome"] = outcome
        if outcome == "error":
            entry["lastError"] = str(error)[:240]
        else:
            entry.pop("lastError", None)
    return next_state


def merge_audit_reports(primary: dict[str, Any], secondary: dict[str, Any]) -> dict[str, Any]:
    confirmed_by_address = {
        row.get("address"): row
        for row in primary.get("confirmedCexSuggestions", [])
        if row.get("address")
    }
    for row in secondary.get("confirmedCexSuggestions", []):
        if row.get("address"):
            confirmed_by_address.setdefault(row["address"], row)
    confirmed_addresses = set(confirmed_by_address)
    non_cex = [
        row for row in primary.get("nonCexTagged", []) + secondary.get("nonCexTagged", [])
        if row.get("address") not in confirmed_addresses
    ]
    errors = dict(primary.get("errors", {}))
    errors.update({f"debank:{key}": value for key, value in secondary.get("errors", {}).items()})
    return {
        "confirmedCexSuggestions": list(confirmed_by_address.values()),
        "nonCexTagged": non_cex,
        "noPublicTag": sorted(
            address
            for address in set(primary.get("noPublicTag", []) + secondary.get("noPublicTag", []))
            if address not in confirmed_addresses
        ),
        "errors": errors,
        "queriedCount": int(primary.get("queriedCount", 0)) + int(secondary.get("queriedCount", 0)),
        "debankQueriedCount": int(secondary.get("queriedCount", 0)),
    }


def extract_public_explorer_metadata(page_html: str) -> dict[str, Any]:
    """Extract the public nametag from an Etherscan address-page title."""
    title_match = re.search(r"<title[^>]*>(.*?)</title>", page_html or "", re.I | re.S)
    if not title_match:
        return {}
    title = html.unescape(re.sub(r"\s+", " ", title_match.group(1))).strip()
    nametag = title.split("|", 1)[0].strip()
    nametag = re.sub(r":\s*0x[a-fA-F0-9]{4,40}(?:\.{2,3})?.*$", "", nametag).strip()
    return {
        "nametag": nametag,
        "url": "",
        "shortdescription": "",
        "labels": [],
        "labels_slug": [],
        "publicTitle": title,
    }


def fetch_public_explorer_metadata(
    address: str,
    session: requests.Session,
) -> tuple[dict[str, Any] | None, str | None]:
    try:
        response = session.get(
            ETHERSCAN_ADDRESS_PAGE.format(address=address),
            headers={"User-Agent": PUBLIC_EXPLORER_USER_AGENT},
            timeout=30,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        return None, f"request_failed: {exc}"
    metadata = extract_public_explorer_metadata(response.text)
    return (metadata or None), None


def run_public_page_audit(
    candidates: list[dict[str, Any]],
    delay: float,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Audit candidates against public Etherscan labels; never mutate runtime labels."""
    session = session or requests.Session()
    confirmed: list[dict[str, Any]] = []
    non_cex_tagged: list[dict[str, Any]] = []
    no_tag: list[str] = []
    errors: dict[str, str] = {}

    for idx, candidate in enumerate(candidates, start=1):
        address = candidate["address"]
        metadata, error = fetch_public_explorer_metadata(address, session)
        if error:
            errors[address] = error
        elif not metadata:
            no_tag.append(address)
        elif is_cex_metadata(metadata):
            confirmed.append(
                {
                    **candidate,
                    "suggestedLabel": clean_suggestion_label(metadata),
                    "source": "etherscan-public-page",
                    "etherscan": {
                        "nametag": metadata.get("nametag"),
                        "publicTitle": metadata.get("publicTitle"),
                    },
                }
            )
        else:
            non_cex_tagged.append(
                {
                    **candidate,
                    "source": "etherscan-public-page",
                    "etherscan": {
                        "nametag": metadata.get("nametag"),
                        "publicTitle": metadata.get("publicTitle"),
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
        message = str(payload.get("message") or "")
        result_message = str(payload.get("result") or "")
        if result_message and result_message != message:
            message = f"{message}: {result_message}" if message else result_message
        if not message:
            message = "unknown_error"
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
    parser.add_argument("--no-debank", action="store_true", help="Skip rendered DeBank CEX badge checks.")
    parser.add_argument(
        "--state-file",
        default=str(DEFAULT_ROTATION_STATE),
        help="Persistent DeBank candidate-rotation state JSON.",
    )
    parser.add_argument(
        "--debank-max-candidates",
        type=int,
        default=int(os.environ.get("DOLO_CEX_DEBANK_MAX_CANDIDATES", "20")),
    )
    args = parser.parse_args()

    labels = load_labels()
    candidates = collect_candidates(
        labels,
        holder_min=args.holder_min,
        flow_min=args.flow_min,
        max_candidates=args.max_candidates,
        include_known_cex=args.include_known_cex,
    )
    state_file = Path(args.state_file)
    rotation_state = load_debank_rotation_state(state_file)
    rotation_summary: dict[str, Any] = {
        "stateFile": state_file.name,
        "eligible": 0,
        "selected": 0,
        "newCandidates": 0,
        "rechecks": 0,
        "excludedConfirmed": 0,
        "attempted": 0,
    }

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
    debank_status = "skipped"
    if args.no_api:
        api_status = "disabled_by_flag"
        debank_status = "disabled_with_api"
    else:
        if api_key:
            api_status = "attempted"
            api_report = run_api_audit(candidates, api_key, delay=args.delay)
        else:
            api_status = "missing_ETHERSCAN_API_KEY"
        api_blocked = (
            not api_key
            or (api_report["errors"] and api_report["queriedCount"] <= 2)
        )
        if api_blocked:
            api_report = run_public_page_audit(candidates, delay=args.delay)
            api_status = (
                "public_fallback_partial"
                if api_report["errors"]
                else "public_fallback_completed"
            )
        elif api_report["queriedCount"]:
            api_status = "completed"

        if not args.no_debank and args.debank_max_candidates > 0:
            confirmed_addresses = {
                row.get("address") for row in api_report.get("confirmedCexSuggestions", [])
            }
            debank_candidates, rotation_summary = select_debank_rotation_candidates(
                candidates,
                rotation_state,
                confirmed_addresses,
                args.debank_max_candidates,
            )
            rotation_summary["stateFile"] = state_file.name
            chrome_binary = find_chrome_binary()
            if chrome_binary and debank_candidates:
                debank_report = run_debank_page_audit(
                    debank_candidates,
                    delay=args.delay,
                    chrome_binary=chrome_binary,
                )
                rotation_state = record_debank_rotation_results(
                    rotation_state,
                    debank_report,
                    attempted_at=datetime.now(timezone.utc).isoformat(),
                )
                save_debank_rotation_state(state_file, rotation_state)
                rotation_summary["attempted"] = int(debank_report.get("queriedCount", 0))
                api_report = merge_audit_reports(api_report, debank_report)
                api_status = f"{api_status}_plus_debank"
                debank_status = "completed"
            elif not chrome_binary:
                debank_status = "browser_missing"
            else:
                debank_status = "no_candidates"
        elif args.no_debank:
            debank_status = "disabled_by_flag"
        else:
            debank_status = "candidate_limit_zero"

    api_report["debankStatus"] = debank_status

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
            "provider": "Etherscan V2 nametag + public address pages + DeBank direct CEX badges",
            "status": api_status,
            "note": "Only direct entity labels are candidates. Funded-by relationships and behavioral heuristics never promote a wallet; suggestions remain advisory.",
            **api_report,
        },
        "existingCexLabels": existing_cex,
        "watchLabels": watch_labels,
        "rankedCandidates": candidates,
        "debankRotation": rotation_summary,
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
