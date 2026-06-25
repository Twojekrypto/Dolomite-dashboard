#!/usr/bin/env python3
"""
Fetch Dolomite fees/revenue data from DeFiLlama's fees adapter.

The dashboard is static, so this stores the small revenue dataset used by the
Revenue tab instead of calling the API from every visitor's browser.
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone

import requests


DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(DATA_DIR, "dolomite_revenue.json")
ONCHAIN_AUDIT_FILE = os.path.join(DATA_DIR, "data", "dolomite-revenue-onchain-audit.json")
BASE_URL = "https://api.llama.fi/summary/fees/dolomite"
BORROW_FEE_REBATE_METADATA_URL = "https://api.dolomite.io/liquidity-mining/ve-dolo-rebate/metadata"
BERACHAIN_CHAIN_ID = "80094"
REQUEST_TIMEOUTS = (
    (10, 45),
    (10, 75),
    (10, 120),
)


def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def day_from_timestamp(timestamp):
    return datetime.fromtimestamp(int(timestamp), tz=timezone.utc).strftime("%Y-%m-%d")


def safe_number(value):
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


def safe_decimal_number(value):
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return 0.0


def chain_value(breakdown, chain):
    chain_payload = breakdown.get(chain, {}) if isinstance(breakdown, dict) else {}
    if isinstance(chain_payload, dict):
        return sum(safe_number(value) for value in chain_payload.values())
    return safe_number(chain_payload)


def fetch_metric(data_type):
    url = f"{BASE_URL}?dataType={data_type}"
    headers = {
        "Accept": "application/json",
        "User-Agent": "dolomite-dashboard-revenue/1.0",
    }
    last_error = None
    for attempt, timeout in enumerate(REQUEST_TIMEOUTS, start=1):
        try:
            print(f"   Fetching {data_type}, attempt {attempt}/{len(REQUEST_TIMEOUTS)}")
            response = requests.get(url, timeout=timeout, headers=headers)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError(f"{data_type} response is not a JSON object")
            if len(data.get("totalDataChart") or []) < 30:
                raise ValueError(f"{data_type} chart has too few rows")
            if len(data.get("totalDataChartBreakdown") or []) < 30:
                raise ValueError(f"{data_type} chain breakdown has too few rows")
            return data
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            print(f"   Attempt {attempt} failed: {exc}")
            if attempt < len(REQUEST_TIMEOUTS):
                time.sleep(5 * attempt)
    raise RuntimeError(f"Unable to fetch {data_type}: {last_error}")


def fetch_borrow_fee_rebate_metadata():
    headers = {
        "Accept": "application/json",
        "User-Agent": "dolomite-dashboard-revenue/1.0",
    }
    try:
        response = requests.get(BORROW_FEE_REBATE_METADATA_URL, timeout=(10, 30), headers=headers)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        print(f"   Borrow fee rebate metadata unavailable: {exc}")
        return None
    metadata = payload.get("metadata") if isinstance(payload, dict) else None
    return metadata if isinstance(metadata, dict) else None


def chart_map(rows):
    out = {}
    for row in rows or []:
        if not isinstance(row, list) or len(row) < 2:
            continue
        out[int(row[0])] = safe_number(row[1])
    return out


def breakdown_map(rows):
    out = {}
    for row in rows or []:
        if not isinstance(row, list) or len(row) < 2:
            continue
        out[int(row[0])] = row[1] if isinstance(row[1], dict) else {}
    return out


def merge_series(revenue_data, fees_data):
    revenue_chart = chart_map(revenue_data.get("totalDataChart"))
    fees_chart = chart_map(fees_data.get("totalDataChart"))
    revenue_breakdowns = breakdown_map(revenue_data.get("totalDataChartBreakdown"))
    fees_breakdowns = breakdown_map(fees_data.get("totalDataChartBreakdown"))
    timestamps = sorted(set(revenue_chart) | set(fees_chart))
    chains = sorted({
        chain
        for ts in timestamps
        for source in (revenue_breakdowns.get(ts, {}), fees_breakdowns.get(ts, {}))
        for chain in source.keys()
    })

    rows = []
    for ts in timestamps:
        revenue = revenue_chart.get(ts, 0.0)
        fees = fees_chart.get(ts, 0.0)
        day = day_from_timestamp(ts)
        chain_rows = {}
        for chain in chains:
            chain_revenue = chain_value(revenue_breakdowns.get(ts, {}), chain)
            chain_fees = chain_value(fees_breakdowns.get(ts, {}), chain)
            if chain_revenue > 0 or chain_fees > 0:
                chain_rows[chain] = {
                    "feesUSD": round(chain_fees, 6),
                    "revenueUSD": round(chain_revenue, 6),
                    "supplySideRevenueUSD": round(max(chain_fees - chain_revenue, 0.0), 6),
                }

        rows.append({
            "timestamp": ts,
            "date": day,
            "feesUSD": round(fees, 6),
            "revenueUSD": round(revenue, 6),
            "supplySideRevenueUSD": round(max(fees - revenue, 0.0), 6),
            "protocolCut": round(revenue / fees, 8) if fees > 0 else 0,
            "chains": chain_rows,
        })
    return rows


def window_chain_totals(series, days):
    rows = series[-days:] if days > 0 else series
    totals = {}
    for row in rows:
        for chain, payload in row.get("chains", {}).items():
            entry = totals.setdefault(chain, {
                "feesUSD": 0.0,
                "revenueUSD": 0.0,
                "supplySideRevenueUSD": 0.0,
            })
            entry["feesUSD"] += safe_number(payload.get("feesUSD"))
            entry["revenueUSD"] += safe_number(payload.get("revenueUSD"))
            entry["supplySideRevenueUSD"] += safe_number(payload.get("supplySideRevenueUSD"))
    return {
        chain: {key: round(value, 6) for key, value in values.items()}
        for chain, values in sorted(totals.items(), key=lambda item: item[1]["revenueUSD"], reverse=True)
    }


def window_sum(series, days, key):
    rows = series[-days:] if days > 0 else series
    return sum(safe_number(row.get(key)) for row in rows)


def latest_series_value(series, key, fallback):
    latest = series[-1] if series else {}
    value = latest.get(key)
    if isinstance(value, bool):
        return safe_number(fallback)
    if isinstance(value, (int, float)):
        return safe_number(value)
    return safe_number(fallback)


def load_onchain_audit(path=ONCHAIN_AUDIT_FILE):
    try:
        with open(path) as f:
            payload = json.load(f)
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def expected_onchain_audit_target_date(now=None):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return (now.astimezone(timezone.utc).date() - timedelta(days=2)).isoformat()


def date_is_before(left, right):
    try:
        left_date = datetime.fromisoformat(str(left)).date()
        right_date = datetime.fromisoformat(str(right)).date()
    except (TypeError, ValueError):
        return False
    return left_date < right_date


def summarize_onchain_audit_chains(onchain_audit):
    chains = onchain_audit.get("chains") if isinstance(onchain_audit, dict) else None
    if not isinstance(chains, dict):
        return {}

    fields = (
        "status",
        "feesUSD",
        "revenueUSD",
        "defillamaFeesUSD",
        "defillamaRevenueUSD",
        "feesDiffPct",
        "revenueDiffPct",
        "feesDiffUnbounded",
        "revenueDiffUnbounded",
        "protocolCut",
        "defillamaProtocolCut",
        "protocolCutDiff",
        "warnReasons",
        "infoReasons",
        "missingReasons",
        "defillamaChainMissing",
        "priceFallbackCount",
        "priceOmissionCount",
        "rawTokenCount",
        "error",
    )
    summarized = {}
    for chain, payload in sorted(chains.items()):
        if not isinstance(payload, dict):
            continue
        summarized[str(chain)] = {field: payload[field] for field in fields if field in payload}
    return summarized


def int_or_none(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalized_borrow_fee_rebate_metadata(metadata):
    if not isinstance(metadata, dict):
        return {
            "status": "not_available",
            "netting": "not_netted",
            "source": BORROW_FEE_REBATE_METADATA_URL,
            "chains": {},
        }

    all_chain_info = metadata.get("allChainRebateInfo") or {}
    berachain_info = all_chain_info.get(BERACHAIN_CHAIN_ID)
    chains = {}
    if isinstance(berachain_info, dict):
        market_info = berachain_info.get("marketToRebateInfo") or {}
        rebate_percentage = round(safe_decimal_number(berachain_info.get("rebatePercentage")), 8)
        chains["Berachain"] = {
            "chainId": int(BERACHAIN_CHAIN_ID),
            "status": "active" if rebate_percentage > 0 else "inactive",
            "startEpoch": int_or_none(berachain_info.get("startEpoch")),
            "claimsEnabled": bool(berachain_info.get("claimsEnabled")),
            "rebatePercentage": rebate_percentage,
            "marketCount": len(market_info) if isinstance(market_info, dict) else 0,
            "onchainFeeRebateEpoch": int_or_none((metadata.get("onchainFeeRebateEpochIndexMap") or {}).get(BERACHAIN_CHAIN_ID)),
            "onchainRollingClaimsEpoch": int_or_none((metadata.get("onchainRollingClaimsEpochIndexMap") or {}).get(BERACHAIN_CHAIN_ID)),
        }

    status = "active" if any(chain.get("status") == "active" for chain in chains.values()) else "inactive"
    return {
        "status": status,
        "netting": "not_netted",
        "source": BORROW_FEE_REBATE_METADATA_URL,
        "veDoloStartTimestamp": int_or_none(metadata.get("veDoloStartTimestamp")),
        "veDoloHoldingFactor": round(safe_decimal_number(metadata.get("veDoloHoldingFactor")), 8),
        "currentEpochIndex": int_or_none(metadata.get("currentEpochIndex")),
        "currentEpochStartTimestamp": int_or_none(metadata.get("currentEpochStartTimestamp")),
        "chains": chains,
    }


def borrow_fee_rebate_status(rebate_metadata):
    if rebate_metadata.get("status") == "active":
        return "active_pre_rebate_not_netted"
    if rebate_metadata.get("status") == "inactive":
        return "inactive"
    return "not_available"


def onchain_audit_assurance(onchain_audit, now=None):
    expected_target = expected_onchain_audit_target_date(now)
    if not onchain_audit:
        return {
            "onchainAuditStatus": "not_run",
            "onchainAuditRawStatus": "not_run",
            "onchainAuditTargetDate": None,
            "onchainAuditExpectedTargetDate": expected_target,
            "onchainAuditStale": False,
            "onchainAuditMaxRevenueDiffPct": None,
            "onchainAuditMaxFeesDiffPct": None,
            "onchainAuditRevenueDiffUnbounded": False,
            "onchainAuditFeesDiffUnbounded": False,
            "onchainAuditChains": {},
        }
    summary = onchain_audit.get("summary") or {}
    raw_status = str(onchain_audit.get("status") or "missing")
    target_date = onchain_audit.get("targetDate")
    stale = date_is_before(target_date, expected_target)
    return {
        "onchainAuditStatus": "stale" if stale else raw_status,
        "onchainAuditRawStatus": raw_status,
        "onchainAuditTargetDate": target_date,
        "onchainAuditExpectedTargetDate": expected_target,
        "onchainAuditStale": stale,
        "onchainAuditMaxRevenueDiffPct": summary.get("maxRevenueDiffPct"),
        "onchainAuditMaxFeesDiffPct": summary.get("maxFeesDiffPct"),
        "onchainAuditRevenueDiffUnbounded": bool(summary.get("revenueDiffUnbounded")),
        "onchainAuditFeesDiffUnbounded": bool(summary.get("feesDiffUnbounded")),
        "onchainAuditGeneratedAt": onchain_audit.get("generatedAt"),
        "onchainAuditChains": summarize_onchain_audit_chains(onchain_audit),
    }


def metric_totals(revenue_data, fees_data, series):
    # DeFiLlama aggregate windows can briefly lag or revise while the chart rows
    # are updating. Keep every displayed total tied to the same saved series that
    # powers the chart and chain breakdowns.
    fees_24h = latest_series_value(series, "feesUSD", fees_data.get("total24h"))
    revenue_24h = latest_series_value(series, "revenueUSD", revenue_data.get("total24h"))
    previous = series[-2] if len(series) >= 2 else {}
    previous_revenue = previous.get("revenueUSD", revenue_data.get("total48hto24h"))
    previous_fees = previous.get("feesUSD", fees_data.get("total48hto24h"))
    return {
        "dailyRevenueUSD": round(revenue_24h, 6),
        "dailyFeesUSD": round(fees_24h, 6),
        "dailySupplySideRevenueUSD": round(max(fees_24h - revenue_24h, 0.0), 6),
        "dailyProtocolCut": round(revenue_24h / fees_24h, 8) if fees_24h > 0 else 0,
        "previousDailyRevenueUSD": round(safe_number(previous_revenue), 6),
        "previousDailyFeesUSD": round(safe_number(previous_fees), 6),
        "revenue7dUSD": round(window_sum(series, 7, "revenueUSD"), 6),
        "fees7dUSD": round(window_sum(series, 7, "feesUSD"), 6),
        "revenue30dUSD": round(window_sum(series, 30, "revenueUSD"), 6),
        "fees30dUSD": round(window_sum(series, 30, "feesUSD"), 6),
        "revenueAllTimeUSD": round(safe_number(revenue_data.get("totalAllTime")), 6),
        "feesAllTimeUSD": round(safe_number(fees_data.get("totalAllTime")), 6),
    }


def build_output(revenue_data, fees_data, onchain_audit=None, borrow_fee_rebate_metadata=None, now=None):
    series = merge_series(revenue_data, fees_data)
    if len(series) < 30:
        raise ValueError("Merged revenue series has too few rows")

    latest = series[-1]
    if onchain_audit is None:
        onchain_audit = load_onchain_audit()
    rebate_metadata = normalized_borrow_fee_rebate_metadata(borrow_fee_rebate_metadata)
    generated_at = utc_now_iso()
    return {
        "schemaVersion": 1,
        "protocol": "Dolomite",
        "source": "DeFiLlama fees adapter",
        "sourceUrls": {
            "dailyRevenue": f"{BASE_URL}?dataType=dailyRevenue",
            "dailyFees": f"{BASE_URL}?dataType=dailyFees",
            "adapter": "https://github.com/DefiLlama/dimension-adapters/tree/master/fees/dolomite",
            "onchainAudit": "data/dolomite-revenue-onchain-audit.json",
            "borrowFeeRebates": BORROW_FEE_REBATE_METADATA_URL,
            "doloModuleDocs": "https://docs.dolomite.io/smart-contract-addresses/module-dolo",
        },
        "generatedAt": generated_at,
        "lastUpdated": generated_at,
        "methodology": {
            "fees": "Interest paid by borrowers.",
            "revenue": "Gross protocol-retained borrower interest before borrower rebate programs.",
            "supplySideRevenue": "The portion of borrower interest paid to lenders.",
            "formula": "dailyRevenue = interestEarned * (1 - earningsRate); supplySideRevenue = dailyFees - dailyRevenue",
            "scope": "Dolomite borrow-interest economics from the DeFiLlama adapter. Gas fees, token emissions, treasury transfers, trading spreads, liquidator earnings and protocol liquidation-rake attribution are excluded.",
            "sourceLimitations": [
                "DeFiLlama adapter estimates daily interest from borrow index movement and borrowed principal snapshots.",
                "This is protocol-retained borrow interest, not a direct treasury cashflow audit.",
                "Berachain veDOLO borrow-fee rebates are not netted from revenue yet; displayed revenue is pre-rebate gross retained borrow interest.",
                "Current-day values can be revised by DeFiLlama until the adapter window fully settles.",
            ],
        },
        "assurance": {
            "classification": "adapter-estimated protocol borrow-interest revenue",
            "confidence": "high for retained borrow-interest direction/split when the independent onchain audit is pass; warn/stale audit states should be treated as data-quality caveats",
            "rollingTotalsSource": "Saved daily series rows, matching chart and chain breakdowns for borrow interest",
            "netRevenueAfterBorrowFeeRebates": "not netted; Berachain veDOLO borrow-fee rebates are distributed by a separate rebate program",
            "borrowFeeRebateStatus": borrow_fee_rebate_status(rebate_metadata),
            **onchain_audit_assurance(onchain_audit, now=now),
        },
        "borrowFeeRebates": rebate_metadata,
        "totals": metric_totals(revenue_data, fees_data, series),
        "latest": latest,
        "chainTotals7d": window_chain_totals(series, 7),
        "chainTotals30d": window_chain_totals(series, 30),
        "series": series,
    }


def main():
    print("Fetching Dolomite revenue data...")
    try:
        revenue_data = fetch_metric("dailyRevenue")
        fees_data = fetch_metric("dailyFees")
        rebate_metadata = fetch_borrow_fee_rebate_metadata()
        output = build_output(revenue_data, fees_data, borrow_fee_rebate_metadata=rebate_metadata)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(output, f, separators=(",", ":"))

        print(f"Saved {os.path.basename(OUTPUT_FILE)} ({os.path.getsize(OUTPUT_FILE) / 1024:.0f} KB)")
        print(f"Daily revenue: ${output['totals']['dailyRevenueUSD']:,.0f}")
        print(f"Daily fees: ${output['totals']['dailyFeesUSD']:,.0f}")
    except Exception as exc:
        print(f"Revenue fetch failed: {exc}")
        if os.path.exists(OUTPUT_FILE):
            print(f"Keeping existing {OUTPUT_FILE}")
            return
        raise


if __name__ == "__main__":
    main()
