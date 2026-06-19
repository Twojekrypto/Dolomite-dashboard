#!/usr/bin/env python3
"""
Fetch Dolomite fees/revenue data from DeFiLlama's fees adapter.

The dashboard is static, so this stores the small revenue dataset used by the
Revenue tab instead of calling the API from every visitor's browser.
"""

import json
import os
import time
from datetime import datetime, timezone

import requests


DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(DATA_DIR, "dolomite_revenue.json")
LIQUIDATION_HISTORY_FILE = os.path.join(DATA_DIR, "liquidation_history.json")
BASE_URL = "https://api.llama.fi/summary/fees/dolomite"
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


def confirmed_liquidation_fee_daily(path=LIQUIDATION_HISTORY_FILE):
    daily = {}
    try:
        with open(path) as f:
            payload = json.load(f)
    except (OSError, ValueError):
        return daily

    for row in payload.get("liquidationHistory", []) or []:
        if row.get("protocolFeeSource") != "feeAgentTransfer":
            continue
        amount = safe_number(row.get("protocolFeeUSD"))
        if amount <= 0:
            continue
        timestamp = row.get("timestamp")
        if not isinstance(timestamp, (int, float)):
            continue
        day = day_from_timestamp(timestamp)
        daily[day] = daily.get(day, 0.0) + amount
    return daily


def merge_series(revenue_data, fees_data, liquidation_fee_daily=None):
    revenue_chart = chart_map(revenue_data.get("totalDataChart"))
    fees_chart = chart_map(fees_data.get("totalDataChart"))
    revenue_breakdowns = breakdown_map(revenue_data.get("totalDataChartBreakdown"))
    fees_breakdowns = breakdown_map(fees_data.get("totalDataChartBreakdown"))
    liquidation_fee_daily = liquidation_fee_daily or {}
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
        liquidation_fees = safe_number(liquidation_fee_daily.get(day))
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
            "liquidationFeesUSD": round(liquidation_fees, 6),
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
        "dailyLiquidationFeesUSD": round(latest_series_value(series, "liquidationFeesUSD", 0), 6),
        "dailySupplySideRevenueUSD": round(max(fees_24h - revenue_24h, 0.0), 6),
        "dailyProtocolCut": round(revenue_24h / fees_24h, 8) if fees_24h > 0 else 0,
        "previousDailyRevenueUSD": round(safe_number(previous_revenue), 6),
        "previousDailyFeesUSD": round(safe_number(previous_fees), 6),
        "revenue7dUSD": round(window_sum(series, 7, "revenueUSD"), 6),
        "fees7dUSD": round(window_sum(series, 7, "feesUSD"), 6),
        "liquidationFees7dUSD": round(window_sum(series, 7, "liquidationFeesUSD"), 6),
        "revenue30dUSD": round(window_sum(series, 30, "revenueUSD"), 6),
        "fees30dUSD": round(window_sum(series, 30, "feesUSD"), 6),
        "liquidationFees30dUSD": round(window_sum(series, 30, "liquidationFeesUSD"), 6),
        "revenueAllTimeUSD": round(safe_number(revenue_data.get("totalAllTime")), 6),
        "feesAllTimeUSD": round(safe_number(fees_data.get("totalAllTime")), 6),
        "liquidationFeesAllTimeUSD": round(window_sum(series, 0, "liquidationFeesUSD"), 6),
    }


def build_output(revenue_data, fees_data, liquidation_fee_daily=None):
    series = merge_series(revenue_data, fees_data, liquidation_fee_daily)
    if len(series) < 30:
        raise ValueError("Merged revenue series has too few rows")

    latest = series[-1]
    return {
        "schemaVersion": 1,
        "protocol": "Dolomite",
        "source": "DeFiLlama fees adapter",
        "sourceUrls": {
            "dailyRevenue": f"{BASE_URL}?dataType=dailyRevenue",
            "dailyFees": f"{BASE_URL}?dataType=dailyFees",
            "adapter": "https://github.com/DefiLlama/dimension-adapters/tree/master/fees/dolomite",
            "liquidationFees": "liquidation_history.json",
            "liquidationFeeDocs": "https://docs.dolomite.io/risk-management",
        },
        "generatedAt": utc_now_iso(),
        "lastUpdated": utc_now_iso(),
        "methodology": {
            "fees": "Interest paid by borrowers.",
            "revenue": "The portion of borrower interest retained by the protocol.",
            "liquidationFees": "Confirmed Dolomite liquidation fee-rake transfers to the protocol feeAgent, sourced from same-transaction Dolomite subgraph Transfer rows.",
            "supplySideRevenue": "The portion of borrower interest paid to lenders.",
            "formula": "dailyRevenue = interestEarned * (1 - earningsRate); supplySideRevenue = dailyFees - dailyRevenue; liquidationFees = sum(confirmed feeAgent transfer USD in liquidation txs)",
            "scope": "Dolomite borrow-interest economics plus confirmed protocol liquidation fee-rake transfers. Gas fees, token emissions, treasury transfers outside confirmed liquidation fee transfers, trading spreads and external cost basis are excluded.",
            "sourceLimitations": [
                "DeFiLlama adapter estimates daily interest from borrow index movement and borrowed principal snapshots.",
                "This is protocol-retained borrow interest, not a direct treasury cashflow audit.",
                "Liquidation fees are counted only when liquidation_history.json contains a same-transaction Transfer to the Dolomite feeAgent; unconfirmed penalty estimates are excluded.",
                "Current-day values can be revised by DeFiLlama until the adapter window fully settles.",
            ],
        },
        "assurance": {
            "classification": "adapter-estimated protocol borrow-interest revenue plus confirmed liquidation fee-agent transfers",
            "confidence": "high for retained borrow-interest direction/split and high for liquidation fee transfers that are present in the Dolomite subgraph; not absolute for full cash-accounting revenue",
            "rollingTotalsSource": "Saved daily series rows, matching chart and chain breakdowns for borrow interest; liquidation fees are top-level daily stream values",
        },
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
        output = build_output(revenue_data, fees_data, confirmed_liquidation_fee_daily())
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
