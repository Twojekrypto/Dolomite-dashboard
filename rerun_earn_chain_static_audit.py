#!/usr/bin/env python3
"""
Run a strict static EARN audit rerun for every market on a chain.

This runner is local/private only. It prefers the regenerated strict
earn-verified-ledger files and writes one report per market plus a chain summary.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from audit_earn_asset import build_static_report, load_snapshot_payload, latest_snapshot_date
from build_earn_verified_ledger import OUTPUT_DIR as VERIFIED_LEDGER_DIR, ROOT


DEFAULT_OUTPUT_ROOT = ROOT / "data" / "earn-audit-reruns"


def utc_now_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def discover_markets(chain: str, snapshot_date: str) -> List[dict]:
    payload = load_snapshot_payload(snapshot_date)
    chain_data = (payload.get("snapshots") or {}).get(chain) or {}
    markets: Dict[str, dict] = {}
    for addr_data in chain_data.values():
        for mid_raw, market in ((addr_data.get("markets") or {})).items():
            wei = int(str(market.get("wei") or "0"))
            if wei <= 0:
                continue
            mid = str(mid_raw)
            row = markets.setdefault(mid, {
                "marketId": mid,
                "symbol": str(market.get("symbol") or "UNK"),
                "token": str(market.get("token") or "").lower(),
                "decimals": int(str(market.get("decimals") or "18")),
                "holderCount": 0,
            })
            row["holderCount"] += 1
    return sorted(markets.values(), key=lambda row: (int(row["marketId"]), row["symbol"]))


def rebuild_verified_ledger(chain: str) -> None:
    cmd = [
        "python3",
        str(ROOT / "build_earn_verified_ledger.py"),
        "--chain",
        chain,
        "--all-addresses",
    ]
    subprocess.run(cmd, check=True, cwd=str(ROOT))


def _worker_run_static(task: Tuple[str, str, str, str, str]) -> dict:
    chain, symbol, market_id, snapshot_date, verified_ledger_dir = task
    report = build_static_report(
        chain,
        symbol,
        market_id,
        snapshot_date,
        verified_ledger_dir=Path(verified_ledger_dir),
    )
    return report


def run_rerun(
    chain: str,
    snapshot_date: str,
    *,
    output_root: Path,
    verified_ledger_dir: Path,
    workers: int,
    market_ids: Optional[Sequence[str]] = None,
    limit_markets: Optional[int] = None,
) -> dict:
    markets = discover_markets(chain, snapshot_date)
    selected_market_ids = {str(mid) for mid in (market_ids or [])}
    if selected_market_ids:
        markets = [row for row in markets if row["marketId"] in selected_market_ids]
    if limit_markets is not None:
        markets = markets[: max(0, int(limit_markets))]

    run_id = f"{chain}-strict-static-{snapshot_date}-{utc_now_slug()}"
    run_dir = output_root / chain / run_id
    reports_dir = run_dir / "reports"
    unresolved_dir = run_dir / "unresolved"
    reports_dir.mkdir(parents=True, exist_ok=True)
    unresolved_dir.mkdir(parents=True, exist_ok=True)

    tasks = [
        (
            chain,
            str(row["symbol"]),
            str(row["marketId"]),
            snapshot_date,
            str(verified_ledger_dir),
        )
        for row in markets
    ]

    summary_rows = []
    if workers <= 1:
        iterator = enumerate(tasks, start=1)
        for index, task in iterator:
            report = _worker_run_static(task)
            row = next(row for row in markets if row["marketId"] == task[2])
            label = f"{row['marketId']}_{row['symbol']}"
            write_json(reports_dir / f"{label}.json", report)
            write_json(unresolved_dir / f"{label}.json", {
                "generatedAt": report.get("generatedAt"),
                "chain": report.get("chain"),
                "symbol": report.get("symbol"),
                "marketId": report.get("marketId"),
                "snapshotDate": report.get("snapshotDate"),
                "inputCount": report.get("unresolvedCount") or 0,
                "unresolved": report.get("unresolved") or [],
            })
            summary_rows.append({
                **row,
                "statusCounts": report.get("statusCounts") or {},
                "methodCounts": report.get("methodCounts") or {},
                "rootCauseCounts": report.get("rootCauseCounts") or {},
                "resolvedCount": int(report.get("resolvedCount") or 0),
                "unresolvedCount": int(report.get("unresolvedCount") or 0),
                "reportPath": str(reports_dir / f"{label}.json"),
            })
            print(f"[{index}/{len(tasks)}] {chain}:{row['marketId']} {row['symbol']} unresolved={report.get('unresolvedCount', 0)}")
    else:
        future_map = {}
        with ProcessPoolExecutor(max_workers=workers) as executor:
            for task in tasks:
                future = executor.submit(_worker_run_static, task)
                future_map[future] = task
            completed = 0
            for future in as_completed(future_map):
                completed += 1
                task = future_map[future]
                report = future.result()
                row = next(row for row in markets if row["marketId"] == task[2])
                label = f"{row['marketId']}_{row['symbol']}"
                write_json(reports_dir / f"{label}.json", report)
                write_json(unresolved_dir / f"{label}.json", {
                    "generatedAt": report.get("generatedAt"),
                    "chain": report.get("chain"),
                    "symbol": report.get("symbol"),
                    "marketId": report.get("marketId"),
                    "snapshotDate": report.get("snapshotDate"),
                    "inputCount": report.get("unresolvedCount") or 0,
                    "unresolved": report.get("unresolved") or [],
                })
                summary_rows.append({
                    **row,
                    "statusCounts": report.get("statusCounts") or {},
                    "methodCounts": report.get("methodCounts") or {},
                    "rootCauseCounts": report.get("rootCauseCounts") or {},
                    "resolvedCount": int(report.get("resolvedCount") or 0),
                    "unresolvedCount": int(report.get("unresolvedCount") or 0),
                    "reportPath": str(reports_dir / f"{label}.json"),
                })
                print(f"[{completed}/{len(tasks)}] {chain}:{row['marketId']} {row['symbol']} unresolved={report.get('unresolvedCount', 0)}")

    summary_rows.sort(key=lambda row: (-int(row["unresolvedCount"]), int(row["marketId"])))
    summary = {
        "runId": run_id,
        "generatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "chain": chain,
        "snapshotDate": snapshot_date,
        "marketCount": len(summary_rows),
        "workers": workers,
        "strictMode": True,
        "verifiedLedgerDir": str(verified_ledger_dir),
        "markets": summary_rows,
    }
    write_json(run_dir / "summary.json", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Run strict static EARN audit rerun for every market on a chain")
    parser.add_argument("--chain", default="arbitrum")
    parser.add_argument("--snapshot-date", default=None)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--verified-ledger-dir", default=str(VERIFIED_LEDGER_DIR))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--market-id", action="append", default=[])
    parser.add_argument("--limit-markets", type=int, default=None)
    parser.add_argument("--rebuild-ledger", action="store_true")
    args = parser.parse_args()

    snapshot_date = args.snapshot_date or latest_snapshot_date(args.chain)
    if args.rebuild_ledger:
        rebuild_verified_ledger(args.chain)

    summary = run_rerun(
        args.chain,
        snapshot_date,
        output_root=Path(args.output_root),
        verified_ledger_dir=Path(args.verified_ledger_dir),
        workers=max(1, int(args.workers)),
        market_ids=args.market_id,
        limit_markets=args.limit_markets,
    )
    print(json.dumps({
        "runId": summary["runId"],
        "chain": summary["chain"],
        "snapshotDate": summary["snapshotDate"],
        "marketCount": summary["marketCount"],
        "summaryPath": str(Path(args.output_root) / args.chain / summary["runId"] / "summary.json"),
    }, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
