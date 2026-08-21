#!/usr/bin/env python3
"""Emit EARN SLA annotations and fail only after the six-hour boundary."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def evaluate_sla(payload: dict, active_repair_chains=None) -> dict:
    summary = (payload or {}).get("summary") or {}
    warning_chains = sorted({str(chain) for chain in (summary.get("slaWarningChains") or [])})
    critical_chains = sorted({str(chain) for chain in (summary.get("slaCriticalChains") or [])})
    active_repairs = {
        str(chain).strip().lower()
        for chain in (active_repair_chains or [])
        if str(chain).strip()
    }
    all_chains_covered = "__all__" in active_repairs
    uncovered_critical = [
        chain for chain in critical_chains
        if not all_chains_covered and chain.lower() not in active_repairs
    ]
    if uncovered_critical:
        names = ", ".join(uncovered_critical)
        return {
            "status": "critical",
            "exitCode": 2,
            "message": f"EARN SLA exceeded 6h for: {names}. No matching repair is currently active; operator attention is required.",
            "warningChains": warning_chains,
            "criticalChains": uncovered_critical,
        }
    if critical_chains:
        names = ", ".join(critical_chains)
        return {
            "status": "repairing",
            "exitCode": 0,
            "message": f"EARN SLA exceeded 6h for: {names}. A matching automatic repair is running or queued.",
            "warningChains": warning_chains,
            "criticalChains": [],
        }
    if warning_chains:
        names = ", ".join(warning_chains)
        return {
            "status": "warning",
            "exitCode": 0,
            "message": f"EARN freshness exceeded 2h for: {names}. Automatic repair is running or queued.",
            "warningChains": warning_chains,
            "criticalChains": [],
        }
    return {
        "status": "ok",
        "exitCode": 0,
        "message": "All monitored EARN chains are within the freshness SLA.",
        "warningChains": [],
        "criticalChains": [],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Check public EARN freshness SLA")
    parser.add_argument("status", type=Path)
    parser.add_argument("--summary", type=Path)
    parser.add_argument(
        "--active-repairs",
        type=Path,
        help="Optional file containing one actively repaired chain per line.",
    )
    args = parser.parse_args()
    payload = json.loads(args.status.read_text(encoding="utf-8"))
    active_repairs = set()
    if args.active_repairs and args.active_repairs.exists():
        for line in args.active_repairs.read_text(encoding="utf-8").splitlines():
            chain = line.split("\t", 1)[0].strip()
            if chain:
                active_repairs.add(chain)
    result = evaluate_sla(payload, active_repair_chains=active_repairs)
    annotation = "error" if result["status"] == "critical" else ("warning" if result["status"] in {"warning", "repairing"} else "notice")
    print(f"::{annotation} title=EARN freshness SLA::{result['message']}")
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        with args.summary.open("a", encoding="utf-8") as handle:
            handle.write("## EARN freshness SLA\n\n")
            handle.write(f"**{result['status'].upper()}**: {result['message']}\n")
    return int(result["exitCode"])


if __name__ == "__main__":
    raise SystemExit(main())
