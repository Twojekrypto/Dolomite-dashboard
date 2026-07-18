#!/usr/bin/env python3
"""Emit EARN SLA annotations and fail only after the six-hour boundary."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def evaluate_sla(payload: dict) -> dict:
    summary = (payload or {}).get("summary") or {}
    warning_chains = sorted({str(chain) for chain in (summary.get("slaWarningChains") or [])})
    critical_chains = sorted({str(chain) for chain in (summary.get("slaCriticalChains") or [])})
    if critical_chains:
        names = ", ".join(critical_chains)
        return {
            "status": "critical",
            "exitCode": 2,
            "message": f"EARN SLA exceeded 6h for: {names}. Automatic repair was dispatched; operator attention is required.",
            "warningChains": warning_chains,
            "criticalChains": critical_chains,
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
    args = parser.parse_args()
    payload = json.loads(args.status.read_text(encoding="utf-8"))
    result = evaluate_sla(payload)
    annotation = "error" if result["status"] == "critical" else ("warning" if result["status"] == "warning" else "notice")
    print(f"::{annotation} title=EARN freshness SLA::{result['message']}")
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        with args.summary.open("a", encoding="utf-8") as handle:
            handle.write("## EARN freshness SLA\n\n")
            handle.write(f"**{result['status'].upper()}**: {result['message']}\n")
    return int(result["exitCode"])


if __name__ == "__main__":
    raise SystemExit(main())
