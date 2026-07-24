#!/usr/bin/env python3
"""
Local checks for strict EARN audit tooling.

Runs:
  - py_compile on core Python entrypoints
  - node --check on generated LIVE_AUDIT_JS
  - unittest discovery for audit/config tests
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

from audit_earn_asset import ROOT, build_live_audit_js


PYTHON_FILES = [
    ROOT / "apply_earn_subaccount_history_delta.py",
    ROOT / "audit_earn_asset.py",
    ROOT / "build_earn_subaccount_history.py",
    ROOT / "build_earn_quality_status.py",
    ROOT / "build_earn_bundle.py",
    ROOT / "build_earn_historical_prices.py",
    ROOT / "build_earn_representative_audit.py",
    ROOT / "build_earn_resolved_interest_ledger.py",
    ROOT / "build_earn_verified_ledger.py",
    ROOT / "build_earn_verified_ledger_shards.py",
    ROOT / "earn_strict_replay.py",
    ROOT / "earn_strict_rpc_evidence.py",
    ROOT / "earn_live_config.py",
    ROOT / "materialize_earn_subaccount_history.py",
    ROOT / "plan_earn_subaccount_history_incremental.py",
    ROOT / "plan_earn_subaccount_history_repairs.py",
    ROOT / "scripts" / "plan_earn_watchdog_dispatch.py",
    ROOT / "run_earn_chain_live_rerun.py",
    ROOT / "run_earn_canonical_history_refresh.py",
    ROOT / "run_earn_data_correctness_pipeline.py",
    ROOT / "run_earn_subaccount_history_incremental.py",
    ROOT / "select_earn_canonical_hot_addresses.py",
    ROOT / "update_earn_freshness_status.py",
    ROOT / "update_dolo_price.py",
    ROOT / "scripts" / "smoke_live_pages.py",
]

FOCUSED_TEST_MODULES = [
    "tests.test_earn_strict_replay",
    "tests.test_earn_strict_rpc_evidence",
    "tests.test_build_earn_resolved_interest_ledger",
    "tests.test_select_earn_canonical_hot_addresses",
]

JAVASCRIPT_FILES = [
    ROOT / "dolo-address-labels.js",
    ROOT / "earn" / "earn-draft.js",
    ROOT / "earn" / "earn-core.js",
    ROOT / "dashboard-core.js",
    ROOT / "route-loader.js",
]


def run(cmd: list[str], *, cwd: Path) -> None:
    proc = subprocess.run(cmd, cwd=str(cwd), check=False)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def main() -> int:
    run(["python3", "build_earn_bundle.py", "--check"], cwd=ROOT)
    run(["python3", "-m", "py_compile", *[str(path) for path in PYTHON_FILES]], cwd=ROOT)

    for js_file in JAVASCRIPT_FILES:
        run(["node", "--check", str(js_file)], cwd=ROOT)

    with tempfile.NamedTemporaryFile("w", suffix="_earn_live_audit.js", delete=False, encoding="utf-8") as tmp:
        tmp.write(build_live_audit_js())
        js_path = Path(tmp.name)
    try:
        run(["node", "--check", str(js_path)], cwd=ROOT)
    finally:
        js_path.unlink(missing_ok=True)

    run(["python3", "-m", "unittest", *FOCUSED_TEST_MODULES], cwd=ROOT)
    run(["python3", "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py"], cwd=ROOT)
    print("All EARN audit checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
