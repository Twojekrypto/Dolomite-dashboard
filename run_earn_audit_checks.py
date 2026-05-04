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
    ROOT / "earn_live_config.py",
    ROOT / "materialize_earn_subaccount_history.py",
    ROOT / "plan_earn_subaccount_history_incremental.py",
    ROOT / "plan_earn_subaccount_history_repairs.py",
    ROOT / "run_earn_chain_live_rerun.py",
    ROOT / "run_earn_canonical_history_refresh.py",
    ROOT / "run_earn_data_correctness_pipeline.py",
    ROOT / "run_earn_subaccount_history_incremental.py",
]


def run(cmd: list[str], *, cwd: Path) -> None:
    proc = subprocess.run(cmd, cwd=str(cwd), check=False)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def main() -> int:
    run(["python3", "-m", "py_compile", *[str(path) for path in PYTHON_FILES]], cwd=ROOT)

    with tempfile.NamedTemporaryFile("w", suffix="_earn_live_audit.js", delete=False, encoding="utf-8") as tmp:
        tmp.write(build_live_audit_js())
        js_path = Path(tmp.name)
    try:
        run(["node", "--check", str(js_path)], cwd=ROOT)
    finally:
        js_path.unlink(missing_ok=True)

    run(["python3", "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py"], cwd=ROOT)
    print("All EARN audit checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
