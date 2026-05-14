#!/usr/bin/env python3
"""
Data Validation Guard — runs BEFORE git commit in CI/CD.
If any check fails, exits with code 1 → workflow skips commit → production data stays intact.

Usage:
    python3 validate_data.py                     # validate all files
    python3 validate_data.py dolo_flows.json     # validate specific file(s)
"""

import json
import sys
import os
from datetime import datetime, timezone

# ── Validation Rules ─────────────────────────────────────────────────────────
# Each file has: required top-level keys, optional nested checks, and min-size thresholds.

def _is_iso_datetime(value):
    try:
        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return True
    except (TypeError, ValueError):
        return False


EXPECTED_TVL_CHAINS = {
    "Ethereum",
    "Berachain",
    "Botanix",
    "Polygon zkEVM",
    "Mantle",
    "Arbitrum",
    "X Layer",
}
NON_CHAIN_TVL_KEYS = {
    "borrowed",
    "staking",
    "pool2",
    "vesting",
    "offers",
    "treasury",
    "cex",
    "governance",
}


def _age_hours(value):
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 3600


def _fresh_timestamp(value, max_hours=4):
    try:
        return 0 <= _age_hours(value) <= max_hours
    except (TypeError, ValueError):
        return False


def _nearly_equal(a, b, rel=1e-8, abs_tol=5.0):
    try:
        a = float(a)
        b = float(b)
    except (TypeError, ValueError):
        return False
    return abs(a - b) <= max(abs_tol, max(abs(a), abs(b)) * rel)


def _current_chain_tvls(data):
    current = data.get("currentChainTvls", {})
    return {
        key: float(value)
        for key, value in current.items()
        if isinstance(value, (int, float))
        and "-" not in key
        and key.lower() not in NON_CHAIN_TVL_KEYS
    }


def _borrowed_chain_tvls(data):
    current = data.get("currentChainTvls", {})
    return {
        key[:-9]: float(value)
        for key, value in current.items()
        if isinstance(value, (int, float)) and key.endswith("-borrowed")
    }


def _has_expected_tvl_chains(data):
    return EXPECTED_TVL_CHAINS.issubset(set(_current_chain_tvls(data)))


def _dolomite_tvl_totals_reconcile(data):
    chains = _current_chain_tvls(data)
    borrowed = _borrowed_chain_tvls(data)
    total_tvl = data.get("totalTvl")
    total_borrowed = data.get("totalBorrowed")
    supply = data.get("supplyLiquidity")
    return (
        _nearly_equal(sum(chains.values()), total_tvl)
        and _nearly_equal(sum(borrowed.values()), total_borrowed)
        and _nearly_equal(float(total_tvl) + float(total_borrowed), supply)
        and _nearly_equal(data.get("currentChainTvls", {}).get("borrowed"), total_borrowed)
    )


def _dolomite_token_sums_reconcile(data):
    chains = _current_chain_tvls(data)
    borrowed = _borrowed_chain_tvls(data)
    chain_tokens = data.get("chainTokensInUsd", {})
    for chain in EXPECTED_TVL_CHAINS:
        token_sum = sum(
            float(value)
            for value in chain_tokens.get(chain, {}).values()
            if isinstance(value, (int, float))
        )
        supply = chains.get(chain, 0.0) + borrowed.get(chain, 0.0)
        if not _nearly_equal(token_sum, supply):
            return False

    latest = (data.get("tokensInUsd") or [{}])[-1].get("tokens", {})
    total_tokens = sum(float(value) for value in latest.values() if isinstance(value, (int, float)))
    return _nearly_equal(total_tokens, data.get("supplyLiquidity"))


def _dolomite_chain_meta_complete(data):
    meta = data.get("chainMeta", {})
    if not EXPECTED_TVL_CHAINS.issubset(set(meta)):
        return False
    for chain in EXPECTED_TVL_CHAINS:
        block = meta.get(chain, {})
        if not block.get("blockNumber") or not block.get("blockHash") or not block.get("blockTimestamp"):
            return False
    return True


def _dolomite_stale_chains_known(data):
    stale = set(data.get("staleChains", []))
    return stale.issubset(EXPECTED_TVL_CHAINS)


def _defillama_history_valid(data):
    rows = data.get("tvl", [])
    if len(rows) < 1000:
        return False
    previous = 0
    for row in rows:
        date = row.get("date")
        value = row.get("totalLiquidityUSD")
        if not isinstance(date, int) or not isinstance(value, (int, float)) or value <= 0:
            return False
        if date <= previous:
            return False
        previous = date
    return True


RULES = {
    "dolo_flows.json": {
        "required_keys": ["timestamp", "dolo_price", "periods"],
        "checks": [
            ("periods must have data",     lambda d: len(d.get("periods", {})) >= 3),
            ("dolo_price must be positive", lambda d: d.get("dolo_price", 0) > 0),
        ],
        "min_bytes": 50_000,
    },
    "dolo_holders.json": {
        "required_keys": ["contract", "timestamp", "stats", "holders"],
        "checks": [
            ("holders list must not be empty", lambda d: len(d.get("holders", [])) >= 10),
        ],
        "min_bytes": 10_000,
    },
    "vedolo_holders.json": {
        "required_keys": ["contract", "timestamp", "stats", "holders"],
        "checks": [
            ("holders must have entries", lambda d: len(d.get("holders", [])) >= 10),
        ],
        "min_bytes": 100_000,
    },
    "odolo_flows.json": {
        "required_keys": ["timestamp", "periods"],
        "checks": [
            ("periods must have data", lambda d: len(d.get("periods", {})) >= 3),
        ],
        "min_bytes": 50_000,
    },
    "vedolo_flows.json": {
        "required_keys": ["timestamp", "total_unlocks", "total_locks"],
        "checks": [],
        "min_bytes": 10_000,
    },
    "liquidation_risk.json": {
        "required_keys": ["generatedAt", "globalStats", "chainStats"],
        "checks": [
            ("chainStats must have entries", lambda d: len(d.get("chainStats", {})) >= 1),
        ],
        "min_bytes": 10_000,
    },
    "exercisers_by_address.json": {
        "required_keys": ["updated", "total_addresses", "exercisers"],
        "checks": [
            ("exercisers must have entries", lambda d: len(d.get("exercisers", [])) >= 5),
        ],
        "min_bytes": 50_000,
    },
    "early_exits.json": {
        "required_keys": ["stats", "recent_exits"],
        "checks": [],
        "min_bytes": 1_000,
    },
    "dolomite_tvl.json": {
        "required_keys": ["totalTvl", "totalBorrowed", "supplyLiquidity", "currentChainTvls", "tokensInUsd", "chainTokensInUsd", "chainMeta", "staleChains", "last_updated"],
        "checks": [
            ("totalTvl must be positive", lambda d: d.get("totalTvl", 0) > 0),
            ("last_updated must be fresh", lambda d: _fresh_timestamp(d.get("last_updated"))),
            ("all expected TVL chains must be present", _has_expected_tvl_chains),
            ("chain metadata must be complete", _dolomite_chain_meta_complete),
            ("stale chain list must be known chains only", _dolomite_stale_chains_known),
            ("TVL totals must reconcile", _dolomite_tvl_totals_reconcile),
            ("token sums must reconcile with supply liquidity", _dolomite_token_sums_reconcile),
        ],
        "min_bytes": 1_000,
    },
    "defillama_data.json": {
        "required_keys": ["tvl", "name", "currentChainTvls", "tokensInUsd", "chainTokensInUsd", "last_updated"],
        "checks": [
            ("last_updated must be fresh", lambda d: _fresh_timestamp(d.get("last_updated"))),
            ("all expected TVL chains must be present", _has_expected_tvl_chains),
            ("TVL history must be sorted and populated", _defillama_history_valid),
        ],
        "min_bytes": 10_000,
    },
    "vedolo_stats.json": {
        "required_keys": ["stats", "timestamp"],
        "checks": [],
        "min_bytes": 100,
    },
    "vedolo_expiry.json": {
        "required_keys": ["buckets", "total_dolo", "timestamp"],
        "checks": [],
        "min_bytes": 100,
    },
    "dolo_price.json": {
        "required_keys": ["price"],
        "checks": [
            ("price must be positive", lambda d: d.get("price", 0) > 0),
        ],
        "min_bytes": 50,
    },
    "odolo_contract_data.json": {
        "required_keys": ["totalSupply", "decimals"],
        "checks": [],
        "min_bytes": 50,
    },
    "metrics_snapshot.json": {
        "required_keys": ["snapshots"],
        "checks": [],
        "min_bytes": 100,
    },
    "avg_lock_data.json": {
        "required_keys": ["avg_lock_days", "total_exercises"],
        "checks": [],
        "min_bytes": 50,
    },
    "assets_live.json": {
        "required_keys": ["version", "generatedAt", "source", "rowCount", "chainCount", "chains", "rows"],
        "checks": [
            ("generatedAt must be ISO datetime", lambda d: _is_iso_datetime(d.get("generatedAt"))),
            ("rows must have entries", lambda d: len(d.get("rows", [])) >= 50),
            ("rowCount must match rows", lambda d: d.get("rowCount") == len(d.get("rows", []))),
            ("all configured chains must be present", lambda d: d.get("chainCount") >= 7 and len(d.get("chains", [])) >= 7),
        ],
        "min_bytes": 10_000,
    },
}

# ── Data files stored in data/ subdirectories ────────────────────────────────
# These use glob-style matching for directories with multiple files
DATA_DIR_RULES = {
    "data/earn-netflow": {
        "min_files": 1,
        "file_ext": ".json",
    },
    "data/earn-snapshots": {
        "min_files": 1,
        "file_ext": ".json",
    },
}


# ── Validation Engine ────────────────────────────────────────────────────────

class ValidationResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self.errors = []

    def ok(self, msg):
        self.passed += 1
        print(f"  ✅ {msg}")

    def fail(self, msg):
        self.failed += 1
        self.errors.append(msg)
        print(f"  ❌ {msg}")

    def skip(self, msg):
        self.skipped += 1
        print(f"  ⏭️  {msg}")


def validate_file(filepath, rules, result):
    """Validate a single JSON data file against its rules."""
    print(f"\n📄 {filepath}")

    # Check file exists
    if not os.path.exists(filepath):
        result.skip(f"File not found (may not be generated by this workflow)")
        return

    # Check minimum file size
    file_size = os.path.getsize(filepath)
    min_bytes = rules.get("min_bytes", 0)
    if file_size < min_bytes:
        result.fail(f"File too small: {file_size:,} bytes (min: {min_bytes:,})")
        return
    result.ok(f"Size: {file_size:,} bytes (min: {min_bytes:,})")

    # Parse JSON
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        result.fail(f"Invalid JSON: {e}")
        return
    result.ok("Valid JSON")

    # Check required keys
    for key in rules.get("required_keys", []):
        if key in data:
            result.ok(f"Key '{key}' present")
        else:
            result.fail(f"Missing required key: '{key}'")

    # Run custom checks
    for desc, check_fn in rules.get("checks", []):
        try:
            if check_fn(data):
                result.ok(desc)
            else:
                result.fail(f"Check failed: {desc}")
        except Exception as e:
            result.fail(f"Check error ({desc}): {e}")


def validate_data_dir(dirpath, rules, result):
    """Validate a data subdirectory (must contain minimum number of files)."""
    print(f"\n📁 {dirpath}/")

    if not os.path.isdir(dirpath):
        result.skip(f"Directory not found")
        return

    ext = rules.get("file_ext", ".json")
    files = [f for f in os.listdir(dirpath) if f.endswith(ext)]
    min_files = rules.get("min_files", 1)

    if len(files) >= min_files:
        result.ok(f"{len(files)} {ext} files (min: {min_files})")
    else:
        result.fail(f"Only {len(files)} {ext} files (min: {min_files})")

    # Validate each JSON file in directory is parseable
    bad = 0
    for fname in files:
        fpath = os.path.join(dirpath, fname)
        try:
            with open(fpath) as f:
                json.load(f)
        except json.JSONDecodeError:
            bad += 1
            result.fail(f"Invalid JSON: {fname}")

    if bad == 0 and files:
        result.ok(f"All {len(files)} files valid JSON")


def main():
    print("=" * 60)
    print("🔍 Dolomite Data Validation Guard")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    result = ValidationResult()

    # Determine which files to validate
    if len(sys.argv) > 1:
        # Validate specific files passed as arguments
        targets = sys.argv[1:]
        for target in targets:
            basename = os.path.basename(target)
            if basename in RULES:
                validate_file(target, RULES[basename], result)
            else:
                print(f"\n⚠️  No rules defined for: {target}")
                result.skip(f"No validation rules for {target}")
    else:
        # Validate all known files
        for filename, rules in RULES.items():
            validate_file(filename, rules, result)

        for dirpath, rules in DATA_DIR_RULES.items():
            validate_data_dir(dirpath, rules, result)

    # Summary
    print("\n" + "=" * 60)
    print(f"📊 Results: {result.passed} passed, {result.failed} failed, {result.skipped} skipped")

    if result.errors:
        print("\n🚨 FAILURES:")
        for err in result.errors:
            print(f"   • {err}")
        print("\n⛔ Validation FAILED — commit will be skipped to protect production data.")
        sys.exit(1)
    else:
        print("\n✅ All validations passed — safe to commit.")
        sys.exit(0)


if __name__ == "__main__":
    main()
