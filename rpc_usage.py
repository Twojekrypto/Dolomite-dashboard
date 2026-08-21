#!/usr/bin/env python3
"""
Best-effort RPC usage accounting for the data-pipeline scripts.

Single source of truth for estimating how much of a provider's free-tier budget
a run consumes. Standard-library only (NO third-party imports) so it is safe to
import from scripts that run without `requests`/`web3` installed (e.g. the
stdlib-only `scan_earn_netflow.py` netflow workflows).

Usage:
    import rpc_usage
    rpc_usage.record_request("eth_getLogs")          # one call
    rpc_usage.record_methods(["eth_call", "eth_call"])  # a batch

At process exit a one-line summary prints to stdout (the GitHub Actions log):

    📊 RPC usage [scan_earn_netflow.py]: 412 requests, ~24,720 CU est. [eth_getLogs×400, eth_blockNumber×12]

Knobs (env):
    RPC_USAGE_QUIET=1        suppress the end-of-run print
    RPC_USAGE_LOG=<path>     also append a JSON trend record (opt-in, off by default)

Compute-unit (CU) costs are Alchemy's published values for EVM standard
JSON-RPC methods (verified 2026-06). A JSON-RPC batch is billed as the SUM of
its member methods, so each member is counted individually. The estimate is
provider-agnostic: it is useful even for scripts on public RPCs, as a measure
of "what this run would cost on Alchemy".
"""

import atexit
import json
import os
import sys
import threading
import time
from collections import Counter
from urllib.parse import urlparse

# Approximate Alchemy CU costs for EVM standard JSON-RPC methods.
_CU_COST = {
    "eth_getLogs": 60,
    "eth_call": 26,
    "eth_getTransactionReceipt": 20,
    "eth_getBlockByNumber": 20,
    "eth_getBlockByHash": 20,
    "eth_getStorageAt": 20,
    "eth_getBalance": 20,
    "eth_getCode": 20,
    "eth_getTransactionByHash": 20,
    "eth_gasPrice": 20,
    "eth_blockNumber": 10,
    "net_version": 0,
    "eth_chainId": 0,
}
_CU_DEFAULT_COST = 20  # unknown methods: assume a standard read

_usage_counts = Counter()
_provider_counts = {}
_usage_lock = threading.Lock()


def methods_from_payload(payload):
    """Extract JSON-RPC method name(s) from a single or batch payload."""
    if isinstance(payload, list):
        return [item.get("method") for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload.get("method")]
    return []


def record_methods(methods):
    """Tally served JSON-RPC methods (best-effort, thread-safe)."""
    with _usage_lock:
        for method in methods:
            if method:
                _usage_counts[method] += 1


def record_request(method, count=1):
    """Tally `count` served calls of a single JSON-RPC method."""
    if not method:
        return
    with _usage_lock:
        _usage_counts[method] += int(count)


def provider_name(endpoint):
    """Return a key-safe provider identifier suitable for public CI logs."""
    endpoint = str(endpoint or "").strip()
    host = urlparse(endpoint).hostname or "rpc"
    matching_env_names = sorted(
        env_name
        for env_name, value in os.environ.items()
        if (env_name.endswith("_RPC") or "_RPC_" in env_name)
        and str(value or "").strip() == endpoint
    )
    if matching_env_names:
        return f"{host} ({matching_env_names[0]})"
    return host


def _provider_counter(endpoint):
    name = provider_name(endpoint)
    if name not in _provider_counts:
        _provider_counts[name] = Counter({
            "http_success": 0,
            "http_failure": 0,
            "rate_limited": 0,
            "served_methods": 0,
        })
    return _provider_counts[name]


def record_provider_success(endpoint, served_methods=1):
    """Record one successful HTTP RPC request and its logical method count."""
    with _usage_lock:
        counts = _provider_counter(endpoint)
        counts["http_success"] += 1
        counts["served_methods"] += max(0, int(served_methods))


def record_provider_failure(endpoint, *, rate_limited=False):
    """Record one failed provider attempt without retaining its secret URL."""
    with _usage_lock:
        counts = _provider_counter(endpoint)
        counts["http_failure"] += 1
        if rate_limited:
            counts["rate_limited"] += 1


def estimate_cu(counts):
    """Estimated Alchemy compute units for a {method: count} mapping."""
    return sum(_CU_COST.get(method, _CU_DEFAULT_COST) * n for method, n in counts.items())


def usage_summary():
    """Snapshot of logical usage plus key-safe per-provider failover counts."""
    with _usage_lock:
        counts = dict(_usage_counts)
        providers = {
            name: dict(provider_counts)
            for name, provider_counts in _provider_counts.items()
        }
    return {
        "requests": sum(counts.values()),
        "estimated_cu": estimate_cu(counts),
        "by_method": counts,
        "by_provider": providers,
    }


def reset_usage():
    """Clear the in-process usage tally (mainly for tests)."""
    with _usage_lock:
        _usage_counts.clear()
        _provider_counts.clear()


def _script_name():
    return os.path.basename(sys.argv[0]) if sys.argv and sys.argv[0] else "python"


def _append_usage_log(path, summary):
    record = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": _script_name(),
        "requests": summary["requests"],
        "estimated_cu": summary["estimated_cu"],
        "by_method": summary["by_method"],
        "by_provider": summary["by_provider"],
    }
    try:
        history = []
        if os.path.exists(path):
            with open(path, "r") as handle:
                loaded = json.load(handle)
            if isinstance(loaded, list):
                history = loaded
        history.append(record)
        history = history[-500:]  # keep the most recent runs only
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w") as handle:
            json.dump(history, handle, indent=2)
        os.replace(tmp_path, path)
    except (OSError, ValueError) as exc:
        print(f"⚠️ Could not write RPC usage log {path!r}: {exc}", flush=True)


def emit_usage_summary():
    """Print a one-line RPC/CU summary; optionally append a trend record."""
    summary = usage_summary()
    if not summary["requests"] and not summary["by_provider"]:
        return
    if os.environ.get("RPC_USAGE_QUIET", "").strip().lower() not in ("1", "true", "yes"):
        top = ", ".join(
            f"{method}×{count}"
            for method, count in sorted(summary["by_method"].items(), key=lambda kv: -kv[1])
        )
        provider_top = ", ".join(
            f"{name}: ok {counts['http_success']}, fail {counts['http_failure']}, "
            f"429 {counts['rate_limited']}, served {counts['served_methods']}"
            for name, counts in sorted(summary["by_provider"].items())
        )
        provider_suffix = f" | providers [{provider_top}]" if provider_top else ""
        # Write to stderr, never stdout: several pipelines capture a child
        # process's stdout and json.loads() it (e.g. run_earn_canonical_history_
        # refresh.py), so a summary line on stdout would corrupt that JSON.
        print(
            f"📊 RPC usage [{_script_name()}]: {summary['requests']} requests, "
            f"~{summary['estimated_cu']:,} CU est. [{top}]{provider_suffix}",
            file=sys.stderr,
            flush=True,
        )
    log_path = os.environ.get("RPC_USAGE_LOG", "").strip()
    if log_path:
        _append_usage_log(log_path, summary)


atexit.register(emit_usage_summary)
