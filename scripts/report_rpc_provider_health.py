#!/usr/bin/env python3
"""Run one key-safe eth_blockNumber health probe per selected RPC secret."""

import argparse
import json
import os
import re
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")


def _is_rate_limited(error):
    if isinstance(error, dict) and error.get("code") == 429:
        return True
    message = str(error or "").lower()
    return any(token in message for token in (
        "429", "rate limit", "too many", "throttl", "capacity", "quota",
    ))


def probe_provider(env_name, expected_chain_id, timeout=12):
    if not ENV_NAME_RE.fullmatch(env_name):
        raise ValueError(f"Invalid provider environment name: {env_name!r}")
    endpoint = os.environ.get(env_name, "").strip()
    if not endpoint:
        return "skip", "not configured", "rpc"

    host = urlparse(endpoint).hostname or "rpc"
    payload = json.dumps([
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
        {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 2},
    ]).encode()
    request = Request(
        endpoint,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "Dolomite-RPC-Health/1.0"},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            data = json.loads(response.read())
    except HTTPError as exc:
        category = "rate_limited" if exc.code == 429 else f"http_{exc.code}"
        return "fail", category, host
    except (URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError):
        return "fail", "transport_or_invalid_response", host

    if isinstance(data, dict) and data.get("error"):
        category = "rate_limited" if _is_rate_limited(data["error"]) else "json_rpc_error"
        return "fail", category, host
    if not isinstance(data, list):
        return "fail", "invalid_batch_response", host
    by_id = {
        entry.get("id"): entry
        for entry in data
        if isinstance(entry, dict)
    }
    for request_id in (1, 2):
        entry = by_id.get(request_id) or {}
        if entry.get("error"):
            category = "rate_limited" if _is_rate_limited(entry["error"]) else "json_rpc_error"
            return "fail", category, host
    try:
        chain_id = int((by_id.get(1) or {}).get("result") or "0x0", 16)
        block = int((by_id.get(2) or {}).get("result") or "0x0", 16)
    except (AttributeError, TypeError, ValueError):
        return "fail", "invalid_block_number", host
    if chain_id != expected_chain_id:
        return (
            "fail",
            f"wrong_chain expected {expected_chain_id} got {chain_id}",
            host,
        )
    if block <= 0:
        return "fail", "invalid_block_number", host
    return "ok", f"chain {chain_id}, block {block:,}", host


def parse_provider_spec(spec):
    env_name, separator, chain_id_text = str(spec or "").partition("=")
    if not separator:
        raise ValueError(f"Provider must use ENV_NAME=CHAIN_ID format: {spec!r}")
    if not ENV_NAME_RE.fullmatch(env_name):
        raise ValueError(f"Invalid provider environment name: {env_name!r}")
    try:
        chain_id = int(chain_id_text)
    except ValueError as exc:
        raise ValueError(f"Invalid expected chain ID: {chain_id_text!r}") from exc
    if chain_id <= 0:
        raise ValueError(f"Invalid expected chain ID: {chain_id_text!r}")
    return env_name, chain_id


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", action="append", required=True)
    parser.add_argument("--timeout", type=int, default=12)
    args = parser.parse_args(argv)

    failed = 0
    for provider_spec in args.provider:
        env_name, expected_chain_id = parse_provider_spec(provider_spec)
        status, detail, host = probe_provider(
            env_name,
            expected_chain_id,
            timeout=max(1, args.timeout),
        )
        if status == "skip":
            print(f"SKIP {env_name}: {detail}")
            continue
        if status == "ok":
            print(f"OK {env_name} [{host}]: {detail}")
            continue
        failed += 1
        print(f"FAIL {env_name} [{host}]: {detail}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
