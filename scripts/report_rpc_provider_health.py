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


def probe_provider(env_name, timeout=12):
    if not ENV_NAME_RE.fullmatch(env_name):
        raise ValueError(f"Invalid provider environment name: {env_name!r}")
    endpoint = os.environ.get(env_name, "").strip()
    if not endpoint:
        return "skip", "not configured", "rpc"

    host = urlparse(endpoint).hostname or "rpc"
    payload = json.dumps({
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1,
    }).encode()
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
    try:
        block = int(data.get("result") or "0x0", 16)
    except (AttributeError, TypeError, ValueError):
        return "fail", "invalid_block_number", host
    if block <= 0:
        return "fail", "invalid_block_number", host
    return "ok", f"block {block:,}", host


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", action="append", required=True)
    parser.add_argument("--timeout", type=int, default=12)
    args = parser.parse_args(argv)

    failed = 0
    for env_name in args.provider:
        status, detail, host = probe_provider(env_name, timeout=max(1, args.timeout))
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
