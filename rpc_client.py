#!/usr/bin/env python3
"""
Shared JSON-RPC client for the data-pipeline scripts.

Single source of truth for:
- per-chain RPC endpoint lists (env-injected secrets first, public fallbacks),
- endpoint rotation with retry + exponential backoff + jitter,
- mandatory request timeouts,
- explicit error logging (no silent swallowing).

Consumers: fetch_odolo_contract.py, fetch_early_exits.py, update_data.py,
generate_dolo_holders.py, generate_vedolo_flows.py, generate_odolo_flows.py.
Remaining multi-chain scripts (generate_reward_claim_events, scan_earn_netflow,
supply_*/earn_* pipelines) keep their own call helpers for now — migrate one
script per change, verified against its own workflow/tests (see AGENTS.md).
"""

import os
import random
import time
from urllib.parse import urlparse

import requests

DEFAULT_TIMEOUT = 15
DEFAULT_RETRIES_PER_ENDPOINT = 2
BACKOFF_BASE_SECONDS = 0.8

# Secrets injected by GitHub Actions (never hardcode keys here).
CHAIN_ENV_KEYS = {
    "berachain": ["ALCHEMY_BERACHAIN_RPC", "ALCHEMY_BERACHAIN_RPC_2", "ALCHEMY_BERACHAIN_RPC_3"],
    "ethereum": ["ALCHEMY_ETHEREUM_RPC", "ALCHEMY_ETHEREUM_RPC_2", "ALCHEMY_ETHEREUM_RPC_3"],
    "arbitrum": ["ALCHEMY_ARBITRUM_RPC", "ALCHEMY_ARBITRUM_RPC_2", "ALCHEMY_ARBITRUM_RPC_3"],
    "mantle": ["ALCHEMY_MANTLE_RPC", "ALCHEMY_MANTLE_RPC_2"],
    "xlayer": ["ALCHEMY_XLAYER_RPC"],
    "botanix": ["ALCHEMY_BOTANIX_RPC", "ALCHEMY_BOTANIX_RPC_2", "ALCHEMY_BOTANIX_RPC_3"],
}

PUBLIC_ENDPOINTS = {
    "berachain": [
        "https://rpc.berachain.com/",
        "https://berachain-rpc.publicnode.com/",
        "https://berachain.drpc.org/",
    ],
    "ethereum": [
        "https://eth.drpc.org/",
        "https://ethereum-rpc.publicnode.com/",
        "https://rpc.ankr.com/eth",
    ],
    "arbitrum": [
        "https://arb1.arbitrum.io/rpc",
        "https://arbitrum-one-rpc.publicnode.com",
        "https://arbitrum.drpc.org",
    ],
    "mantle": [
        "https://rpc.mantle.xyz",
        "https://mantle-rpc.publicnode.com",
    ],
    "xlayer": [
        "https://rpc.xlayer.tech",
        "https://xlayerrpc.okx.com",
    ],
    "botanix": [
        # lessons.md: prefer rpc.botanixlabs.com for full eth_getLogs ranges;
        # ankr is a fallback that can reject large ranges.
        "https://rpc.botanixlabs.com",
        "https://rpc.ankr.com/botanix_mainnet",
    ],
}


def get_endpoints(chain):
    """Endpoint list for a chain: env-injected secrets first, then public RPCs."""
    endpoints = []
    for env_key in CHAIN_ENV_KEYS.get(chain, []):
        value = os.environ.get(env_key, "").strip()
        if value:
            endpoints.append(value)
    endpoints.extend(PUBLIC_ENDPOINTS.get(chain, []))
    if not endpoints:
        raise ValueError(f"No RPC endpoints configured for chain {chain!r}")
    return endpoints


def safe_host(url):
    """Hostname only — safe to log/serialize (full URLs may contain API keys)."""
    return urlparse(url).hostname or "rpc"


class RpcError(Exception):
    """All endpoints failed for a request."""


class RpcClient:
    def __init__(self, chain=None, endpoints=None, timeout=DEFAULT_TIMEOUT,
                 retries_per_endpoint=DEFAULT_RETRIES_PER_ENDPOINT, quiet=False):
        if endpoints is None:
            if chain is None:
                raise ValueError("Provide either chain or endpoints")
            endpoints = get_endpoints(chain)
        self.endpoints = list(endpoints)
        self.timeout = timeout
        self.retries_per_endpoint = retries_per_endpoint
        self.quiet = quiet
        self._idx = 0  # sticky: keep using the endpoint that last worked
        self.last_endpoint = None

    def _log(self, message):
        if not self.quiet:
            print(message, flush=True)

    def _post(self, url, payload):
        resp = requests.post(url, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _attempts(self):
        """Yield (endpoint, attempt_no) starting from the sticky index."""
        n = len(self.endpoints)
        for round_no in range(self.retries_per_endpoint):
            for offset in range(n):
                yield self.endpoints[(self._idx + offset) % n], round_no * n + offset

    def _request(self, payload, describe):
        last_error = None
        n = len(self.endpoints)
        for url, attempt in self._attempts():
            try:
                result = self._post(url, payload)
                self._idx = self.endpoints.index(url)
                self.last_endpoint = url
                return result
            except Exception as exc:  # requests errors, JSON errors, HTTP errors
                last_error = exc
                self._log(f"⚠️ RPC {describe} failed on {safe_host(url)} "
                          f"(attempt {attempt + 1}): {exc}")
                # Exponential backoff with jitter between full rotation rounds
                if (attempt + 1) % n == 0:
                    delay = BACKOFF_BASE_SECONDS * (2 ** ((attempt + 1) // n - 1))
                    time.sleep(delay + random.uniform(0, 0.4))
        raise RpcError(f"All RPC endpoints failed for {describe}: {last_error}")

    def call(self, method, params):
        """Single JSON-RPC call. Returns the `result` field."""
        payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
        data = self._request(payload, method)
        if isinstance(data, dict) and data.get("error"):
            raise RpcError(f"{method} returned error: {data['error']}")
        return data.get("result") if isinstance(data, dict) else data

    def eth_call_batch(self, calls, block="latest"):
        """Batch of eth_call requests. `calls` = [(to, data), …].
        Returns hex results in input order."""
        payload = [
            {"jsonrpc": "2.0", "method": "eth_call",
             "params": [{"to": to, "data": data}, block], "id": i + 1}
            for i, (to, data) in enumerate(calls)
        ]
        results = self._request(payload, f"eth_call batch x{len(calls)}")
        if isinstance(results, dict):
            results = [results]
        by_id = {r.get("id"): r for r in results}
        out = []
        for i in range(1, len(calls) + 1):
            entry = by_id.get(i, {})
            if entry.get("error"):
                raise RpcError(f"eth_call id={i} returned error: {entry['error']}")
            out.append(entry.get("result", "0x0"))
        return out


def decode_uint256(hex_str):
    """Decode a 0x-prefixed hex word to int (0 for empty results)."""
    if not hex_str or hex_str in ("0x", "0x0"):
        return 0
    clean = hex_str.replace("0x", "")[:64]
    return int(clean, 16) if clean else 0
