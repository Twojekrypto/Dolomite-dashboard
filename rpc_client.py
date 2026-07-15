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
import re
import time
from urllib.parse import urlparse

import requests

import rpc_usage

DEFAULT_TIMEOUT = 15
DEFAULT_RETRIES_PER_ENDPOINT = 2
BACKOFF_BASE_SECONDS = 0.8

# Secrets injected by GitHub Actions (never hardcode keys here).
CHAIN_ENV_KEYS = {
    "berachain": ["ALCHEMY_BERACHAIN_RPC", "QUICKNODE_BERACHAIN_RPC_2", "DRPC_BERACHAIN_RPC_ZEN", "ALCHEMY_BERACHAIN_RPC_2", "ALCHEMY_BERACHAIN_RPC_3"],
    "ethereum": ["ALCHEMY_ETHEREUM_RPC_KAT", "ALCHEMY_ETHEREUM_RPC_DAN", "ALCHEMY_ETHEREUM_RPC_ZEN", "ALCHEMY_ETHEREUM_RPC", "ALCHEMY_ETHEREUM_RPC_2", "ALCHEMY_ETHEREUM_RPC_3"],
    "arbitrum": ["ALCHEMY_ARBITRUM_RPC_KAT", "ALCHEMY_ARBITRUM_RPC_DAN", "ALCHEMY_ARBITRUM_RPC_ZEN", "ALCHEMY_ARBITRUM_RPC", "ALCHEMY_ARBITRUM_RPC_2", "ALCHEMY_ARBITRUM_RPC_3"],
    "mantle": ["ALCHEMY_MANTLE_RPC", "ALCHEMY_MANTLE_RPC_2"],
    "xlayer": ["ALCHEMY_XLAYER_RPC"],
    "botanix": ["ALCHEMY_BOTANIX_RPC", "ALCHEMY_BOTANIX_RPC_2", "ALCHEMY_BOTANIX_RPC_3"],
    "polygon_zkevm": ["ALCHEMY_POLYGONZKEVM_RPC_ZEN", "DRP_POLYGONZKEVM_RPC_TWO", "POLYGONZKEVM_RPC", "POLYGONZKEVM_RPC_2"],
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
    "polygon_zkevm": [
        "https://zkevm-rpc.com/",
        "https://polygon-zkevm-rpc.publicnode.com/",
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


# Key-bearing URL path/query fragments as emitted by provider endpoints
# (e.g. alchemy /v2/<key>, infura /v3/<key>, drpc ?dkey=<key>).
_KEY_PATTERN = re.compile(r"(/v2/|/v3/|/api/|/rpc/|[?&](?:api)?d?key=)[A-Za-z0-9_-]{8,}")


def sanitize_error(exc):
    """Stringify an exception with any URL-embedded API keys redacted.

    requests' ConnectionError/Timeout messages include the full request URL
    (`HTTPSConnectionPool(host=...): ... url: /v2/<KEY>`); GitHub Actions'
    secret masking only matches the entire secret string, so a key appearing
    as a bare path segment would leak into public logs.
    """
    return _KEY_PATTERN.sub(r"\1***", str(exc))


class RpcError(Exception):
    """All endpoints failed for a request."""


def _endpoint_attempts(endpoints, retries_per_endpoint):
    n = len(endpoints)
    for round_no in range(retries_per_endpoint):
        for offset in range(n):
            yield endpoints[offset], round_no * n + offset


def _retry_after_seconds(response, attempt, *, rate_limited=False):
    """Return a bounded cooldown for rate-limited responses, if applicable."""
    headers = getattr(response, "headers", {}) or {}
    retry_after = headers.get("Retry-After") if hasattr(headers, "get") else None
    try:
        seconds = float(retry_after)
    except (TypeError, ValueError):
        status_code = getattr(response, "status_code", None)
        if status_code != 429 and not rate_limited:
            return None
        seconds = BACKOFF_BASE_SECONDS * (2 ** min(attempt, 4))
    if seconds < 0:
        return None
    return min(seconds, 60.0)


def _is_rate_limited_error(entry):
    if not isinstance(entry, dict):
        return False
    error = entry.get("error")
    if not isinstance(error, dict):
        return False
    if error.get("code") == 429:
        return True
    message = str(error.get("message", "")).lower()
    return (
        "rate limit" in message
        or "too many" in message
        or "throttl" in message
        or "capacity" in message
    )


def rpc_single_request(endpoints, payload, timeout=DEFAULT_TIMEOUT,
                       retries_per_endpoint=DEFAULT_RETRIES_PER_ENDPOINT,
                       quiet=False, describe="request"):
    """Run one JSON-RPC payload across endpoint fallbacks."""
    endpoints = list(endpoints or [])
    if not endpoints:
        raise RpcError(f"No RPC endpoints configured for {describe}")

    last_error = None
    n = len(endpoints)
    for url, attempt in _endpoint_attempts(endpoints, retries_per_endpoint):
        try:
            resp = requests.post(url, json=payload, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict):
                rpc_usage.record_methods(rpc_usage.methods_from_payload(payload))
                return data
            raise ValueError("JSON-RPC response was not an object")
        except Exception as exc:
            last_error = exc
            if not quiet:
                print(
                    f"⚠️ RPC {describe} failed on {safe_host(url)} "
                    f"(attempt {attempt + 1}): {type(exc).__name__}: {sanitize_error(exc)}",
                    flush=True,
                )
            if (attempt + 1) % n == 0:
                delay = BACKOFF_BASE_SECONDS * (2 ** ((attempt + 1) // n - 1))
                time.sleep(delay + random.uniform(0, 0.4))

    raise RpcError(
        f"All RPC endpoints failed for {describe}: "
        f"{type(last_error).__name__}: {sanitize_error(last_error)}"
    )


def rpc_batch_requests(endpoints, payloads, timeout=DEFAULT_TIMEOUT,
                       retries_per_endpoint=DEFAULT_RETRIES_PER_ENDPOINT,
                       batch_size=50, min_batch_interval_seconds=0,
                       quiet=False, describe="batch"):
    """Run JSON-RPC batch payloads with endpoint fallback.

    Returns (responses_by_id, missing_ids). Error entries are returned to the
    caller so it can decide whether to retry individually or treat them as data.
    """
    endpoints = list(endpoints or [])
    if not endpoints:
        raise RpcError(f"No RPC endpoints configured for {describe}")

    responses_by_id = {}
    missing_ids = []
    payloads = list(payloads or [])
    if not payloads:
        return responses_by_id, missing_ids

    try:
        min_batch_interval_seconds = max(0.0, float(min_batch_interval_seconds))
    except (TypeError, ValueError):
        raise ValueError("min_batch_interval_seconds must be a nonnegative number")

    previous_batch_finished_at = None

    for start in range(0, len(payloads), max(1, int(batch_size or 1))):
        if previous_batch_finished_at is not None and min_batch_interval_seconds:
            remaining = min_batch_interval_seconds - (
                time.monotonic() - previous_batch_finished_at
            )
            if remaining > 0:
                time.sleep(remaining)
        chunk = payloads[start:start + max(1, int(batch_size or 1))]
        chunk_ids = [payload.get("id") for payload in chunk]
        chunk_responses = {}
        fallback_responses = {}
        last_error = None
        n = len(endpoints)

        for url, attempt in _endpoint_attempts(endpoints, retries_per_endpoint):
            try:
                resp = requests.post(url, json=chunk, timeout=timeout)
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, dict):
                    data = [data]
                if not isinstance(data, list):
                    raise ValueError("JSON-RPC batch response was not a list")
                matching_entries = {
                    item.get("id"): item
                    for item in data
                    if isinstance(item, dict) and item.get("id") in chunk_ids
                }
                by_id = {
                    item_id: item
                    for item_id, item in matching_entries.items()
                    if not item.get("error")
                }
                fallback_responses.update(by_id)
                error_entries = [
                    item for item in matching_entries.values() if item.get("error")
                ]
                if by_id and not error_entries:
                    chunk_responses = {**fallback_responses, **by_id}
                    break
                if error_entries:
                    last_error = RpcError("JSON-RPC batch contained error entries")
                    if any(_is_rate_limited_error(item) for item in error_entries):
                        delay = _retry_after_seconds(resp, attempt, rate_limited=True)
                        if delay is not None:
                            time.sleep(delay)
                    elif (attempt + 1) % n == 0:
                        delay = BACKOFF_BASE_SECONDS * (2 ** ((attempt + 1) // n - 1))
                        time.sleep(delay + random.uniform(0, 0.4))
                    continue
                raise ValueError("JSON-RPC batch response contained no matching ids")
            except Exception as exc:
                last_error = exc
                if not quiet:
                    print(
                        f"⚠️ RPC {describe} failed on {safe_host(url)} "
                        f"(attempt {attempt + 1}): {type(exc).__name__}: {sanitize_error(exc)}",
                        flush=True,
                    )
                delay = _retry_after_seconds(getattr(exc, "response", None), attempt)
                if delay is not None:
                    time.sleep(delay)
                elif (attempt + 1) % n == 0:
                    delay = BACKOFF_BASE_SECONDS * (2 ** ((attempt + 1) // n - 1))
                    time.sleep(delay + random.uniform(0, 0.4))

        if not chunk_responses:
            chunk_responses = fallback_responses
        responses_by_id.update(chunk_responses)
        missing_ids.extend([item_id for item_id in chunk_ids if item_id not in chunk_responses])
        previous_batch_finished_at = time.monotonic()
        if chunk_responses:
            rpc_usage.record_methods(rpc_usage.methods_from_payload(chunk))
        if not chunk_responses and last_error and not quiet:
            print(
                f"⚠️ RPC {describe} batch chunk {start // max(1, int(batch_size or 1)) + 1} "
                f"failed completely: {type(last_error).__name__}: {sanitize_error(last_error)}",
                flush=True,
            )

    return responses_by_id, missing_ids


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
                rpc_usage.record_methods(rpc_usage.methods_from_payload(payload))
                return result
            except Exception as exc:  # requests errors, JSON errors, HTTP errors
                last_error = exc
                # NOTE: never log the raw exception — requests' connection errors
                # embed the full request URL (incl. the API key path segment),
                # and GitHub's secret masking won't catch a partial-URL leak.
                self._log(f"⚠️ RPC {describe} failed on {safe_host(url)} "
                          f"(attempt {attempt + 1}): {type(exc).__name__}: {sanitize_error(exc)}")
                # Exponential backoff with jitter between full rotation rounds
                if (attempt + 1) % n == 0:
                    delay = BACKOFF_BASE_SECONDS * (2 ** ((attempt + 1) // n - 1))
                    time.sleep(delay + random.uniform(0, 0.4))
        raise RpcError(
            f"All RPC endpoints failed for {describe}: "
            f"{type(last_error).__name__}: {sanitize_error(last_error)}"
        )

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
