"""Fail-closed Etherscan-compatible requests with an unsupported-chain fallback."""

import os
import re
import time
from urllib.parse import urlsplit

import requests


ETHERSCAN_V2 = "https://api.etherscan.io/v2/api"


def explorer_get_with_retry(url, *, attempts=3, **kwargs):
    """Retry the identical page on transient failures, never access/quota errors."""
    for attempt in range(max(1, attempts)):
        try:
            return explorer_get(url, **kwargs)
        except (ExplorerError, requests.RequestException) as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            retryable = exc.retryable if isinstance(exc, ExplorerError) else status in {429, 500, 502, 503, 504}
            if not retryable or attempt + 1 >= max(1, attempts):
                raise
            headers = getattr(getattr(exc, "response", None), "headers", {}) or {}
            try:
                delay = float(headers.get("Retry-After", 0))
            except (ValueError, TypeError):
                delay = 0
            time.sleep(min(30, max(2 ** (attempt + 1), delay)))


class ExplorerError(RuntimeError):
    """Sanitized API rejection; only explicitly classified transients retry."""

    def __init__(self, message, *, category="invalid", response=None):
        super().__init__(message)
        self.category = category
        self.response = response
        self.retryable = category == "transient"


def explorer_get(url, *, params, timeout, session=None, **kwargs):
    """Keep filters/pagination intact; never interpret an API failure as no events."""
    client = session or requests

    def fetch(endpoint, query):
        try:
            result = client.get(endpoint, params=query, timeout=timeout, **kwargs)
            result.raise_for_status()
        except requests.RequestException as exc:
            # requests' normal exception string can contain the API key URL.
            status = getattr(getattr(exc, "response", None), "status_code", None)
            raise requests.HTTPError(
                f"Explorer request failed on {urlsplit(endpoint).hostname} (HTTP {status})",
                response=getattr(exc, "response", None),
            ) from None
        return result

    def parse(result):
        try:
            return result.json()
        except ValueError:
            raise ExplorerError("Explorer returned malformed JSON", response=result) from None

    result = fetch(url, params)
    payload = parse(result)
    match = re.fullmatch(
        r"https://api\.routescan\.io/v2/network/mainnet/evm/(\d+)/etherscan/api", url
    )
    if isinstance(payload, dict) and str(payload.get("message", "")).lower() == "chain not supported" and match:
        chain_id = match.group(1)
        key_names = ["ETHERSCAN_API_KEY"]
        if chain_id == "80094":
            key_names.append("BERASCAN_API_KEY")
        api_keys = list(dict.fromkeys(
            os.environ.get(name, "").strip() for name in key_names
            if os.environ.get(name, "").strip()
        ))
        if not api_keys:
            raise ExplorerError(f"Explorer chain {chain_id} unavailable; configure ETHERSCAN_API_KEY", category="unsupported", response=result)
        for api_key in api_keys:
            result = fetch(ETHERSCAN_V2, {**params, "chainid": chain_id, "apikey": api_key})
            payload = parse(result)
            # A configured but revoked primary must not shadow the backup.
            # Only credential rejection permits another key, never throttling
            # or an incomplete/malformed result. Final validation stays closed.
            invalid_key = (isinstance(payload, dict)
                           and str(payload.get("status")) == "0"
                           and "invalid api key" in str(payload.get("result", "")).lower())
            if not invalid_key:
                break
    if not isinstance(payload, dict):
        raise ExplorerError("Explorer returned an invalid response object", response=result)
    # Proxy methods return JSON-RPC rather than an Etherscan status envelope.
    if params.get("module") == "proxy":
        if payload.get("error") or "result" not in payload:
            raise ExplorerError("Explorer proxy request failed", response=result)
        return result
    if str(payload.get("status")) not in {"0", "1"} or "result" not in payload:
        raise ExplorerError("Explorer returned an invalid status/result envelope", response=result)
    if str(payload.get("status")) == "0":
        rows = payload.get("result")
        text = f"{payload.get('message', '')} {rows if isinstance(rows, str) else ''}".lower()
        # Empty arrays are the legacy successful end-of-pagination response.
        message = str(payload.get("message", "OK")).strip().lower()
        empty_messages = {"no records found", "no transactions found"}
        empty = (rows == [] and message in {
            "ok", "no records found", "no transactions found"
        }) or (
            (rows is None or isinstance(rows, str))
            and message in empty_messages
            and (rows is None or str(rows).strip().lower() in empty_messages | {""})
        )
        if not empty:
            reason = f"{payload.get('message', '')}: {rows if isinstance(rows, str) else ''}"
            for key in (os.environ.get("ETHERSCAN_API_KEY"), os.environ.get("BERASCAN_API_KEY"), params.get("apikey")):
                if key:
                    reason = reason.replace(str(key), "[redacted]")
            reason = re.sub(r"https?://\S+", "[URL redacted]", reason)
            reason = " ".join(reason.split())[:300]
            category = "rejected"
            if any(token in text for token in ("pro subscription", "pro endpoint", "paid plan", "upgrade", "invalid api key", "unauthorized", "access denied", "quota", "daily limit", "monthly limit")):
                category = "access"
            elif not any(token in text for token in empty_messages) and any(token in text for token in ("rate limit", "too many requests", "temporarily unavailable", "server busy", "query timeout")):
                category = "transient"
            raise ExplorerError(f"Explorer rejected request ({reason}); publication stopped", category=category, response=result)
    if (str(payload.get("status")) == "1"
            and params.get("action") in {"getLogs", "txlist", "tokentx", "tokenholderlist"}
            and not isinstance(payload.get("result"), list)):
        raise ExplorerError("Explorer returned a malformed list result", response=result)
    return result
