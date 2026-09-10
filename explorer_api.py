"""Fail-closed Etherscan-compatible requests with an unsupported-chain fallback."""

import os
import re
from urllib.parse import urlsplit

import requests


ETHERSCAN_V2 = "https://api.etherscan.io/v2/api"


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

    result = fetch(url, params)
    payload = result.json()
    match = re.fullmatch(
        r"https://api\.routescan\.io/v2/network/mainnet/evm/(\d+)/etherscan/api", url
    )
    if isinstance(payload, dict) and str(payload.get("message", "")).lower() == "chain not supported" and match:
        chain_id = match.group(1)
        api_key = os.environ.get("ETHERSCAN_API_KEY", "").strip()
        if not api_key and chain_id == "80094":
            api_key = os.environ.get("BERASCAN_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError(f"Explorer chain {chain_id} unavailable; configure ETHERSCAN_API_KEY")
        result = fetch(ETHERSCAN_V2, {**params, "chainid": chain_id, "apikey": api_key})
        payload = result.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Explorer returned an invalid response object")
    if str(payload.get("status")) == "0":
        rows = payload.get("result")
        text = f"{payload.get('message', '')} {rows if isinstance(rows, str) else ''}".lower()
        # Empty arrays are the legacy successful end-of-pagination response.
        empty = (rows == [] and str(payload.get("message", "OK")).lower() in {
            "ok", "no records found", "no transactions found"
        }) or (
            (rows is None or isinstance(rows, str))
            and ("no transactions found" in text or "no records" in text)
        )
        if not empty:
            reason = f"{payload.get('message', '')}: {rows if isinstance(rows, str) else ''}"
            for key in (os.environ.get("ETHERSCAN_API_KEY"), os.environ.get("BERASCAN_API_KEY"), params.get("apikey")):
                if key:
                    reason = reason.replace(str(key), "[redacted]")
            reason = re.sub(r"https?://\S+", "[URL redacted]", reason)
            reason = " ".join(reason.split())[:300]
            raise RuntimeError(f"Explorer rejected request ({reason}); publication stopped")
    return result
