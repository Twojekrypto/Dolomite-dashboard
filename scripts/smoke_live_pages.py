#!/usr/bin/env python3
"""Small post-deploy smoke check for GitHub Pages routes and shared assets."""

from __future__ import annotations

import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import json


ROUTE_CHECKS = {
    "/": ("dolo-preview.html", "Dolomite"),
    "/dolo/": ("dolo-preview.html", "Dolomite"),
    "/assets/": ("assets-preview.html", "Assets"),
    "/tvl/": ("tvl-preview.html", "TVL"),
    "/odolo/": ("odolo-preview.html", "oDOLO"),
    "/vedolo/": ("vedolo-preview.html", "veDOLO"),
    "/earn/": ("dashboard-core.html", "Earn"),
    "/borrow/": ("liquidation-preview.html", "Borrow"),
    "/supply/": ("liquidation-preview.html", "Supply"),
    "/revenue/": ("revenue-preview.html", "Revenue"),
    "/liquidation/": ("liquidation-preview.html", "Borrow"),
}

ASSET_CHECKS = {
    "/dolo-address-labels.js": ("cloneDoloAddressLabels", "confidence", "source"),
    "/dolo-preview.html": ("dolo-address-labels.js", "DOLO Holders"),
    "/dashboard-core.html": ("dolo-address-labels.js", "Earn"),
    "/liquidation-preview.html": ("dolo-address-labels.js", "Supply"),
    "/revenue-preview.html": ("dolomite_revenue.json", "Revenue"),
}
WLFI_MIN_USD = 10_000_000


def join_url(base_url: str, path: str) -> str:
    return urllib.parse.urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))


def fetch_text(url: str, *, timeout: int = 20) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "dolomite-dashboard-smoke/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        status = getattr(response, "status", 200)
        if status >= 400:
            raise RuntimeError(f"{url} returned HTTP {status}")
        return response.read().decode("utf-8", errors="replace")


def assert_contains(url: str, expected: tuple[str, ...], *, attempts: int = 6, sleep_seconds: int = 10) -> None:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            body = fetch_text(url)
            missing = [needle for needle in expected if needle not in body]
            if not missing:
                print(f"PASS {url}")
                return
            raise AssertionError(f"{url} missing: {', '.join(missing)}")
        except (AssertionError, OSError, urllib.error.URLError) as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(sleep_seconds)
    raise RuntimeError(str(last_error))


def assert_not_contains(body: str, unexpected: tuple[str, ...], url: str) -> None:
    found = [needle for needle in unexpected if needle in body]
    if found:
        raise RuntimeError(f"{url} still contains stale smoke blockers: {', '.join(found)}")


def fetch_json(url: str, *, timeout: int = 20) -> dict:
    return json.loads(fetch_text(url, timeout=timeout))


def assert_tvl_composition_live(base_url: str, *, attempts: int = 6, sleep_seconds: int = 10) -> None:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        cache = f"smoke={int(time.time())}"
        route_url = join_url(base_url, f"/tvl/?{cache}")
        preview_url = join_url(base_url, f"/tvl-preview.html?{cache}")
        tvl_json_url = join_url(base_url, f"/dolomite_tvl.json?{cache}")
        try:
            route = fetch_text(route_url)
            if "stale-token-fix-20260528" not in route:
                raise AssertionError(f"{route_url} missing current TVL preview cache-bust")

            preview = fetch_text(preview_url)
            expected_preview = (
                "Supply Over Time",
                'const tokens = officialTokens[chain] || llamaTokens[chain];',
                "WLFI.2c1e57c98e3cad7282c40941b36ee606.svg",
            )
            missing_preview = [needle for needle in expected_preview if needle not in preview]
            if missing_preview:
                raise AssertionError(f"{preview_url} missing: {', '.join(missing_preview)}")
            assert_not_contains(
                preview,
                ("TVL Over Time", "staleKeys.has(chainKey(chain)) && llamaTokens[chain]"),
                preview_url,
            )

            tvl_data = fetch_json(tvl_json_url)
            latest = (tvl_data.get("tokensInUsd") or [{}])[-1].get("tokens", {})
            chain_tokens = tvl_data.get("chainTokensInUsd", {})
            wlfi_global = float(latest.get("WLFI", 0))
            wlfi_ethereum = float(chain_tokens.get("Ethereum", {}).get("WLFI", 0))
            if wlfi_global < WLFI_MIN_USD or wlfi_ethereum < WLFI_MIN_USD:
                raise AssertionError(
                    f"{tvl_json_url} WLFI too small or missing: "
                    f"global=${wlfi_global:,.0f}, ethereum=${wlfi_ethereum:,.0f}"
                )
            print(f"PASS TVL WLFI composition ${wlfi_global:,.0f}")
            return
        except (AssertionError, OSError, ValueError, urllib.error.URLError) as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(sleep_seconds)
    raise RuntimeError(str(last_error))


def main(argv: list[str]) -> int:
    base_url = argv[1] if len(argv) > 1 else "https://twojekrypto.github.io/Dolomite-dashboard/"
    for path, expected in ROUTE_CHECKS.items():
        assert_contains(join_url(base_url, path), expected)
    for path, expected in ASSET_CHECKS.items():
        assert_contains(join_url(base_url, path + "?smoke=1"), expected)
    assert_tvl_composition_live(base_url)
    print("Live Pages smoke checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
