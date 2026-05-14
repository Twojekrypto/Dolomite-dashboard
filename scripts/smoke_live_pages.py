#!/usr/bin/env python3
"""Small post-deploy smoke check for GitHub Pages routes and shared assets."""

from __future__ import annotations

import sys
import time
import urllib.error
import urllib.parse
import urllib.request


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
    "/liquidation/": ("liquidation-preview.html", "Borrow"),
}

ASSET_CHECKS = {
    "/dolo-address-labels.js": ("cloneDoloAddressLabels", "confidence", "source"),
    "/dolo-preview.html": ("dolo-address-labels.js", "DOLO Holders"),
    "/dashboard-core.html": ("dolo-address-labels.js", "Earn"),
    "/liquidation-preview.html": ("dolo-address-labels.js", "Supply"),
}


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


def main(argv: list[str]) -> int:
    base_url = argv[1] if len(argv) > 1 else "https://twojekrypto.github.io/Dolomite-dashboard/"
    for path, expected in ROUTE_CHECKS.items():
        assert_contains(join_url(base_url, path), expected)
    for path, expected in ASSET_CHECKS.items():
        assert_contains(join_url(base_url, path + "?smoke=1"), expected)
    print("Live Pages smoke checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
