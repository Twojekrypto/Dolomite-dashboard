import json
import unittest
from html.parser import HTMLParser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BEACON_SRC = "https://static.cloudflareinsights.com/beacon.min.js"
BEACON_TOKEN = "930335c0b8864fdf8d9748c2432adaed"


class _ScriptTagParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.scripts = []
        self.meta = []

    def handle_starttag(self, tag, attrs):
        if tag == "script":
            self.scripts.append(dict(attrs))
        elif tag == "meta":
            self.meta.append(dict(attrs))


class CloudflareAnalyticsContractTests(unittest.TestCase):
    def test_every_public_entrypoint_uses_exactly_one_analytics_installation_path(self):
        entrypoints = [ROOT / "index.html", *sorted(ROOT.glob("*/index.html"))]
        self.assertGreater(len(entrypoints), 1)

        for entrypoint in entrypoints:
            with self.subTest(entrypoint=entrypoint.relative_to(ROOT)):
                parser = _ScriptTagParser()
                parser.feed(entrypoint.read_text(encoding="utf-8"))
                beacons = [
                    script
                    for script in parser.scripts
                    if script.get("src") == BEACON_SRC or "data-cf-beacon" in script
                ]
                uses_route_loader = any(
                    script.get("src", "").split("?", 1)[0].endswith("route-loader.js")
                    for script in parser.scripts
                )

                if uses_route_loader:
                    self.assertEqual(beacons, [], "route-loader injects the shared beacon")
                else:
                    self.assertEqual(len(beacons), 1)
                    self.assertEqual(beacons[0].get("type"), "module")
                    self.assertEqual(beacons[0].get("src"), BEACON_SRC)
                    self.assertEqual(
                        json.loads(beacons[0].get("data-cf-beacon", "")),
                        {"token": BEACON_TOKEN},
                    )

    def test_content_security_policies_allow_the_official_beacon_script(self):
        for document in sorted(ROOT.glob("*.html")):
            with self.subTest(document=document.name):
                parser = _ScriptTagParser()
                parser.feed(document.read_text(encoding="utf-8"))
                policies = [
                    meta.get("content", "")
                    for meta in parser.meta
                    if meta.get("http-equiv", "").lower() == "content-security-policy"
                ]
                for policy in policies:
                    if "script-src" in policy:
                        self.assertIn("https://static.cloudflareinsights.com", policy)


if __name__ == "__main__":
    unittest.main()
