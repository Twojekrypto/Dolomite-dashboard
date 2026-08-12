import unittest
from html.parser import HTMLParser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class AddressMatchTableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "table":
            return
        attributes = dict(attrs)
        if "data-address-match-cells" not in attributes:
            return
        self.tables.append(
            {
                "id": attributes.get("id", ""),
                "classes": frozenset(attributes.get("class", "").split()),
            }
        )


class AddressMatchTableScopeTest(unittest.TestCase):
    def _opted_in_tables(self):
        records = []
        for path in sorted(ROOT.glob("*-preview.html")):
            parser = AddressMatchTableParser()
            parser.feed(path.read_text(encoding="utf-8"))
            records.extend((path.name, table) for table in parser.tables)
        return records

    def test_only_audited_repeating_wallet_tables_opt_in(self):
        records = self._opted_in_tables()
        identified = {
            (filename, table["id"])
            for filename, table in records
            if table["id"]
        }
        class_only = {
            (filename, table["classes"])
            for filename, table in records
            if not table["id"]
        }

        self.assertEqual(
            identified,
            {
                ("vedolo-preview.html", "exits-table"),
                ("vedolo-preview.html", "locks-table"),
                ("vedolo-preview.html", "unlocks-table"),
                ("vedolo-preview.html", "claimable-table"),
                ("dolo-preview.html", "dolo-lp-table"),
                ("odolo-preview.html", "tbl-latest-ex"),
                ("odolo-preview.html", "tbl-latest-pair"),
                ("liquidation-preview.html", "positions-table"),
                ("liquidation-preview.html", "liquidation-history-table"),
                ("liquidation-preview.html", "supply-activity-table"),
            },
        )
        self.assertEqual(
            class_only,
            {
                (
                    "liquidation-preview.html",
                    frozenset({"positions-table", "sim-atrisk-table"}),
                )
            },
        )
        self.assertEqual(len(records), 11)

    def test_opted_in_pages_render_canonical_full_addresses(self):
        vedolo = (ROOT / "vedolo-preview.html").read_text(encoding="utf-8")
        odolo = (ROOT / "odolo-preview.html").read_text(encoding="utf-8")
        liquidation = (ROOT / "liquidation-preview.html").read_text(encoding="utf-8")

        for renderer in ("function exitAddressCell", "function flowAddressCell", "function holderCell"):
            self.assertIn(renderer, vedolo)
        self.assertGreaterEqual(vedolo.count("data-full-addr"), 3)

        for renderer in ("function renderLatestExercises", "function renderLatestPairs"):
            self.assertIn(renderer, odolo)
        self.assertGreaterEqual(odolo.count("data-full-addr"), 2)

        self.assertIn("function walletCell", (ROOT / "dolo-preview.html").read_text(encoding="utf-8"))

        for renderer in ("renderDoloAddressTools", "renderActivityAddressTools"):
            self.assertIn(renderer, liquidation)
        self.assertGreaterEqual(liquidation.count("data-full-addr"), 2)

    def test_opted_in_pages_load_the_shared_assets_with_fresh_cache_keys(self):
        version = "20260812-row-focusout"
        for preview_name in (
            "assets-preview.html",
            "dolo-preview.html",
            "portfolio-preview.html",
            "revenue-preview.html",
            "rewards-preview.html",
            "tvl-preview.html",
            "vedolo-preview.html",
            "odolo-preview.html",
            "liquidation-preview.html",
        ):
            html = (ROOT / preview_name).read_text(encoding="utf-8")
            self.assertIn(f"shared-hover-tooltips.css?v={version}", html, preview_name)
            self.assertIn(f"shared-hover-tooltips.js?v={version}", html, preview_name)

        liquidation = (ROOT / "liquidation-preview.html").read_text(encoding="utf-8")
        marker = "window.__DOLO_INLINE_TOOLTIP_ACTIVE=true"
        head = liquidation.split("</head>", 1)[0]
        self.assertIn(marker, liquidation)
        self.assertNotIn(f'src="shared-hover-tooltips.js?v={version}"', head)
        self.assertLess(
            liquidation.index(marker),
            liquidation.index(f"shared-hover-tooltips.css?v={version}"),
        )
        self.assertIn("if(!window.__DOLO_ADDRESS_MATCH_LOADED)", liquidation)
        self.assertIn("setTimeout(loadAddressMatch,0)", liquidation)
        self.assertIn(
            f"fallback.src='shared-hover-tooltips.js?v={version}'",
            liquidation,
        )

        for route_name in (
            "assets/index.html",
            "dolo/index.html",
            "portfolio/index.html",
            "revenue/index.html",
            "rewards/index.html",
            "tvl/index.html",
            "vedolo/index.html",
            "odolo/index.html",
            "borrow/index.html",
            "liquidation/index.html",
            "supply/index.html",
        ):
            route = (ROOT / route_name).read_text(encoding="utf-8")
            self.assertIn("row-address-peers-20260809", route, route_name)

if __name__ == "__main__":
    unittest.main()
