import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def css_rule(source: str, selector: str) -> str:
    match = re.search(re.escape(selector) + r"\s*\{([^}]*)\}", source)
    if not match:
        raise AssertionError(f"Missing CSS rule for {selector}")
    return re.sub(r"\s+", "", match.group(1))


class ExpandedDetailsSurfaceContracts(unittest.TestCase):
    def assert_graphite_gold_detail_surface(
        self,
        path: str,
        *,
        expanded_selector: str,
        expanded_rail_selector: str,
        detail_selector: str,
        detail_rail_selector: str,
        panel_selector: str,
        panel_rail_selector: str,
    ) -> None:
        source = (ROOT / path).read_text()

        expanded = css_rule(source, expanded_selector)
        self.assertIn("background:var(--gold-wash)", expanded)
        self.assertIn("border-bottom-color:transparent", expanded)

        expanded_rail = css_rule(source, expanded_rail_selector)
        self.assertIn("width:2px", expanded_rail)
        self.assertIn("background:var(--gold)", expanded_rail)
        self.assertIn("box-shadow:0012pxvar(--gold-line)", expanded_rail)

        detail = css_rule(source, detail_selector)
        self.assertIn("position:relative", detail)
        self.assertIn("padding:0", detail)
        self.assertIn(
            "background:linear-gradient(90deg,rgba(201,162,39,.035),var(--bg-1)42px)",
            detail,
        )
        self.assertIn("border-bottom-color:var(--line-2)", detail)
        self.assertIn("white-space:normal", detail)

        detail_rail = css_rule(source, detail_rail_selector)
        self.assertIn("width:2px", detail_rail)
        self.assertIn("background:var(--gold)", detail_rail)
        self.assertIn("box-shadow:0012pxvar(--gold-line)", detail_rail)

        panel = css_rule(source, panel_selector)
        self.assertIn("width:100%", panel)
        self.assertIn("border:0", panel)
        self.assertIn("border-radius:0", panel)
        self.assertIn("background:transparent", panel)
        self.assertIn("box-shadow:none", panel)
        self.assertIn("overflow:visible", panel)

        panel_rail = css_rule(source, panel_rail_selector)
        self.assertIn("display:none", panel_rail)

    def test_top_odolo_exercisers_matches_supply_pool_health_details(self):
        self.assert_graphite_gold_detail_surface(
            "odolo-preview.html",
            expanded_selector="#tbl-ex > tbody > tr.ex-row-open > td",
            expanded_rail_selector="#tbl-ex > tbody > tr.ex-row-open > td:first-child::before",
            detail_selector=".tbl > tbody > tr.ex-detail-row > td",
            detail_rail_selector=".tbl > tbody > tr.ex-detail-row > td::before",
            panel_selector=".ex-detail-panel",
            panel_rail_selector=".ex-detail-panel::before",
        )

    def test_dolomite_assets_matches_supply_pool_health_details(self):
        self.assert_graphite_gold_detail_surface(
            "assets-preview.html",
            expanded_selector=".tbl tbody tr.expanded td",
            expanded_rail_selector=".tbl tbody tr.data.expanded td:first-child::before",
            detail_selector=".exp-row > td",
            detail_rail_selector=".exp-row > td::before",
            panel_selector=".asset-detail-panel",
            panel_rail_selector=".asset-detail-panel::before",
        )

    def test_vedolo_holders_matches_supply_pool_health_details(self):
        self.assert_graphite_gold_detail_surface(
            "vedolo-preview.html",
            expanded_selector="#holders-table .holder-data-row.holder-row-open td",
            expanded_rail_selector="#holders-table .holder-data-row.holder-row-open td:first-child::before",
            detail_selector="#holders-table .holder-detail-row > td",
            detail_rail_selector="#holders-table .holder-detail-row > td::before",
            panel_selector="#holders-table .holder-detail-panel",
            panel_rail_selector="#holders-table .holder-detail-panel::before",
        )

    def test_earn_supply_and_past_assets_match_supply_pool_health_details(self):
        source = (ROOT / "earn/earn-draft.css").read_text()

        expanded = css_rule(
            source,
            "body.earn-draft-route .earn-asset-table tbody tr.earn-data-row.expanded td,\n"
            "body.earn-draft-route .earn-past-table tbody tr.earn-data-row.expanded td",
        )
        self.assertIn("background:var(--earn-gold-wash)!important", expanded)
        self.assertIn("border-bottom-color:transparent!important", expanded)

        expanded_rail = css_rule(
            source,
            "body.earn-draft-route .earn-asset-table tbody tr.earn-data-row.expanded td:first-child::before,\n"
            "body.earn-draft-route .earn-past-table tbody tr.earn-data-row.expanded td:first-child::before",
        )
        self.assertIn("width:2px!important", expanded_rail)
        self.assertIn("background:var(--earn-gold)!important", expanded_rail)
        self.assertIn(
            "box-shadow:0012pxvar(--earn-gold-line)!important", expanded_rail
        )

        detail = css_rule(source, "body.earn-draft-route .earn-detail-row > td")
        self.assertIn("position:relative!important", detail)
        self.assertIn("padding:0!important", detail)
        self.assertIn(
            "background:linear-gradient(90deg,rgba(201,162,39,.035),var(--earn-bg-1)42px)!important",
            detail,
        )
        self.assertIn("border-bottom-color:var(--earn-line-2)!important", detail)
        self.assertIn("white-space:normal!important", detail)

        detail_rail = css_rule(
            source, "body.earn-draft-route .earn-detail-row > td::before"
        )
        self.assertIn("width:2px!important", detail_rail)
        self.assertIn("background:var(--earn-gold)!important", detail_rail)
        self.assertIn(
            "box-shadow:0012pxvar(--earn-gold-line)!important", detail_rail
        )

        panel = css_rule(source, "body.earn-draft-route .earn-detail-stack")
        self.assertIn("width:100%!important", panel)
        self.assertIn("border:0!important", panel)
        self.assertIn("border-radius:0!important", panel)
        self.assertIn("background:transparent!important", panel)
        self.assertIn("box-shadow:none!important", panel)
        self.assertIn("overflow:visible!important", panel)

        panel_rail = css_rule(
            source, "body.earn-draft-route .earn-detail-stack::before"
        )
        self.assertIn("display:none!important", panel_rail)


if __name__ == "__main__":
    unittest.main()
