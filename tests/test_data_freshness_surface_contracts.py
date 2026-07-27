import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TVL_STYLES = (ROOT / "tvl" / "supply-health.css").read_text(encoding="utf-8")
SUPPLY_VIEW = (ROOT / "liquidation-preview.html").read_text(encoding="utf-8")
SUPPLY_STYLES = (ROOT / "supply" / "supply-draft.css").read_text(encoding="utf-8")
DOLO_VIEW = (ROOT / "dolo-preview.html").read_text(encoding="utf-8")
PORTFOLIO_VIEW = (ROOT / "portfolio-preview.html").read_text(encoding="utf-8")


class DataFreshnessSurfaceContractsTest(unittest.TestCase):
    def test_supply_health_selected_network_has_assets_style_checker(self):
        self.assertIn(".supply-health-chain-dropdown .dd-opt-check {", TVL_STYLES)
        self.assertIn("width: 15px", TVL_STYLES)
        self.assertIn(".tvl-dd-opt.active .dd-opt-check", TVL_STYLES)
        self.assertIn(".tvl-dd-opt.active .dd-opt-check svg", TVL_STYLES)

    def test_supply_cards_share_authoritative_data_timestamp(self):
        self.assertIn("_meta { block { timestamp } }", SUPPLY_VIEW)
        self.assertIn('id="supply-intel-asof"', SUPPLY_VIEW)
        self.assertIn('id="supply-leaderboard-data-updated"', SUPPLY_VIEW)
        self.assertIn('id="supply-activity-data-updated"', SUPPLY_VIEW)
        self.assertIn("function renderSupplyDataFreshness", SUPPLY_VIEW)
        self.assertIn("Data updated ·", SUPPLY_VIEW)
        self.assertIn("Data updating", SUPPLY_VIEW)

    def test_asset_activity_uses_solid_holder_surface(self):
        self.assertIn("background: var(--bg-2, #141417) !important", SUPPLY_STYLES)

    def test_fresh_wallets_use_solid_holder_surface(self):
        self.assertIn(".fresh-wallets-card{", DOLO_VIEW)
        fresh_block = DOLO_VIEW.split(".fresh-wallets-card{", 1)[1].split("}", 1)[0]
        self.assertIn("background:var(--bg-2)", fresh_block)
        self.assertNotIn("linear-gradient", fresh_block)

    def test_position_activity_uses_consistent_surface_and_correct_units(self):
        rail = PORTFOLIO_VIEW.split(
            "#pf-exercises-section .pf-exercise-summary.selected-market-rail{", 1
        )[1].split("}", 1)[0]
        self.assertIn("background:var(--bg-2)", rail)
        self.assertIn(
            'fmtCompact(lockedVe)} <span class="unit">DOLO</span>',
            PORTFOLIO_VIEW,
        )
        self.assertNotIn(
            'fmtCompact(claimVe)} <span class="unit">veDOLO</span>',
            PORTFOLIO_VIEW,
        )
        self.assertNotIn(
            'fmtCompact(currentVote)} <span class="unit">veDOLO</span>',
            PORTFOLIO_VIEW,
        )


if __name__ == "__main__":
    unittest.main()
