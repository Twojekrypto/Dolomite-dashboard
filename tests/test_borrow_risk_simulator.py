import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PREVIEW = ROOT / "liquidation-preview.html"
RISK_DATA = ROOT / "liquidation_risk.json"


class BorrowRiskSimulatorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = PREVIEW.read_text(encoding="utf-8")

    def test_impact_map_rows_keep_wallet_identity(self):
        self.assertIn("addr: p.address", self.source)
        self.assertIn("chain: p.chain", self.source)
        self.assertIn("accountNumber: p.accountNumber", self.source)
        self.assertIn("data-sim-risk-idx", self.source)
        self.assertIn("sim-risk-focus", self.source)
        self.assertIn("renderSimRiskAddressTools", self.source)
        self.assertIn("sim-atrisk-action", self.source)
        self.assertNotIn("Acct ${account.length", self.source)

    def test_impact_map_buckets_are_interactive(self):
        self.assertIn("sim-dist-focus", self.source)
        self.assertIn("data-sim-dist-idx", self.source)
        self.assertIn("window._simDistPinnedIdx", self.source)
        self.assertIn("window._simRiskPinnedIdx", self.source)
        self.assertIn("setDistFocus(window._simDistPinnedIdx, false)", self.source)
        self.assertIn("setRiskFocus(window._simRiskPinnedIdx, false)", self.source)

    def test_simulator_uses_same_position_set_for_counts_and_percentages(self):
        self.assertIn("(p.collateralUSD || 0) >= 0.01", self.source)
        self.assertIn("(p.debtUSD || 0) >= 0.01", self.source)
        self.assertIn("window._simTotalDebt = Math.round(window._simPositions.reduce", self.source)
        self.assertIn("window._simTotalCol = Math.round(window._simPositions.reduce", self.source)
        self.assertNotIn("window._simTotalDebt = Math.round(noDust.reduce", self.source)
        self.assertNotIn("window._simTotalCol = Math.round(noDust.reduce", self.source)

    def test_route_shells_keep_risk_and_label_cache_busts(self):
        for route in (ROOT / "borrow" / "index.html", ROOT / "liquidation" / "index.html"):
            with self.subTest(route=route):
                text = route.read_text(encoding="utf-8")
                self.assertIn("risk-impact-polish-20260529", text)
                self.assertIn("dolo-label-cleanup-20260514", text)

    def test_slider_outcome_mini_chart_removed(self):
        self.assertNotIn('id="sim-impact-value"', self.source)
        self.assertNotIn('id="sim-impact-fill"', self.source)

    def test_liquidation_json_has_identity_for_simulated_positions(self):
        payload = json.loads(RISK_DATA.read_text(encoding="utf-8"))
        simulated = [
            row for row in payload["positions"]
            if (row.get("collateralUSD") or 0) + (row.get("debtUSD") or 0) >= 10
            and row.get("healthFactor") is not None
            and row.get("healthFactor") > 0
            and (row.get("collateralUSD") or 0) >= 0.01
            and (row.get("debtUSD") or 0) >= 0.01
        ]
        self.assertGreater(len(simulated), 0)
        self.assertFalse([row for row in simulated if not row.get("address")])
        self.assertFalse([row for row in simulated if not row.get("chain")])


if __name__ == "__main__":
    unittest.main()
