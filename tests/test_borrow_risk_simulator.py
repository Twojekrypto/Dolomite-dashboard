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
        self.assertIn("sim-risk-search", self.source)
        self.assertIn("renderSimRiskAddressTools", self.source)
        self.assertIn("sim-atrisk-addr-link", self.source)
        self.assertNotIn('class="sim-atrisk-action"', self.source)
        self.assertNotIn("Acct ${account.length", self.source)
        self.assertNotIn("sim-risk-focus", self.source)

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
                self.assertIn("risk-simulator-token-popover-layer-20260529", text)
                self.assertIn("dolo-label-cleanup-20260514", text)

    def test_slider_outcome_mini_chart_removed(self):
        self.assertNotIn('id="sim-impact-value"', self.source)
        self.assertNotIn('id="sim-impact-fill"', self.source)

    def test_result_summary_is_consolidated(self):
        self.assertIn("sim-result-summary", self.source)
        self.assertIn("Scenario result", self.source)
        self.assertIn("sim-risk-details", self.source)
        self.assertIn("SIM_RISK_PAGE_SIZE = 10", self.source)
        self.assertIn("window.simRiskGoPage", self.source)
        self.assertIn("window.simRiskSetSearch", self.source)
        self.assertIn("sim-risk-search", self.source)
        self.assertIn('class="sim-risk-search-key" aria-hidden="true">/</span>', self.source)
        self.assertIn("window.focusSimRiskSearch = focusSimRiskSearch", self.source)
        self.assertIn("event.key !== '/'", self.source)
        self.assertIn("setSimRiskDetailsOpen(true)", self.source)
        self.assertIn("sim-risk-total-label", self.source)
        self.assertIn('class="flow-pager-info" id="sim-risk-page-label"', self.source)
        self.assertIn("visibleRanked.slice(pageStart, pageEnd)", self.source)
        self.assertIn("SIM_RISK_PAGE_SIZE = 10", self.source)
        self.assertIn("body.route-liquidation .sim-atrisk-list", self.source)
        self.assertIn("height: 330px !important;", self.source)
        self.assertIn("max-height: 330px !important;", self.source)
        self.assertIn("scrollbar-gutter: stable !important;", self.source)
        self.assertIn("listEl.scrollTop = 0;", self.source)
        self.assertIn("renderSimImpact(simResults, hasCustomMove)", self.source)
        self.assertIn("#sim-card.sim-mode-multi #sim-impact-viz", self.source)
        self.assertNotIn('id="sim-atrisk-sub"', self.source)
        self.assertNotIn("Wallet risk ranking", self.source)
        self.assertNotIn("Lowest health after scenario", self.source)
        self.assertIn("sim-multi-chip-row", self.source)
        self.assertIn("resetMultiAssetBasket", self.source)
        self.assertIn("getDefaultMultiAssetMoves", self.source)
        self.assertIn("window._multiAssetDefaultsInitialized", self.source)
        self.assertIn("const fallback = ['WBTC', 'WETH', 'WLFI'];", self.source)
        self.assertIn("addPreferredToken(['WBTC', 'BTC']);", self.source)
        self.assertIn("addPreferredToken(['WETH', 'ETH']);", self.source)
        self.assertIn("addPreferredToken(['WLFI']);", self.source)
        self.assertNotIn("tokens.slice(0, 4).map", self.source)
        self.assertIn("pct: 5", self.source)
        self.assertIn("const initialPct = typeof defaultPct === 'number' ? defaultPct : 0;", self.source)
        self.assertIn("positionMultiTokenPopoverFor", self.source)
        self.assertIn("positionOpenMultiTokenPopovers", self.source)
        self.assertIn("revealMultiAssetRow", self.source)
        self.assertIn("arguments.length === 0", self.source)
        self.assertIn("opens-above", self.source)
        self.assertIn("spaceBelow < 172", self.source)
        self.assertIn("has-open-popover", self.source)
        self.assertIn("z-index: 10060 !important;", self.source)
        self.assertIn("row.classList.add('has-open-popover')", self.source)
        self.assertIn("row.classList.remove('has-open-popover')", self.source)
        self.assertIn("addMultiAssetRow(move.token, move.pct, { reveal: false })", self.source)
        self.assertIn("scrollTarget?.closest?.('#sim-multi-rows')", self.source)
        self.assertIn("window.addEventListener('resize', positionOpenMultiTokenPopovers)", self.source)
        self.assertIn("grid-template-columns: 150px 92px !important;", self.source)
        self.assertIn("order: 1 !important;", self.source)
        self.assertIn("order: 2 !important;", self.source)
        self.assertIn("height: 184px !important;", self.source)
        self.assertIn("max-height: 184px !important;", self.source)
        self.assertIn("height: 190px !important;", self.source)
        self.assertIn("height: 322px !important;", self.source)
        self.assertIn("overflow-y: auto !important;", self.source)
        self.assertIn("scrollbar-gutter: stable !important;", self.source)
        self.assertNotIn("pct: -20", self.source)
        self.assertNotIn("const initialPct = typeof defaultPct === 'number' ? defaultPct : -25;", self.source)
        self.assertNotIn("addMultiAssetRow('WBTC', -25)", self.source)
        self.assertIn("toggleMultiBasketEditor", self.source)
        self.assertIn("Edit Basket", self.source)
        self.assertIn("#sim-multi-panel .sim-multi-summary-main", self.source)
        self.assertIn("display: none !important;", self.source)
        self.assertNotIn("Scenario applied to", self.source)
        self.assertIn("Wallet risk list", self.source)
        self.assertIn("View wallets", self.source)
        self.assertIn("View wallet risk list", self.source)
        self.assertIn("Hide wallets", self.source)
        self.assertIn("Hide wallet risk list", self.source)
        self.assertIn("@media (max-width: 900px)", self.source)
        self.assertIn("#sim-card.sim-mode-multi #sim-impact-viz .sim-viz-dist > .sim-viz-head", self.source)
        self.assertIn("#sim-card.sim-mode-multi #sim-impact-viz .sim-viz-focus", self.source)
        self.assertIn("#sim-card.sim-mode-multi #sim-impact-viz .sim-dist-chart", self.source)
        self.assertIn('grid-template-areas:', self.source)
        self.assertIn('"scenario result"', self.source)
        self.assertIn("grid-area: scenario", self.source)
        self.assertIn("grid-area: result", self.source)
        self.assertIn("grid-area: risk", self.source)
        self.assertIn('class="liquidation-sim-card sim-mode-multi"', self.source)
        self.assertIn('class="sim-multi-panel visible is-editing"', self.source)
        self.assertIn("setSimTab('multi')", self.source)
        self.assertIn("Set token moves and see how borrow health changes.", self.source)
        self.assertIn("Choose tokens and set negative or positive moves.", self.source)
        self.assertIn("body.route-liquidation .sim-dist-ghost", self.source)
        self.assertIn("background: transparent !important;", self.source)
        self.assertNotIn("background: rgba(148,163,184,.13) !important;", self.source)
        self.assertNotIn("All selected moves are 0%", self.source)
        self.assertNotIn("No token moves selected", self.source)
        self.assertIn("is-zero", self.source)
        self.assertIn('data-tooltip="Remove token"', self.source)
        self.assertIn("if (raw.includes('.')) raw = raw.replace", self.source)
        self.assertNotIn(".slice(0, 5)", self.source)
        self.assertNotIn("has the smallest simulated buffer", self.source)
        self.assertNotIn('class="liquidation-sim-metric liq"', self.source)

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
