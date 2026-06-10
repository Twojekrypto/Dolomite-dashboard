import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PORTFOLIO_HTML = ROOT / "portfolio-preview.html"


class PortfolioPreviewContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = PORTFOLIO_HTML.read_text(encoding="utf-8")

    def test_open_borrows_uses_risk_positions_ux(self):
        self.assertIn("liquidation_risk.json", self.html)
        self.assertIn("buildRiskBorrowRows", self.html)
        self.assertIn('class="pf-table pf-borrow-positions"', self.html)
        self.assertIn('data-sort="hf" data-table="bor"', self.html)
        self.assertIn('data-sort="account" data-table="bor">Position', self.html)
        self.assertIn('data-sort="emode" data-table="bor"', self.html)
        self.assertIn('class="pf-col-spacer" aria-hidden="true"', self.html)
        self.assertIn("function emodeCell(active)", self.html)
        self.assertIn("pf-emode-badge", self.html)
        self.assertIn("E-Mode applies special risk parameters", self.html)
        self.assertIn(">Open borrow</div>", self.html)
        self.assertNotIn("positionLabel(r)", self.html)
        self.assertNotIn("Position #", self.html)
        self.assertIn("'dGM' : DOLO_CDN + 'GMX.", self.html)
        self.assertIn("'dGMX' : DOLO_CDN + 'GMX.", self.html)
        self.assertNotIn(">Account<span", self.html)
        self.assertNotIn("Acct ", self.html)
        self.assertNotIn('pf-mini-chip">E-mode', self.html)
        self.assertIn("buildFallbackBorrowRows", self.html)
        self.assertIn("riskBorrows.length ? riskBorrows : buildFallbackBorrowRows(chainResults)", self.html)
        self.assertIn(".pf-hf-badge.unknown", self.html)
        self.assertIn("pf-hf-dot", self.html)
        self.assertIn("${hfText(r.hf)}", self.html)
        self.assertIn('td colspan="7"', self.html)
        self.assertIn("collateralTokens", self.html)
        self.assertIn("debtTokens", self.html)

    def test_odolo_pending_summary_does_not_count_paired_dolo(self):
        self.assertIn("const total = held + ve + vOTok;", self.html)
        self.assertNotIn("const total = held + ve + vPair;", self.html)
        self.assertIn("DOLO paired · returned on claim", self.html)
        self.assertIn("const exClaimTxs = exTxs.filter(t => t && t.usdc != null);", self.html)
        self.assertIn("const exVe = exClaimTxs.reduce", self.html)
        self.assertNotIn("const exVe = exTxs.reduce", self.html)
        self.assertIn("option cost", self.html)
        self.assertIn("oDOLO converted into veDOLO", self.html)
        self.assertNotIn("DOLO paid ·", self.html)
        self.assertNotIn("DOLO paired · in veDOLO", self.html)

    def test_vedolo_odolo_exercises_exclude_pairing_noise(self):
        self.assertIn('id="pf-exercises-section"', self.html)
        self.assertIn("veDOLO / oDOLO Exercises", self.html)
        self.assertIn("veDOLO minted", self.html)
        self.assertIn('class="pf-table pf-exercise-table"', self.html)
        self.assertIn("vedolo_flows.json", self.html)
        self.assertIn("buildExerciseRows(cardData.exer, vedoloFlows, addr)", self.html)
        self.assertIn("filter(tx => tx.usdc != null)", self.html)
        self.assertIn('action: "Claim veDOLO"', self.html)
        self.assertIn('if (lock.isOdolo) return;', self.html)
        self.assertIn('action: "Lock veDOLO"', self.html)
        self.assertIn("DOLO locked, no purchase price", self.html)
        self.assertNotIn('action: "Pair"', self.html)
        self.assertIn('data-sort="paid" data-table="ex">USDC Paid', self.html)
        self.assertNotIn('data-sort="paid" data-table="ex">Cost', self.html)
        self.assertIn('const paidHtml = r.isStableClaim ? fmtUSD(r.paid) : "—";', self.html)
        self.assertNotIn('${esc(r.paidToken)}</span>` : "—";', self.html)
        self.assertIn("pf-latest-date", self.html)
        self.assertIn("pf-ex-green", self.html)
        self.assertIn("txExplorer(r.chain, r.hash)", self.html)
        self.assertIn('id="pf-exercises-summary"', self.html)
        self.assertIn("renderExerciseSummary(rows)", self.html)
        self.assertIn("const claimRows = rows.filter(r => r.isStableClaim);", self.html)
        self.assertIn("const lockRows = rows.filter(r => !r.isStableClaim);", self.html)
        self.assertIn("oDOLO claims", self.html)
        self.assertIn("Avg price", self.html)
        self.assertIn("USDC per veDOLO", self.html)
        self.assertIn("DOLO locked", self.html)

    def test_hash_update_stays_on_portfolio_route_under_base_tag(self):
        self.assertIn('history.replaceState(null, "", `${location.pathname}${location.search}#${addr}`);', self.html)
        self.assertNotIn('history.replaceState(null, "", "#" + addr);', self.html)


if __name__ == "__main__":
    unittest.main()
