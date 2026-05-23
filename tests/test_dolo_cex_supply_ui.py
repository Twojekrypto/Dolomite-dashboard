import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOLO_PREVIEW = ROOT / "dolo-preview.html"


class DoloCexSupplyUiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = DOLO_PREVIEW.read_text(encoding="utf-8")

    def test_cex_supply_positive_changes_render_as_risk_color(self):
        brush_start = self.source.index("function paintCexSupplyBrushWindow()")
        brush_end = self.source.index("function scheduleCexSupplyChartRender()")
        brush_source = self.source[brush_start:brush_end]

        self.assertIn("function cexSupplyRiskClass(delta)", self.source)
        self.assertIn('if(value > 0) return "down";', self.source)
        self.assertIn('if(value < 0) return "up";', self.source)
        self.assertIn("const riskClass = change ? cexSupplyRiskClass(change.delta) : \"\";", brush_source)
        self.assertIn("const deltaClass = cexSupplyRiskClass(delta);", self.source)
        self.assertNotIn('change.delta >= 0 ? "up" : "down"', brush_source)


if __name__ == "__main__":
    unittest.main()
