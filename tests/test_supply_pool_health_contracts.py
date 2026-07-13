import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "supply" / "supply-draft.js"
STYLES = ROOT / "supply" / "supply-draft.css"
ROUTE = ROOT / "supply" / "index.html"


class SupplyPoolHealthContractsTest(unittest.TestCase):
    def test_pool_health_refreshes_when_the_selected_supply_chain_changes(self):
        source = SCRIPT.read_text(encoding="utf-8")

        self.assertIn("function syncSupplyHealthChain()", source)
        self.assertIn("supplyHealthExpandedKey = ''", source)
        self.assertIn("supplyChainSelect.addEventListener('change', syncSupplyHealthChain)", source)

    def test_pool_health_rows_expose_keyboard_and_expanded_state(self):
        source = SCRIPT.read_text(encoding="utf-8")

        self.assertIn('tabindex="0"', source)
        self.assertIn('aria-expanded="${expanded ? \'true\' : \'false\'}"', source)
        self.assertIn("event.key === 'Enter' || event.key === ' '", source)

    def test_pool_health_uses_the_shared_dense_table_interaction_states(self):
        styles = STYLES.read_text(encoding="utf-8")

        self.assertIn(".supply-health-row:hover td", styles)
        self.assertIn(".supply-health-row:focus-visible td", styles)
        self.assertIn(".supply-health-row.expanded td", styles)
        self.assertIn(".supply-health-row td:first-child::before", styles)

    def test_supply_route_busts_the_polished_pool_health_assets(self):
        route = ROUTE.read_text(encoding="utf-8")

        self.assertIn("pool-health-ux-20260713", route)


if __name__ == "__main__":
    unittest.main()
