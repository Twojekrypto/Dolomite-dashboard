import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from plan_earn_subaccount_history_repairs import build_repair_plan


class PlanEarnSubaccountHistoryRepairsTest(unittest.TestCase):
    def test_repair_progress_keys_include_cohort_size(self):
        first = "0x1111111111111111111111111111111111111111"
        second = "0x2222222222222222222222222222222222222222"
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            history_dir = root / "history"
            chain_dir = history_dir / "arbitrum"
            chain_dir.mkdir(parents=True)
            (chain_dir / f"{first}.json").write_text('{"lastScannedBlock":90}', encoding="utf-8")
            (chain_dir / f"{second}.json").write_text('{"lastScannedBlock":90}', encoding="utf-8")

            two_address_plan = build_repair_plan(
                "arbitrum",
                history_dir=history_dir,
                events_dir=root / "events",
                output_dir=root / "plans",
                addresses=[first, second],
                target_block=100,
                workers=2,
            )
            one_address_plan = build_repair_plan(
                "arbitrum",
                history_dir=history_dir,
                events_dir=root / "events",
                output_dir=root / "plans",
                addresses=[first],
                target_block=100,
                workers=2,
            )

        two_key = two_address_plan["tasks"][0]["progressKey"]
        one_key = one_address_plan["tasks"][0]["progressKey"]
        self.assertIn("2a", two_key)
        self.assertIn("1a", one_key)
        self.assertNotEqual(two_key, one_key)


if __name__ == "__main__":
    unittest.main()
