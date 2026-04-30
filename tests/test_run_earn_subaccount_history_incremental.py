import unittest

from run_earn_subaccount_history_incremental import _should_build_fresh_plan


class RunEarnSubaccountHistoryIncrementalTest(unittest.TestCase):
    def test_continue_does_not_auto_refresh_completed_cycle(self):
        self.assertFalse(
            _should_build_fresh_plan(
                status={"complete": True},
                refresh_plan=False,
            )
        )

    def test_refresh_plan_explicitly_starts_new_cycle(self):
        self.assertTrue(
            _should_build_fresh_plan(
                status={"complete": False},
                refresh_plan=True,
            )
        )


if __name__ == "__main__":
    unittest.main()
