import unittest

from run_earn_data_correctness_pipeline import _materialize_task_argv, _progress_satisfies_task


class RunEarnDataCorrectnessPipelineTest(unittest.TestCase):
    def test_completed_progress_must_match_task_target_block(self):
        task = {"progressKey": "repair-t200-m1of2", "targetBlock": 200}

        self.assertFalse(_progress_satisfies_task({"status": "completed", "targetBlock": 150}, task))
        self.assertTrue(_progress_satisfies_task({"status": "completed", "targetBlock": 200}, task))
        self.assertTrue(_progress_satisfies_task({"status": "completed", "targetBlock": 250}, task))

    def test_completed_progress_without_task_target_remains_valid(self):
        self.assertTrue(_progress_satisfies_task({"status": "completed"}, {"progressKey": "m1"}))
        self.assertFalse(_progress_satisfies_task({"status": "running"}, {"progressKey": "m1"}))

    def test_materialize_repair_address_file_is_not_sliced_twice(self):
        argv = _materialize_task_argv(
            {
                "chain": "mantle",
                "progressKey": "repair-m2",
                "eventsDir": "/tmp/events",
                "outputDir": "/tmp/history",
                "addressFile": "/tmp/repair-m2.txt",
                "startIndex": 286,
                "endIndex": 572,
            }
        )

        self.assertIn("--address-file", argv)
        self.assertNotIn("--start-index", argv)
        self.assertNotIn("--end-index", argv)


if __name__ == "__main__":
    unittest.main()
