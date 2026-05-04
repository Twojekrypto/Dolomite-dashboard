import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import run_earn_canonical_history_refresh as canonical_refresh
import run_earn_data_correctness_pipeline as correctness_pipeline
import run_earn_subaccount_history_incremental as incremental_runner


class EarnRunnerLaunchSessionTest(unittest.TestCase):
    def test_alive_checks_require_current_runner_session(self):
        modules = [canonical_refresh, correctness_pipeline, incremental_runner]
        for module in modules:
            with self.subTest(module=module.__name__):
                with patch.object(module, "_is_pid_alive", return_value=True):
                    self.assertTrue(
                        module._is_launch_run_alive({
                            "pid": 12345,
                            "runnerSessionId": module.RUNNER_SESSION_ID,
                        })
                    )
                    self.assertFalse(
                        module._is_launch_run_alive({
                            "pid": 12345,
                            "runnerSessionId": "github-old-run-1",
                        })
                    )

    def test_checkpoint_cleanup_only_terminates_current_session_workers(self):
        with TemporaryDirectory() as tmpdir:
            launch_path = Path(tmpdir) / "launch.json"
            stale_proc = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                start_new_session=True,
            )
            current_proc = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                start_new_session=True,
            )
            try:
                canonical_refresh._write_json(
                    launch_path,
                    {
                        "runs": [
                            {
                                "progressKey": "stale",
                                "pid": stale_proc.pid,
                                "argv": ["python3", "worker.py"],
                                "runnerSessionId": "github-previous-1",
                            },
                            {
                                "progressKey": "current",
                                "pid": current_proc.pid,
                                "argv": ["python3", "worker.py"],
                                "runnerSessionId": canonical_refresh.RUNNER_SESSION_ID,
                            },
                        ]
                    },
                )

                terminated = canonical_refresh._terminate_launch_processes(launch_path)

                self.assertEqual([row["progressKey"] for row in terminated], ["current"])
                current_proc.wait(timeout=5)
                self.assertIsNone(stale_proc.poll())
                payload = canonical_refresh._read_json(launch_path, {})
                rows = {row["progressKey"]: row for row in payload["runs"]}
                self.assertIn("terminatedAt", rows["current"])
                self.assertNotIn("terminatedAt", rows["stale"])
            finally:
                for proc in (stale_proc, current_proc):
                    if proc.poll() is None:
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
