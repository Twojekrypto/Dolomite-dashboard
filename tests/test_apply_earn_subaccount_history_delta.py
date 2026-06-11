import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from apply_earn_subaccount_history_delta import apply_delta_histories


class ApplyEarnSubaccountHistoryDeltaTest(unittest.TestCase):
    def test_partial_delta_history_can_be_metadata_stamped_to_target(self):
        address = "0x1111111111111111111111111111111111111111"
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_dir = root / "events"
            events_dir.mkdir(parents=True)
            (events_dir / "manifest.json").write_text(
                json.dumps({"chains": {"arbitrum": {"globalFromBlock": 101, "globalToBlock": 110}}}),
                encoding="utf-8",
            )
            history_dir = root / "history"
            chain_dir = history_dir / "arbitrum"
            chain_dir.mkdir(parents=True)
            (chain_dir / f"{address}.json").write_text(
                json.dumps(
                    {
                        "version": 1,
                        "chain": "arbitrum",
                        "address": address,
                        "lastScannedBlock": 105,
                        "scanRange": {"fromBlock": 1, "toBlock": 105},
                        "accounts": {},
                        "summary": {},
                    }
                ),
                encoding="utf-8",
            )

            payload = apply_delta_histories(
                "arbitrum",
                events_dir=events_dir,
                history_dir=history_dir,
                output_dir=history_dir,
                addresses=[address],
                progress_key="a1",
                start_index=0,
                end_index=1,
            )

            history = json.loads((chain_dir / f"{address}.json").read_text(encoding="utf-8"))
        self.assertEqual(payload["updatedAddressCount"], 1)
        self.assertEqual(payload["staleExistingCount"], 0)
        self.assertEqual(payload["stampedAddressCount"], 1)
        self.assertEqual(history["lastScannedBlock"], 110)
        self.assertEqual(history["scanRange"]["toBlock"], 110)


if __name__ == "__main__":
    unittest.main()
