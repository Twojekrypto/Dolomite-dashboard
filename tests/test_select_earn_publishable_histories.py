import json
import tempfile
import unittest
from pathlib import Path

from scripts.select_earn_publishable_histories import select_publishable_histories


class SelectEarnPublishableHistoriesTest(unittest.TestCase):
    def _write_history(self, root: Path, path_chain: str, path_address: str, **overrides) -> None:
        payload = {
            "version": 1,
            "chain": path_chain,
            "address": path_address,
            "lastScannedBlock": 250,
            "scanRange": {"fromBlock": 100, "toBlock": 250},
            "accounts": {},
            "summary": {},
        }
        payload.update(overrides)
        path = root / path_chain / f"{path_address}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def test_selects_only_histories_complete_from_protocol_start_to_locked_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            complete = "0x" + "1" * 40
            stale = "0x" + "2" * 40
            truncated = "0x" + "3" * 40
            wrong_chain = "0x" + "4" * 40
            missing = "0x" + "5" * 40
            self._write_history(root, "berachain", complete)
            self._write_history(
                root,
                "berachain",
                stale,
                lastScannedBlock=249,
                scanRange={"fromBlock": 100, "toBlock": 249},
            )
            self._write_history(
                root,
                "berachain",
                truncated,
                scanRange={"fromBlock": 101, "toBlock": 250},
            )
            self._write_history(root, "berachain", wrong_chain, chain="arbitrum")

            selected, rejected = select_publishable_histories(
                chain="berachain",
                addresses=[complete, stale, truncated, wrong_chain, missing],
                history_dir=root,
                start_block=100,
                target_block=250,
            )

            self.assertEqual([complete], selected)
            self.assertEqual("stale_target", rejected[stale])
            self.assertEqual("truncated_start", rejected[truncated])
            self.assertEqual("wrong_chain", rejected[wrong_chain])
            self.assertEqual("missing_history", rejected[missing])

    def test_rejects_invalid_address_identity_and_deduplicates_selection_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            valid = "0x" + "a" * 40
            mismatch = "0x" + "b" * 40
            self._write_history(root, "arbitrum", valid)
            self._write_history(root, "arbitrum", mismatch, address="0x" + "c" * 40)

            selected, rejected = select_publishable_histories(
                chain="arbitrum",
                addresses=[valid.upper(), valid, mismatch, "not-an-address"],
                history_dir=root,
                start_block=100,
                target_block=250,
            )

            self.assertEqual([valid], selected)
            self.assertEqual("wrong_address", rejected[mismatch])
            self.assertEqual("invalid_address", rejected["not-an-address"])


if __name__ == "__main__":
    unittest.main()
