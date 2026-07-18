import unittest

from build_earn_resolved_interest_ledger import build_resolved_ledger


ADDRESS = "0x1111111111111111111111111111111111111111"
TOKEN = "0x2222222222222222222222222222222222222222"


def snapshot_payload(par="100", wei="120"):
    return {
        "date": "2026-07-18",
        "snapshots": {
            "arbitrum": {
                ADDRESS: {
                    "markets": {
                        "7": {"token": TOKEN, "symbol": "TEST", "decimals": 0, "par": par, "wei": wei},
                    }
                }
            }
        },
        "chainMetadata": {
            "arbitrum": {
                "blockNumber": 123,
                "interestIndexes": {
                    "7": {
                        "token": TOKEN,
                        "supplyIndex": "1200000000000000000",
                        "borrowIndex": "1300000000000000000",
                    }
                },
            }
        },
    }


def history_payload(events, *, last_scanned=130, has_borrow=False):
    return {
        "chain": "arbitrum",
        "address": ADDRESS,
        "lastScannedBlock": last_scanned,
        "scanRange": {"fromBlock": 1, "toBlock": last_scanned},
        "sourceMetadata": {"latestSnapshotDate": "2026-07-18"},
        "accounts": {
            "0": {
                "account": "0",
                "accountKnown": True,
                "hasBorrow": has_borrow,
                "markets": {
                    "7": {"events": events},
                },
            }
        },
    }


class BuildEarnResolvedInterestLedgerTests(unittest.TestCase):
    def test_builds_exact_open_supply_ledger_from_pinned_snapshot(self):
        history = history_payload([{
            "blockNumber": 100,
            "transactionIndex": 1,
            "logIndex": 2,
            "deltaWei": "100",
            "newPar": "100",
            "accountKnown": True,
        }])

        ledger = build_resolved_ledger(
            "arbitrum",
            ADDRESS,
            "2026-07-18",
            snapshot_payload(),
            history,
            generated_at="2026-07-18T12:00:00Z",
        )

        self.assertEqual(123, ledger["comparisonBlock"])
        self.assertEqual("20", ledger["markets"]["7"]["earnYield"])
        self.assertEqual("20", ledger["markets"]["7"]["openSupplyYield"])
        self.assertEqual("0", ledger["markets"]["7"]["settledSupplyYield"])
        self.assertTrue(ledger["replayVerificationData"]["7"]["rawVerified"])

    def test_counts_fully_closed_cycle_as_settled_supply_yield(self):
        payload = snapshot_payload(par="50", wei="60")
        history = history_payload([
            {"blockNumber": 90, "transactionIndex": 1, "logIndex": 1, "deltaWei": "100", "newPar": "100", "accountKnown": True},
            {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2, "deltaWei": "-110", "newPar": "0", "accountKnown": True},
            {"blockNumber": 110, "transactionIndex": 1, "logIndex": 3, "deltaWei": "50", "newPar": "50", "accountKnown": True},
        ])

        ledger = build_resolved_ledger(
            "arbitrum", ADDRESS, "2026-07-18", payload, history,
            generated_at="2026-07-18T12:00:00Z",
        )

        self.assertEqual("10", ledger["markets"]["7"]["settledSupplyYield"])
        self.assertEqual("10", ledger["markets"]["7"]["openSupplyYield"])
        self.assertEqual("20", ledger["markets"]["7"]["earnYield"])

    def test_rejects_partial_reduction_without_exact_event_interest_index(self):
        history = history_payload([
            {"blockNumber": 90, "transactionIndex": 1, "logIndex": 1, "deltaWei": "100", "newPar": "100", "accountKnown": True},
            {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2, "deltaWei": "-55", "newPar": "50", "accountKnown": True},
        ])

        ledger = build_resolved_ledger(
            "arbitrum", ADDRESS, "2026-07-18", snapshot_payload(par="50", wei="60"), history,
            generated_at="2026-07-18T12:00:00Z",
        )

        self.assertIsNone(ledger)

    def test_rejects_borrow_route_and_history_behind_snapshot_block(self):
        event = {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2, "deltaWei": "100", "newPar": "100", "accountKnown": True}
        self.assertIsNone(build_resolved_ledger(
            "arbitrum", ADDRESS, "2026-07-18", snapshot_payload(), history_payload([event], has_borrow=True),
            generated_at="2026-07-18T12:00:00Z",
        ))
        self.assertIsNone(build_resolved_ledger(
            "arbitrum", ADDRESS, "2026-07-18", snapshot_payload(), history_payload([event], last_scanned=122),
            generated_at="2026-07-18T12:00:00Z",
        ))

    def test_rejects_snapshot_mismatch_instead_of_publishing_inference(self):
        event = {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2, "deltaWei": "100", "newPar": "99", "accountKnown": True}
        ledger = build_resolved_ledger(
            "arbitrum", ADDRESS, "2026-07-18", snapshot_payload(), history_payload([event]),
            generated_at="2026-07-18T12:00:00Z",
        )
        self.assertIsNone(ledger)

    def test_rejects_head_fresh_history_without_full_canonical_backfill(self):
        event = {"blockNumber": 100, "transactionIndex": 1, "logIndex": 2, "deltaWei": "100", "newPar": "100", "accountKnown": True}
        history = history_payload([event])
        history["scanRange"]["fromBlock"] = 29_750_001

        ledger = build_resolved_ledger(
            "arbitrum", ADDRESS, "2026-07-18", snapshot_payload(), history,
            generated_at="2026-07-18T12:00:00Z",
        )

        self.assertIsNone(ledger)


if __name__ == "__main__":
    unittest.main()
