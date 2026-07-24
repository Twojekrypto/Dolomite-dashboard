import unittest

from earn_strict_replay import (
    INDEX_SCALE,
    build_strict_replay,
    par_to_wei_round_half_up,
)


ADDRESS = "0x1111111111111111111111111111111111111111"


def action(key, block, market, new_par, *, account="0", flow_type="d"):
    return {
        "eventKey": key,
        "blockNumber": block,
        "transactionIndex": 1,
        "logIndex": len(key),
        "accountKnown": True,
        "account": account,
        "marketId": str(market),
        "newPar": str(new_par),
        "deltaWei": "0",
        "flowType": flow_type,
    }


def history(events, *, from_block=1, last_scanned=130, account_known=True):
    accounts = {}
    for event in events:
        account = str(event.get("account", "0"))
        market = str(event["marketId"])
        account_row = accounts.setdefault(account, {
            "account": account,
            "accountKnown": account_known,
            "markets": {},
        })
        account_row["markets"].setdefault(market, {"events": []})["events"].append(event)
    return {
        "chain": "arbitrum",
        "address": ADDRESS,
        "lastScannedBlock": last_scanned,
        "scanRange": {"fromBlock": from_block, "toBlock": last_scanned},
        "accounts": accounts,
    }


def evidence(events, positions, *, current_indexes=None, event_indexes=None, comparison_block=123):
    if current_indexes is None:
        current_indexes = {
            str(event["marketId"]): {
                "supplyIndex": str(12 * INDEX_SCALE // 10),
                "borrowIndex": str(13 * INDEX_SCALE // 10),
            }
            for event in events
        }
    if event_indexes is None:
        event_indexes = {
            event["eventKey"]: {
                "supplyIndex": str(INDEX_SCALE),
                "borrowIndex": str(INDEX_SCALE),
            }
            for event in events
        }
    return {
        "comparisonBlock": comparison_block,
        "protocolStartBlock": 1,
        "eventIndexes": event_indexes,
        "currentIndexes": current_indexes,
        "currentPositions": positions,
    }


class EarnStrictReplayTest(unittest.TestCase):
    def test_rounds_signed_par_to_wei_half_up(self):
        self.assertEqual(2, par_to_wei_round_half_up(1, INDEX_SCALE + INDEX_SCALE // 2))
        self.assertEqual(-2, par_to_wei_round_half_up(-1, INDEX_SCALE + INDEX_SCALE // 2))

    def test_verifies_open_supply_with_zero_diffs(self):
        events = [action("e1", 100, "7", 100)]
        result = build_strict_replay(
            history(events),
            evidence(events, {"0|7": {"par": "100", "wei": "120"}}),
        )

        self.assertEqual("verified", result["markets"]["7"]["strictStatus"])
        self.assertEqual("20", result["markets"]["7"]["openSupplyYield"])
        self.assertEqual("0", result["verification"]["7"]["supplyParDiff"])
        self.assertEqual("0", result["verification"]["7"]["supplyWeiDiff"])

    def test_verifies_exact_zero_closed_and_reopened_cycle(self):
        events = [
            action("e1", 90, "7", 100),
            action("e2", 100, "7", 0, flow_type="w"),
            action("e3", 110, "7", 50),
        ]
        indexes = {
            "e1": {"supplyIndex": str(INDEX_SCALE), "borrowIndex": str(INDEX_SCALE)},
            "e2": {"supplyIndex": str(11 * INDEX_SCALE // 10), "borrowIndex": str(INDEX_SCALE)},
            "e3": {"supplyIndex": str(11 * INDEX_SCALE // 10), "borrowIndex": str(INDEX_SCALE)},
        }
        result = build_strict_replay(
            history(events),
            evidence(
                events,
                {"0|7": {"par": "50", "wei": "60"}},
                event_indexes=indexes,
            ),
        )

        self.assertEqual("10", result["markets"]["7"]["settledSupplyYield"])
        self.assertEqual("5", result["markets"]["7"]["openSupplyYield"])
        self.assertEqual("15", result["markets"]["7"]["earnYield"])

    def test_verifies_partial_supply_reduction(self):
        events = [
            action("e1", 90, "7", 100),
            action("e2", 100, "7", 50, flow_type="w"),
        ]
        result = build_strict_replay(
            history(events),
            evidence(
                events,
                {"0|7": {"par": "50", "wei": "65"}},
                current_indexes={
                    "7": {
                        "supplyIndex": str(13 * INDEX_SCALE // 10),
                        "borrowIndex": str(INDEX_SCALE),
                    },
                },
                event_indexes={
                    "e1": {"supplyIndex": str(INDEX_SCALE), "borrowIndex": str(INDEX_SCALE)},
                    "e2": {
                        "supplyIndex": str(12 * INDEX_SCALE // 10),
                        "borrowIndex": str(INDEX_SCALE),
                    },
                },
            ),
        )

        self.assertEqual("10", result["markets"]["7"]["settledSupplyYield"])
        self.assertEqual("15", result["markets"]["7"]["openSupplyYield"])
        self.assertEqual("25", result["markets"]["7"]["earnYield"])

    def test_accepts_transfer_and_trade_updates(self):
        events = [
            action("x1", 90, "7", 40, flow_type="x"),
            action("s1", 100, "7", 100, flow_type="s"),
        ]
        result = build_strict_replay(
            history(events),
            evidence(events, {"0|7": {"par": "100", "wei": "120"}}),
        )

        self.assertEqual("verified", result["verification"]["7"]["status"])
        self.assertEqual(["s", "x"], sorted({row["flowType"] for row in result["eventTrace"]}))

    def test_accepts_liquidation_and_vaporization_updates(self):
        events = [
            action("l1", 90, "7", 60, flow_type="l"),
            action("v1", 100, "7", 100, flow_type="v"),
        ]
        result = build_strict_replay(
            history(events),
            evidence(events, {"0|7": {"par": "100", "wei": "120"}}),
        )

        self.assertEqual("verified", result["verification"]["7"]["status"])
        self.assertEqual(["l", "v"], sorted({row["flowType"] for row in result["eventTrace"]}))

    def test_classifies_positive_balance_as_collateral_when_account_has_open_borrow(self):
        events = [
            action("s1", 90, "7", 100),
            action("b1", 91, "8", -50, flow_type="w"),
        ]
        result = build_strict_replay(
            history(events),
            evidence(
                events,
                {
                    "0|7": {"par": "100", "wei": "120"},
                    "0|8": {"par": "-50", "wei": "-65"},
                },
                current_indexes={
                    "7": {"supplyIndex": str(12 * INDEX_SCALE // 10), "borrowIndex": str(INDEX_SCALE)},
                    "8": {"supplyIndex": str(INDEX_SCALE), "borrowIndex": str(13 * INDEX_SCALE // 10)},
                },
            ),
        )

        self.assertEqual("100", result["markets"]["7"]["currentCollateralSupplyPar"])
        self.assertEqual("0", result["markets"]["7"]["currentSupplyPar"])
        self.assertEqual("20", result["markets"]["7"]["openCollateralYield"])

    def test_verifies_open_borrow_without_counting_debt_cost_as_earn_yield(self):
        events = [action("b1", 90, "7", -50, flow_type="w")]
        result = build_strict_replay(
            history(events),
            evidence(events, {"0|7": {"par": "-50", "wei": "-65"}}),
        )

        self.assertEqual("50", result["markets"]["7"]["currentBorrowPar"])
        self.assertEqual("-15", result["markets"]["7"]["openBorrowYield"])
        self.assertEqual("0", result["markets"]["7"]["earnYield"])

    def test_verifies_supply_to_borrow_sign_flip(self):
        events = [
            action("e1", 90, "7", 100),
            action("e2", 100, "7", -50, flow_type="s"),
        ]
        result = build_strict_replay(
            history(events),
            evidence(
                events,
                {"0|7": {"par": "-50", "wei": "-75"}},
                current_indexes={
                    "7": {
                        "supplyIndex": str(13 * INDEX_SCALE // 10),
                        "borrowIndex": str(15 * INDEX_SCALE // 10),
                    },
                },
                event_indexes={
                    "e1": {"supplyIndex": str(INDEX_SCALE), "borrowIndex": str(INDEX_SCALE)},
                    "e2": {
                        "supplyIndex": str(12 * INDEX_SCALE // 10),
                        "borrowIndex": str(13 * INDEX_SCALE // 10),
                    },
                },
            ),
        )

        self.assertEqual("20", result["markets"]["7"]["settledSupplyYield"])
        self.assertEqual("-10", result["markets"]["7"]["openBorrowYield"])
        self.assertEqual("20", result["markets"]["7"]["earnYield"])

    def test_missing_event_index_fails_closed(self):
        events = [action("e1", 100, "7", 100)]
        result = build_strict_replay(
            history(events),
            evidence(
                events,
                {"0|7": {"par": "100", "wei": "120"}},
                event_indexes={},
            ),
        )

        self.assertNotIn("7", result["markets"])
        self.assertEqual("coverage_incomplete", result["verification"]["7"]["status"])
        self.assertEqual("missing_event_index", result["verification"]["7"]["reason"])

    def test_unknown_account_fails_closed(self):
        events = [action("e1", 100, "7", 100)]
        invalid_history = history(events, account_known=False)
        result = build_strict_replay(
            invalid_history,
            evidence(events, {"0|7": {"par": "100", "wei": "120"}}),
        )

        self.assertNotIn("7", result["markets"])
        self.assertEqual("unknown_account", result["verification"]["7"]["reason"])

    def test_history_starting_after_protocol_start_fails_closed(self):
        events = [action("e1", 100, "7", 100)]
        result = build_strict_replay(
            history(events, from_block=2),
            evidence(events, {"0|7": {"par": "100", "wei": "120"}}),
        )

        self.assertEqual("coverage_incomplete", result["strictStatus"])
        self.assertEqual("history_starts_after_protocol_start", result["reason"])
        self.assertEqual({}, result["markets"])

    def test_history_behind_comparison_block_fails_closed(self):
        events = [action("e1", 100, "7", 100)]
        result = build_strict_replay(
            history(events, last_scanned=122),
            evidence(events, {"0|7": {"par": "100", "wei": "120"}}),
        )

        self.assertEqual("coverage_incomplete", result["strictStatus"])
        self.assertEqual("stale_comparison_block", result["reason"])

    def test_exact_par_mismatch_is_not_reconciled(self):
        events = [action("e1", 100, "7", 100)]
        result = build_strict_replay(
            history(events),
            evidence(events, {"0|7": {"par": "99", "wei": "120"}}),
        )

        self.assertNotIn("7", result["markets"])
        self.assertEqual("mismatch", result["verification"]["7"]["status"])
        self.assertEqual("-1", result["verification"]["7"]["supplyParDiff"])
        self.assertFalse(result["verification"]["7"]["replayStateAdjusted"])

    def test_exact_wei_mismatch_is_not_reconciled(self):
        events = [action("e1", 100, "7", 100)]
        result = build_strict_replay(
            history(events),
            evidence(events, {"0|7": {"par": "100", "wei": "119"}}),
        )

        self.assertNotIn("7", result["markets"])
        self.assertEqual("mismatch", result["verification"]["7"]["status"])
        self.assertEqual("-1", result["verification"]["7"]["supplyWeiDiff"])
        self.assertFalse(result["verification"]["7"]["replayStateAdjusted"])


if __name__ == "__main__":
    unittest.main()
