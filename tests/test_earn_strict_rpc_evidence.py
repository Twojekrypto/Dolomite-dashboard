import json
import unittest

from earn_strict_rpc_evidence import fetch_strict_evidence


ADDRESS = "0x1111111111111111111111111111111111111111"
INDEX_SCALE = 10**18


def word(value):
    return f"{int(value):064x}"


def signed_words(value):
    value = int(value)
    return word(1 if value >= 0 else 0), word(abs(value))


def encode_current_index(borrow_index, supply_index):
    return "0x" + word(borrow_index) + word(supply_index) + word(123)


def encode_account_balances(market_id, par, wei):
    market_offset = 4 * 32
    token_offset = market_offset + 2 * 32
    par_offset = token_offset + 2 * 32
    wei_offset = par_offset + 3 * 32
    par_sign, par_value = signed_words(par)
    wei_sign, wei_value = signed_words(wei)
    return "0x" + "".join((
        word(market_offset),
        word(token_offset),
        word(par_offset),
        word(wei_offset),
        word(1),
        word(market_id),
        word(1),
        word(int("22" * 20, 16)),
        word(1),
        par_sign,
        par_value,
        word(1),
        wei_sign,
        wei_value,
    ))


def index_log(*, market=7, borrow=12 * INDEX_SCALE // 10, supply=12 * INDEX_SCALE // 10,
              transaction_index=0, log_index=1):
    return {
        "topics": [
            "0xf4626fd1187f91e6761ffb8a6ac3e8d9235a4a92da54e43feb0c57c4a4a322ab",
            "0x" + word(market),
        ],
        "data": "0x" + word(borrow) + word(supply),
        "blockNumber": "0x64",
        "transactionIndex": hex(transaction_index),
        "logIndex": hex(log_index),
    }


def history_payload():
    return {
        "chain": "arbitrum",
        "address": ADDRESS,
        "lastScannedBlock": 130,
        "scanRange": {"fromBlock": 1, "toBlock": 130},
        "accounts": {
            "5": {
                "account": "5",
                "accountKnown": True,
                "markets": {
                    "7": {
                        "events": [{
                            "eventKey": "e1",
                            "blockNumber": 100,
                            "transactionIndex": 1,
                            "logIndex": 2,
                            "deltaWei": "-60",
                            "newPar": "-50",
                            "accountKnown": True,
                        }],
                    },
                },
            },
        },
    }


class FakeRpcClient:
    def __init__(self, logs):
        self.logs = logs
        self.calls = []

    def call(self, method, params):
        self.calls.append((method, params))
        if method == "eth_getLogs":
            return list(self.logs)
        raise AssertionError(f"Unexpected RPC method: {method}")

    def eth_call_batch(self, calls, block="latest"):
        self.calls.append(("eth_call_batch", calls, block))
        results = []
        for _to, data in calls:
            if data.startswith("0x56ea84b2"):
                results.append(encode_current_index(
                    12 * INDEX_SCALE // 10,
                    12 * INDEX_SCALE // 10,
                ))
            elif data.startswith("0x6a8194e7"):
                results.append(encode_account_balances(7, -50, -60))
            else:
                raise AssertionError(f"Unexpected calldata: {data}")
        return results


class EarnStrictRpcEvidenceTest(unittest.TestCase):
    def test_fetches_exact_event_index_and_pinned_signed_position(self):
        fake_client = FakeRpcClient([index_log()])

        evidence = fetch_strict_evidence(
            "arbitrum",
            ADDRESS,
            history_payload(),
            comparison_block=123,
            client=fake_client,
        )

        self.assertEqual(str(12 * INDEX_SCALE // 10), evidence["eventIndexes"]["e1"])
        self.assertEqual(str(12 * INDEX_SCALE // 10), evidence["eventIndexPairs"]["e1"]["supplyIndex"])
        self.assertEqual("-50", evidence["currentPositions"]["5|7"]["par"])
        self.assertEqual("-60", evidence["currentPositions"]["5|7"]["wei"])
        self.assertEqual("0x7b", next(
            call[2] for call in fake_client.calls if call[0] == "eth_call_batch"
        ))
        self.assertNotIn("rpcUrl", json.dumps(evidence))
        self.assertNotIn("endpoint", json.dumps(evidence).lower())

    def test_missing_index_log_before_action_is_explicit_and_never_ratio_derived(self):
        fake_client = FakeRpcClient([
            index_log(transaction_index=1, log_index=3),
        ])
        payload = history_payload()
        payload["accounts"]["5"]["markets"]["7"]["events"][0]["deltaWei"] = "-60"
        payload["accounts"]["5"]["markets"]["7"]["events"][0]["newPar"] = "-50"

        evidence = fetch_strict_evidence(
            "arbitrum",
            ADDRESS,
            payload,
            comparison_block=123,
            client=fake_client,
        )

        self.assertNotIn("e1", evidence["eventIndexes"])
        self.assertEqual("missing_event_index", evidence["errors"]["e1"])
        self.assertNotEqual(str(12 * INDEX_SCALE // 10), evidence["eventIndexes"].get("e1"))


if __name__ == "__main__":
    unittest.main()
