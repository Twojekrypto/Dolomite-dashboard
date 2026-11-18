import importlib.util
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import generate_supply_activity_overview as overview
from wlfi_markets import MINIMUM


def event(account=None, **changes):
    value = {'sourceId': 'deposit-1', 'type': 'deposit', 'timestamp': 1794976067,
             'txHash': '0x' + 'a' * 64, 'amount': '1.000000000000000001',
             'usd': '1.002', 'wallet': '0x' + 'b' * 40, 'ambiguous': False}
    if account is not None:
        value['account'] = account
    value.update(changes)
    return value


class ActivityAttributionTests(unittest.TestCase):
    def test_metadata_uses_actual_source_leg_account_and_preserves_row_values(self):
        value = event('0x' + 'c' * 40 + '-' + str(MINIMUM))
        row = overview.event_row(value, 8, chain='ethereum')
        self.assertEqual(row[:9], ['deposit-1', 'deposit', 1794976067, '0x' + 'a' * 64,
                                  '1.000000000000000001', '1.002', '0x' + 'b' * 40, '', 8])
        self.assertEqual(row[9], {'accountNumber': str(MINIMUM), 'accountOrigin': 'wlfi'})

    def test_missing_eth_evidence_is_unknown_and_usd_metadata_survives(self):
        row = overview.event_row(event(usd='0'), 0, chain='ethereum')
        self.assertEqual(row[9], {'usdStatus': 'unavailable', 'sourceUsd': '0',
                                  'accountNumber': None, 'accountOrigin': 'unknown'})
        self.assertEqual(overview.event_row(event(), 0, chain='arbitrum')[9]['accountOrigin'], 'other')

    def test_same_wallet_different_source_accounts_are_not_conflated(self):
        matching = overview.event_row(event('0x' + 'b' * 40 + '-' + str(MINIMUM)), 0, chain='ethereum')
        other = overview.event_row(event('0x' + 'b' * 40 + '-0'), 0, chain='ethereum')
        self.assertEqual(matching[6], other[6])
        self.assertEqual((matching[9]['accountOrigin'], other[9]['accountOrigin']), ('wlfi', 'other'))


if __name__ == '__main__':
    unittest.main()
