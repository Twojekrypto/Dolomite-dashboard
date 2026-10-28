"""Financial and publication boundaries for current-rate borrow carry."""
import copy
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / 'build_borrow_earnings.py'
spec = importlib.util.spec_from_file_location('borrow_earnings', MODULE)
model = importlib.util.module_from_spec(spec) if MODULE.exists() else None
if model:
    spec.loader.exec_module(model)

NOW = datetime(2026, 10, 28, 20, 0, tzinfo=timezone.utc)
STAMP = '2026-10-28T20:00:00Z'
A, B = '0x' + '1' * 40, '0x' + '2' * 40
WALLET = '0x' + 'a' * 40
ACCOUNT = '1' + '0' * 76


def part(label, rate, category='nativeYield'):
    return {'label': label, 'interestRate': rate, 'category': category,
            'rewardToken': None, 'merklData': None}


def fixture():
    row = {'chain': 'ethereum', 'address': WALLET, 'accountNumber': ACCOUNT,
           'accountId': WALLET + '-' + ACCOUNT, 'healthFactor': 1.75, 'eMode': False, 'earningsComplete': True,
           'collateralUSD': 9900, 'debtUSD': 9000,
           'collateralTokens': [{'symbol': 'COL', 'tokenAddress': A, 'marketId': '0',
                                 'usd': 10000.0, 'usdExact': '10000', 'amount': '100'}],
           'debtTokens': [{'symbol': 'USD', 'tokenAddress': B, 'marketId': '1',
                           'usd': 5000.0, 'usdExact': '5000', 'amount': '5000'}]}
    risk = {'generatedAtISO': STAMP, 'generatedAt': int(NOW.timestamp()),
            'chainStats': {c: {'total': 1 if c == 'ethereum' else 0} for c in
                           ['ethereum', 'arbitrum', 'berachain']},
            'staleChains': [], 'positions': [row]}
    option = part('oDOLO Rewards APR', '9.99', 'rewards')
    option['rewardToken'] = {'symbol': 'DOLO'}
    cash = part('Net Lending Rewards', '0.10', 'rewards')
    cash['merklData'] = {'opportunityType': 'DOLOMITE_NET_LENDING'}
    rates = [
        {'token': {'tokenAddress': A, 'marketId': '0', 'tokenSymbol': 'COL'},
         'supplyInterestRate': '0.10', 'borrowInterestRate': '0.01',
         'totalSupplyInterestRate': '10.21', 'totalBorrowInterestRate': '0.01',
         'outsideSupplyInterestRateParts': [part('Staking APR', '0.02'), option, cash,
                                            part('Loyalty points', None, 'points')],
         'outsideBorrowInterestRateParts': []},
        {'token': {'tokenAddress': B, 'marketId': '1', 'tokenSymbol': 'USD'},
         'supplyInterestRate': '0.01', 'borrowInterestRate': '0.05',
         'totalSupplyInterestRate': '0.01', 'totalBorrowInterestRate': '0.06',
         'outsideSupplyInterestRateParts': [],
         'outsideBorrowInterestRateParts': [part('Native liability APR', '0.01')]}
    ]
    payloads = {c: {'fetchedAt': STAMP, 'payload': {'interestRates': copy.deepcopy(rates)},
                    'url': 'https://api.dolomite.io/tokens/1/interest-rates'} for c in risk['chainStats']}
    return risk, payloads


class BorrowEarningsTests(unittest.TestCase):
    def build(self, risk=None, rates=None):
        self.assertIsNotNone(model, 'The earnings producer must implement the financial model')
        base_risk, base_rates = fixture()
        return model.build_output(risk if risk is not None else base_risk,
                                  rates if rates is not None else base_rates, NOW)

    def test_raw_principal_signed_components_and_options_exclusion(self):
        row = self.build()['positions'][0]
        self.assertEqual(row['collateralUSD'], '10000')
        self.assertEqual(row['debtUSD'], '5000')
        self.assertEqual(row['annualIncomeUSD'], '1200')
        self.assertEqual(row['annualCostUSD'], '300')
        self.assertEqual(row['annualNetUSD'], '900')
        self.assertEqual(row['netAprPct'], '9')
        self.assertTrue(row['hasConditionalRewards'])
        self.assertEqual(row['accountNumber'], ACCOUNT)

    def test_nonzero_option_changes_do_not_change_ranked_money(self):
        risk, rates = fixture()
        before = self.build(risk, rates)['positions'][0]
        rates['ethereum']['payload']['interestRates'][0]['outsideSupplyInterestRateParts'][1]['interestRate'] = '999999'
        rates['ethereum']['payload']['interestRates'][0]['totalSupplyInterestRate'] = '999999.22'
        after = self.build(risk, rates)['positions'][0]
        self.assertEqual(before, after)

    def test_negative_native_performance_is_not_clamped(self):
        risk, rates = fixture()
        r = rates['ethereum']['payload']['interestRates'][0]
        r['supplyInterestRate'] = '0.01'
        r['outsideSupplyInterestRateParts'][0]['interestRate'] = '-0.02'
        row = self.build(risk, rates)['positions'][0]
        self.assertEqual(row['annualIncomeUSD'], '-100')
        self.assertEqual(row['annualNetUSD'], '-400')
        self.assertEqual(row['netAprPct'], '-4')

    def test_required_native_rate_null_is_unranked(self):
        risk, rates = fixture()
        rates['ethereum']['payload']['interestRates'][0]['outsideSupplyInterestRateParts'][0]['interestRate'] = None
        output = self.build(risk, rates)
        self.assertEqual(output['positions'], [])
        self.assertEqual(output['coverage']['excludedPositions'], 1)

    def test_missing_base_rate_is_not_zero(self):
        risk, rates = fixture()
        del rates['ethereum']['payload']['interestRates'][1]['borrowInterestRate']
        self.assertEqual(self.build(risk, rates)['positions'], [])

    def test_exact_chain_address_and_market_id_join(self):
        risk, rates = fixture()
        rates['ethereum']['payload']['interestRates'][0]['token']['tokenAddress'] = '0x' + '3' * 40
        rates['arbitrum']['payload']['interestRates'][0]['supplyInterestRate'] = '99'
        self.assertEqual(self.build(risk, rates)['positions'], [])
        risk, rates = fixture()
        rates['ethereum']['payload']['interestRates'][0]['token']['marketId'] = '99'
        self.assertEqual(self.build(risk, rates)['positions'], [])

    def test_legacy_missing_identity_is_unranked(self):
        risk, rates = fixture()
        del risk['positions'][0]['collateralTokens'][0]['tokenAddress']
        self.assertEqual(self.build(risk, rates)['positions'], [])

    def test_duplicate_position_identity_fails_instead_of_double_counting(self):
        risk, rates = fixture()
        risk['positions'].append(copy.deepcopy(risk['positions'][0]))
        with self.assertRaises(ValueError):
            self.build(risk, rates)

    def test_duplicate_rate_identity_is_unavailable(self):
        risk, rates = fixture()
        rates['ethereum']['payload']['interestRates'].append(copy.deepcopy(rates['ethereum']['payload']['interestRates'][0]))
        output = self.build(risk, rates)
        self.assertEqual(output['positions'], [])
        self.assertEqual(output['chainStatus']['ethereum']['status'], 'unavailable')

    def test_stale_risk_fails_without_refreshing_generation(self):
        risk, rates = fixture()
        risk['generatedAtISO'] = '2026-10-27T20:00:00Z'
        risk['generatedAt'] -= 86400
        with self.assertRaises(ValueError):
            self.build(risk, rates)

    def test_missing_or_stale_chain_is_not_silently_ranked(self):
        risk, rates = fixture()
        risk['staleChains'] = ['ethereum']
        output = self.build(risk, rates)
        self.assertEqual(output['positions'], [])
        self.assertEqual(output['chainStatus']['ethereum']['status'], 'unavailable')
        risk['staleChains'] = []
        rates['ethereum']['fetchedAt'] = '2026-10-28T17:00:00Z'
        self.assertEqual(self.build(risk, rates)['positions'], [])
        del rates['ethereum']
        self.assertEqual(self.build(risk, rates)['positions'], [])

    def test_all_chains_unavailable_fails_instead_of_empty_success(self):
        risk, _ = fixture()
        with self.assertRaises(ValueError):
            self.build(risk, {})

    def test_empty_successful_source_is_an_honest_empty_result(self):
        risk, rates = fixture()
        risk['positions'] = []
        for x in risk['chainStats'].values():
            x['total'] = 0
        output = self.build(risk, rates)
        self.assertEqual(output['positions'], [])
        self.assertEqual(output['coverage']['sourcePositions'], 0)

    def test_explicit_zero_rates_and_tiny_exact_money_are_retained(self):
        risk, rates = fixture()
        risk['positions'][0]['collateralTokens'][0]['usdExact'] = '0.000000000000000001'
        risk['positions'][0]['debtTokens'][0]['usdExact'] = '0.000000000000000001'
        for r in rates['ethereum']['payload']['interestRates']:
            r['supplyInterestRate'] = '0'
            r['borrowInterestRate'] = '0'
            r['outsideSupplyInterestRateParts'] = []
            r['outsideBorrowInterestRateParts'] = []
        row = self.build(risk, rates)['positions'][0]
        self.assertEqual(row['collateralUSD'], '0.000000000000000001')
        self.assertEqual(row['annualNetUSD'], '0')
        self.assertEqual(row['netAprPct'], '0')

    def test_source_timestamps_and_rate_hashes_are_inspectable(self):
        output = self.build()
        self.assertEqual(output['sources']['positionsGeneratedAt'], STAMP)
        self.assertEqual(output['sources']['rates']['ethereum']['fetchedAt'], STAMP)
        self.assertEqual(len(output['sources']['rates']['ethereum']['sha256']), 64)
        self.assertEqual(output['coverage']['rankedPositions'], len(output['positions']))

    def test_exact_rate_inputs_are_retained_for_independent_rederivation(self):
        risk, rates = fixture()
        output = self.build(risk, rates)
        self.assertEqual(output['sources']['rates']['ethereum'].get('payload'), rates['ethereum']['payload'])
        rates['ethereum']['payload']['interestRates'][0]['supplyInterestRate'] = '99'
        self.assertEqual(output['sources']['rates']['ethereum']['payload']['interestRates'][0]['supplyInterestRate'], '0.10')

    def test_nonfinite_inputs_are_not_ranked(self):
        risk, rates = fixture()
        risk['positions'][0]['collateralTokens'][0]['usdExact'] = 'NaN'
        self.assertEqual(self.build(risk, rates)['positions'], [])

    def test_incomplete_principal_source_and_legacy_unmarked_are_unranked(self):
        risk, rates = fixture()
        risk['positions'][0]['earningsComplete'] = False
        self.assertEqual(self.build(risk, rates)['positions'], [])
        del risk['positions'][0]['earningsComplete']
        self.assertEqual(self.build(risk, rates)['positions'], [])

    def test_rate_source_future_and_beyond_ninety_minutes_is_unavailable(self):
        risk, rates = fixture()
        rates['ethereum']['fetchedAt'] = '2026-10-28T20:06:00Z'
        self.assertEqual(self.build(risk, rates)['positions'], [])
        rates['ethereum']['fetchedAt'] = '2026-10-28T18:29:59Z'
        self.assertEqual(self.build(risk, rates)['positions'], [])

    def test_unknown_component_is_unranked_but_points_and_cash_can_be_excluded(self):
        risk, rates = fixture()
        rates['ethereum']['payload']['interestRates'][0]['outsideSupplyInterestRateParts'].append(part('Mystery', '0.8', 'unknown'))
        self.assertEqual(self.build(risk, rates)['positions'], [])


if __name__ == '__main__':
    unittest.main()
