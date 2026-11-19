import copy
from pathlib import Path
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from unittest import mock

import audit_dolomite_revenue_onchain as audit
import fetch_dolomite_revenue as revenue
import validate_data

NOW = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)


class DisplayedRevenueAssuranceTests(unittest.TestCase):
    def setUp(self):
        self.audit = {
            'targetDate': '2026-11-17', 'generatedAt': '2026-11-19T09:35:50Z',
            'status': 'warn', 'tolerancePct': .02, 'chains': {
                'Berachain': {'status': 'warn', 'feesUSD': 420.546510,
                    'revenueUSD': 84.915744, 'priceOmissionCount': 0,
                    'warnReasons': ['fees_diff_exceeds_tolerance', 'revenue_diff_exceeds_tolerance']},
                'Arbitrum': {'status': 'pass', 'feesUSD': 1163.973173,
                    'revenueUSD': 232.794635, 'priceOmissionCount': 0},
            },
        }
        self.series = [{'date': '2026-11-17', 'chains': {
            'Berachain': {'source': 'onchain-current-index-audit',
                'onchainAuditTargetDate': '2026-11-17',
                'onchainAuditGeneratedAt': self.audit['generatedAt'],
                'feesUSD': 420.546510, 'grossRevenueUSD': 84.915744,
                'borrowFeeRebateUSD': 1.705728, 'revenueUSD': 83.210016,
                'supplySideRevenueUSD': 335.630766},
            'Arbitrum': {'feesUSD': 1161, 'grossRevenueUSD': 232,
                'borrowFeeRebateUSD': 9.055803, 'revenueUSD': 222.944197,
                'supplySideRevenueUSD': 929},
        }}]

    def result(self):
        self.assertTrue(hasattr(revenue, 'displayed_onchain_audit_assurance'),
            'Displayed amounts need their own audit reconciliation')
        return revenue.displayed_onchain_audit_assurance(self.series, self.audit, now=NOW)

    def test_workflow_checkout_excludes_the_large_unrelated_archive(self):
        workflow = (Path(__file__).resolve().parents[1] / '.github/workflows/audit-dolomite-revenue-onchain.yml').read_text()
        self.assertIn('sparse-checkout-cone-mode: false', workflow)
        for path in ['/*.py', '/requirements.txt', '/dolomite_revenue.json', '/veborrow_rebate_history.json', '/data/dolomite-revenue-onchain-*.json', '/tests/', '/.github/workflows/', '/revenue-preview.html', '/revenue/index.html']:
            self.assertIn(path, workflow)

    def test_corrected_berachain_values_are_checked_without_erasing_adapter_warning(self):
        original = copy.deepcopy(self.audit)
        result = self.result()['Berachain']
        self.assertEqual(result['status'], 'pass')
        self.assertEqual(result['reason'], 'current_index_reconciled')
        self.assertEqual(result['adapterStatus'], 'warn')
        self.assertEqual(result['targetDate'], '2026-11-17')
        self.assertEqual(self.audit, original)

    def test_arbitrum_adapter_values_are_checked_with_original_two_percent_tolerance(self):
        self.assertEqual(self.result()['Arbitrum']['status'], 'pass')
        self.series[0]['chains']['Arbitrum'].update(feesUSD=1000, supplySideRevenueUSD=768)
        self.assertEqual(self.result()['Arbitrum']['status'], 'warn')

    def test_unpriced_tokens_stay_warn_even_if_displayed_totals_match(self):
        self.audit['chains']['Berachain']['priceOmissionCount'] = 1
        self.assertEqual(self.result()['Berachain']['status'], 'warn')

    def test_historical_override_never_certifies_another_day(self):
        self.series[0]['date'] = '2026-11-16'
        self.assertEqual(self.result()['Berachain']['status'], 'missing')

    def test_stale_audit_never_earns_a_current_pass(self):
        self.audit['targetDate'] = '2026-11-01'
        self.assertEqual(self.result()['Berachain']['status'], 'stale')

    def test_wrong_onchain_gross_or_net_blocks_publication(self):
        for field in ['grossRevenueUSD', 'revenueUSD', 'supplySideRevenueUSD']:
            with self.subTest(field=field):
                rows = copy.deepcopy(self.series)
                rows[0]['chains']['Berachain'][field] += 1
                self.assertTrue(hasattr(revenue, 'displayed_onchain_audit_assurance'))
                with self.assertRaises(ValueError):
                    revenue.displayed_onchain_audit_assurance(rows, self.audit, now=NOW)

    def test_mismatched_override_date_or_generation_blocks_publication(self):
        for field in ['onchainAuditTargetDate', 'onchainAuditGeneratedAt']:
            with self.subTest(field=field):
                rows = copy.deepcopy(self.series)
                rows[0]['chains']['Berachain'][field] = 'old'
                self.assertTrue(hasattr(revenue, 'displayed_onchain_audit_assurance'))
                with self.assertRaises(ValueError):
                    revenue.displayed_onchain_audit_assurance(rows, self.audit, now=NOW)

    def test_signed_rebate_correction_is_not_clamped(self):
        row = self.series[0]['chains']['Berachain']
        row.update(borrowFeeRebateUSD=-1, revenueUSD=85.915744)
        self.assertEqual(self.result()['Berachain']['status'], 'pass')

    def test_validation_rejects_missing_or_forged_displayed_audit(self):
        self.assertTrue(hasattr(validate_data, '_dolomite_revenue_displayed_audit_valid'))
        check = validate_data._dolomite_revenue_displayed_audit_valid
        assurance = revenue.onchain_audit_assurance(self.audit, now=NOW)
        data = {'generatedAt': NOW.isoformat(), 'series': self.series, 'assurance': assurance}
        self.assertFalse(check(data))
        assurance['onchainDisplayedAuditChains'] = self.result()
        self.assertTrue(check(data))
        assurance['onchainDisplayedAuditChains']['Arbitrum']['status'] = 'missing'
        self.assertFalse(check(data))


class MissingRevenuePriceTests(unittest.TestCase):
    def test_missing_dpx_price_uses_the_same_historical_market_oracle(self):
        token = '0x6C2C06790b3E3E3c38e12Ee22F8183b37a13EE55'
        row = {'marketId': 20, 'token': token, 'symbol': 'DPX', 'decimals': 18,
            'borrowInterestAmount': Decimal('.000042758127188643'),
            'protocolRevenueAmount': Decimal('.0000085516254377286')}
        w3 = mock.MagicMock()
        functions = w3.eth.contract.return_value.functions
        functions.getMarketTokenAddress.return_value.call.return_value = token
        functions.getMarketPrice.return_value.call.return_value = (4057925613569,)
        functions.decimals.return_value.call.return_value = 18
        with mock.patch.object(audit.rpc_client, 'get_endpoints', return_value=['https://rpc.invalid']), \
             mock.patch.object(audit, 'make_web3', return_value=w3), \
             mock.patch.object(audit, 'find_block_at_or_before', side_effect=[100, 200]), \
             mock.patch.object(audit, 'market_interest_rows', return_value=[row]), \
             mock.patch.object(audit, 'fetch_historical_prices', return_value={}):
            result = audit.audit_chain_onchain('arbitrum', audit.CHAIN_CONFIGS['arbitrum'], 20, 10)
        self.assertEqual(result['priceOmissionCount'], 0)
        self.assertEqual(result['rawTokens'][0]['priceSource'], 'dolomite-market-oracle')
        self.assertAlmostEqual(result['rawTokens'][0]['priceUSD'], .000004057925613569, places=20)
        functions.getMarketPrice.return_value.call.assert_called_once_with(block_identifier=200)

    def test_oracle_rejects_unverified_decimal_scaling(self):
        w3 = mock.MagicMock();functions = w3.eth.contract.return_value.functions
        token = '0x0000000000000000000000000000000000000001'
        functions.getMarketTokenAddress.return_value.call.return_value = token
        functions.getMarketPrice.return_value.call.return_value = (10**18,)
        functions.decimals.return_value.call.return_value = 6
        self.assertIsNone(audit.historical_market_oracle_price(w3, audit.CHAIN_CONFIGS['arbitrum'], {'marketId': 20, 'token': token, 'decimals': 18}, 200))

    def test_oracle_rejects_wrong_token_and_nonpositive_price(self):
        self.assertTrue(hasattr(audit, 'historical_market_oracle_price'))
        w3 = mock.MagicMock();functions = w3.eth.contract.return_value.functions
        token = '0x0000000000000000000000000000000000000001'
        row = {'marketId': 20, 'token': token, 'decimals': 18}
        functions.getMarketTokenAddress.return_value.call.return_value = '0x'+'02'*20
        self.assertIsNone(audit.historical_market_oracle_price(w3, audit.CHAIN_CONFIGS['arbitrum'], row, 200))
        functions.getMarketTokenAddress.return_value.call.return_value = token
        functions.getMarketPrice.return_value.call.return_value = (0,)
        functions.decimals.return_value.call.return_value = 18
        self.assertIsNone(audit.historical_market_oracle_price(w3, audit.CHAIN_CONFIGS['arbitrum'], row, 200))


if __name__ == '__main__':
    unittest.main()
