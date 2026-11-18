import copy
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import fetch_operating_income as income


class OperatingIncomeTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 18, 21, tzinfo=timezone.utc)
        self.windows = income.periods(self.now)
        start, end = self.windows['treasury']
        self.treasury = {'complete': True, 'period': {'startTimestamp': start, 'endTimestamp': end},
                         'chains': {c: {'complete': True, 'netInterestUSD': n} for c, n in
                                    [('ethereum', '1100'), ('arbitrum', '-10'), ('berachain', '5800')]}}
        start, end = self.windows['liquidations']
        self.liquidations = {'complete': True, 'period': {'startTimestamp': start, 'endTimestamp': end},
                             'chains': {c: {'complete': True, 'grossProtocolFeeUSD': n} for c, n in
                                        [('ethereum', '6000'), ('arbitrum', '1200'), ('berachain', '2800')]}}

    def build(self):
        return income.build_snapshot(self.treasury, self.liquidations, self.now)

    def test_exact_common_utc_windows_and_calendar_months(self):
        self.assertEqual(self.windows['treasury'], (1792368000, 1794960000))
        self.assertEqual(self.windows['liquidations'], (1763424000, 1794960000))
        leap = income.periods(datetime(2024, 2, 29, 18, tzinfo=timezone.utc))
        self.assertEqual(datetime.fromtimestamp(leap['liquidations'][0], timezone.utc).date().isoformat(), '2023-02-28')
        with self.assertRaises(ValueError):
            income.periods(datetime(2026, 11, 18))

    def test_partner_cut_only_liquidations_and_signed_treasury_preserved(self):
        original = copy.deepcopy(self.liquidations)
        result = self.build()
        self.assertEqual(result['period'], {'from': '2026-10-19', 'to': '2026-11-18', 'days': 30})
        self.assertEqual(result['treasury']['netInterestUSD'], '6890')
        self.assertEqual(result['treasury']['chains']['ethereum']['netInterestUSD'], '1100')
        self.assertEqual(result['liquidations']['grossUSD'], '10000')
        self.assertEqual(result['liquidations']['retainedUSD'], '5200')
        self.assertEqual(result['liquidations']['chains']['ethereum']['retainedProtocolFeeUSD'], '1200')
        self.assertTrue(result['liquidations']['monthlyRetainedUSD'].startswith('433.333333'))
        self.assertEqual(self.liquidations, original)
        self.assertTrue(result['coverage']['complete'])
        self.assertEqual(result, self.build(), 'building twice never compounds a partner cut')

    def test_incomplete_mismatched_chain_or_period_rejected(self):
        for target in [self.treasury, self.liquidations]:
            original = copy.deepcopy(target)
            for mutate in [lambda d: d.update(complete=False), lambda d: d['chains'].pop('arbitrum'),
                           lambda d: d['chains'].update(mantle={}),
                           lambda d: d['chains']['ethereum'].update(complete=False),
                           lambda d: d['period'].update(endTimestamp=1)]:
                target.clear(); target.update(copy.deepcopy(original)); mutate(target)
                with self.assertRaises(ValueError):
                    self.build()
            target.clear(); target.update(original)

    def test_invalid_money_never_becomes_zero(self):
        for value in ['NaN', 'Infinity', '', None, 123, True, '1e20', '1,000']:
            self.treasury['chains']['ethereum']['netInterestUSD'] = value
            with self.assertRaises(ValueError):
                self.build()
        self.treasury['chains']['ethereum']['netInterestUSD'] = '0'
        self.liquidations['chains']['ethereum']['grossProtocolFeeUSD'] = '-1'
        with self.assertRaises(ValueError):
            self.build()

    def test_collector_failure_does_not_replace_good_file(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'latest.json'
            output.write_text('previous verified output')
            def broken(*args, **kwargs):
                raise RuntimeError('source incomplete')
            with self.assertRaisesRegex(RuntimeError, 'incomplete'):
                income.refresh({'chains': {}, 'treasury': {}, 'liquidations': {}}, output, now=self.now,
                               treasury_collector=lambda *a, **k: self.treasury,
                               liquidation_collector=broken)
            self.assertEqual(output.read_text(), 'previous verified output')

    def test_atomic_roundtrip_and_component_options(self):
        calls = []
        def collector(report):
            def run(chains, start, end, *, options):
                calls.append((chains, start, end, options)); return report
            return run
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'new' / 'latest.json'
            result = income.refresh({'chains': {'marker': True}, 'treasury': {'requestAttempts': 2},
                                     'liquidations': {'requestAttempts': 3}}, output, now=self.now,
                                    treasury_collector=collector(self.treasury),
                                    liquidation_collector=collector(self.liquidations))
            self.assertEqual(json.loads(output.read_text()), result)
            self.assertEqual(calls[0][1:3], self.windows['treasury'])
            self.assertEqual(calls[1][1:3], self.windows['liquidations'])
            self.assertEqual(calls[1][3]['requestAttempts'], 3)
            self.assertEqual(list(output.parent.glob('*.tmp')), [])


if __name__ == '__main__':
    unittest.main()
