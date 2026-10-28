import copy
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import generate_supply_activity_overview as overview


GENERATED = '2026-10-28T08:36:41.018173Z'
NOW = datetime(2026, 10, 28, 10, 0, tzinfo=timezone.utc)
END = 1793176601
START = 1790584601
CHAIN_NAMES = ('ethereum', 'arbitrum', 'berachain')


def leg(kind='deposit', serial=1, timestamp=END - 10, **changes):
    result = {
        'id': f'0xtransaction-{serial}--0x0000000000000000000000000000000000000001',
        'sourceId': f'{kind}-{serial}', 'type': kind,
        'timestamp': timestamp, 'txHash': '0x' + 'a' * 64,
        'amount': '123456789.123456789123456789',
        'usd': '987654321.987654321987654321',
        'wallet': '0x' + 'b' * 40, 'secondaryAddress': '',
        'tokenId': '0x0000000000000000000000000000000000000001',
        'ambiguous': False,
    }
    result.update(changes)
    return result


def group(*legs):
    first = legs[0]
    return [first['sourceId'] + '-wallet-' + first['wallet'], first['type'],
            first['timestamp'], first['txHash'], '999', '0', first['wallet'], '',
            {'version': 2, 'sourceIds': sorted({x['sourceId'] for x in legs}),
             'legs': list(legs), 'usdStatus': 'unavailable',
             'status': 'verified', 'amountBasis': 'net-wallet-balance-change'}]


class SupplyActivityOverviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = {'schemaVersion': 1, 'generatedAt': GENERATED,
                         'refreshedChains': list(CHAIN_NAMES), 'chains': []}
        for chain in CHAIN_NAMES:
            entry = {'chain': chain, 'tokensAvailable': 1, 'tokensWritten': 1,
                     'tokensSkipped': 0, 'skippedTokens': [], 'tokens': []}
            self.manifest['chains'].append(entry)
            self.write_asset(chain, [group(leg())])
        self.save_manifest()

    def save_manifest(self):
        (self.root / 'manifest.json').write_text(json.dumps(self.manifest))

    def chain(self, name):
        return next(c for c in self.manifest['chains'] if c['chain'] == name)

    def path(self, chain, token_id='0x0000000000000000000000000000000000000001'):
        return self.root / 'recent' / chain / (token_id + '.json')

    def write_asset(self, chain, rows, token_id='0x0000000000000000000000000000000000000001', **changes):
        payload = {'schemaVersion': 2, 'source': 'static-subgraph-activity',
                   '__supplyActivityCompact': 1, 'scope': '30d', 'chain': chain,
                   'tokenId': token_id, 'symbol': 'TOKEN', 'name': 'Token Name',
                   'marketId': '1', 'generatedAt': GENERATED, 'rows': rows}
        payload.update(changes)
        path = self.path(chain, token_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload))
        tokens = self.chain(chain)['tokens']
        entry = next((x for x in tokens if x['tokenId'] == token_id), None)
        if entry is None:
            entry = {'tokenId': token_id}
            tokens.append(entry)
        entry.update({'symbol': 'TOKEN', 'marketId': '1', 'generatedAt': GENERATED,
                      'recentEvents': len(rows),
                      'recentPath': f'data/supply-activity/recent/{chain}/{token_id}.json'})
        self.save_manifest()

    def build(self, now=NOW):
        return overview.build_overview(self.root, now=now)

    def test_all_three_chains_preserve_event_grain_and_decimal_strings(self):
        result = self.build()
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['coverage']['includedAssets'], 3)
        self.assertEqual(result['generatedAt'], GENERATED)
        self.assertEqual(result['periodStart'], START)
        self.assertEqual(result['periodEnd'], END)
        self.assertEqual([t['chain'] for t in result['tokens']], list(CHAIN_NAMES))
        self.assertEqual(len(result['rows']), 3)
        self.assertEqual(result['rows'][0][4:6],
                         ['123456789.123456789123456789', '987654321.987654321987654321'])
        self.assertEqual(result['tokens'][0]['name'], 'Token Name')

    def test_extracts_each_deposit_and_withdraw_from_same_transaction(self):
        self.write_asset('ethereum', [group(leg('withdraw', 1), leg('deposit', 2))])
        result = self.build()
        rows = [r for r in result['rows'] if result['tokens'][r[8]]['chain'] == 'ethereum']
        self.assertEqual({r[0] for r in rows}, {'deposit-2', 'withdraw-1'})
        self.assertEqual({r[1] for r in rows}, {'deposit', 'withdraw'})
        self.assertEqual({r[3] for r in rows}, {'0x' + 'a' * 64})
        self.assertEqual({r[4] for r in rows}, {'123456789.123456789123456789'})

    def test_filters_window_inclusively_and_sorts_newest_first(self):
        self.write_asset('ethereum', [group(leg(serial=1, timestamp=START - 1)),
                                      group(leg(serial=2, timestamp=START)),
                                      group(leg(serial=3, timestamp=END)),
                                      group(leg(serial=4, timestamp=END + 1))])
        result = self.build()
        rows = [r for r in result['rows'] if result['tokens'][r[8]]['chain'] == 'ethereum']
        self.assertEqual([r[0] for r in rows], ['deposit-3', 'deposit-2'])
        self.assertEqual([r[2] for r in result['rows']], sorted([r[2] for r in result['rows']], reverse=True))

    def test_deduplicates_events_without_deduplicating_across_chains(self):
        self.write_asset('ethereum', [group(leg()), group(leg())])
        result = self.build()
        self.assertEqual(len(result['rows']), 3)
        self.assertEqual({result['tokens'][r[8]]['chain'] for r in result['rows']}, set(CHAIN_NAMES))

    def test_conflicting_event_evidence_makes_chain_unavailable(self):
        self.write_asset('ethereum', [group(leg()), group(leg(amount='2'))])
        result = self.build()
        self.assertEqual(result['status'], 'partial')
        self.assertIsNone(result['chains'][0]['rowCount'])
        self.assertEqual(result['chains'][0]['includedAssets'], 0)
        self.assertEqual(len(result['rows']), 2)

    def test_transfers_and_trades_do_not_hide_nested_deposit_legs(self):
        self.write_asset('ethereum', [group(leg('transfer', 1), leg('deposit', 2), leg('trade', 3))])
        result = self.build()
        rows = [r for r in result['rows'] if result['tokens'][r[8]]['chain'] == 'ethereum']
        self.assertEqual([r[0] for r in rows], ['deposit-2'])

    def test_missing_usd_is_null_while_exact_quote_and_zero_amount_survive(self):
        legs = [leg(serial=1, usd=None), leg(serial=2, usd='0'),
                leg(serial=3, amount='0', usd='0'), leg(serial=4, usd='0.000000000000000001')]
        self.write_asset('ethereum', [group(*legs)])
        rows = {r[0]: r for r in self.build()['rows'] if r[8] == 0}
        self.assertIsNone(rows['deposit-1'][5])
        self.assertIsNone(rows['deposit-2'][5])
        self.assertEqual(rows['deposit-2'][9]['sourceUsd'], '0')
        self.assertEqual(rows['deposit-3'][5], '0')
        self.assertEqual(rows['deposit-4'][5], '0.000000000000000001')

    def test_empty_valid_asset_is_complete_with_zero_events(self):
        self.write_asset('ethereum', [])
        result = self.build()
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['chains'][0]['rowCount'], 0)
        self.assertEqual(result['chains'][0]['includedAssets'], 1)

    def test_explicitly_empty_market_universe_is_complete(self):
        self.chain('ethereum').update(tokens=[], tokensAvailable=0, tokensWritten=0)
        self.save_manifest()
        result = self.build()
        self.assertEqual(result['chains'][0]['status'], 'complete')
        self.assertEqual(result['chains'][0]['rowCount'], 0)

    def test_legacy_source_rows_keep_individual_events(self):
        row = ['deposit-1', 'deposit', END - 10, '0x' + 'a' * 64,
               '0.000000000000000001', '0.000000000000000002', '0x' + 'b' * 40, '']
        self.write_asset('ethereum', [row], schemaVersion=1)
        result = self.build()
        self.assertEqual(result['rows'][0][:8], row)

    def test_missing_asset_file_is_unavailable_rather_than_empty(self):
        self.path('ethereum').unlink()
        result = self.build()
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['coverage']['unavailableChains'], ['ethereum'])
        self.assertIsNone(result['chains'][0]['rowCount'])
        self.assertEqual(result['coverage']['includedAssets'], 2)

    def test_missing_chain_has_unknown_coverage_not_zero(self):
        self.manifest['chains'].pop(0)
        self.save_manifest()
        result = self.build()
        self.assertIsNone(result['chains'][0]['expectedAssets'])
        self.assertIsNone(result['coverage']['expectedAssets'])
        self.assertEqual(result['status'], 'partial')

    def test_skipped_or_unmanifested_assets_prevent_complete_claim(self):
        self.chain('ethereum')['tokensAvailable'] = 2
        self.chain('ethereum')['tokensSkipped'] = 1
        skipped = {'tokenId': '0x' + 'f' * 40, 'reason': 'fetch_error', 'supplyLiquidityUSD': '0'}
        self.chain('ethereum')['skippedTokens'] = [skipped]
        self.save_manifest()
        result = self.build()
        self.assertEqual(result['chains'][0]['skippedTokens'], [skipped])
        self.assertEqual(result['chains'][0]['status'], 'unavailable')
        self.assertEqual(result['chains'][0]['expectedAssets'], 2)

    def test_stale_source_is_not_retimestamped_or_published_as_fresh(self):
        result = self.build(now=NOW + timedelta(days=2))
        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['generatedAt'], GENERATED)
        self.assertEqual(result['rows'], [])
        self.assertTrue(all(c['rowCount'] is None for c in result['chains']))

    def test_future_manifest_is_unavailable(self):
        result = self.build(now=NOW - timedelta(days=1))
        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['rows'], [])

    def test_mixed_payload_and_manifest_token_generations_are_unavailable(self):
        for target in ('payload', 'token'):
            with self.subTest(target=target):
                self.write_asset('ethereum', [group(leg())])
                if target == 'payload':
                    payload = json.loads(self.path('ethereum').read_text())
                    payload['generatedAt'] = '2026-10-28T07:36:41.018173Z'
                    self.path('ethereum').write_text(json.dumps(payload))
                else:
                    self.chain('ethereum')['tokens'][0]['generatedAt'] = '2026-10-28T07:36:41.018173Z'
                    self.save_manifest()
                result = self.build()
                self.assertEqual(result['chains'][0]['status'], 'unavailable')
                self.assertEqual(len(result['rows']), 2)

    def test_missing_or_ambiguous_leg_evidence_is_unavailable(self):
        broken = group(leg())
        cases = [copy.deepcopy(broken) for _ in range(3)]
        cases[0][8]['legs'] = []
        cases[1][8]['sourceIds'].append('withdraw-absent')
        cases[2][8]['legs'][0]['ambiguous'] = True
        for row in cases:
            with self.subTest(row=row):
                self.write_asset('ethereum', [row])
                self.assertEqual(self.build()['chains'][0]['status'], 'unavailable')

    def test_source_deposit_cannot_disappear_as_an_unrecognized_leg_type(self):
        broken = group(leg())
        broken[8]['legs'][0]['type'] = 'unexpected'
        self.write_asset('ethereum', [broken])
        self.assertEqual(self.build()['chains'][0]['status'], 'unavailable')

    def test_float_amounts_and_manifest_row_count_mismatch_are_unavailable(self):
        self.write_asset('ethereum', [group(leg(amount=0.123456789))])
        self.assertEqual(self.build()['chains'][0]['status'], 'unavailable')
        self.write_asset('ethereum', [group(leg())])
        self.chain('ethereum')['tokens'][0]['recentEvents'] = 2
        self.save_manifest()
        self.assertEqual(self.build()['chains'][0]['status'], 'unavailable')

    def test_cli_writes_unavailable_payload_and_exits_nonzero_for_stale_source(self):
        target = self.root / 'overview.json'
        command = [sys.executable, str(Path(overview.__file__)), '--input-dir', str(self.root),
                   '--output', str(target), '--now', '2026-10-31T10:00:00Z']
        process = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(process.returncode, 1)
        self.assertEqual(json.loads(target.read_text())['status'], 'unavailable')

    def test_workflow_preserves_overview_for_targeted_refreshes(self):
        workflow = (Path(overview.__file__).parent / '.github/workflows/update-supply-history.yml').read_text()
        match = re.search(r'      - name: Generate static supply history\n        run: \|\n(.*?)(?=\n      - name:)', workflow, re.S)
        self.assertIsNotNone(match)
        block = textwrap.dedent(match.group(1))
        cases = [
            ({'chains': 'ethereum'}, 'preserved'),
            ({'chains': 'arbitrum,berachain'}, 'preserved'),
            ({'chains': ' berachain, ethereum , arbitrum '}, 'refreshed'),
            ({'chains': 'ethereum,arbitrum,berachain'}, 'refreshed'),
            ({'chains': 'ethereum,arbitrum,berachain', 'activity_recent_token_limit': '1'}, 'preserved'),
            ({'chains': 'ethereum,arbitrum,berachain', 'tokens': '0x' + 'a' * 40}, 'preserved'),
            ({'chains': 'ethereum,arbitrum,berachain', 'activity_recent_enabled': 'false'}, 'preserved'),
        ]
        stub = '''python3() {
          if [ "$1" = "generate_supply_activity_overview.py" ]; then
            printf refreshed > overview-marker
          fi
        }
        '''
        for inputs, expected in cases:
            with self.subTest(inputs=inputs):
                (self.root / 'overview-marker').write_text('preserved')
                script = re.sub(r'\$\{\{ github.event.inputs.(\w+) \}\}',
                                lambda m: inputs.get(m.group(1), ''), block)
                process = subprocess.run(['bash', '-e', '-c', stub + script], cwd=self.root,
                                         capture_output=True, text=True)
                self.assertEqual(process.returncode, 0, process.stderr)
                self.assertEqual((self.root / 'overview-marker').read_text(), expected)


if __name__ == '__main__':
    unittest.main()
