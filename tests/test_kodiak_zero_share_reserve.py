"""Kodiak minimum shares are a proven reserve, never a wallet or a burn.

Production evidence: Berachain Island 0x5e4fe86c4f6fb2203fbfb77fd6ec3cb300b639aa,
block 4,129,783, tx 0x94eb730cab2c1c49d90a3cabd7fdc7e5a49593093271dc88ceacde987858c1a1:
Transfer(0, 0, 1000); balanceOf(0) changes from 0 to 1000 at that block.
Fixtures use small independent amounts; no production address/amount exception.
"""
import unittest
from unittest.mock import patch

import generate_dolo_liquidity as lp
from tests.test_lp_resilience import ADDRESS, ALICE, BOB, log


class KodiakZeroShareReserveTests(unittest.TestCase):
    def transfers(self):
        return [lp.normalize_rpc_log(row) for row in [
            log(10, 7, lp.ZERO_ADDRESS, lp.ZERO_ADDRESS),
            log(11, 100, recipient=ALICE, tx='bb'),
            log(12, 30, ALICE, BOB, tx='cc'),
            log(13, 5, BOB, lp.ZERO_ADDRESS, tx='dd'),
        ]]

    def reconcile(self, transfers, total, balances):
        def read_balances(chain, calls):
            self.assertEqual(chain, 'berachain')
            return {call['id']: (balances[call['args'][0]],) for call in calls}, {}
        with patch.object(lp, '_routescan_logs', return_value=transfers), \
                patch.object(lp, '_batch_eth_call_args', side_effect=read_balances):
            return lp._reconcile_transfer_holder_balances(
                'berachain', 80094, ADDRESS, 10, 13, total)

    def test_island_reconciles_zero_mint_without_treating_user_burn_as_reserve(self):
        balances = {lp.ZERO_ADDRESS: 7, ALICE: 70, BOB: 25}
        result = self.reconcile(self.transfers(), 102, balances)
        self.assertEqual(result, balances)

    def test_zero_reserve_must_match_pinned_balance_even_when_total_matches(self):
        # Same total, but one raw share assigned to the wrong owner must fail.
        with self.assertRaisesRegex(RuntimeError, 'match Transfer replay'):
            self.reconcile(self.transfers(), 102, {lp.ZERO_ADDRESS: 6, ALICE: 71, BOB: 25})

    def test_missing_zero_mint_is_not_patched_from_current_balance(self):
        with self.assertRaisesRegex(ValueError, 'total supply'):
            self.reconcile(self.transfers()[1:], 102, {lp.ZERO_ADDRESS: 7, ALICE: 70, BOB: 25})

    def test_non_island_replay_keeps_existing_zero_address_semantics(self):
        self.assertEqual(lp.replay_erc20_share_balances(self.transfers(), 95),
                         {ALICE: 70, BOB: 25})

    def test_reserve_is_not_a_wallet_and_is_not_redistributed_to_users(self):
        underlying = {'id': 'test-island', 'quality': 'verified',
                      'amount0Raw': '1020', 'doloRaw': '1020',
                      'amount1Raw': '2040', 'pairedRaw': '2040'}
        rows = lp.allocate_kodiak_island_position(
            underlying, {'address': ADDRESS, 'totalShares': 102,
                         'balances': {lp.ZERO_ADDRESS: 7, ALICE: 70, BOB: 25}}, [])
        users = {row['beneficialOwner']: row for row in rows if row['beneficialOwner']}
        self.assertEqual(set(users), {ALICE, BOB})
        self.assertEqual(users[ALICE]['doloRaw'], '700')
        self.assertEqual(users[BOB]['doloRaw'], '250')
        reserves = [row for row in rows if not row['beneficialOwner']]
        self.assertEqual(len(reserves), 1)
        reserve = reserves[0]
        self.assertEqual(reserve['custodian'], lp.ZERO_ADDRESS)
        self.assertEqual(reserve['attributionPath'], 'zero_address_reserve')
        self.assertEqual(reserve['positionStatus'], 'custodied_unresolved')
        self.assertEqual(reserve['doloRaw'], '70')
        self.assertEqual(reserve['pairedRaw'], '140')
        self.assertEqual(sum(int(row['doloRaw']) for row in rows), 1020)
        self.assertEqual(sum(int(row['pairedRaw']) for row in rows), 2040)


if __name__ == '__main__':
    unittest.main()
