import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import generate_odolo_flows as flows

ROOT = Path(__file__).resolve().parents[1]


def response(payload):
    value = Mock()
    value.json.return_value = payload
    return value


def log(block, amount=1):
    return {'address': flows.ODOLO_CONTRACT, 'topics': [flows.TRANSFER_TOPIC, '0x'+'0'*24+'1'*40, '0x'+'0'*24+'2'*40], 'data': hex(amount), 'blockNumber': hex(block)}


class TransferRecoveryTests(unittest.TestCase):
    def test_missing_result_never_becomes_an_empty_success(self):
        with patch.object(flows, 'RPC_URLS', ['https://rpc.invalid']), patch.object(flows.requests, 'post', return_value=response({})), patch.object(flows.time, 'sleep'):
            with self.assertRaises(RuntimeError):
                flows.fetch_transfer_logs(1, 2)

    def test_successful_provider_stays_first_on_later_chunks(self):
        calls = []
        def post(url, **kwargs):
            calls.append(url)
            return response({'error': {'code': 429, 'message': 'monthly capacity'}} if url.endswith('limited') else {'result': []})
        with patch.object(flows, 'CHUNK_SIZE', 2), patch.object(flows, 'RPC_URLS', ['https://rpc/limited', 'https://rpc/healthy']), patch.object(flows.requests, 'post', side_effect=post), patch.object(flows.time, 'sleep'):
            self.assertEqual(flows.fetch_transfer_logs(1, 6), [])
        self.assertEqual(calls, ['https://rpc/limited', 'https://rpc/healthy', 'https://rpc/healthy', 'https://rpc/healthy'])

    def test_failed_scan_keeps_only_complete_chunks_and_resumes_with_overlap(self):
        with tempfile.TemporaryDirectory() as tmp:
            checkpoint = str(Path(tmp)/'full.json')
            def first(url, **kwargs):
                start = int(kwargs['json']['params'][0]['fromBlock'], 16)
                if start == 1:
                    return response({'result': [log(1), log(2)]})
                raise RuntimeError('temporary provider failure')
            with patch.object(flows, 'CHUNK_SIZE', 2), patch.object(flows, 'RPC_URLS', ['https://rpc/healthy']), patch.object(flows.requests, 'post', side_effect=first), patch.object(flows.time, 'sleep'):
                with self.assertRaises(RuntimeError):
                    flows.fetch_transfer_logs(1, 6, checkpoint_path=checkpoint)
            saved = json.loads(Path(checkpoint).read_text())
            self.assertEqual(saved['nextBlock'], 3)
            self.assertEqual([x[3] for x in saved['transfers']], [1, 2])
            self.assertNotIn('schema_version', saved, 'partial scans cannot masquerade as a complete flow ledger')
            ranges = []
            def second(url, **kwargs):
                q = kwargs['json']['params'][0]
                start,end = int(q['fromBlock'],16),int(q['toBlock'],16)
                ranges.append((start,end))
                return response({'result': [log(3)] if start == 2 else []})
            with patch.object(flows, 'REORG_BUFFER_BLOCKS', 1), patch.object(flows, 'CHUNK_SIZE', 2), patch.object(flows, 'RPC_URLS', ['https://rpc/healthy']), patch.object(flows.requests, 'post', side_effect=second), patch.object(flows.time, 'sleep'):
                result = flows.fetch_transfer_logs(1, 6, checkpoint_path=checkpoint)
            self.assertEqual(ranges[0], (2,3))
            self.assertEqual([x[3] for x in result], [1,3], 'orphaned overlap data must be replaced')

    def test_checkpoint_identity_cannot_cross_requested_ranges(self):
        with tempfile.TemporaryDirectory() as tmp:
            checkpoint = str(Path(tmp)/'full.json')
            with patch.object(flows, 'RPC_URLS', ['https://rpc/healthy']), patch.object(flows.requests, 'post', return_value=response({'result': []})), patch.object(flows.time, 'sleep'):
                flows.fetch_transfer_logs(1, 2, checkpoint_path=checkpoint)
            calls = []
            def post(url, **kwargs):
                calls.append(int(kwargs['json']['params'][0]['fromBlock'],16))
                return response({'result': []})
            with patch.object(flows, 'RPC_URLS', ['https://rpc/healthy']), patch.object(flows.requests, 'post', side_effect=post), patch.object(flows.time, 'sleep'):
                flows.fetch_transfer_logs(100, 101, checkpoint_path=checkpoint)
            self.assertEqual(calls, [100])

    def test_malformed_chunk_is_not_partially_appended_before_retry(self):
        results = [response({'result': [log(1), {'bad': 'log'}]}), response({'result': [log(1)]})]
        with patch.object(flows, 'RPC_URLS', ['https://rpc/a','https://rpc/b']), patch.object(flows.requests, 'post', side_effect=results), patch.object(flows.time, 'sleep'):
            self.assertEqual(len(flows.fetch_transfer_logs(1,2)),1)


class PublicationIsolationTests(unittest.TestCase):
    def test_claim_publication_is_independent_of_the_long_flow_scan(self):
        workflow = (ROOT/'.github/workflows/update-reward-claim-events.yml').read_text()
        self.assertIn('REWARD_CLAIM_CHAINS: arbitrum,berachain', workflow)
        self.assertIn('DRPC_BERACHAIN_RPC_ZEN', workflow)
        config = json.loads((ROOT/'config/dashboard_freshness.json').read_text())
        rule = next(x for x in config['artifacts'] if x['id']=='reward-claim-berachain')
        self.assertEqual(rule['workflow'], 'update-reward-claim-events.yml')

    def test_flow_workflow_keeps_checkpoints_even_when_generation_fails(self):
        workflow = (ROOT/'.github/workflows/update-odolo-flows.yml').read_text()
        self.assertIn('group: odolo-flows', workflow)
        self.assertIn('sparse-checkout-cone-mode: false', workflow)
        self.assertIn('odolo_flow_checkpoints', workflow)
        self.assertIn('actions/cache/save@v5', workflow)
        self.assertIn('always()', workflow)
        self.assertIn('timeout-minutes: 60', workflow)
        self.assertIn('DRPC_BERACHAIN_RPC_ZEN', workflow)
        self.assertIn('QUICKNODE_BERACHAIN_RPC_2', workflow)
        self.assertNotIn('git add odolo_flows.json data/odolo-claim-events.json', workflow)


if __name__ == '__main__':
    unittest.main()
