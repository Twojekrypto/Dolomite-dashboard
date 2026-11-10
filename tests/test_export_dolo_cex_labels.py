import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
ADDRESS = '0x4d7a15dc073fa6ffd1c142ccde64f6f676166a73'
IDENTITY = dict(label='Binance', type='cex', confidence='confirmed', source='debank-public-label', sourceUrl='https://debank.com/profile/' + ADDRESS, verifiedAt='2026-11-05')


class ExportCexLabelsTest(unittest.TestCase):
    def test_export_contains_only_direct_public_exchange_identities(self):
        rows = {ADDRESS: IDENTITY}
        for n, change in enumerate([dict(confidence='potential'), dict(source='flow-audit'), dict(sourceUrl='https://example.com/'), dict(type='watch'), dict(label='<script>')], start=1):
            address = '0x' + str(n) * 40
            rows[address] = {**IDENTITY, 'sourceUrl': 'https://debank.com/profile/' + address, **change}
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            labels = tmp / 'labels.js'
            labels.write_text('window.cloneDoloAddressLabels=()=>(' + json.dumps(rows) + ');')
            output = tmp / 'feed.js'
            result = subprocess.run([sys.executable, str(ROOT / 'export_dolo_cex_labels.py'), '--labels', str(labels), '--output', str(output)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            node = subprocess.run(['node', '-e', 'global.window={};require(process.argv[1]);console.log(JSON.stringify(window.DOLO_LIVE_CEX_LABELS));', str(output)], capture_output=True, text=True, check=True)
            self.assertEqual(json.loads(node.stdout), {ADDRESS: IDENTITY})


if __name__ == '__main__':
    unittest.main()
