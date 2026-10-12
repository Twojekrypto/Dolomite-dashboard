import importlib.util
import subprocess
import tempfile
import unittest
import json
import time
from unittest import mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('pages_snapshots', ROOT / 'scripts/refresh_pages_snapshots.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class PagesSnapshotTests(unittest.TestCase):
    def test_validated_data_is_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'data.json').write_bytes(b'old')
            calls = []
            def run(command, **kwargs):
                calls.append(command)
                if command == ['generate']: (root / 'data.json').write_bytes(b'new')
            with mock.patch.object(module, 'check_freshness'):
                self.assertTrue(module.refresh_group('test', ['data.json'], [['generate']], root, run))
            self.assertEqual((root / 'data.json').read_bytes(), b'new')
            self.assertEqual(calls[-1][1:], ['validate_data.py', 'data.json'])

    def test_failed_generation_restores_the_original_timestamp_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = b'{"generatedAt":"old","value":100}'
            (root / 'data.json').write_bytes(original)
            def run(command, **kwargs):
                (root / 'data.json').write_bytes(b'{"generatedAt":"now","value":0}')
                raise subprocess.CalledProcessError(1, command)
            self.assertFalse(module.refresh_group('test', ['data.json'], [['generate']], root, run))
            self.assertEqual((root / 'data.json').read_bytes(), original)

    def test_invalid_tvl_history_rolls_back_both_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            files = ['snapshot.json', 'history.json']
            for name in files: (root / name).write_bytes(b'old-' + name.encode())
            def run(command, **kwargs):
                for name in files: (root / name).write_bytes(b'inconsistent')
                if 'validate_data.py' in command: raise subprocess.CalledProcessError(1, command)
            self.assertFalse(module.refresh_group('TVL', files, [['generate']], root, run))
            for name in files: self.assertEqual((root / name).read_bytes(), b'old-' + name.encode())

    def test_publication_refreshes_before_packaging_and_runs_its_tests(self):
        workflow = (ROOT / '.github/workflows/pages.yml').read_text()
        self.assertLess(workflow.index('python3 scripts/refresh_pages_snapshots.py'),
                        workflow.index('rsync -a'))
        self.assertIn("test_refresh_pages_snapshots.py", workflow)

    def test_fresh_wrapper_cannot_publish_old_fallback_rates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'assets_live.json').write_text(json.dumps({
                'generatedAt': int(time.time()), 'rateFallbackChains': ['ethereum'],
                'rateFallbackSourceGeneratedAt': int(time.time()) - 3600,
            }))
            with self.assertRaises(ValueError):
                module.check_freshness(['assets_live.json'], root)


if __name__ == '__main__': unittest.main()
