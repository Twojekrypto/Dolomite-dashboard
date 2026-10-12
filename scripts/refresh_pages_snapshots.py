"""Refresh short-cadence Pages data with existing generators and validators."""
import json
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from check_dashboard_freshness import assess, load_config
GROUPS = (
    ('Assets', ['assets_live.json'], [['node', 'scripts/build_assets_live.mjs']]),
    ('DOLO price', ['dolo_price.json'], [[sys.executable, 'update_dolo_price.py']]),
    ('TVL', ['dolomite_tvl.json', 'dolomite_total_supply_history.json'],
     [[sys.executable, 'fetch_dolomite_tvl.py'],
      [sys.executable, 'fetch_dolomite_total_supply_history.py']]),
)


def check_freshness(files, root):
    config = load_config(ROOT / 'config/dashboard_freshness.json')
    for rule in config['artifacts']:
        if rule['path'] in files:
            payload = json.loads((root / rule['path']).read_text())
            if assess(payload, rule, int(time.time()))['state'] not in ('fresh', 'early_due'):
                raise ValueError('Candidate snapshot does not meet its freshness policy')


def refresh_group(name, files, commands, root=ROOT, runner=subprocess.run):
    """Accept a validated group atomically; failures keep the original bytes."""
    with tempfile.TemporaryDirectory(prefix='pages-snapshots-') as directory:
        backup = Path(directory)
        for filename in files:
            shutil.copy2(root / filename, backup / filename)
        try:
            for command in commands:
                runner(command, cwd=root, check=True, timeout=180)
            runner([sys.executable, 'validate_data.py', *files], cwd=root,
                   check=True, timeout=60)
            check_freshness(files, root)
        except (subprocess.SubprocessError, OSError, ValueError):
            for filename in files:
                shutil.copy2(backup / filename, root / filename)
            print(f'::warning::{name} refresh failed; retaining the verified snapshot and its original timestamps.')
            return False
    print(f'{name}: refreshed and validated for Pages.')
    return True


def main():
    for name, files, commands in GROUPS:
        refresh_group(name, files, commands)


if __name__ == '__main__':
    main()
