#!/usr/bin/env python3
"""Export only address-bound public CEX identities for the Cloudflare fallback registry."""
import argparse
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parent
PUBLIC_PREFIXES = {
    'debank-public-label': 'https://debank.com/profile/',
    'etherscan-public-label': 'https://etherscan.io/address/',
}


def confirmed_cex_labels(labels):
    result = {}
    for address, info in labels.items():
        prefix = PUBLIC_PREFIXES.get(info.get('source'))
        label = info.get('label', '')
        if (not re.fullmatch(r'0x[a-f0-9]{40}', address)
                or info.get('type') != 'cex' or info.get('confidence') != 'confirmed'
                or not prefix or info.get('sourceUrl', '').lower() != prefix + address
                or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', info.get('verifiedAt', ''))
                or not isinstance(label, str) or not label.strip() or len(label) > 80
                or re.search(r'["\\{}<>\x00-\x1f]', label)):
            continue
        result[address] = {key: info[key] for key in ('label', 'type', 'source', 'confidence', 'sourceUrl', 'verifiedAt')}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--labels', type=Path, default=ROOT / 'dolo-address-labels.js')
    parser.add_argument('--output', type=Path, default=ROOT / 'dolo-confirmed-cex.js')
    args = parser.parse_args()
    script = 'global.window={};require(process.argv[1]);console.log(JSON.stringify(window.cloneDoloAddressLabels()));'
    proc = subprocess.run(['node', '-e', script, str(args.labels.resolve())], check=True, capture_output=True, text=True)
    labels = confirmed_cex_labels(json.loads(proc.stdout))
    args.output.write_text('/* Generated from address-bound public exchange labels. */\nwindow.DOLO_LIVE_CEX_LABELS = ' + json.dumps(labels, sort_keys=True, ensure_ascii=True, separators=(',', ':')) + ';\n', encoding='utf-8')
    print(f'Published {len(labels)} confirmed CEX identities to {args.output.name}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
