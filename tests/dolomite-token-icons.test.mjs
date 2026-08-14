import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';

import * as iconSync from '../scripts/sync_dolomite_token_icons.mjs';

const {
  buildOfficialIconRegistry,
  renderOfficialIconRegistry,
} = iconSync;

const root = path.resolve(import.meta.dirname, '..');
const officialBtccxIcon = 'https://app.dolomite.io/static/media/BTCcx.45aa66746e8c8acfeb7ee02edfb29f1a.svg';

test('official manifest fetch exposes a deterministic retry boundary', () => {
  assert.equal(typeof iconSync.fetchOfficialManifest, 'function');
});

test('official manifest fetch uses the supplied transport boundary', async () => {
  const manifest = {
    files: {
      'static/media/DOLO.svg': './static/media/DOLO.hash.svg',
    },
  };
  const actual = await iconSync.fetchOfficialManifest({
    fetchImpl: async () => ({
      ok: true,
      status: 200,
      json: async () => manifest,
    }),
  });

  assert.deepEqual(actual, manifest);
});

test('official manifest fetch retries a terminated response body', async () => {
  let attempts = 0;
  const manifest = {
    files: {
      'static/media/DOLO.svg': './static/media/DOLO.hash.svg',
    },
  };
  const actual = await iconSync.fetchOfficialManifest({
    fetchImpl: async () => {
      attempts += 1;
      if (attempts === 1) {
        return {
          ok: true,
          status: 200,
          json: async () => {
            throw new TypeError('terminated');
          },
        };
      }
      return {
        ok: true,
        status: 200,
        json: async () => manifest,
      };
    },
    sleepImpl: async () => {},
  });

  assert.deepEqual(actual, manifest);
  assert.equal(attempts, 2);
});

test('official manifest fetch retries HTTP 429 and then succeeds', async () => {
  let attempts = 0;
  const manifest = { files: {} };
  const actual = await iconSync.fetchOfficialManifest({
    fetchImpl: async () => {
      attempts += 1;
      return attempts === 1
        ? { ok: false, status: 429, json: async () => ({}) }
        : { ok: true, status: 200, json: async () => manifest };
    },
    sleepImpl: async () => {},
  });

  assert.deepEqual(actual, manifest);
  assert.equal(attempts, 2);
});

test('official manifest fetch stops after three transport failures', async () => {
  let attempts = 0;
  await assert.rejects(
    iconSync.fetchOfficialManifest({
      fetchImpl: async () => {
        attempts += 1;
        throw new TypeError('socket closed');
      },
      sleepImpl: async () => {},
    }),
    /socket closed/,
  );

  assert.equal(attempts, 3);
});

test('official manifest fetch does not retry permanent HTTP 404', async () => {
  let attempts = 0;
  await assert.rejects(
    iconSync.fetchOfficialManifest({
      fetchImpl: async () => {
        attempts += 1;
        return { ok: false, status: 404, json: async () => ({}) };
      },
      sleepImpl: async () => {},
    }),
    /HTTP 404/,
  );

  assert.equal(attempts, 1);
});

test('official Dolomite asset manifest produces the BTCcx icon and ignores non-media files', () => {
  const registry = buildOfficialIconRegistry({
    files: {
      'main.js': './static/js/main.d140ecf3.js',
      'static/media/BTCcx.svg': './static/media/BTCcx.45aa66746e8c8acfeb7ee02edfb29f1a.svg',
      'static/media/USDC.png': './static/media/USDC.1234.png',
    },
  });

  assert.equal(registry.BTCcx, officialBtccxIcon);
  assert.equal(registry.USDC, 'https://app.dolomite.io/static/media/USDC.1234.png');
  assert.equal(registry['main.js'], undefined);
});

test('generated browser registry resolves exact and case-insensitive Dolomite symbols', () => {
  const source = renderOfficialIconRegistry({ BTCcx: officialBtccxIcon });
  const context = {};
  vm.runInNewContext(source, context);

  assert.equal(context.DOLOMITE_TOKEN_ICONS.BTCcx, officialBtccxIcon);
  assert.equal(context.DOLOMITE_TOKEN_ICONS.BTCCX, officialBtccxIcon);
  assert.equal(context.getDolomiteOfficialTokenIcon('BTCcx'), officialBtccxIcon);
  assert.equal(context.getDolomiteOfficialTokenIcon('btccx'), officialBtccxIcon);
});

test('all token table runtimes load the shared generated Dolomite icon registry', () => {
  const htmlFiles = [
    'assets-preview.html',
    'portfolio-preview.html',
    'tvl-preview.html',
    'liquidation-preview.html',
    'liq-monitor.html',
    'dashboard-core.html',
    'earn/earn-core.html',
  ];

  for (const file of htmlFiles) {
    const source = fs.readFileSync(path.join(root, file), 'utf8');
    assert.match(source, /dolomite-token-icons\.generated\.js/, `${file} should load the shared registry`);
  }
});

test('scheduled Assets refresh also publishes the generated official icon registry', () => {
  const workflow = fs.readFileSync(path.join(root, '.github/workflows/update-assets-live.yml'), 'utf8');
  assert.match(workflow, /node scripts\/sync_dolomite_token_icons\.mjs/);
  assert.match(workflow, /git add assets_live\.json dolomite-token-icons\.generated\.js/);
});
