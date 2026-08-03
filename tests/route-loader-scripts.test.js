const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const source = fs.readFileSync('route-loader.js', 'utf8');
const supplyRoute = fs.readFileSync('supply/index.html', 'utf8');

test('route loader supports an explicit blocking script before fetched inline runtime', async () => {
  let written = '';
  const context = vm.createContext({
    console,
    window: {},
    document: {
      body: { innerHTML: '' },
      open() {},
      write(value) { written = value; },
      close() {},
    },
    fetch: async () => ({ ok: true, text: async () => '<html><head></head><body><script>startRuntime()</script></body></html>' }),
  });
  vm.runInContext(source, context);

  await context.window.loadDoloRoute({
    label: 'Supply',
    target: '../liquidation-preview.html',
    scripts: [{ src: 'supply/supply-draft.js?v=test', defer: false }],
  });

  assert.match(written, /<script src="supply\/supply-draft\.js\?v=test"><\/script>/);
  assert.doesNotMatch(written, /<script defer src="supply\/supply-draft\.js\?v=test"/);
  assert.ok(
    written.indexOf('supply/supply-draft.js?v=test') < written.indexOf('startRuntime()'),
    'blocking helper must be emitted before fetched inline runtime',
  );
});

test('Supply requests its presentation helper as a blocking route script', () => {
  assert.match(supplyRoute, /"src": "supply\/supply-draft\.js\?v=[^"]+"/);
  assert.match(supplyRoute, /"defer": false/);
});
