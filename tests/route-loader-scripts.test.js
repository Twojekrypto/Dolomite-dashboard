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
      body: { innerHTML: '', appendChild() {} },
      querySelector() { return null; },
      createElement() { return { setAttribute() {} }; },
      open() {},
      write(value) { written = value; },
      close() {},
    },
    setTimeout(callback) { callback(); },
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

test('route loader appends one Cloudflare Analytics beacon after rendering the page', async () => {
  let written = '';
  const appended = [];
  const scheduled = [];
  const document = {
    body: null,
    querySelector() { return null; },
    createElement(tagName) {
      return {
        tagName,
        attributes: {},
        setAttribute(name, value) { this.attributes[name] = value; },
      };
    },
    open() {},
    write(value) { written = value; },
    close() {},
  };
  const context = vm.createContext({
    console,
    window: {},
    document,
    setTimeout(callback) { scheduled.push(callback); },
    fetch: async () => ({
      ok: true,
      text: async () => '<html><head></head><body><main>Dashboard</main></body></html>',
    }),
  });
  vm.runInContext(source, context);

  await context.window.loadDoloRoute({
    label: 'DOLO',
    target: 'dolo-preview.html',
    version: 'test',
    base: './',
  });

  assert.equal(scheduled.length, 1);
  scheduled.shift()();
  assert.equal(scheduled.length, 1);

  document.body = {
    innerHTML: '',
    appendChild(node) { appended.push(node); },
  };
  scheduled.shift()();

  assert.doesNotMatch(written, /static\.cloudflareinsights\.com/);
  assert.equal(appended.length, 1);
  assert.equal(appended[0].tagName, 'script');
  assert.equal(appended[0].type, 'module');
  assert.equal(appended[0].src, 'https://static.cloudflareinsights.com/beacon.min.js');
  assert.equal(
    appended[0].attributes['data-cf-beacon'],
    '{"token":"930335c0b8864fdf8d9748c2432adaed"}',
  );
});
