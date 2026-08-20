"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.resolve(__dirname, "..");
const loader = fs.readFileSync(path.join(root, "route-loader.js"), "utf8");
const portfolio = fs.readFileSync(path.join(root, "portfolio/index.html"), "utf8");
const vedolo = fs.readFileSync(path.join(root, "vedolo/index.html"), "utf8");

for (const asset of ["wallet-table-ux.css", "wallet-table-ux.js", "vedolo-position-activity.js", "dolo-address-overrides.js"]) {
  assert.ok(loader.includes(asset), `route-loader must manage ${asset}`);
}
assert.match(portfolio, /route-loader-table-ux-20260820/);
assert.match(vedolo, /route-loader-table-ux-20260820/);

async function renderPreview(previewHtml) {
  let output = "";
  const context = {
    window: {},
    console,
    fetch: async () => ({ ok: true, text: async () => previewHtml }),
    setTimeout: () => 0,
    document: {
      body: {},
      open() {},
      write(value) { output = value; },
      close() {},
      querySelector() { return null; },
      createElement() { return { setAttribute() {} }; },
    },
  };
  context.window = context;
  vm.createContext(context);
  vm.runInContext(loader, context, { filename: "route-loader.js" });
  await context.loadDoloRoute({ label: "Fixture", target: "fixture.html", version: "test" });
  return output;
}

(async () => {
  const existing = await renderPreview(`<!doctype html><html><head>
    <link rel="stylesheet" href="wallet-table-ux.css?v=old-css">
    <script src="wallet-table-ux.js?v=old-js"></script>
    <script src="vedolo-position-activity.js?v=old-helper"></script>
  </head><body></body></html>`);
  assert.match(existing, /wallet-table-ux\.css\?v=20260820-table-ux-v1/);
  assert.match(existing, /wallet-table-ux\.js\?v=20260820-table-ux-v1/);
  assert.match(existing, /vedolo-position-activity\.js\?v=20260820-new-lock-v1/);
  assert.equal((existing.match(/wallet-table-ux\.css/g) || []).length, 1, "existing shared CSS must not be duplicated");
  assert.equal((existing.match(/wallet-table-ux\.js/g) || []).length, 1, "existing shared JS must not be duplicated");
  assert.equal((existing.match(/vedolo-position-activity\.js/g) || []).length, 1, "existing helper must not be duplicated");
  assert.equal((existing.match(/dolo-address-overrides\.js/g) || []).length, 1, "address overrides must be injected once");

  const missing = await renderPreview('<!doctype html><html><head></head><body></body></html>');
  assert.equal((missing.match(/wallet-table-ux\.css/g) || []).length, 1);
  assert.equal((missing.match(/wallet-table-ux\.js/g) || []).length, 1);
  assert.equal((missing.match(/dolo-address-overrides\.js/g) || []).length, 1);
  assert.equal((missing.match(/vedolo-position-activity\.js/g) || []).length, 0, "helper is injected only on pages that use it");

  console.log("route-loader asset tests passed");
})().catch(error => {
  console.error(error);
  process.exit(1);
});
