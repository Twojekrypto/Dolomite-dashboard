const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const root = path.resolve(__dirname, "..");
const portfolio = fs.readFileSync(path.join(root, "portfolio-preview.html"), "utf8");
const walletUxSource = fs.readFileSync(path.join(root, "wallet-table-ux.js"), "utf8");
const walletUx = require(path.join(root, "wallet-table-ux.js"));
const assets = fs.readFileSync(path.join(root, "assets-preview.html"), "utf8");
const dolo = fs.readFileSync(path.join(root, "dolo-preview.html"), "utf8");
const odolo = fs.readFileSync(path.join(root, "odolo-preview.html"), "utf8");
const vedolo = fs.readFileSync(path.join(root, "vedolo-preview.html"), "utf8");
const liquidation = fs.readFileSync(path.join(root, "liquidation-preview.html"), "utf8");

const routes = ["odolo", "pair", "airdrop", "direct", "transfer", "merge", "split", "extend"];

test("portfolio route selection turns an all-routes view into the exact clicked route", () => {
  assert.deepEqual(walletUx.routeSelectionPlan(routes, "direct", routes), ["direct"]);
  assert.deepEqual(walletUx.routeSelectionPlan(["direct"], "transfer", routes), ["direct", "transfer"]);
  assert.deepEqual(walletUx.routeSelectionPlan(["direct", "transfer"], "all", routes), routes);
});

test("portfolio owns the shared route-selection model and uses shared route icons in router cells", () => {
  assert.match(portfolio, /data-route-model="native"/);
  assert.match(portfolio, /DoloWalletTableUX\.routeSelectionPlan\(\[\.\.\.filterState\.routes\], route, EXERCISE_ROUTES\.map\(item => item\.key\)\)/);
  assert.match(portfolio, /sharedActivity\.routeIconHtml\(route, false, className\)/);
  assert.match(portfolio, /pf-route-icon/);
});

test("all requested address-table searches expose the shared no-results state", () => {
  assert.match(walletUx.emptyStateMessage("", "No results found"), /No results found/);
  assert.match(walletUxSource, /function normalizeSearchNoResults/);
  assert.match(walletUxSource, /tbl-holders/);
  assert.match(walletUxSource, /flows-acc-body/);
  assert.match(walletUxSource, /flows-out-body/);
  assert.match(walletUxSource, /tbl-cb/);
  assert.match(walletUxSource, /tbl-ex/);
  assert.match(dolo, /dolo-lp-search/);
  assert.match(assets, /No assets found/);
  assert.match(liquidation, /No results found/);
});

test("veDOLO table footers clip the three requested lower corners", () => {
  assert.match(vedolo, /\.holders-card \.tbl-foot,\.claimable-card \.tbl-foot,\.flow-col \.tbl-foot\{[\s\S]*?border-radius:0 0 var\(--r-xl\) var\(--r-xl\)/);
  assert.match(vedolo, /\.position-activity-card \.tbl-foot\{[\s\S]*?border-radius:0 0 var\(--r-xl\) var\(--r-xl\)/);
});
